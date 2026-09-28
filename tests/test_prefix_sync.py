"""Unit tests for prefix/glob-scoped sync and find_prefix.

These tests do NOT require S3 — they create manifests directly on disk
and mock sync_record to avoid network access, following the pattern in
test_conflict_detection.py.
"""

import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock

import pytest

from datamanifest.datamanifest import (
    DataManifest,
    UnknownKeyError,
)
from datamanifest.main import sync_main, parse_args


def _create_test_manifest(tmpdir, records=None):
    """Create a minimal manifest + local_config for testing without S3.

    Returns the manifest file path.
    """
    manifest_path = os.path.join(tmpdir, "test.data_manifest.tsv")
    local_config_path = manifest_path + ".local_config"
    checkout_prefix = os.path.join(tmpdir, "checkout")
    cache_prefix = os.path.join(tmpdir, "cache")
    os.makedirs(checkout_prefix, exist_ok=True)
    os.makedirs(cache_prefix, exist_ok=True)

    if records is None:
        records = [
            ("alpha/one.txt", "v1", "aaa", "h1", "100", "", ""),
            ("alpha/two.txt", "v2", "bbb", "h2", "200", "", ""),
            ("alpha/sub/three.txt", "v3", "ccc", "h3", "300", "", ""),
            ("beta/four.txt", "v4", "ddd", "h4", "400", "", ""),
            ("beta/five.txt", "v5", "eee", "h5", "500", "", ""),
            ("gamma.txt", "v6", "fff", "h6", "600", "", ""),
        ]

    with open(manifest_path, "w") as f:
        f.write("#MANIFEST_VERSION=3\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://test-bucket/test-prefix\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./test_cache/\n")
        f.write("key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n")
        for rec in records:
            f.write("\t".join(rec) + "\n")

    with open(local_config_path, "w") as f:
        f.write("MANIFEST_VERSION=3\n")
        f.write(f"CHECKOUT_PREFIX={checkout_prefix}\n")
        f.write(f"LOCAL_CACHE_PREFIX={cache_prefix}\n")

    return manifest_path


@pytest.fixture()
def tmpdir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


# ---------------------------------------------------------------------------
# find_prefix tests
# ---------------------------------------------------------------------------

class TestFindPrefix:
    def test_matches_prefix(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        result = dm.find_prefix("alpha/")
        assert sorted(result) == ["alpha/one.txt", "alpha/sub/three.txt", "alpha/two.txt"]
        dm.close()

    def test_matches_deeper_prefix(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        result = dm.find_prefix("alpha/sub/")
        assert result == ["alpha/sub/three.txt"]
        dm.close()

    def test_no_match(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        result = dm.find_prefix("nonexistent/")
        assert result == []
        dm.close()

    def test_empty_prefix_raises(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        with pytest.raises(ValueError, match="non-empty"):
            dm.find_prefix("")
        dm.close()

    def test_partial_key_prefix(self, tmpdir):
        """find_prefix("al") should match "alpha/" keys (startswith, not path-aware)."""
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        result = dm.find_prefix("al")
        assert len(result) == 3  # all alpha/ keys
        dm.close()

    def test_does_not_delegate_to_glob(self, tmpdir):
        """find_prefix must use str.startswith, NOT glob(prefix + '*')."""
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        with patch.object(dm, 'glob', wraps=dm.glob) as mock_glob:
            dm.find_prefix("alpha/")
            mock_glob.assert_not_called()
        dm.close()


# ---------------------------------------------------------------------------
# sync(keys=...) tests
# ---------------------------------------------------------------------------

class TestSyncKeys:
    def test_sync_keys_filters_correctly(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        synced_keys = []
        with patch.object(dm, 'sync_record', side_effect=lambda k, **kw: synced_keys.append(k)):
            dm.sync(keys=["alpha/one.txt", "beta/four.txt"])
        assert sorted(synced_keys) == ["alpha/one.txt", "beta/four.txt"]
        dm.close()

    def test_sync_keys_unknown_key_raises(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        synced_keys = []
        with patch.object(dm, 'sync_record', side_effect=lambda k, **kw: synced_keys.append(k)):
            with pytest.raises(UnknownKeyError) as exc_info:
                dm.sync(keys=["alpha/one.txt", "DOES_NOT_EXIST", "ALSO_MISSING"])
            # Should mention both unknown keys
            assert "2 key(s) not found" in str(exc_info.value)
            assert "DOES_NOT_EXIST" in str(exc_info.value)
            assert "ALSO_MISSING" in str(exc_info.value)
        # No sync_record calls should have been made
        assert synced_keys == []
        dm.close()

    def test_sync_keys_unknown_is_keyerror_subclass(self, tmpdir):
        """UnknownKeyError should be catchable as KeyError."""
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        with pytest.raises(KeyError):
            dm.sync(keys=["NOPE"])
        dm.close()

    def test_sync_keys_empty_list_noop(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        synced_keys = []
        with patch.object(dm, 'sync_record', side_effect=lambda k, **kw: synced_keys.append(k)):
            dm.sync(keys=[])  # should not raise
        assert synced_keys == []
        dm.close()

    def test_sync_keys_none_syncs_all(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        synced_keys = []
        with patch.object(dm, 'sync_record', side_effect=lambda k, **kw: synced_keys.append(k)):
            dm.sync(keys=None)
        assert sorted(synced_keys) == sorted(dm.keys())
        dm.close()

    def test_sync_no_args_syncs_all(self, tmpdir):
        """sync() with no args should sync all keys (backward compat)."""
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        synced_keys = []
        with patch.object(dm, 'sync_record', side_effect=lambda k, **kw: synced_keys.append(k)):
            dm.sync()
        assert sorted(synced_keys) == sorted(dm.keys())
        dm.close()

    def test_sync_keys_upfront_validation(self, tmpdir):
        """Unknown keys must be caught BEFORE any downloads start."""
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        synced_keys = []
        with patch.object(dm, 'sync_record', side_effect=lambda k, **kw: synced_keys.append(k)):
            with pytest.raises(UnknownKeyError):
                dm.sync(keys=["alpha/one.txt", "BOGUS"])
        # Even the valid key should NOT have been synced
        assert synced_keys == []
        dm.close()

    def test_sync_keyword_only(self, tmpdir):
        """sync() args must be keyword-only (cannot be passed positionally)."""
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        with pytest.raises(TypeError):
            dm.sync(True)  # fast=True passed positionally
        dm.close()

    def test_sync_many_unknown_keys_truncated(self, tmpdir):
        """When many keys are unknown, the message should be sensibly truncated."""
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        unknown = [f"bad_{i}" for i in range(25)]
        with pytest.raises(UnknownKeyError) as exc_info:
            dm.sync(keys=unknown)
        msg = str(exc_info.value)
        assert "25 key(s) not found" in msg
        assert "and 15 more" in msg
        dm.close()


# ---------------------------------------------------------------------------
# sync_prefix tests
# ---------------------------------------------------------------------------

class TestSyncPrefix:
    def test_sync_prefix_matches(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        synced_keys = []
        with patch.object(dm, 'sync_record', side_effect=lambda k, **kw: synced_keys.append(k)):
            result = dm.sync_prefix("alpha/")
        assert sorted(synced_keys) == ["alpha/one.txt", "alpha/sub/three.txt", "alpha/two.txt"]
        assert sorted(result) == sorted(synced_keys)
        dm.close()

    def test_sync_prefix_no_match_raises(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        with pytest.raises(ValueError, match="No keys match prefix"):
            dm.sync_prefix("nonexistent/")
        dm.close()

    def test_sync_prefix_empty_raises(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        with pytest.raises(ValueError, match="non-empty"):
            dm.sync_prefix("")
        dm.close()

    def test_sync_prefix_returns_matched_keys(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        with patch.object(dm, 'sync_record', side_effect=lambda k, **kw: None):
            result = dm.sync_prefix("beta/")
        assert sorted(result) == ["beta/five.txt", "beta/four.txt"]
        dm.close()

    def test_sync_prefix_error_collection(self, tmpdir):
        """When some keys fail, RuntimeError should list only the failures."""
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)

        def mock_sync_record(key, **kwargs):
            if key == "alpha/two.txt":
                raise IOError("download failed")

        with patch.object(dm, 'sync_record', side_effect=mock_sync_record):
            with pytest.raises(RuntimeError, match="Failed to sync 1 of 3"):
                dm.sync_prefix("alpha/")
        dm.close()


# ---------------------------------------------------------------------------
# sync_glob tests
# ---------------------------------------------------------------------------

class TestSyncGlob:
    def test_sync_glob_matches(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        synced_keys = []
        with patch.object(dm, 'sync_record', side_effect=lambda k, **kw: synced_keys.append(k)):
            result = dm.sync_glob("alpha/*.txt")
        # fnmatch * crosses /, so it matches alpha/sub/three.txt too
        assert sorted(synced_keys) == ["alpha/one.txt", "alpha/sub/three.txt", "alpha/two.txt"]
        assert sorted(result) == sorted(synced_keys)
        dm.close()

    def test_sync_glob_no_match_raises(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        with pytest.raises(ValueError, match="No keys match pattern"):
            dm.sync_glob("*.xyz")
        dm.close()

    def test_sync_glob_returns_matched_keys(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        with patch.object(dm, 'sync_record', side_effect=lambda k, **kw: None):
            result = dm.sync_glob("beta/*")
        assert sorted(result) == ["beta/five.txt", "beta/four.txt"]
        dm.close()

    def test_sync_glob_exact_match(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        dm = DataManifest(manifest_path)
        synced_keys = []
        with patch.object(dm, 'sync_record', side_effect=lambda k, **kw: synced_keys.append(k)):
            result = dm.sync_glob("gamma.txt")
        assert result == ["gamma.txt"]
        assert synced_keys == ["gamma.txt"]
        dm.close()


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

class TestCLI:
    def test_cli_prefix_flag(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        with patch('datamanifest.main.DataManifest') as MockDM:
            mock_dm = MagicMock()
            MockDM.return_value = mock_dm
            mock_dm.__enter__ = MagicMock(return_value=mock_dm)
            mock_dm.__exit__ = MagicMock(return_value=False)
            mock_dm.values.return_value = []

            sync_main(manifest_path, fast=True, prefix="alpha/")
            mock_dm.sync_prefix.assert_called_once()
            call_kwargs = mock_dm.sync_prefix.call_args
            assert call_kwargs[0][0] == "alpha/"

    def test_cli_glob_flag(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        with patch('datamanifest.main.DataManifest') as MockDM:
            mock_dm = MagicMock()
            MockDM.return_value = mock_dm
            mock_dm.__enter__ = MagicMock(return_value=mock_dm)
            mock_dm.__exit__ = MagicMock(return_value=False)
            mock_dm.values.return_value = []

            sync_main(manifest_path, fast=True, glob_pattern="*.txt")
            mock_dm.sync_glob.assert_called_once()
            call_kwargs = mock_dm.sync_glob.call_args
            assert call_kwargs[0][0] == "*.txt"

    def test_cli_no_filter_syncs_all(self, tmpdir):
        manifest_path = _create_test_manifest(tmpdir)
        with patch('datamanifest.main.DataManifest') as MockDM:
            mock_dm = MagicMock()
            MockDM.return_value = mock_dm
            mock_dm.__enter__ = MagicMock(return_value=mock_dm)
            mock_dm.__exit__ = MagicMock(return_value=False)
            mock_dm.values.return_value = []

            sync_main(manifest_path, fast=True)
            mock_dm.sync.assert_called_once()

    def test_cli_prefix_and_glob_mutual_exclusion(self):
        """argparse should reject --prefix and --glob together."""
        import sys
        test_args = [
            "dm", "--quiet", "sync", "test.data_manifest.tsv",
            "--prefix", "foo/", "--glob", "bar/*"
        ]
        with patch.object(sys, 'argv', test_args):
            with pytest.raises(SystemExit) as exc_info:
                parse_args()
            # argparse exits with code 2 for usage errors
            assert exc_info.value.code == 2

    # The tests above call sync_main() directly, which leaves the argparse -> sync_main
    # wiring untested: a typo in the attribute name (args.glob vs args.glob_pattern)
    # would pass every one of them. These two drive main() end to end instead.
    @pytest.mark.parametrize("flag,value,expected_method", [
        ("--prefix", "alpha/", "sync_prefix"),
        ("--glob", "*.txt", "sync_glob"),
    ])
    def test_main_wires_flag_through_to_the_right_method(
        self, tmpdir, flag, value, expected_method
    ):
        import sys
        from datamanifest.main import main
        manifest_path = _create_test_manifest(tmpdir)
        argv = ["dm", "--quiet", "sync", str(manifest_path), flag, value]
        with patch.object(sys, 'argv', argv), \
                patch('datamanifest.main.DataManifest') as MockDM:
            mock_dm = MagicMock()
            MockDM.return_value = mock_dm
            mock_dm.__enter__ = MagicMock(return_value=mock_dm)
            mock_dm.__exit__ = MagicMock(return_value=False)
            mock_dm.values.return_value = []
            main()

        called = getattr(mock_dm, expected_method)
        called.assert_called_once()
        assert called.call_args[0][0] == value
        # and the other two dispatch paths must NOT have fired
        for other in {"sync_prefix", "sync_glob", "sync"} - {expected_method}:
            getattr(mock_dm, other).assert_not_called()

    def test_main_empty_prefix_does_not_silently_sync_everything(self, tmpdir):
        """`--prefix ""` must reach sync_prefix (which raises), not fall through to sync().

        Truthiness dispatch would make an empty prefix sync every record -- failing
        toward doing MORE work than asked, which is the dangerous direction.
        """
        import sys
        from datamanifest.main import main
        manifest_path = _create_test_manifest(tmpdir)
        argv = ["dm", "--quiet", "sync", str(manifest_path), "--prefix", ""]
        with patch.object(sys, 'argv', argv), \
                patch('datamanifest.main.DataManifest') as MockDM:
            mock_dm = MagicMock()
            MockDM.return_value = mock_dm
            mock_dm.__enter__ = MagicMock(return_value=mock_dm)
            mock_dm.__exit__ = MagicMock(return_value=False)
            mock_dm.values.return_value = []
            main()

        mock_dm.sync_prefix.assert_called_once()
        assert mock_dm.sync_prefix.call_args[0][0] == ""
        mock_dm.sync.assert_not_called()
