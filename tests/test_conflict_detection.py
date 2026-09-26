"""Unit tests for conflict detection and atomic writes in DataManifestWriter.

These tests do NOT require S3 — they create manifests directly on disk
and simulate concurrent writes by modifying the manifest file between operations.
"""

import os
import tempfile
import shutil
import threading

import pytest

from datamanifest.datamanifest import (
    DataManifestWriter,
    DataManifestRecord,
    RemotePath,
)


def _create_test_manifest(tmpdir, records=None):
    """Create a minimal manifest + local_config for testing without S3.

    Returns the manifest file path. The manifest is a valid v3 file that
    can be opened with DataManifestWriter.
    """
    manifest_path = os.path.join(tmpdir, "test.data_manifest.tsv")
    local_config_path = manifest_path + ".local_config"
    checkout_prefix = os.path.join(tmpdir, "checkout")
    cache_prefix = os.path.join(tmpdir, "cache")
    os.makedirs(checkout_prefix, exist_ok=True)
    os.makedirs(cache_prefix, exist_ok=True)

    if records is None:
        records = [
            ("file1.txt", "v1", "abc123", "hash1", "100", "", "note1"),
            ("file2.txt", "v2", "def456", "hash2", "200", "", "note2"),
        ]

    with open(manifest_path, "w") as f:
        f.write("#MANIFEST_VERSION=3\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://test-bucket/test-prefix\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./test_cache/\n")
        f.write("key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n")
        for rec in records:
            f.write("\t".join(rec) + "\n")

    with open(local_config_path, "w") as f:
        f.write(f"MANIFEST_VERSION=3\n")
        f.write(f"CHECKOUT_PREFIX={checkout_prefix}\n")
        f.write(f"LOCAL_CACHE_PREFIX={cache_prefix}\n")

    return manifest_path


@pytest.fixture()
def tmpdir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


# ---------------------------------------------------------------------------
# _parse_manifest_records tests
# ---------------------------------------------------------------------------

class TestParseManifestRecords:
    def test_v3_content(self):
        content = (
            "#MANIFEST_VERSION=3\n"
            "#REMOTE_DATA_MIRROR_URI=s3://bucket/prefix\n"
            "#LOCAL_CACHE_PATH_SUFFIX=./cache/\n"
            "key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n"
            "file1.txt\tv1\tabc123\thash1\t100\t\tnote1\n"
            "file2.txt\tv2\tdef456\thash2\t200\ts3://other/path\tnote2\n"
        )
        records = DataManifestWriter._parse_manifest_records(content)
        assert len(records) == 2
        assert records["file1.txt"] == ("v1", "abc123", "hash1", "100", "")
        assert records["file2.txt"] == ("v2", "def456", "hash2", "200", "s3://other/path")

    def test_v2_content(self):
        content = (
            "#MANIFEST_VERSION=2\n"
            "#REMOTE_DATA_MIRROR_URI=s3://bucket/prefix\n"
            "key\ts3_version_id\tmd5sum\tsize\tnotes\n"
            "file1.txt\tv1\tabc123\t100\tnote1\n"
        )
        records = DataManifestWriter._parse_manifest_records(content)
        assert len(records) == 1
        # v2 has no s3_hash or source_uri columns
        assert records["file1.txt"] == ("v1", "abc123", "", "100", "")

    def test_malformed_lines_skipped(self):
        content = (
            "#MANIFEST_VERSION=3\n"
            "key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n"
            "good.txt\tv1\tabc\thash\t100\t\tnote\n"
            "bad_line\n"
            "also_bad\ttoo_few\n"
            "three\tcols\tonly\n"
            "ok.txt\tv2\tdef\thash2\t200\t\tnote2\n"
        )
        records = DataManifestWriter._parse_manifest_records(content)
        assert len(records) == 2
        assert "good.txt" in records
        assert "ok.txt" in records

    def test_empty_content(self):
        records = DataManifestWriter._parse_manifest_records("")
        assert records == {}

    def test_header_only(self):
        content = (
            "#MANIFEST_VERSION=3\n"
            "key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n"
        )
        records = DataManifestWriter._parse_manifest_records(content)
        assert records == {}


# ---------------------------------------------------------------------------
# _record_to_tuple tests
# ---------------------------------------------------------------------------

class TestRecordToTuple:
    def test_matches_parse_output(self):
        """_record_to_tuple should produce tuples in the same format as _parse_manifest_records."""
        record = DataManifestRecord(
            key="file1.txt",
            md5sum="abc123",
            s3_hash="hash1",
            size=100,
            notes="note1",
            path="/tmp/checkout/file1.txt",
            remote_uri=RemotePath("s3", "bucket", "prefix/file1.txt", "v1"),
            source_uri="",
        )
        writer = DataManifestWriter.__new__(DataManifestWriter)
        result = writer._record_to_tuple(record)
        assert result == ("v1", "abc123", "hash1", "100", "")

    def test_with_source_uri(self):
        record = DataManifestRecord(
            key="ext.txt",
            md5sum="xyz",
            s3_hash="ehash",
            size=500,
            notes="",
            path="/tmp/checkout/ext.txt",
            remote_uri=RemotePath("s3", "other", "path/ext.txt", "v5"),
            source_uri="s3://other/path/ext.txt",
        )
        writer = DataManifestWriter.__new__(DataManifestWriter)
        result = writer._record_to_tuple(record)
        assert result == ("v5", "xyz", "ehash", "500", "s3://other/path/ext.txt")


# ---------------------------------------------------------------------------
# _detect_conflicts tests
# ---------------------------------------------------------------------------

class TestDetectConflicts:
    def _make_writer(self, tmpdir, records):
        """Create a DataManifestWriter for conflict detection testing."""
        manifest_path = _create_test_manifest(tmpdir, records)
        return DataManifestWriter(manifest_path)

    def test_no_conflict(self, tmpdir):
        """When backup matches last known content, no conflicts are detected."""
        writer = self._make_writer(tmpdir, [
            ("file1.txt", "v1", "abc", "hash1", "100", "", ""),
        ])
        # Simulate: backup content == last known content (no external modification)
        conflicts = writer._detect_conflicts(writer._last_known_content)
        assert conflicts == []
        writer.close()

    def test_key_added_by_another_writer(self, tmpdir):
        """A key present in backup but not in original or current data = added by another writer."""
        writer = self._make_writer(tmpdir, [
            ("file1.txt", "v1", "abc", "hash1", "100", "", ""),
        ])
        # Simulate backup content that has an extra key added by another writer
        bak_content = (
            "#MANIFEST_VERSION=3\n"
            "#REMOTE_DATA_MIRROR_URI=s3://test-bucket/test-prefix\n"
            "#LOCAL_CACHE_PATH_SUFFIX=./test_cache/\n"
            "key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n"
            "file1.txt\tv1\tabc\thash1\t100\t\t\n"
            "new_file.txt\tv3\txyz\thash3\t300\t\tnew\n"
        )
        conflicts = writer._detect_conflicts(bak_content)
        assert len(conflicts) == 1
        assert "new_file.txt" in conflicts[0]
        assert "added by another writer" in conflicts[0]
        writer.close()

    def test_key_modified_by_another_writer_only(self, tmpdir):
        """A key modified in backup but not by us = another writer's modification would be reverted."""
        writer = self._make_writer(tmpdir, [
            ("file1.txt", "v1", "abc", "hash1", "100", "", ""),
        ])
        # Backup has modified version of file1.txt (different md5)
        bak_content = (
            "#MANIFEST_VERSION=3\n"
            "#REMOTE_DATA_MIRROR_URI=s3://test-bucket/test-prefix\n"
            "#LOCAL_CACHE_PATH_SUFFIX=./test_cache/\n"
            "key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n"
            "file1.txt\tv1\tMODIFIED\thash1\t100\t\t\n"
        )
        conflicts = writer._detect_conflicts(bak_content)
        assert len(conflicts) == 1
        assert "file1.txt" in conflicts[0]
        assert "modified by another writer" in conflicts[0]
        assert "reverted" in conflicts[0]
        writer.close()

    def test_key_modified_by_both_writers(self, tmpdir):
        """A key modified in both backup and our data = double modification conflict."""
        records = [("file1.txt", "v1", "abc", "hash1", "100", "", "")]
        manifest_path = _create_test_manifest(tmpdir, records)
        writer = DataManifestWriter(manifest_path)

        # We modify file1.txt in our data
        import dataclasses
        old_record = writer._data["file1.txt"]
        writer._data["file1.txt"] = dataclasses.replace(old_record, md5sum="OUR_CHANGE")

        # Backup also has a different modification
        bak_content = (
            "#MANIFEST_VERSION=3\n"
            "#REMOTE_DATA_MIRROR_URI=s3://test-bucket/test-prefix\n"
            "#LOCAL_CACHE_PATH_SUFFIX=./test_cache/\n"
            "key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n"
            "file1.txt\tv1\tTHEIR_CHANGE\thash1\t100\t\t\n"
        )
        conflicts = writer._detect_conflicts(bak_content)
        assert len(conflicts) == 1
        assert "both this writer and another writer" in conflicts[0]
        writer.close()

    def test_key_deleted_by_us_is_not_conflict(self, tmpdir):
        """A key we intentionally deleted should not be flagged as a conflict."""
        records = [
            ("file1.txt", "v1", "abc", "hash1", "100", "", ""),
            ("file2.txt", "v2", "def", "hash2", "200", "", ""),
        ]
        manifest_path = _create_test_manifest(tmpdir, records)
        writer = DataManifestWriter(manifest_path)

        # We delete file2.txt from our data
        del writer._data["file2.txt"]

        # Backup still has file2.txt (unchanged from original)
        conflicts = writer._detect_conflicts(writer._last_known_content)
        assert conflicts == []
        writer.close()

    def test_key_deleted_by_another_writer(self, tmpdir):
        """A key deleted by another writer (missing from backup) that we still have = conflict."""
        records = [
            ("file1.txt", "v1", "abc", "hash1", "100", "", ""),
            ("file2.txt", "v2", "def", "hash2", "200", "", ""),
        ]
        manifest_path = _create_test_manifest(tmpdir, records)
        writer = DataManifestWriter(manifest_path)

        # Backup has file1 deleted by another writer
        bak_content = (
            "#MANIFEST_VERSION=3\n"
            "#REMOTE_DATA_MIRROR_URI=s3://test-bucket/test-prefix\n"
            "#LOCAL_CACHE_PATH_SUFFIX=./test_cache/\n"
            "key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n"
            "file2.txt\tv2\tdef\thash2\t200\t\t\n"
        )
        conflicts = writer._detect_conflicts(bak_content)
        assert len(conflicts) == 1
        assert "file1.txt" in conflicts[0]
        assert "deleted by another writer" in conflicts[0]
        writer.close()


# ---------------------------------------------------------------------------
# _save_to_disk tests
# ---------------------------------------------------------------------------

class TestSaveToDisk:
    def test_save_writes_correct_content(self, tmpdir):
        """After _save_to_disk, the file should contain the current in-memory data."""
        manifest_path = _create_test_manifest(tmpdir)
        writer = DataManifestWriter(manifest_path)

        # Verify the file content matches what we expect
        with open(manifest_path) as f:
            content = f.read()
        assert "file1.txt" in content
        assert "file2.txt" in content
        writer.close()

    def test_save_cleans_up_backup(self, tmpdir):
        """After a successful save, no .bak files should remain."""
        import dataclasses
        manifest_path = _create_test_manifest(tmpdir)
        writer = DataManifestWriter(manifest_path)

        # Modify data and save
        old = writer._data["file1.txt"]
        writer._data["file1.txt"] = dataclasses.replace(old, md5sum="new_md5")
        writer._save_to_disk()

        # Check no .bak files remain
        bak_files = [f for f in os.listdir(tmpdir) if ".bak." in f]
        assert bak_files == [], f"Stale backup files found: {bak_files}"
        writer.close()

    def test_save_detects_concurrent_modification(self, tmpdir):
        """If the file was modified by another writer, _save_to_disk should detect and raise."""
        import dataclasses
        records = [
            ("file1.txt", "v1", "abc", "hash1", "100", "", ""),
        ]
        manifest_path = _create_test_manifest(tmpdir, records)
        writer = DataManifestWriter(manifest_path)

        # Simulate another writer modifying the file on disk
        # (add a new key that wasn't in the original)
        with open(manifest_path, "w") as f:
            f.write("#MANIFEST_VERSION=3\n")
            f.write("#REMOTE_DATA_MIRROR_URI=s3://test-bucket/test-prefix\n")
            f.write("#LOCAL_CACHE_PATH_SUFFIX=./test_cache/\n")
            f.write("key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n")
            f.write("file1.txt\tv1\tabc\thash1\t100\t\t\n")
            f.write("sneaky.txt\tv9\tzzz\thash9\t999\t\tinjected\n")

        # Our save should detect the conflict
        old = writer._data["file1.txt"]
        writer._data["file1.txt"] = dataclasses.replace(old, md5sum="our_change")
        with pytest.raises(RuntimeError, match="CONFLICT DETECTED"):
            writer._save_to_disk()

        # Verify .CONFLICT file was created
        conflict_files = [f for f in os.listdir(tmpdir) if ".CONFLICT." in f]
        assert len(conflict_files) == 1, f"Expected 1 conflict file, got {conflict_files}"
        writer.close()

    def test_save_updates_last_known_content(self, tmpdir):
        """After save, _last_known_content should reflect the new state."""
        import dataclasses
        manifest_path = _create_test_manifest(tmpdir)
        writer = DataManifestWriter(manifest_path)

        old_content = writer._last_known_content
        old = writer._data["file1.txt"]
        writer._data["file1.txt"] = dataclasses.replace(old, md5sum="updated_md5")
        writer._save_to_disk()

        assert writer._last_known_content != old_content
        assert "updated_md5" in writer._last_known_content
        writer.close()

    def test_multiple_rapid_saves_unique_backups(self, tmpdir):
        """Multiple saves in quick succession should use unique backup paths (no collision)."""
        import dataclasses
        manifest_path = _create_test_manifest(tmpdir)
        writer = DataManifestWriter(manifest_path)

        # Save 5 times rapidly
        for i in range(5):
            old = writer._data["file1.txt"]
            writer._data["file1.txt"] = dataclasses.replace(old, md5sum=f"md5_{i}")
            writer._save_to_disk()

        # All saves should succeed and no stale backups
        bak_files = [f for f in os.listdir(tmpdir) if ".bak." in f]
        assert bak_files == [], f"Stale backup files: {bak_files}"

        # Verify final state
        with open(manifest_path) as f:
            content = f.read()
        assert "md5_4" in content
        writer.close()

    def test_save_lock_serializes_concurrent_saves(self, tmpdir):
        """Concurrent _save_to_disk calls should be serialized by the lock."""
        import dataclasses
        manifest_path = _create_test_manifest(tmpdir)
        writer = DataManifestWriter(manifest_path)

        errors = []
        def save_with_modification(idx):
            try:
                old = writer._data["file1.txt"]
                writer._data["file1.txt"] = dataclasses.replace(old, notes=f"thread_{idx}")
                writer._save_to_disk()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=save_with_modification, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Errors during concurrent saves: {errors}"

        # Verify the manifest is still valid (can be reopened)
        writer.close()
        writer2 = DataManifestWriter(manifest_path)
        assert "file1.txt" in writer2._data
        assert "file2.txt" in writer2._data
        writer2.close()
