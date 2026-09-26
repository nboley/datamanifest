"""Tests for parallel sync and thread safety in DataManifest.sync().

These tests require S3 access to the test bucket.
"""

import os
import tempfile
import shutil

import pytest

from datamanifest.datamanifest import (
    DataManifest,
    DataManifestWriter,
    DEFAULT_FOLDER_PERMISSIONS,
    random_string,
    _check_s3_versioning_enabled,
)
from test_datamanifest import (
    purge_s3_prefix,
    S3_TEST_BUCKET,
    S3_TEST_BASE_PATH,
    _find_current_git_hash,
)


GIT_HASH = _find_current_git_hash()


@pytest.fixture(scope="session")
def check_s3():
    """Verify S3 bucket is accessible and versioned."""
    try:
        _check_s3_versioning_enabled(S3_TEST_BUCKET)
    except Exception as e:
        pytest.skip(f"S3 bucket not available: {e}")


@pytest.fixture()
def cleandir():
    d = tempfile.mkdtemp()
    os.chmod(d, DEFAULT_FOLDER_PERMISSIONS)
    yield d
    shutil.rmtree(d)


@pytest.fixture()
def s3_cleanup():
    uris = []
    yield uris.append
    for uri in uris:
        purge_s3_prefix(uri)


@pytest.fixture()
def parallel_manifest(cleandir, check_s3, s3_cleanup):
    """Create a manifest with 5 files for parallel sync testing."""
    remote_uri = f"s3://{S3_TEST_BUCKET}/{S3_TEST_BASE_PATH}/{GIT_HASH}-parallel-{random_string(16)}"
    s3_cleanup(remote_uri)

    cache = os.path.join(cleandir, "cache")
    os.makedirs(cache, mode=DEFAULT_FOLDER_PERMISSIONS)
    checkout = os.path.join(cleandir, "checkout")
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")

    dm = DataManifestWriter.new(manifest_path, remote_uri, checkout_prefix=checkout, local_cache_prefix=cache)

    for i in range(5):
        fname = f"file_{i}.txt"
        fpath = os.path.join(cleandir, fname)
        with open(fpath, "w") as f:
            f.write(f"content_{i}" * 100)
        dm.add(fname, fpath)

    dm.close()
    yield manifest_path


def test_parallel_sync_completes(parallel_manifest, cleandir):
    """Parallel sync with max_workers=4 should successfully sync all records."""
    # Create a fresh checkout to sync into
    checkout2 = os.path.join(cleandir, "checkout2")
    cache2 = os.path.join(cleandir, "cache2")
    os.makedirs(cache2, mode=DEFAULT_FOLDER_PERMISSIONS)

    dm = DataManifest.checkout(parallel_manifest, checkout_prefix=checkout2, local_cache_prefix=cache2, force=True)
    # Remove cached/checked-out files to force re-download
    shutil.rmtree(checkout2)
    shutil.rmtree(cache2)
    os.makedirs(checkout2)
    os.makedirs(cache2, mode=DEFAULT_FOLDER_PERMISSIONS)

    # Re-open and sync
    dm.close()
    dm = DataManifest(parallel_manifest)
    dm.sync(fast=True, max_workers=4)

    # Verify all 5 files are synced
    for i in range(5):
        key = f"file_{i}.txt"
        assert key in dm
        record = dm.get(key, validate=False)
        assert os.path.exists(record.path), f"{key} not synced"
    dm.close()


def test_parallel_sync_single_worker(parallel_manifest, cleandir):
    """Sync with max_workers=1 should work (sequential fallback)."""
    checkout2 = os.path.join(cleandir, "checkout2")
    cache2 = os.path.join(cleandir, "cache2")
    os.makedirs(cache2, mode=DEFAULT_FOLDER_PERMISSIONS)

    dm = DataManifest.checkout(parallel_manifest, checkout_prefix=checkout2, local_cache_prefix=cache2, force=True)
    dm.close()
    dm = DataManifest(parallel_manifest)
    # Should not raise
    dm.sync(fast=True, max_workers=1)

    for i in range(5):
        key = f"file_{i}.txt"
        record = dm.get(key, validate=False)
        assert os.path.exists(record.path)
    dm.close()


def test_parallel_sync_error_collection(cleandir, check_s3, s3_cleanup):
    """When multiple records fail, all errors should be collected in a single RuntimeError."""
    remote_uri = f"s3://{S3_TEST_BUCKET}/{S3_TEST_BASE_PATH}/{GIT_HASH}-errors-{random_string(16)}"
    s3_cleanup(remote_uri)

    cache = os.path.join(cleandir, "cache")
    os.makedirs(cache, mode=DEFAULT_FOLDER_PERMISSIONS)
    checkout = os.path.join(cleandir, "checkout")
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")

    dm = DataManifestWriter.new(manifest_path, remote_uri, checkout_prefix=checkout, local_cache_prefix=cache)

    # Add a real file
    real_path = os.path.join(cleandir, "real.txt")
    with open(real_path, "w") as f:
        f.write("real content")
    dm.add("real.txt", real_path)
    dm.close()

    # Now corrupt the manifest to have records pointing to bad S3 keys
    # by adding fake records directly to the TSV
    with open(manifest_path, "r") as f:
        content = f.read()

    with open(manifest_path, "w") as f:
        f.write(content)
        # Add records with non-existent version IDs — these will fail on sync
        f.write("ghost1.txt\tBADVERSION1\taaa\tbbb\t10\t\tbad record\n")
        f.write("ghost2.txt\tBADVERSION2\tccc\tddd\t20\t\tbad record\n")

    # Open fresh and sync — should collect errors for ghost records
    dm = DataManifest(manifest_path)
    with pytest.raises(RuntimeError, match="Failed to sync"):
        dm.sync(fast=True, max_workers=4)
    dm.close()


def test_parallel_sync_default_workers(parallel_manifest):
    """Default max_workers=8 should work without error."""
    dm = DataManifest(parallel_manifest)
    # All files already synced from fixture — this should just validate
    dm.sync(fast=True)
    dm.close()
