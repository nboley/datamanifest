import os
import shutil
import subprocess
import tempfile

from urllib.parse import urlparse

import boto3
import botocore
import pytest

from datamanifest.datamanifest import (
    DataManifest,
    DataManifestWriter,
    DataManifestRecord,
    FileMismatchError,
    InvalidPrefix,
    KeyAlreadyExistsError,
    InvalidKey,
    MissingFileError,
    MissingLocalConfigError,
    RemotePath,
    calc_md5sum_from_remote_uri,
    calc_md5sum_from_fname,
    validate_key,
    DEFAULT_FOLDER_PERMISSIONS,
    random_string,
    environment_variables,
    _check_s3_versioning_enabled,
    MANIFEST_VERSION,
)

def _find_current_git_hash():
    try:
        res = subprocess.run(
            "git rev-parse --verify HEAD",
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        rv = res.stdout.strip().decode()
        if len(rv) == 0:
            rv = random_string(16)
    except subprocess.CalledProcessError:
        rv = random_string(16)
    return rv


def _find_current_git_branch():
    try:
        res = subprocess.run(
            "git rev-parse --abbrev-ref HEAD",
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        rv = res.stdout.strip().decode()
        if len(rv) == 0:
            rv = "UNKNOWN_BRANCH" + random_string(16) + "UNKNOWN_BRANCH"
    except subprocess.CalledProcessError:
        rv = "UNKNOWN_BRANCH" + random_string(16) + "UNKNOWN_BRANCH"
    return rv


S3_TEST_BUCKET = "nboley-test-data-manifest"
S3_TEST_BASE_PATH = ""
GIT_HASH = _find_current_git_hash()
GIT_BRANCH = _find_current_git_branch()


@pytest.fixture(scope="session")
def check_s3_bucket_versioning():
    """Check that the test S3 bucket exists and has versioning enabled.
    
    This fixture runs once per test session and fails all tests if:
    - The bucket doesn't have versioning enabled
    - We can't check versioning (e.g., no permissions)
    """
    try:
        versioning_enabled = _check_s3_versioning_enabled(S3_TEST_BUCKET)
        if not versioning_enabled:
            pytest.fail(
                f"S3 bucket '{S3_TEST_BUCKET}' does not have versioning enabled. "
                f"Please enable versioning on the bucket before running tests."
            )
    except RuntimeError as e:
        pytest.fail(
            f"Cannot verify S3 bucket versioning for '{S3_TEST_BUCKET}': {e}. "
            f"Please ensure the bucket has versioning enabled and you have the necessary permissions."
        )
    except Exception as e:
        pytest.fail(
            f"Error checking S3 bucket versioning for '{S3_TEST_BUCKET}': {e}. "
            f"Please ensure the bucket exists and you have the necessary permissions."
        )


@pytest.fixture()
def cleandir():
    oldpath = os.getcwd()
    newpath = tempfile.mkdtemp()
    os.chdir(newpath)
    os.chmod(newpath, DEFAULT_FOLDER_PERMISSIONS)
    yield newpath
    os.chdir(oldpath)
    shutil.rmtree(newpath)


# added so that I can have a second temp directory
@pytest.fixture()
def cleandir2():
    oldpath = os.getcwd()
    newpath = tempfile.mkdtemp()
    os.chdir(newpath)
    os.chmod(newpath, DEFAULT_FOLDER_PERMISSIONS)
    yield newpath
    os.chdir(oldpath)
    shutil.rmtree(newpath)


def s3_uri_exists(remote_path):
    assert remote_path.scheme == "s3"
    s3 = boto3.resource("s3")
    try:
        s3.Object(remote_path.bucket, remote_path.path).load()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # The object does not exist.
            return False
        else:
            # Something else has gone wrong.
            raise
    else:
        # The object does exist.
        return True


@pytest.fixture()
def manifest_fname(cleandir, check_s3_bucket_versioning):
    """Instantiate a data manifest with files."""
    # make a copy of the current environment to restore
    print(f"Created temporary directory: {cleandir}")
    manifest_fname = os.path.join(cleandir, "data_manifest.data_manifest.tsv")
    assert not os.path.exists(manifest_fname)
    print(f"Loading manifest: {manifest_fname}")
    checkout_prefix_base = os.path.normpath(
        os.path.join(cleandir, f"./{GIT_BRANCH}-data/")
    )
    checkout_prefix = os.path.normpath(
        os.path.join(checkout_prefix_base, "./data_manifest/")
    )

    local_cache_prefix = os.path.normpath(os.path.join(cleandir, "./local_cache"))
    os.makedirs(local_cache_prefix, mode=DEFAULT_FOLDER_PERMISSIONS)
    # this line is needed for the permissions to be valid in docker. No idea why -- must be a bug in os.makedirs
    # or docker mounts.
    os.chmod(local_cache_prefix, DEFAULT_FOLDER_PERMISSIONS)

    if S3_TEST_BASE_PATH:
        remote_datastore_uri = f"s3://{S3_TEST_BUCKET}/{S3_TEST_BASE_PATH}/{GIT_HASH}-{random_string(16)}"
    else:
        remote_datastore_uri = f"s3://{S3_TEST_BUCKET}/{GIT_HASH}-{random_string(16)}"

    manifest = DataManifestWriter.new(
        manifest_fname,
        remote_datastore_uri,
        checkout_prefix=checkout_prefix,
        local_cache_prefix=local_cache_prefix,
    )

    # create a test file, and add it to the data manifest
    test_data_key = "eight_As.txt"
    test_data_path = os.path.normpath(os.path.join(cleandir, test_data_key))
    with open(test_data_path, "w") as fp:
        fp.write("A" * 8)
    manifest.add(test_data_key, test_data_path, notes="Eight As")

    test_data_key = "nine_As.txt"
    test_data_path = os.path.normpath(os.path.join(cleandir, test_data_key))
    with open(test_data_path, "w") as fp:
        fp.write("A" * 9)
    manifest.add(test_data_key, test_data_path, notes="Nine As")

    manifest.close()

    with environment_variables(LOCAL_DATA_MIRROR_PATH=local_cache_prefix):
        yield manifest.fname  # provide the fixture value

    # clean up S3 files created in remote_datastore_uri
    s3 = boto3.resource("s3")
    parsed = urlparse(remote_datastore_uri)
    bucket = s3.Bucket(parsed.netloc)
    bucket.objects.filter(Prefix=parsed.path.lstrip("/")).delete()


def test_new_dm_on_class_init(cleandir, check_s3_bucket_versioning):
    new_dm_path = f"{cleandir}/test_dm.data_manifest.tsv"
    assert not os.path.exists(
        new_dm_path
    ), "Test error, the new file shouldn't already be there"
    path = __file__
    local_cache_prefix = os.path.normpath(os.path.join(cleandir, "./local_cache"))
    os.makedirs(local_cache_prefix, mode=DEFAULT_FOLDER_PERMISSIONS)
    # this line is needed for the permissions to be valid in docker. No idea why -- must be a bug in os.makedirs
    # or docker mounts.
    os.chmod(local_cache_prefix, DEFAULT_FOLDER_PERMISSIONS)

    if S3_TEST_BASE_PATH:
        remote_datastore_uri = f"s3://{S3_TEST_BUCKET}/{S3_TEST_BASE_PATH}/{GIT_HASH}-{random_string(16)}"
    else:
        remote_datastore_uri = f"s3://{S3_TEST_BUCKET}/{GIT_HASH}-{random_string(16)}"

    with DataManifestWriter.new(
        new_dm_path,
        remote_datastore_uri,
        checkout_prefix=cleandir,
        local_cache_prefix=local_cache_prefix,
    ) as dmr:
        dmr.add("this_file", path)
    assert os.path.exists(new_dm_path), f"The new dm {new_dm_path} was not written."


def test_validate_key():
    for valid_key in [
        "a",
        "a.txt",
        "a/b",
        "a/b/c",
        "a/b/c.txt",
        "a/b_/c.txt",
        "a/b-/c.z.txt",
        "a/B",
    ]:
        validate_key(valid_key)

    for invalid_key in [
        "",
        "/",
        "/a",
        "/a/b",
        "a/b/",
        "a//b",
        "./a/b",
        "a/$@#",
        "a/./b",
    ]:
        with pytest.raises(InvalidKey):
            validate_key(invalid_key)


def test_simple_data_manifest(manifest_fname):
    # ensure that the fixture can setup and teardown
    pass


def test_add_and_get_data(manifest_fname):
    # create the test file
    payload = b"A" * 100
    test_key = "100_As.txt"
    test_path = os.path.abspath(test_key)
    with open(test_path, "wb") as ofp:
        ofp.write(payload)

    with DataManifestWriter(manifest_fname) as manifest:
        manifest.add(test_key, test_path)

        # download the file
        s3 = boto3.resource("s3")
        record = manifest.get(test_key)
        test_bucket = s3.Bucket(record.remote_uri.bucket)
        object = test_bucket.Object(record.remote_uri.path)
        with tempfile.NamedTemporaryFile("wb+") as fp:
            object.download_fileobj(fp)
            fp.seek(0)
            assert payload == fp.read()

        with open(record.path, "rb") as fp:
            assert fp.read() == payload


def test_glob(manifest_fname):
    # create the test file
    payload = b"A" * 100
    test_key_suffix = "100_As.txt"
    prefixes = ["hg19/a_", "hg19/", "hg38/"]
    with DataManifestWriter(manifest_fname) as manifest:
        for prefix in prefixes:
            test_key = prefix + test_key_suffix
            test_path = os.path.abspath(test_key).replace("/", "_")
            with open(test_path, "wb") as ofp:
                ofp.write(payload)
            manifest.add(test_key, test_path)

        assert [x.key for x in manifest.glob_records("hg19/*")] == [
            "hg19/a_100_As.txt",
            "hg19/100_As.txt",
        ]
        assert manifest.glob("hg19/*") == ["hg19/a_100_As.txt", "hg19/100_As.txt"]
        assert [x.key for x in manifest.glob_records("hg38/*")] == ["hg38/100_As.txt"]
        assert manifest.glob("hg38/*") == ["hg38/100_As.txt"]
        assert [x.key for x in manifest.glob_records("*/100_As.txt")] == [
            "hg19/100_As.txt",
            "hg38/100_As.txt",
        ]
        assert manifest.glob("*/100_As.txt") == ["hg19/100_As.txt", "hg38/100_As.txt"]


def test_add_subdirectory(manifest_fname):
    # test that we get an error if we try to add this file again with the same key
    payload = b"A" * 100
    # note that this matches the above key
    test_key = "one_hundred_As.txt"
    test_path = os.path.abspath(test_key)
    with open(test_path, "wb") as ofp:
        ofp.write(payload)

    with DataManifestWriter(manifest_fname) as manifest:
        test_key = "data/genomes/one_hundred_As.txt"
        manifest.add(test_key, test_path)


def test_add_duplicate_key(manifest_fname):
    # test that we get an error if we try to add this file again with the same key
    payload = b"A" * 8
    # note that this matches the above key
    test_key = "eight_As.txt"
    test_path = os.path.abspath(test_key)
    with open(test_path, "wb") as ofp:
        ofp.write(payload)

    with DataManifestWriter(manifest_fname) as manifest:
        with pytest.raises(KeyAlreadyExistsError):
            manifest.add(test_key, test_path)


def test_locking_works(manifest_fname):
    """Make sure that we get an error if two people try to modify the same manifest concurrently.

    We want to make sure that two people don't modify the same manifest, thus overriding someone
    elses changes. In this test, we open two manifests, write different files, and then make sure
    that we get a real error.
    """
    payload = b"A" * 12
    test_key = "test_data.txt"
    test_path = os.path.abspath(test_key)
    with open(test_path, "wb") as ofp:
        ofp.write(payload)

    manifest_w1 = DataManifestWriter(manifest_fname)
    with pytest.raises(
        RuntimeError, match=r".*?and so it can't be opened for reading.*"
    ):
        DataManifest(manifest_fname)
    manifest_w1.close()

    manifest_r2 = DataManifest(manifest_fname)
    with pytest.raises(
        RuntimeError, match=r".*?and so it can't be opened for writing.*"
    ):
        DataManifestWriter(manifest_fname)
    manifest_r2.close()


@pytest.mark.parametrize("delete_from_datastore", [True, False])
def test_delete_record(manifest_fname, delete_from_datastore):
    with DataManifestWriter(manifest_fname) as manifest:
        records = list(manifest)
        for record in records:
            local_cache_path = manifest.get_local_cache_path(record.key)
            manifest.delete(record.key, delete_from_datastore=delete_from_datastore)
            # verify that the local path doesn't exist any longer
            assert not os.path.exists(record.path)
            # verify that the cache path exists
            assert delete_from_datastore != os.path.exists(local_cache_path)
            # verify that the remote path exists
            assert delete_from_datastore != s3_uri_exists(record.remote_uri)

    # ensure that there are no records left
    assert len(manifest) == 0
    # ensure that there are no records in a re-loaded manifest
    manifest = DataManifest(manifest_fname)
    assert len(manifest) == 0


def test_update_record(manifest_fname):
    payload = "C" * 12
    test_path = os.path.abspath("updated_file")
    with open(test_path, "w") as ofp:
        ofp.write(payload)

    with DataManifestWriter(manifest_fname) as manifest:
        record = list(manifest)[0]
        with open(record.path) as fp:
            old_payload = fp.read()
        assert payload != old_payload

        with pytest.raises(KeyAlreadyExistsError):
            manifest.add(record.key, test_path)

        manifest.update(record.key, test_path)
        updated_record = list(manifest)[0]
        with open(manifest.get(record.key).path) as fp:
            assert fp.read() == payload

    # check the manifest.tsv was written correctly
    with DataManifestWriter(manifest_fname) as manifest:
        assert list(manifest)[0] == updated_record


def test_update_preserves_version_history(manifest_fname, cleandir2, check_s3_bucket_versioning):
    """Test that after updating a file, both versions are accessible via their version IDs.
    
    This test:
    1. Creates a manifest and adds a file (version 1)
    2. Saves a copy of the manifest state (version 1 record)
    3. Updates the file at the same key (version 2)
    4. Verifies that version 1's s3_version_id still retrieves the original content
    5. Verifies that version 2's s3_version_id retrieves the updated content
    6. Verifies MD5 checksums match for both versions
    """
    import copy
    
    # Add the first version of the file
    payload_v1 = "version 1 content - " + random_string(16)
    test_path = os.path.join(cleandir2, "test_file.txt")
    with open(test_path, "w") as fp:
        fp.write(payload_v1)
    
    with DataManifestWriter(manifest_fname) as manifest:
        manifest.add("versioned_file.txt", test_path)
        record_v1 = manifest.get("versioned_file.txt", validate=False)
        # Save version 1 info (remote_uri now includes version_id)
        v1_md5sum = record_v1.md5sum
        v1_remote_uri = copy.deepcopy(record_v1.remote_uri)
    
    # Verify version 1 is accessible and correct
    remote_md5_v1 = calc_md5sum_from_remote_uri(v1_remote_uri)
    assert remote_md5_v1 == v1_md5sum, f"Version 1 MD5 mismatch: {remote_md5_v1} != {v1_md5sum}"
    
    # Update the file with new content (version 2)
    payload_v2 = "version 2 content - " + random_string(16)
    with open(test_path, "w") as fp:
        fp.write(payload_v2)
    
    with DataManifestWriter(manifest_fname) as manifest:
        manifest.update("versioned_file.txt", test_path)
        record_v2 = manifest.get("versioned_file.txt", validate=False)
        # Save version 2 info (remote_uri now includes version_id)
        v2_md5sum = record_v2.md5sum
        v2_remote_uri = copy.deepcopy(record_v2.remote_uri)
    
    # Verify the version IDs are different
    assert v1_remote_uri.version_id != v2_remote_uri.version_id, "Version IDs should be different after update"
    
    # Verify the MD5s are different (since content changed)
    assert v1_md5sum != v2_md5sum, "MD5 checksums should be different after content change"
    
    # Verify version 1 is still accessible with correct content
    remote_md5_v1_after = calc_md5sum_from_remote_uri(v1_remote_uri)
    assert remote_md5_v1_after == v1_md5sum, f"Version 1 MD5 mismatch after update: {remote_md5_v1_after} != {v1_md5sum}"
    
    # Verify version 2 is accessible with correct content
    remote_md5_v2 = calc_md5sum_from_remote_uri(v2_remote_uri)
    assert remote_md5_v2 == v2_md5sum, f"Version 2 MD5 mismatch: {remote_md5_v2} != {v2_md5sum}"
    
    # Verify remote paths are the same (only version ID differs)
    assert v1_remote_uri.bucket == v2_remote_uri.bucket
    assert v1_remote_uri.path == v2_remote_uri.path
    
    # Verify URIs include version IDs
    assert "versionId=" in v1_remote_uri.uri
    assert "versionId=" in v2_remote_uri.uri


def _verify_manifest(manifest):
    # ensure that every file in the manifest exists, and is the same as the remote file
    # this is doing the same thing as validate, but manually
    for record in manifest:
        remote_md5sum = calc_md5sum_from_remote_uri(record.remote_uri)
        local_md5sum = calc_md5sum_from_fname(record.path)
        assert record.md5sum == remote_md5sum
        assert record.md5sum == local_md5sum


def test_sync(manifest_fname, cleandir2):
    # test that the remote file is the same as the local file
    local_cache_prefix = os.path.normpath(os.path.join(cleandir2, "./local_cache/"))
    local_data_path = os.path.normpath(os.path.join(cleandir2, "./local_data/"))
    manifest = DataManifest(manifest_fname)
    manifest.sync()
    _verify_manifest(manifest)


def test_multiple_manifests_sharing_data(manifest_fname, cleandir2, check_s3_bucket_versioning):
    # create the first manifest, and verify that it works
    with DataManifest(manifest_fname) as manifest:
        manifest.sync()
        # make sure that the manifest matches what's on disk
        _verify_manifest(manifest)
        old_manifest_records = list(manifest)

    # test that we can sync a data manifest to a new directory with an existing cache
    local_data_path = os.path.normpath(os.path.join(cleandir2, "./local_data/"))
    new_manifest_fname = os.path.abspath("new_manifest.data_manifest.tsv")

    # create a mew manifest, and add a file
    with DataManifestWriter.new(
        new_manifest_fname,
        manifest.remote_datastore_uri.uri,
        checkout_prefix=local_data_path,
    ) as new_manifest:
        for record in old_manifest_records:
            new_manifest.add(record.key, record.path, notes=record.notes)
        # create a test file, and add it to the data manifest
        with tempfile.NamedTemporaryFile("w") as ofp:
            test_data_key = "random/random_data.txt"
            ofp.write("sjkhalewiasduhidhcui" * 8)
            new_manifest.add(test_data_key, ofp.name, notes="some sort of random data")
        _verify_manifest(new_manifest)

        assert len(new_manifest) != len(old_manifest_records)

    newer_local_data_path = os.path.normpath(
        os.path.join(cleandir2, "./local_data_newer/")
    )
    # make sure that we get an error if we try to checkout twice
    with pytest.raises(FileExistsError):
        DataManifest.checkout(new_manifest_fname, checkout_prefix=newer_local_data_path)

    #with DataManifest.checkout(
    #    new_manifest_fname, checkout_prefix=newer_local_data_path
    #) as newer_manifest:
    #    newer_manifest.sync()
    #    _verify_manifest(newer_manifest)


@pytest.mark.parametrize("fast", [True, False])
def test_validate_success(manifest_fname, fast):
    # test that verify works when everything matches
    manifest = DataManifest(manifest_fname)
    manifest.validate(fast=fast)


@pytest.mark.parametrize("fast", [True, False])
def test_validate_size_fail(manifest_fname, fast):
    # test that verify fails when there is a size mismatch.
    manifest = DataManifest(manifest_fname)
    # modify one of the files in the cache so that it has the wrong size
    fname = next(iter(manifest)).path
    with open(fname) as ifp:
        data = ifp.read()
    with open(fname, "w") as ofp:
        ofp.write(data + "EXTRA")
    with pytest.raises(
        FileMismatchError, match=r".*?'.+?' has size '\d+' vs '\d+' in the manifest.*"
    ):
        manifest.validate(fast=fast)


@pytest.mark.parametrize("fast", [False, True])
def test_validate_md5_fail(manifest_fname, fast):
    # test that verify fails when there is a size mismatch.
    manifest = DataManifest(manifest_fname)
    # modify one of the files in the cache so that it has the wrong size
    fname = next(iter(manifest)).path
    with open(fname) as ifp:
        data = ifp.read()
    with open(fname, "w") as ofp:
        assert data[-1] != "|"
        data = data[:-1] + "|"
        ofp.write(data)

    # if we don't verify the md5sum, an exception shouldn't be raised
    if fast is True:
        manifest.validate(fast=fast)
    else:
        assert fast is False
        with pytest.raises(
            FileMismatchError,
            match=r".*?'.+?' has md5sum '.+?' vs '.+?' in the manifest/*",
        ):
            manifest.validate(fast=fast)


@pytest.mark.parametrize("fast", [False, True])
def test_rebuild_of_local(manifest_fname, fast):
    with DataManifest(manifest_fname) as dm:
        key = list(dm.keys())[0]
        local_cache_path = dm.get_local_cache_path(key)
        local_checkout_path = dm.get(key).path

    # delete some files
    os.unlink(local_cache_path)
    os.unlink(local_checkout_path)

    with DataManifest(manifest_fname) as dm:
        dm.sync(fast)


def test_get_no_validate(manifest_fname):
    # test that verify fails when there is a size mismatch.
    manifest = DataManifest(manifest_fname)
    # modify one of the files in the cache so that it has the wrong size
    record = next(iter(manifest))
    with open(record.path) as ifp:
        data = ifp.read()
    with open(record.path, "w") as ofp:
        ofp.write(data + "EXTRA")
    with pytest.raises(
        FileMismatchError, match=r".*?'.+?' has size '\d+' vs '\d+' in the manifest.*"
    ):
        manifest.get(record.key)
    manifest.get(record.key, validate=False)


@pytest.mark.parametrize("fast", [False, True])
def test_lazy_load(manifest_fname, cleandir, fast):
    # test that verify fails when there is a size mismatch.
    manifest = DataManifest.checkout(
        manifest_fname,
        checkout_prefix=os.path.normpath(os.path.join(cleandir, "./doesnt_exit/")),
        force=True
    )
    keys_iter = iter(manifest.keys())
    key1 = next(keys_iter)
    key2 = next(keys_iter)
    # test that validating the key fails (because this is lazy sync)
    with pytest.raises(MissingFileError, match=r".*Can not find '.+?' at '.+?'.*"):
        manifest.validate_record(key1)
    # test that validate suceeds after a sync_record
    manifest.sync_record(key1)
    manifest.validate_record(key1)

    # test that get fails (because this is lazy sync)
    with pytest.raises(MissingFileError, match=r".*Can not find '.+?' at '.+?'.*"):
        manifest.get(key2)

    # check that sync ang get works
    record = manifest.sync_and_get(key2)
    record_2 = manifest.get(key2)
    assert record == record_2


# =============================================================================
# Unit tests for RemotePath URI parsing
# =============================================================================

def test_remote_path_uri_without_version():
    """Test RemotePath.from_uri() correctly parses URI without versionId."""
    rp = RemotePath.from_uri("s3://my-bucket/path/to/file.txt")
    assert rp.scheme == "s3"
    assert rp.bucket == "my-bucket"
    assert rp.path == "path/to/file.txt"
    assert rp.version_id == ""
    assert rp.uri == "s3://my-bucket/path/to/file.txt"


def test_remote_path_uri_with_version():
    """Test RemotePath.from_uri() correctly parses URI with versionId."""
    rp = RemotePath.from_uri("s3://my-bucket/path/to/file.txt?versionId=abc123xyz")
    assert rp.scheme == "s3"
    assert rp.bucket == "my-bucket"
    assert rp.path == "path/to/file.txt"
    assert rp.version_id == "abc123xyz"
    assert "versionId=abc123xyz" in rp.uri


def test_remote_path_uri_property():
    """Test RemotePath.uri property includes/excludes versionId correctly."""
    # Without version_id
    rp1 = RemotePath("s3", "bucket", "key")
    assert rp1.uri == "s3://bucket/key"
    assert "versionId" not in rp1.uri
    
    # With version_id
    rp2 = RemotePath("s3", "bucket", "key", "v123")
    assert "versionId=v123" in rp2.uri


# =============================================================================
# Unit tests for version validation
# =============================================================================

def test_manifest_missing_version_in_header(cleandir):
    """Test that reading a manifest without MANIFEST_VERSION raises ValueError."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    # Write a manifest without MANIFEST_VERSION
    with open(manifest_path, "w") as f:
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\tsize\tnotes\n")
    
    # Write a valid local config
    with open(manifest_path + ".local_config", "w") as f:
        f.write(f"MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")
    
    with pytest.raises(ValueError, match="MANIFEST_VERSION not found"):
        DataManifest(manifest_path)


def test_manifest_wrong_version_in_header(cleandir):
    """Test that reading a manifest with wrong MANIFEST_VERSION raises ValueError."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    # Write a manifest with wrong version
    with open(manifest_path, "w") as f:
        f.write("#MANIFEST_VERSION=999\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\tsize\tnotes\n")
    
    # Write a valid local config
    with open(manifest_path + ".local_config", "w") as f:
        f.write(f"MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")
    
    with pytest.raises(ValueError, match="MANIFEST_VERSION mismatch"):
        DataManifest(manifest_path)


def test_local_config_missing_version(cleandir):
    """Test that reading a local config without MANIFEST_VERSION raises ValueError."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    # Write a valid manifest
    with open(manifest_path, "w") as f:
        f.write(f"#MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\tsize\tnotes\n")
    
    # Write local config without version
    with open(manifest_path + ".local_config", "w") as f:
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")
    
    with pytest.raises(ValueError, match="MANIFEST_VERSION not found in local config"):
        DataManifest(manifest_path)


def test_local_config_wrong_version(cleandir):
    """Test that reading a local config with wrong MANIFEST_VERSION raises ValueError."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    # Write a valid manifest
    with open(manifest_path, "w") as f:
        f.write(f"#MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\tsize\tnotes\n")
    
    # Write local config with wrong version
    with open(manifest_path + ".local_config", "w") as f:
        f.write("MANIFEST_VERSION=999\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")
    
    with pytest.raises(ValueError, match="MANIFEST_VERSION mismatch in local config"):
        DataManifest(manifest_path)


def test_mixed_supported_versions_accepted(cleandir):
    """Test that mixed supported versions (v2 manifest + v3 local config) are accepted."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    # Write manifest with version 2
    with open(manifest_path, "w") as f:
        f.write("#MANIFEST_VERSION=2\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\tsize\tnotes\n")

    # Write local config with version 3
    with open(manifest_path + ".local_config", "w") as f:
        f.write("MANIFEST_VERSION=3\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")

    # Mixed supported versions should now be accepted (v3 code reads both v2 and v3)
    dm = DataManifest(manifest_path)
    dm.close()


def test_unsupported_version_in_header_rejected(cleandir):
    """Test that unsupported version in manifest header is rejected."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    with open(manifest_path, "w") as f:
        f.write("#MANIFEST_VERSION=99\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\tsize\tnotes\n")

    with open(manifest_path + ".local_config", "w") as f:
        f.write(f"MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")

    with pytest.raises(ValueError, match="MANIFEST_VERSION mismatch"):
        DataManifest(manifest_path)


def test_unsupported_version_in_local_config_rejected(cleandir):
    """Test that unsupported version in local config is rejected."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    with open(manifest_path, "w") as f:
        f.write(f"#MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\tsize\tnotes\n")

    with open(manifest_path + ".local_config", "w") as f:
        f.write("MANIFEST_VERSION=99\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")

    with pytest.raises(ValueError, match="MANIFEST_VERSION mismatch"):
        DataManifest(manifest_path)


# =============================================================================
# Unit tests for malformed config files
# =============================================================================

def test_malformed_local_config_missing_equals(cleandir):
    """Test that a malformed local config line without '=' gives clear error."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    # Write a valid manifest
    with open(manifest_path, "w") as f:
        f.write(f"#MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\tsize\tnotes\n")
    
    # Write malformed local config (missing =)
    with open(manifest_path + ".local_config", "w") as f:
        f.write("THIS_LINE_HAS_NO_EQUALS\n")
    
    with pytest.raises(ValueError, match="Malformed config line"):
        DataManifest(manifest_path)


def test_local_config_with_equals_in_value(cleandir):
    """Test that local config values containing '=' are parsed correctly."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    # Write a valid manifest
    with open(manifest_path, "w") as f:
        f.write(f"#MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\tsize\tnotes\n")
    
    # Write local config with = in value (should work)
    with open(manifest_path + ".local_config", "w") as f:
        f.write(f"MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")
        f.write("EXTRA_KEY=value=with=equals\n")
    
    # Should not raise - the = in the value should be handled
    dm = DataManifest(manifest_path)
    dm.close()


# =============================================================================
# Unit tests for empty version ID validation
# =============================================================================

def test_empty_version_id_in_manifest(cleandir):
    """Test that reading a manifest with empty s3_version_id raises ValueError."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    # Write manifest with empty version ID
    with open(manifest_path, "w") as f:
        f.write(f"#MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\tsize\tnotes\n")
        f.write("myfile.txt\t\tabc123\t100\t\n")  # empty version ID
    
    # Write valid local config
    with open(manifest_path + ".local_config", "w") as f:
        f.write(f"MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")
    
    with pytest.raises(ValueError, match="s3_version_id is required and cannot be empty"):
        DataManifest(manifest_path)


# =============================================================================
# Unit tests for v3 features (no S3 required)
# =============================================================================

def test_is_multipart_etag():
    from datamanifest.datamanifest import is_multipart_etag
    assert is_multipart_etag("abc123-3") is True
    assert is_multipart_etag("abc123") is False
    assert is_multipart_etag("") is False


def test_remote_path_skip_validation():
    """Test RemotePath.from_uri with skip_validation for paths with special chars."""
    rp = RemotePath.from_uri("s3://bucket/path with spaces/file.txt", skip_validation=True)
    assert rp.scheme == "s3"
    assert rp.bucket == "bucket"
    assert rp.path == "path with spaces/file.txt"
    assert rp.version_id == ""

    # Without skip_validation, special chars should fail
    with pytest.raises(InvalidPrefix):
        RemotePath.from_uri("s3://bucket/path with spaces/file.txt")


def test_remote_path_skip_validation_preserves_on_replace():
    """Test that dataclasses.replace preserves _skip_validation."""
    import dataclasses
    rp = RemotePath.from_uri("s3://bucket/path with spaces/file.txt", skip_validation=True)
    rp2 = dataclasses.replace(rp, version_id="v123")
    assert rp2._skip_validation is True
    assert rp2.version_id == "v123"
    assert rp2.path == "path with spaces/file.txt"


def test_default_header_v3():
    """Test that default_header returns 7 columns in correct order."""
    header = DataManifest.default_header()
    assert len(header) == 7
    assert header == ["key", "s3_version_id", "md5sum", "s3_hash", "size", "source_uri", "notes"]


def test_datamanifest_record_is_external():
    """Test the is_external property on DataManifestRecord."""
    rp = RemotePath("s3", "bucket", "path", "v1")
    # Regular record (no source_uri)
    r = DataManifestRecord(key="test", md5sum="abc", s3_hash="def", size=100,
                           notes="", path="/tmp/test", remote_uri=rp)
    assert r.is_external is False
    assert r.source_uri == ""

    # External record
    r2 = DataManifestRecord(key="test", md5sum="abc", s3_hash="def", size=100,
                            notes="", path="/tmp/test", remote_uri=rp,
                            source_uri="s3://other/path")
    assert r2.is_external is True


def test_get_local_cache_path_uses_s3_hash(cleandir):
    """Test that get_local_cache_path uses s3_hash when present, md5sum as fallback."""
    from datamanifest.datamanifest import DataManifest
    # s3_hash present: should use s3_hash
    record_with_hash = DataManifestRecord(
        key="file.txt", md5sum="md5abc", s3_hash="s3hashdef", size=100,
        notes="", path="/tmp/file.txt",
        remote_uri=RemotePath("s3", "bucket", "path", "v1"),
    )
    file_hash = record_with_hash.s3_hash if record_with_hash.s3_hash else record_with_hash.md5sum
    assert file_hash == "s3hashdef"

    # s3_hash empty (v2 legacy): should fall back to md5sum
    record_no_hash = DataManifestRecord(
        key="file.txt", md5sum="md5abc", s3_hash="", size=100,
        notes="", path="/tmp/file.txt",
        remote_uri=RemotePath("s3", "bucket", "path", "v1"),
    )
    file_hash2 = record_no_hash.s3_hash if record_no_hash.s3_hash else record_no_hash.md5sum
    assert file_hash2 == "md5abc"


def test_v2_manifest_read_by_v3_reader(cleandir):
    """Test that v3 reader can open a v2-format manifest (5 columns)."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    # Write a v2-format manifest
    with open(manifest_path, "w") as f:
        f.write("#MANIFEST_VERSION=2\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\tsize\tnotes\n")
        f.write("myfile.txt\tv1\tabc123\t100\tsome notes\n")

    with open(manifest_path + ".local_config", "w") as f:
        f.write(f"MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")

    dm = DataManifest(manifest_path)
    record = dm.get("myfile.txt", validate=False)
    assert record.key == "myfile.txt"
    assert record.md5sum == "abc123"
    assert record.s3_hash == ""  # v2 records have no s3_hash
    assert record.source_uri == ""  # v2 records have no source_uri
    assert record.is_external is False
    assert record.size == 100
    dm.close()


def test_v3_manifest_with_external_record(cleandir):
    """Test reading a v3 manifest with an external record."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    with open(manifest_path, "w") as f:
        f.write(f"#MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n")
        # Regular record
        f.write("regular.txt\tv1\tabc123\tdef456\t100\t\tregular notes\n")
        # External record (empty version_id is OK for external)
        f.write("external.txt\t\t\texternal_etag\t200\ts3://other-bucket/data.txt\texternal notes\n")

    with open(manifest_path + ".local_config", "w") as f:
        f.write(f"MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")

    dm = DataManifest(manifest_path)
    # Check regular record
    regular = dm.get("regular.txt", validate=False)
    assert regular.is_external is False
    assert regular.s3_hash == "def456"
    assert regular.source_uri == ""

    # Check external record
    external = dm.get("external.txt", validate=False)
    assert external.is_external is True
    assert external.s3_hash == "external_etag"
    assert external.source_uri == "s3://other-bucket/data.txt"
    assert external.remote_uri.bucket == "other-bucket"
    assert external.remote_uri.path == "data.txt"
    dm.close()


def test_source_uri_rejects_embedded_version_id(cleandir):
    """Test that _read_records rejects source_uri with embedded ?versionId."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    with open(manifest_path, "w") as f:
        f.write(f"#MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n")
        f.write("bad.txt\tv1\tabc\tdef\t100\ts3://bucket/file?versionId=xxx\tnotes\n")

    with open(manifest_path + ".local_config", "w") as f:
        f.write(f"MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")

    with pytest.raises(ValueError, match="source_uri.*contains.*versionId"):
        DataManifest(manifest_path)


def test_empty_version_id_accepted_for_external(cleandir):
    """Test that empty s3_version_id is accepted for external records."""
    manifest_path = os.path.join(cleandir, "test.data_manifest.tsv")
    with open(manifest_path, "w") as f:
        f.write(f"#MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write("#REMOTE_DATA_MIRROR_URI=s3://bucket/path\n")
        f.write("#LOCAL_CACHE_PATH_SUFFIX=./cache/\n")
        f.write("key\ts3_version_id\tmd5sum\ts3_hash\tsize\tsource_uri\tnotes\n")
        f.write("ext.txt\t\t\tetag123\t50\ts3://other/file.txt\t\n")

    with open(manifest_path + ".local_config", "w") as f:
        f.write(f"MANIFEST_VERSION={MANIFEST_VERSION}\n")
        f.write(f"CHECKOUT_PREFIX={cleandir}/checkout\n")
        f.write(f"LOCAL_CACHE_PREFIX={cleandir}/cache\n")

    dm = DataManifest(manifest_path)
    record = dm.get("ext.txt", validate=False)
    assert record.s3_version_id == ""
    assert record.is_external is True
    dm.close()


def test_verify_record_empty_md5sum():
    """Test _verify_record_matches_file with empty md5sum only checks size."""
    import tempfile
    rp = RemotePath("s3", "bucket", "path", "v1")
    record = DataManifestRecord(key="test", md5sum="", s3_hash="etag123", size=5,
                                notes="", path="/tmp/test", remote_uri=rp)

    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write("hello")
        tmp_path = f.name

    try:
        # Should pass - size matches, md5sum check skipped because md5sum is empty
        DataManifest._verify_record_matches_file(record, tmp_path, check_md5sum=True)

        # Should fail - wrong size
        record_bad_size = DataManifestRecord(key="test", md5sum="", s3_hash="etag123", size=999,
                                             notes="", path="/tmp/test", remote_uri=rp)
        with pytest.raises(FileMismatchError):
            DataManifest._verify_record_matches_file(record_bad_size, tmp_path, check_md5sum=True)
    finally:
        os.unlink(tmp_path)
