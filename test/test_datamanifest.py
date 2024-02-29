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
    load_data_manifest,
    FileMismatchError,
    KeyAlreadyExistsError,
    InvalidKey,
    MissingFileError,
    calc_md5sum_from_remote_uri,
    calc_md5sum_from_fname,
    validate_key,
    MANIFEST_CACHE,
    DEFAULT_FOLDER_PERMISSIONS,
    random_string,
    environment_variables,
    s3_uri_exists,
)

def _find_current_git_hash():
    try:
        res = subprocess.run("git rev-parse --verify HEAD", shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rv = res.stdout.strip().decode()
        if len(rv) == 0:
            rv = random_string(16)
    except subprocess.CalledProcessError:
        rv = random_string(16)
    return rv


def _find_current_git_branch():
    try:
        res = subprocess.run("git rev-parse --abbrev-ref HEAD", shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rv = res.stdout.strip().decode()
        if len(rv) == 0:
            rv = "UNKNOWN_BRANCH" + random_string(16) + "UNKNOWN_BRANCH"
    except subprocess.CalledProcessError:
        rv = "UNKNOWN_BRANCH" + random_string(16) + "UNKNOWN_BRANCH"
    return rv


S3_TEST_BUCKET = "ravel-biotechnology-test"
GIT_HASH = _find_current_git_hash()
GIT_BRANCH = _find_current_git_branch()


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
def manifest_fname(cleandir):
    """Instantiate a data manifest with files."""
    # make a copy of the current environment to restore
    print(f"Created temporary directory: {cleandir}")
    manifest_fname = os.path.join(cleandir, "data_manifest.data_manifest.tsv")
    assert not os.path.exists(manifest_fname)
    print(f"Loading manifest: {manifest_fname}")
    checkout_prefix_base = os.path.normpath(os.path.join(cleandir, f"./{GIT_BRANCH}-data/"))
    checkout_prefix = os.path.normpath(os.path.join(checkout_prefix_base, "./data_manifest/"))

    local_cache_prefix = os.path.normpath(os.path.join(cleandir, "./local_cache"))
    os.makedirs(local_cache_prefix, mode=DEFAULT_FOLDER_PERMISSIONS)
    # this line is needed for the permissions to be valid in docker. No idea why -- must be a bug in os.makedirs
    # or docker mounts.
    os.chmod(local_cache_prefix, DEFAULT_FOLDER_PERMISSIONS)

    remote_datastore_prefix = f"s3://{S3_TEST_BUCKET}/{GIT_HASH}-{random_string(16)}"

    manifest = DataManifestWriter.new(
        manifest_fname,
        checkout_prefix=checkout_prefix,
        local_cache_prefix=local_cache_prefix,
        remote_datastore_prefix=remote_datastore_prefix,
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

    with environment_variables(
        LOCAL_DATA_PATH=checkout_prefix,
        LOCAL_DATA_MIRROR_PATH=local_cache_prefix,
        REMOTE_DATA_MIRROR_URI=remote_datastore_prefix,
    ):
        yield manifest.fname  # provide the fixture value

    # clean up S3 files created in remote_datastore_prefix
    s3 = boto3.resource("s3")
    parsed = urlparse(remote_datastore_prefix)
    bucket = s3.Bucket(parsed.netloc)
    bucket.objects.filter(Prefix=parsed.path.lstrip("/")).delete()


@pytest.mark.parametrize(
    "fname,expected_key",
    [("test.data_manifest.tsv", "test"), ("/scratch/nboley/test.data_manifest.tsv", "test")],
)
def test_get_manifest_key(fname, expected_key):
    assert DataManifest.get_manifest_key(fname) == expected_key


def test_new_dm_on_class_init(cleandir):
    new_dm_path = f"{cleandir}/test_dm.data_manifest.tsv"
    assert not os.path.exists(new_dm_path), "Test error, the new file shouldn't already be there"
    path = __file__
    local_cache_prefix = os.path.normpath(os.path.join(cleandir, "./local_cache"))
    os.makedirs(local_cache_prefix, mode=DEFAULT_FOLDER_PERMISSIONS)
    # this line is needed for the permissions to be valid in docker. No idea why -- must be a bug in os.makedirs
    # or docker mounts.
    os.chmod(local_cache_prefix, DEFAULT_FOLDER_PERMISSIONS)

    remote_datastore_prefix = f"s3://{S3_TEST_BUCKET}/{GIT_HASH}-{random_string(16)}"

    with DataManifestWriter.new(new_dm_path, checkout_prefix=cleandir, local_cache_prefix=local_cache_prefix, remote_datastore_prefix=remote_datastore_prefix) as dmr:
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
    with pytest.raises(RuntimeError, match=r".*?and so it can't be opened for reading.*"):
        load_data_manifest(manifest_fname)
    manifest_w1.close()

    manifest_r2 = load_data_manifest(manifest_fname)
    with pytest.raises(RuntimeError, match=r".*?and so it can't be opened for writing.*"):
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
    manifest = load_data_manifest(manifest_fname)
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

    manifest = load_data_manifest(
        manifest_fname, checkout_prefix=local_data_path, local_cache_prefix=local_cache_prefix,
    )
    manifest.sync()

    _verify_manifest(manifest)


def test_multiple_manifests_sharing_data(manifest_fname, cleandir2):
    # create the first manifest, and verify that it works
    with load_data_manifest(manifest_fname) as manifest:
        manifest.sync()
        # make sure that the manifest matches what's on disk
        _verify_manifest(manifest)
        old_manifest_records = list(manifest)

    # test that we can sync a data manifest to a new directory with an existing cache
    local_data_path = os.path.normpath(os.path.join(cleandir2, "./local_data/"))
    new_manifest_fname = os.path.abspath("new_manifest.data_manifest.tsv")
    # create a mew manifest, and add a file
    with DataManifestWriter.new(new_manifest_fname, checkout_prefix=local_data_path,) as new_manifest:
        for record in old_manifest_records:
            new_manifest.add(record.key, record.path, notes=record.notes)
        # create a test file, and add it to the data manifest
        with tempfile.NamedTemporaryFile("w") as ofp:
            test_data_key = "random/random_data.txt"
            ofp.write("sjkhalewiasduhidhcui" * 8)
            new_manifest.add(test_data_key, ofp.name, notes="some sort of random data")
        _verify_manifest(new_manifest)

        assert len(new_manifest) != len(old_manifest_records)

    newer_local_data_path = os.path.normpath(os.path.join(cleandir2, "./local_data_newer/"))
    with load_data_manifest(new_manifest_fname, checkout_prefix=newer_local_data_path) as newer_manifest:
        newer_manifest.sync()
        _verify_manifest(newer_manifest)


@pytest.mark.parametrize("fast", [True, False])
def test_validate_success(manifest_fname, fast):
    # test that verify works when everything matches
    manifest = load_data_manifest(manifest_fname)
    manifest.validate(fast=fast)


@pytest.mark.parametrize("fast", [True, False])
def test_validate_size_fail(manifest_fname, fast):
    # test that verify fails when there is a size mismatch.
    manifest = load_data_manifest(manifest_fname)
    # modify one of the files in the cache so that it has the wrong size
    fname = next(iter(manifest)).path
    with open(fname) as ifp:
        data = ifp.read()
    with open(fname, "w") as ofp:
        ofp.write(data + "EXTRA")
    with pytest.raises(FileMismatchError, match=r".*?'.+?' has size '\d+' vs '\d+' in the manifest.*"):
        manifest.validate(fast=fast)


@pytest.mark.parametrize("fast", [False, True])
def test_validate_md5_fail(manifest_fname, fast):
    # test that verify fails when there is a size mismatch.
    manifest = load_data_manifest(manifest_fname)
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
            FileMismatchError, match=r".*?'.+?' has md5sum '.+?' vs '.+?' in the manifest/*",
        ):
            manifest.validate(fast=fast)


@pytest.mark.parametrize("fast", [False, True])
def test_rebuild_of_local(manifest_fname, fast):
    with load_data_manifest(manifest_fname) as dm:
        key = list(dm.keys())[0]
        local_cache_path = dm.get_local_cache_path(key)
        local_checkout_path = dm.get(key).path

    # delete some files
    os.unlink(local_cache_path)
    os.unlink(local_checkout_path)
    dm.sync(fast)


def test_get_no_validate(manifest_fname):
    # test that verify fails when there is a size mismatch.
    manifest = load_data_manifest(manifest_fname)
    # modify one of the files in the cache so that it has the wrong size
    record = next(iter(manifest))
    with open(record.path) as ifp:
        data = ifp.read()
    with open(record.path, "w") as ofp:
        ofp.write(data + "EXTRA")
    with pytest.raises(FileMismatchError, match=r".*?'.+?' has size '\d+' vs '\d+' in the manifest.*"):
        manifest.get(record.key)
    manifest.get(record.key, validate=False)


@pytest.mark.parametrize("fast", [False, True])
def test_lazy_load(manifest_fname, cleandir, fast):
    # test that verify fails when there is a size mismatch.
    manifest = load_data_manifest(
        manifest_fname, checkout_prefix=os.path.normpath(os.path.join(cleandir, "./doesnt_exit/")),
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
