import time
import random
from typing import Optional, List
import boto3
import botocore
import codecs
import dataclasses
import fcntl
import fnmatch
import hashlib
import lockfile
import logging
import os
import re
import shutil
import subprocess
import grp
from pathlib import Path
from multiprocessing import Lock
import random
from contextlib import contextmanager
import string
import tempfile

from tqdm import tqdm
from urllib.parse import urlparse

from .config import (
    DEFAULT_FOLDER_PERMISSIONS,
    DEFAULT_FILE_PERMISSIONS,
    MANIFEST_VERSION,
)


logger = logging.getLogger(__name__)


def random_string(length):
    return "".join(
        [random.choice(string.ascii_letters + string.digits) for n in range(length)]
    )


@contextmanager
def environment_variables(**kwargs):
    old_env_vars = {key: os.environ.get(key) for key in kwargs if key in os.environ}
    os.environ.update(kwargs)
    yield
    # delete all of the new env variables
    for key in kwargs:
        del os.environ[key]
    # re-add the old variables
    os.environ.update(old_env_vars)


def s3_uri_exists(s3_uri: str) -> bool:
    """
    Check if an s3 uri exists.  A bucket does not count as an s3 uri.
    """
    remote_path = RemotePath.from_uri(s3_uri)
    if remote_path.path == "":
        return False
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


def _check_s3_versioning_enabled(bucket_name: str) -> bool:
    """
    Check if S3 versioning is enabled on the specified bucket.

    Returns True if versioning is enabled or suspended.
    Raises RuntimeError if versioning is not enabled or if we cannot check (e.g., no permissions).
    """
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_bucket_versioning(Bucket=bucket_name)
        status = response.get("Status", "")
        if status in ["Enabled", "Suspended"]:
            return True
        else:
            raise RuntimeError(
                f"S3 bucket '{bucket_name}' does not have versioning enabled. "
                f"Status: '{status if status else 'NotSet'}'. "
                f"Please enable versioning on the bucket before creating a data manifest. "
                f"See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html"
            )
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "AccessDenied":
            raise RuntimeError(
                f"Cannot check versioning status for S3 bucket '{bucket_name}': Access Denied. "
                f"Please ensure you have 's3:GetBucketVersioning' permission, or manually verify "
                f"that versioning is enabled on the bucket before creating a data manifest."
            ) from e
        else:
            # For other errors (bucket doesn't exist, etc.), let the original error propagate
            raise

class MissingLocalConfigError(Exception):
    pass

class FileAlreadyExistsError(Exception):
    pass


class FileMismatchError(Exception):
    pass


class MissingFileError(Exception):
    pass


class KeyAlreadyExistsError(Exception):
    pass


class InvalidKey(ValueError):
    pass


class InvalidPrefix(ValueError):
    pass


def _extract_permissions(path):
    return int(str(oct(os.stat(path).st_mode))[-4:], base=8)


def _makedirs_and_change_permissions(path, mode, root):
    # note that makedirs could have created multiple directories, so we check and set group
    # name up the tree until we find hte correct group name
    os.makedirs(path, mode=mode, exist_ok=True)


def hex_to_base64(hex_str):
    return (
        codecs.encode(codecs.decode(hex_str, "hex"), "base64").strip().decode("ascii")
    )


def calc_md5sum_from_fname(fname):
    hex_str = (
        subprocess.run(["md5sum", fname], stdout=subprocess.PIPE, check=True)
        .stdout.split()[0]
        .decode("ascii")
    )
    return hex_str


def calc_md5sum_from_fp(fp):
    fpos = fp.tell()
    m = hashlib.md5()
    fp.seek(0)
    m.update(fp.read(10000000).encode("utf8"))
    if fp.read(1) != "":
        raise ValueError(
            f"{fp.name} is too large to calculate the md5 sum from this function."
        )
    fp.seek(fpos)
    return m.hexdigest()


def calc_md5sum_from_remote_uri(remote_path):
    assert isinstance(remote_path, RemotePath)
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(remote_path.bucket)
    remote_object = bucket.Object(remote_path.path)
    with tempfile.NamedTemporaryFile("wb+") as fp:
        remote_object.download_fileobj(fp)
        m = hashlib.md5()
        fp.seek(0)
        m.update(fp.read())
        return m.hexdigest()


def _validate_prefix(prefix, ErrorClass):
    if not re.match(r"^[A-Za-z0-9,_\-/\.]+$", prefix):
        raise ErrorClass(f"{prefix} contains invalid characters")

    if prefix != os.path.normpath(prefix):
        raise ErrorClass(
            f'{prefix} is not a normalized path, try "{os.path.normpath(prefix)}"'
        )


def validate_key(key):
    _validate_prefix(key, InvalidKey)

    if key.startswith("/"):
        raise InvalidKey(f"{key} must be a relative path")


def validate_local_prefix(prefix):
    _validate_prefix(prefix, InvalidPrefix)

    if prefix != os.path.abspath(prefix):
        raise InvalidPrefix(
            f'{prefix} is not an absolute path, try "{os.path.abspath(prefix)}"'
        )


@dataclasses.dataclass
class RemotePath:
    scheme: str
    bucket: str
    path: str

    @classmethod
    def from_uri(cls, uri):
        parsed_uri = urlparse(uri)
        assert parsed_uri.params == ""
        assert parsed_uri.query == ""
        assert parsed_uri.fragment == ""
        return cls(parsed_uri.scheme, parsed_uri.netloc, parsed_uri.path.lstrip("/"))

    def __post_init__(self):
        if self.scheme != "s3":
            raise NotImplementedError(
                "DataManifest currently only supports s3 for the remote cache."
            )
        _validate_prefix(self.path, InvalidPrefix)

    @property
    def uri(self):
        return f"{self.scheme}://{self.bucket}/{self.path}"


@dataclasses.dataclass
class DataManifestRecord:
    key: str
    md5sum: str
    size: int
    notes: str
    path: str
    remote_uri: RemotePath

    @staticmethod
    def header() -> List[str]:
        return ["key", "md5sum", "size", "notes", "path", "remote_uri"]


class DataManifest:
    def _build_new_data_manifest_record(self, key, fname_to_add, notes):
        # find the file's file size and calculate the checksum
        logger.info(f"Calculating md5sum for '{fname_to_add}'")
        md5sum = calc_md5sum_from_fname(fname_to_add)
        logger.info(f"Calculated md5sum '{md5sum}' for '{fname_to_add}'.")
        fsize = int(os.path.getsize(fname_to_add))
        logger.info(f"Calculated filesize '{fsize}' for '{fname_to_add}'.")

        return DataManifestRecord(
            key,
            md5sum,
            fsize,
            notes=notes,
            path=self._build_checkout_path(self.checkout_prefix, key),
            remote_uri=self._build_remote_datastore_uri(
                self.remote_datastore_uri, key, md5sum
            ),
        )

    @staticmethod
    def default_header() -> List[str]:
        full_header = DataManifestRecord.header()
        # Remove these: ["path", "remote_uri"]
        assert full_header[-1] == "remote_uri", full_header
        assert full_header[-2] == "path", full_header
        return full_header[:-2]  # remove last two elements

    @staticmethod
    def _verify_record_matches_file(record, fpath, check_md5sum=True):
        """Verify that the file at 'local_abs_path' matches that in record.

        Checks:
        1) that the file sizes are the same
        2) (if check_md5sums is True) verify that the md5sums match (this is slow).
        """

        # ensure the filesizes match
        local_fsize = os.path.getsize(fpath)
        logger.debug(f"Calculated filesize '{local_fsize}' for '{fpath}'.")
        if local_fsize != int(record.size):
            raise FileMismatchError(
                f"'{fpath}' has size '{local_fsize}' vs '{record.size}' in the manifest"
            )

        # ensure the md5sum matches
        if check_md5sum:
            logger.info(f"Calculating md5sum for '{fpath}'.")
            local_md5sum = calc_md5sum_from_fname(fpath)
            logger.debug(
                f"Calculated md5sum '{local_md5sum}' for '{fpath} vs {record.md5sum} in the record'."
            )
            if local_md5sum != record.md5sum:
                raise FileMismatchError(
                    f"'{fpath}' has md5sum '{local_md5sum}' "
                    f"vs '{record.md5sum}' in the manifest"
                )

    def validate_record(self, key, check_md5sum=True):
        """Validate that record has a valid file.

        Checks:
        1) that the path exists
        2) that the file sizes are the same
        4) (if check_md5sums is True) that the md5sums match (this is slow).
        """
        validate_key(key)
        local_abs_path = self._data[key].path
        # check that the file exists
        if not os.path.exists(local_abs_path):
            raise MissingFileError(f"Can not find '{key}' at '{local_abs_path}'")

        return self._verify_record_matches_file(
            self._data[key], local_abs_path, check_md5sum=check_md5sum
        )

    def _update_local_cache(self, key, fast=False, retries=3):
        """Download key from the remote location to the local location."""
        # if the file already exists in the local cache, then verify it is the
        # same as the remote file
        local_cache_path = self.get_local_cache_path(key)
        logger.info(f"Setting local cache path to '{local_cache_path}'.")

        # occasionally there are dangling lock files, delete them if they are more than 30 minutes old to prevent
        # dead locks
        if os.path.exists(local_cache_path + ".lock.lock"):
            try:
                mins_since_last_modified = (
                    time.time() - os.stat(local_cache_path + ".lock.lock").st_mtime
                ) / 60
            except FileNotFoundError:
                # this happens when the lock file exists, but was cleaned up before we can run os.stat on it
                mins_since_last_modified = 0

            if mins_since_last_modified > 30:
                logger.warning(
                    f"{local_cache_path}.lock is older than 30 minutes, deleting!"
                )
                os.unlink(local_cache_path + ".lock.lock")

        # take out a lockfile to prevent multiple processes from accessing this
        # file at the same time
        # ensure that the path exists for the lockfile to be created
        if not os.path.exists(os.path.dirname(local_cache_path)):
            # otherwise, if we created this, then ensure that the group is correctly set
            os.makedirs(
                os.path.dirname(local_cache_path), mode=DEFAULT_FOLDER_PERMISSIONS, exist_ok=True
            )

        lock = lockfile.LockFile(local_cache_path + ".lock")
        with lock:
            # if local_path already exists, then make sure that it matches the remote file
            if os.path.exists(local_cache_path):
                logger.debug(
                    f"'{key}' already exists in the local cache -- validating that it matches the manifest."
                )
                self._verify_record_matches_file(
                    self._data[key], local_cache_path, check_md5sum=not fast
                )
            else:
                # if it doesn't then download the file
                s3 = boto3.resource("s3")
                bucket = s3.Bucket(self.remote_datastore_uri.bucket)
                remote_key = self._data[key].remote_uri.path
                logger.info(f"Setting remote key to '{remote_key}'.")
                remote_object = bucket.Object(remote_key)
                if os.path.exists(local_cache_path):
                    raise RuntimeError(
                        f"local_cache_path '{local_cache_path}' already exists (this is unexpected)"
                    )
                downloaded = False
                for rr in range(retries):
                    try:
                        remote_object.download_file(local_cache_path)
                        downloaded = True
                    except botocore.exceptions.ResponseStreamingError:
                        logger.error(
                            f"Error downloading '{remote_key}' to '{local_cache_path}'"
                            f"Retrying with retry number {rr+1} after a word from our sponsor..."
                        )
                        time.sleep(random.uniform(10, 60))

                if not downloaded:
                    logger.error(
                        f"Error downloading '{remote_key}' to '{local_cache_path}'"
                    )
                    raise
                # set the permissions and group
                os.chmod(local_cache_path, DEFAULT_FILE_PERMISSIONS)

    def _update_local_checkout(self, key):
        """Create symlink in the local checkout to the local cache"""
        local_cache_path = self.get_local_cache_path(key)
        if not os.path.exists(local_cache_path):
            raise MissingFileError(
                f"'{key}' does not exist in the local cache (at '{local_cache_path}')"
            )

        local_path = self._data[key].path
        logger.info(f"Linking {local_cache_path} to '{local_path}'.")
        if os.path.exists(local_path):
            # Check if local_cache_path is the same as local_path after following filesystem links
            if Path(local_path).resolve().as_posix() != local_cache_path:
                raise FileMismatchError(
                    f"{local_path} points to {Path(local_path).resolve().as_posix()} instead "
                    f"of {local_cache_path}"
                )
        else:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            # remove the symlink if it already exists
            if os.path.islink(local_path):
                old_local_cache_path = os.readlink(local_path)
                assert not os.path.islink(
                    old_local_cache_path
                ), "We should never have nested links created by the data manifest"
                if old_local_cache_path != local_cache_path:
                    os.unlink(local_path)

            os.symlink(local_cache_path, local_path)

    @staticmethod
    def _build_datastore_suffix(key, md5sum):
        return os.path.join(
            os.path.dirname(key), f"./{md5sum}-" + os.path.basename(key)
        )

    @classmethod
    def _build_remote_datastore_uri(cls, remote_datastore_uri, key, md5sum):
        return RemotePath(
            remote_datastore_uri.scheme,
            remote_datastore_uri.bucket,
            os.path.normpath(
                os.path.join(
                    remote_datastore_uri.path,
                    cls._build_datastore_suffix(key, md5sum),
                )
            ),
        )

    def get_local_cache_path(self, key):
        return os.path.normpath(
            os.path.join(
                self.local_cache_prefix,
                self._build_datastore_suffix(key, self._data[key].md5sum),
            )
        )

    def _build_checkout_path(self, checkout_prefix, key):
        rv = os.path.normpath(os.path.join(checkout_prefix, key))
        assert rv.endswith(key.lstrip("./")), str((checkout_prefix, key, rv))
        return rv

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
        return

    def close(self):
        fcntl.flock(self._fp, fcntl.LOCK_UN)
        self._fp.close()

    @staticmethod
    def local_config_path(manifest_path):
        return os.path.normpath(os.path.abspath(manifest_path + ".local_config"))

    @classmethod
    def _read_local_config(cls, manifest_path):
        """Read configuration data from the local config file.

        """
        try:
            config = {}
            with open(cls.local_config_path(manifest_path)) as fp:
                for line_i, line in enumerate(fp):
                    # skip empty lines
                    if line.strip() == "":
                        continue
                    key, val = line.strip().split("=")
                    config[key.strip()] = val.strip()
            # validate version
            if "MANIFEST_VERSION" not in config:
                raise ValueError(
                    f"MANIFEST_VERSION not found in local config file '{cls.local_config_path(manifest_path)}'"
                )
            if config["MANIFEST_VERSION"] != MANIFEST_VERSION:
                raise ValueError(
                    f"MANIFEST_VERSION mismatch in local config file '{cls.local_config_path(manifest_path)}': "
                    f"expected '{MANIFEST_VERSION}', found '{config['MANIFEST_VERSION']}'"
                )
            return config
        except:
            raise MissingLocalConfigError(f"Could not find a local config file at '{cls.local_config_path(manifest_path)}'\nHint: You probably need to run checkout.")

    @staticmethod
    def _load_header(fp):
        config = {}
        for line_i, line in enumerate(fp):
            # skip empty lines
            if line.strip() == "":
                continue
            if line.startswith("#"):
                key, val = line[1:].strip().split("=")
                config[key.strip()] = val.strip()
            else:
                # assume that we're to the header now
                header = line.strip("\n").split("\t")
                break

        # validate version
        if "MANIFEST_VERSION" not in config:
            raise ValueError(
                "MANIFEST_VERSION not found in data manifest header"
            )
        if config["MANIFEST_VERSION"] != MANIFEST_VERSION:
            raise ValueError(
                f"MANIFEST_VERSION mismatch in data manifest: "
                f"expected '{MANIFEST_VERSION}', found '{config['MANIFEST_VERSION']}'"
            )

        fp.seek(0)
        return config, header, line_i

    def _read_records(self, header_offset):
        # read all of the file contents into memory
        data = {}
        for line_i, line in enumerate(self._fp):
            # skip until we are below the header
            if line_i <= header_offset:
                continue
            # skip commented and empty lines
            if line.startswith("#") or line.strip() == "":
                continue

            # parse and store this record to the ordered dict
            key, md5sum, size, *notes = line.strip("\n").split("\t")
            notes = notes[0] if notes else ""
            # make sure the key follows the naming convention
            validate_key(key)

            record = DataManifestRecord(
                key,
                md5sum,
                int(size),
                notes,
                path=self._build_checkout_path(self.checkout_prefix, key),
                remote_uri=self._build_remote_datastore_uri(
                    self.remote_datastore_uri, key, md5sum
                ),
            )
            if record.key in data:
                raise KeyAlreadyExistsError(
                    f"'{record.key}' is duplicated in '{self.fname}'"
                )
            data[record.key] = record

        self._fp.seek(0)

        return data

    def __init__(self, manifest_fname):
        """
        :param checkout_prefix: Path where files are located, e.g., /home/uname/projects/Ravel/data
        """

        # first open the manifest file and take out a lock
        self.fname = manifest_fname

        # open the manifest file, and take out a non-blocking shared lock. This guarantees that a
        # writer can't open the same file until this file is released (but other readers can)
        self._fp = open(manifest_fname, "r")
        try:
            fcntl.flock(self._fp, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(
                f"'{self.fname}' has an exclusive lock from another process and so it can't be opened for reading"
            )

        assert os.path.isfile(self.fname)

        # read the header and extract any config values (currently only the remote datastore)
        manifest_config, self.header, header_offset = self._load_header(
            self._fp
        )
        # find the remote data store prefix. We use passed argument, data manifest config value, and environment
        # variables in that order
        remote_datastore_uri = manifest_config.get("REMOTE_DATA_MIRROR_URI")
        if remote_datastore_uri is None:
            raise ValueError(
                "Must specify the remote_datastore_uri as a config option in the data manifest"
            )
        self.remote_datastore_uri = RemotePath.from_uri(remote_datastore_uri)

        self.local_cache_path_suffix = manifest_config["LOCAL_CACHE_PATH_SUFFIX"]

        # the local config file should be written by a call to checkout
        local_config = self._read_local_config(manifest_fname)
        # validate that manifest and local config versions match
        if manifest_config["MANIFEST_VERSION"] != local_config["MANIFEST_VERSION"]:
            raise ValueError(
                f"MANIFEST_VERSION mismatch between manifest file '{manifest_fname}' "
                f"('{manifest_config['MANIFEST_VERSION']}') and local config file "
                f"('{local_config['MANIFEST_VERSION']}')"
            )
        try:
            self.checkout_prefix = local_config['CHECKOUT_PREFIX']
        except KeyError:
            raise MissingLocalConfigError(f"CHECKOUT_PREFIX not present in the local config file '{self.local_config_path(manifest_fname)}\nHint: May need to checkout again.'")

        try:
            self.local_cache_prefix = local_config['LOCAL_CACHE_PREFIX']
        except KeyError:
            raise MissingLocalConfigError(f"LOCAL_CACHE_PREFIX not present in the local config file '{self.local_config_path(manifest_fname)}\nHint: May need to checkout again.'")

        self._data = self._read_records(header_offset)


    @staticmethod
    def _write_config(ofp, keys_and_values, prepend_hash):
        for key, val in keys_and_values.items():
            if "=" in key:
                raise ValueError(f"config key '{key}' contains a '='")
            if "=" in val:
                raise ValueError(f"config value '{val}' contains a '='")
            print(f"{'#' if prepend_hash else ''}{key}={val}", file=ofp)


    @staticmethod
    def _init_local_cache(local_cache_prefix, local_cache_path_suffix):
        # get local_cache_prefix from the environment if it wasn't passed in
        if local_cache_prefix is None:
            local_cache_prefix = os.environ.get("LOCAL_DATA_MIRROR_PATH", None)
        if local_cache_prefix is None:
            tmp_directory = tempfile.gettempdir()
            local_cache_prefix = os.path.abspath(os.path.join(tmp_directory, local_cache_path_suffix))

        if local_cache_prefix is None:
            raise ValueError(
                "Must either set 'local_cache_prefix', provide 'LOCAL_DATA_MIRROR_PATH' as an environment variable, or ensure LOCAL_CACHE_PATH_SUFFIX is set in the data manifest and that tempfile.gettempdir() returns a valid tmp path"
            )

        # validate the local cache prefix, creating it if necessary
        local_cache_prefix = os.path.abspath(local_cache_prefix)
        validate_local_prefix(local_cache_prefix)
        if not os.path.exists(local_cache_prefix):
            logger.info(
                f"The local cache prefix '{local_cache_prefix}' does not exist but we are creating it"
            )
            os.makedirs(
                local_cache_prefix, mode=DEFAULT_FOLDER_PERMISSIONS, exist_ok=True
            )
            # I don't think that this should be necessary, but I need this for the permissions to be correct
            # in the docker tests. Looks like there may be a bug with docker mounts.
            os.chmod(local_cache_prefix, DEFAULT_FOLDER_PERMISSIONS)

        assert os.path.isdir(local_cache_prefix)
        if _extract_permissions(local_cache_prefix) != DEFAULT_FOLDER_PERMISSIONS:
            raise ValueError(
                f"'Permissions of {local_cache_prefix} must be '{DEFAULT_FOLDER_PERMISSIONS}'"
            )

        return local_cache_prefix

    @staticmethod
    def _init_checkout_prefix(checkout_prefix):
        checkout_prefix = os.path.abspath(checkout_prefix)
        validate_local_prefix(checkout_prefix)

        if not os.path.exists(checkout_prefix):
            os.makedirs(checkout_prefix, exist_ok=True)
        assert os.path.isdir(checkout_prefix)

        return checkout_prefix

    @classmethod
    def checkout(cls, manifest_fname, checkout_prefix, local_cache_prefix=None, force=False):
        """Checkout a data manifest by writing the local config file or raising an error if it already exists.

        """
        with open(manifest_fname) as fp:
            manifest_config, _, _ = cls._load_header(fp)

        local_config_path = cls.local_config_path(manifest_fname)
        checkout_prefix = cls._init_checkout_prefix(checkout_prefix)
        local_cache_prefix = cls._init_local_cache(local_cache_prefix, manifest_config['LOCAL_CACHE_PATH_SUFFIX'])
        try:
            with open(local_config_path, ('w' if force else 'x')) as ofp:
                cls._write_config(
                    ofp,
                    {
                        'MANIFEST_VERSION': MANIFEST_VERSION,
                        'CHECKOUT_PREFIX': checkout_prefix,
                        'LOCAL_CACHE_PREFIX': local_cache_prefix,
                    },
                    prepend_hash=False
                )
        except:
            raise

        return cls(manifest_fname)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return (
            f"DataManifest(key='{self.key}', "
            f"fname='{self.fname}', "
            f"checkout_prefix='{self.checkout_prefix}', "
            f"local_cache_prefix='{self.local_cache_prefix}', "
            f"remote_datastore_uri='{self.remote_datastore_uri.uri}')"
        )

    def __contains__(self, key):
        return key in self._data

    def __len__(self):
        return len(self._data)

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def sync_and_get(self, key, fast=True) -> DataManifestRecord:
        self.sync_record(key, fast=fast)
        return self.get(key, validate=False)  # validate was done in the sync

    def get(self, key, validate=True) -> DataManifestRecord:
        if validate:
            self.validate_record(key, check_md5sum=False)
        return self._data[key]

    def __iter__(self):
        return iter(self.values())

    def sync_record(self, key, fast=False):
        logger.debug(f"Syncing '{key}'")
        # if the file doesn't exist, then add it
        path = self._data[key].path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        lock = lockfile.LockFile(path + ".sync.lock")
        with lock:
            if not os.path.exists(path):
                self._update_local_cache(key, fast=fast)
                self._update_local_checkout(key)
            # if it does exit, verify that it matches the manifest
            else:
                self.validate_record(key, check_md5sum=(not fast))
        return self._data[key]

    def sync(self, fast=False, progress_bar=False):
        """Sync the data manifest.

        If fast is set to True, then skip the md5sum check.
        """
        import concurrent.futures

        assert fast in [True, False]

        for key in tqdm(self.keys(), disable=not progress_bar):
            self.sync_record(key, fast=fast)
        return

        ## Parallel version -- pretty buggy
        # We can use a with statement to ensure threads are cleaned up promptly
        with tqdm(total=len(self.keys()), disable=not progress_bar) as pb:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # Start the load operations and mark each future with its URL
                future_to_key = {
                    executor.submit(sync_record, key): key for key in self.keys()
                }
                for future in concurrent.futures.as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        future.result()
                    except Exception as exc:
                        print("%r generated an exception: %s" % (key, exc))
                    else:
                        pb.update(1)

    def validate(self, fast=False):
        for key in self.keys():
            self.validate_record(key, check_md5sum=(not fast))

    def glob(self, pattern):
        return fnmatch.filter(self.keys(), pattern)

    def glob_records(self, pattern, validate=True):
        return [self.get(k, validate=validate) for k in self.glob(pattern)]


class DataManifestWriter(DataManifest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # open a non-blocking exclusive lock. This prevents any other process from reading or writing
        # this data manifest until we've closed the writer.
        fcntl.flock(self._fp, fcntl.LOCK_UN)
        self._fp.close()
        self._fp = open(self.fname, "r+")
        try:
            fcntl.flock(self._fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(
                f"'{self.fname}' has been opened by another process, and so it can't be opened for writing"
            )

    @classmethod
    def new(
        cls,
        manifest_fname,
        remote_datastore_uri,
        checkout_prefix=None,
        local_cache_prefix=None,
    ):
        """Create a new data manifest at manifest_fname.

        This creates a new empty data manifest, and returns the data manifest object opened in write mode.
        """
        if os.path.exists(manifest_fname):
            raise FileAlreadyExistsError(
                f"A data manifest already exists at {manifest_fname}."
            )

        # parse the remote datastore URI to extract bucket name
        remote_path = RemotePath.from_uri(remote_datastore_uri)
        # check that S3 versioning is enabled on the bucket
        # This will raise RuntimeError if versioning is not enabled or if we cannot check
        _check_s3_versioning_enabled(remote_path.bucket)

        # make any sub-directories needed to create the manifest
        basedir = os.path.dirname(manifest_fname)
        if basedir != '' and not os.path.exists(basedir):
            os.makedirs(basedir)

        # we use this suffix for any local cache. This allows us to default to using the tmp filesystem in
        # cases where local_cache_prefix isn't specified
        local_cache_path_suffix = f"./DATA_MANIFEST_CACHE_{random_string(16)}/"

        with open(manifest_fname, "x") as ofp:
            # write the remote datastore uri
            cls._write_config(
                ofp,
                {
                    'MANIFEST_VERSION': MANIFEST_VERSION,
                    'REMOTE_DATA_MIRROR_URI': remote_datastore_uri,
                    'LOCAL_CACHE_PATH_SUFFIX': local_cache_path_suffix,
                },

                prepend_hash=True
            )
            # write the header
            print("\t".join(cls.default_header()), file=ofp)

        cls.checkout(manifest_fname,  checkout_prefix, local_cache_prefix)

        return cls(manifest_fname)

    def _delete_from_s3_and_cache(self, key):
        """Delete a file from the local cache and from S3.

        We probably don't want to do this very often, but it can be useful if we make a mistake.
        """
        # delete from the local cache
        local_cache_path = self.get_local_cache_path(key)
        if os.path.exists(local_cache_path):
            assert os.path.isfile(local_cache_path)
            os.remove(local_cache_path)
            # recursively remove all empty directories below this until
            # we reach one without any files.
            dirname = local_cache_path
            while True:
                dirname, _ = os.path.split(dirname)
                # don't remove the local cache base directory
                if os.path.normpath(dirname) == os.path.normpath(
                    self.local_cache_prefix
                ):
                    break
                # remove directories until we find one that's not empty
                try:
                    logger.debug(f"Attempting to remove '{dirname}'")
                    os.rmdir(dirname)
                except OSError:
                    break

        # delete from s3
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self._data[key].remote_uri.bucket)
        remote_key = self._data[key].remote_uri.path
        remote_object = bucket.Object(remote_key)
        remote_object.delete()
        remote_object.wait_until_not_exists()

    def _upload_to_s3(self, key, fname_to_add):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self._data[key].remote_uri.bucket)
        remote_key = self._data[key].remote_uri.path
        logger.debug(f"Setting remote key to '{remote_key}'.")
        remote_object = bucket.Object(remote_key)
        try:
            remote_object.load()
        except botocore.exceptions.ClientError as inst:
            # if the object can't be found, then create it
            if inst.response["Error"]["Code"] == "404":
                remote_object.upload_file(fname_to_add)
                remote_object.wait_until_exists()
            # if there was a non-404 error, then re-raise the exception
            else:
                raise inst
        else:
            # note that remote_key contains the md5 sum of the file, so we know that
            # it's safe to use the existing version
            logger.warning(
                f"'{remote_key}' already exists in s3 -- using existing version."
            )

    def write_tsv(self, ofstream):
        self._write_config(
            ofstream,
            {
                'MANIFEST_VERSION': MANIFEST_VERSION,
                'REMOTE_DATA_MIRROR_URI': self.remote_datastore_uri.uri,
                'LOCAL_CACHE_PATH_SUFFIX': self.local_cache_path_suffix,
            },

            prepend_hash=True
        )
        ofstream.write("\t".join(self.header) + "\n")
        for record in self.values():
            # strip off the last two values (path and remote uri)
            ofstream.write(
                "\t".join(str(x) for x in dataclasses.astuple(record)[:-2]) + "\n"
            )

    def _save_to_disk(self):
        """Save the current data to disk."""
        # truncate the file, and re-write it
        self._fp.seek(0)
        backup = self._fp.read()
        try:
            self._fp.seek(0)
            self._fp.truncate()
            self.write_tsv(self._fp)
            self._fp.flush()
            os.fsync(self._fp)
        except Exception as inst:
            logger.error(
                "Exception raised during '_save_to_disk'. \n"
                "Attempting to restore original file, but data manifest may be corrupted. \n"
                "{}".format(inst)
            )
            self._fp.write(backup)
            self._fp.flush()
            os.fsync(self._fp)
            raise

    def _copy_local_file_to_local_cache(self, key, fname):
        """Copy a file into the local cache (to avoid downloading from s3 after an add or update, for example)"""
        # if the file already exists in the local cache, then verify it is the
        # same as the local file
        local_cache_path = self.get_local_cache_path(key)
        logger.info(f"Setting local cache path to '{local_cache_path}'.")

        # if local_path already exists, then make sure that it matches the remote file
        if os.path.exists(local_cache_path):
            logger.info(
                f"'{key}' already exists in the local mirror -- validating that it matches the manifest."
            )
            self._verify_record_matches_file(self._data[key], local_cache_path)
        else:
            os.makedirs(
                os.path.dirname(local_cache_path),
                mode=DEFAULT_FOLDER_PERMISSIONS,
                exist_ok=True,
            )
            shutil.copyfile(fname, local_cache_path)
            os.chmod(local_cache_path, DEFAULT_FILE_PERMISSIONS)

    def _add_or_update(self, key, fname_to_add, notes, is_update):
        """Add or update a file in the manifest.

        Add a file to the manifest and upload the file to GCS.
        """
        validate_key(key)

        if is_update:
            old_local_path = self._data[key].path
            if key not in self:
                raise ValueError(f"'{key}' is not present in '{self.fname}'")
        else:
            if key in self:
                raise KeyAlreadyExistsError(f"'{key}' is duplicated in '{self.fname}'")

        with open(fname_to_add) as _:  # noqa
            pass

        # add the data record into the object
        self._data[key] = self._build_new_data_manifest_record(key, fname_to_add, notes)
        # Add the file to the remote datastore
        self._upload_to_s3(key, fname_to_add)
        # Copy the file to the local cache
        self._copy_local_file_to_local_cache(key, fname_to_add)
        if is_update:
            # remove the symlink for the old file
            os.unlink(old_local_path)
        # TODO consider copying the local file rather than pulling from s3
        # Link the file from the local cache to the local datastore
        self._update_local_checkout(key)
        # update the data manifest on disk
        self._save_to_disk()

    def add(self, key, fname_to_add, notes="", exists_ok=False):
        """Add a file to the manifest and upload to gcp."""
        try:
            self._add_or_update(key, fname_to_add, notes, is_update=False)
        except KeyAlreadyExistsError:
            if not exists_ok:
                raise
            self._verify_record_matches_file(
                self._data[key], fname_to_add, check_md5sum=False
            )

    def update(self, key, fname_to_add, notes=""):
        """Update a file that is in the manifest and upload to gcp."""
        self._add_or_update(key, fname_to_add, notes, is_update=True)

    def delete(self, key, delete_from_datastore=False):
        """Remove a file from the manifest.

        Only set 'delete_from_datastore' if you *really* know what you're doing.
        """
        if key not in self:
            raise KeyError(f"'{key}' does not exist in '{self.fname}'")
        # remove the symlink
        if os.path.exists(self._data[key].path):
            assert os.path.islink(self._data[key].path)
            os.unlink(self._data[key].path)
        # remove the key from the datastore
        if delete_from_datastore:
            self._delete_from_s3_and_cache(key)
        del self._data[key]
        # write the updated manifest to disk
        self._save_to_disk()
