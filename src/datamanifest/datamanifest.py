import time
import random
import threading
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import botocore
import dataclasses
import fnmatch
import hashlib
import logging
import os
import re
import shutil
from pathlib import Path
import io
import string
import tempfile

from tqdm import tqdm
import urllib.request
import urllib.error
from urllib.parse import urlparse, parse_qs

from .config import (
    DEFAULT_FOLDER_PERMISSIONS,
    DEFAULT_FILE_PERMISSIONS,
    MANIFEST_VERSION,
    SUPPORTED_MANIFEST_VERSIONS,
)


logger = logging.getLogger(__name__)


def random_string(length):
    return "".join(
        [random.choice(string.ascii_letters + string.digits) for n in range(length)]
    )


def _check_s3_versioning_enabled(bucket_name: str) -> bool:
    """
    Check if S3 versioning is enabled on the specified bucket.

    Returns True if versioning is enabled.
    Raises RuntimeError if versioning is not enabled or if we cannot check (e.g., no permissions).
    """
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_bucket_versioning(Bucket=bucket_name)
        status = response.get("Status", "")
        if status == "Enabled":
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


class UnknownKeyError(KeyError):
    """Raised when requested keys are not present in the manifest."""
    pass


def _stream_md5(readable):
    """Compute MD5 hex digest by reading in chunks."""
    md5 = hashlib.md5()
    for chunk in iter(lambda: readable.read(8192), b""):
        md5.update(chunk)
    return md5.hexdigest()


def calc_md5sum_from_fname(fname):
    with open(fname, "rb") as f:
        return _stream_md5(f)


def calc_md5sum_from_remote_uri(remote_path):
    """Calculate MD5 checksum of a remote object.

    Args:
        remote_path: RemotePath object with scheme, bucket, path, and version_id
    """
    if not isinstance(remote_path, RemotePath):
        raise TypeError(f"Expected RemotePath, got {type(remote_path)}")
    if remote_path.scheme == "s3":
        if not remote_path.version_id:
            raise ValueError("RemotePath must have a version_id to calculate MD5 from remote")
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(remote_path.bucket)
        remote_object = bucket.Object(remote_path.path)
        with tempfile.NamedTemporaryFile("wb+") as fp:
            remote_object.download_fileobj(fp, ExtraArgs={'VersionId': remote_path.version_id})
            fp.seek(0)
            return _stream_md5(fp)
    elif remote_path.scheme in ("http", "https"):
        with urllib.request.urlopen(remote_path.uri, timeout=300) as resp:
            return _stream_md5(resp)
    else:
        raise ValueError(f"Unsupported scheme: {remote_path.scheme}")


def _normalize_http_etag(raw):
    """Normalize an HTTP ETag value: strip W/ prefix, then surrounding quotes."""
    if raw.startswith('W/'):
        raw = raw[2:]
    return raw.strip('"')


def _http_download_with_retry(url, dest_dir=None, desc='download', retries=3):
    """Download an HTTP(S) resource to a temp file with retry.

    Streams GET to a temp file while computing md5. Retries on 5xx/timeout.

    Args:
        url: HTTP(S) URL to download
        dest_dir: directory for temp file (None = system temp)
        desc: progress bar description
        retries: number of retry attempts

    Returns:
        dict with keys: md5sum, size, etag, local_path
    """
    last_error = None
    for attempt in range(retries):
        try:
            resp = urllib.request.urlopen(url, timeout=300)
            content_length = resp.headers.get('Content-Length')
            total = int(content_length) if content_length else None

            md5 = hashlib.md5()
            size = 0
            tmp_fd, tmp_path = tempfile.mkstemp(dir=dest_dir)
            try:
                with os.fdopen(tmp_fd, 'wb') as tmp_fp:
                    with tqdm(total=total, unit='B', unit_scale=True,
                              desc=desc, disable=total is None) as pbar:
                        while True:
                            chunk = resp.read(8192)
                            if not chunk:
                                break
                            tmp_fp.write(chunk)
                            md5.update(chunk)
                            size += len(chunk)
                            pbar.update(len(chunk))
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise

            raw_etag = resp.headers.get('ETag', '')
            etag = _normalize_http_etag(raw_etag) if raw_etag else ''

            return {
                "md5sum": md5.hexdigest(),
                "size": size,
                "etag": etag,
                "local_path": tmp_path,
            }
        except urllib.error.HTTPError as e:
            if e.code >= 500:
                last_error = e
                logger.warning(f"HTTP {e.code} for {url}, retry {attempt + 1}/{retries}")
                time.sleep(random.uniform(10, 60))
                continue
            raise
        except urllib.error.URLError as e:
            if attempt < retries - 1 and isinstance(e.reason, (TimeoutError, OSError)):
                last_error = e
                logger.warning(f"Network error for {url}: {e}, retry {attempt + 1}/{retries}")
                time.sleep(random.uniform(10, 60))
                continue
            raise

    raise last_error


def _download_http_to_file(url, local_cache_path, record, retries=3):
    """Download an HTTP resource to local_cache_path with md5 verification."""
    cache_dir = os.path.dirname(local_cache_path)
    meta = _http_download_with_retry(
        url, dest_dir=cache_dir, desc=os.path.basename(record.key), retries=retries
    )
    tmp_path = meta["local_path"]
    try:
        if record.md5sum and meta["md5sum"] != record.md5sum:
            raise FileMismatchError(
                f"MD5 mismatch for '{record.key}': "
                f"expected '{record.md5sum}', got '{meta['md5sum']}'. "
                f"The upstream HTTP resource may have changed."
            )
        os.rename(tmp_path, local_cache_path)
        os.chmod(local_cache_path, DEFAULT_FILE_PERMISSIONS)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


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


def _validate_tsv_safe(value, field_name):
    """Reject values containing characters that would corrupt TSV format."""
    if '\t' in value or '\n' in value or '\r' in value:
        raise ValueError(
            f"{field_name} contains characters that would corrupt the TSV format: {value!r}"
        )


def validate_local_prefix(prefix):
    _validate_prefix(prefix, InvalidPrefix)

    if prefix != os.path.abspath(prefix):
        raise InvalidPrefix(
            f'{prefix} is not an absolute path, try "{os.path.abspath(prefix)}"'
        )


def is_multipart_etag(etag: str) -> bool:
    """Return True if the ETag indicates a multipart upload."""
    return "-" in etag


@dataclasses.dataclass
class RemotePath:
    scheme: str
    bucket: str
    path: str
    version_id: str = ""
    _skip_validation: bool = dataclasses.field(default=False, repr=False, compare=False)

    @classmethod
    def from_uri(cls, uri, skip_validation=False):
        parsed_uri = urlparse(uri)
        if not skip_validation:
            # No params check here: urlparse only splits a ';params' segment off
            # the path for schemes in urllib.parse.uses_params, and 's3' is not
            # one of them, while __post_init__ rejects every non-s3 scheme. So
            # parsed_uri.params was always "" and the check could never fire; a
            # URI carrying ';params' keeps it in the path and is rejected by
            # prefix validation. See test_remote_path_from_uri_rejects_params.
            if parsed_uri.fragment:
                raise ValueError(f"Unexpected fragment in URI: {uri}")
        version_id = ""
        if parsed_uri.query:
            query_params = parse_qs(parsed_uri.query)
            if "versionId" in query_params:
                version_id = query_params["versionId"][0]
        return cls(
            parsed_uri.scheme,
            parsed_uri.netloc,
            parsed_uri.path.lstrip("/"),
            version_id,
            _skip_validation=skip_validation,
        )

    def __post_init__(self):
        if self.scheme not in ("s3", "http", "https"):
            raise ValueError(
                f"DataManifest currently only supports s3 and http(s) "
                f"for the remote cache (scheme={self.scheme})"
            )
        if not self._skip_validation:
            if self.scheme == "s3":
                _validate_prefix(self.path, InvalidPrefix)
            # HTTP paths are not subject to S3 path restrictions

    @property
    def uri(self):
        base = f"{self.scheme}://{self.bucket}/{self.path}"
        if self.version_id:
            return f"{base}?versionId={self.version_id}"
        return base


@dataclasses.dataclass
class DataManifestRecord:
    key: str
    md5sum: str
    s3_hash: str
    size: int
    notes: str
    path: str
    remote_uri: RemotePath
    source_uri: str = ""

    @property
    def is_external(self) -> bool:
        return bool(self.source_uri)

    @property
    def s3_version_id(self) -> str:
        """Get the S3 version ID from the remote URI."""
        return self.remote_uri.version_id

    @staticmethod
    def header() -> List[str]:
        return ["key", "s3_version_id", "md5sum", "s3_hash", "size", "source_uri", "notes", "path", "remote_uri"]


class DataManifest:
    @staticmethod
    def _get_s3_object_metadata(s3_uri: str) -> dict:
        """Call head_object on an S3 URI. Returns dict with etag, size, version_id, encryption, sse_customer_algorithm."""
        remote = RemotePath.from_uri(s3_uri, skip_validation=True)
        s3_client = boto3.client("s3")
        kwargs = {"Bucket": remote.bucket, "Key": remote.path}
        if remote.version_id:
            kwargs["VersionId"] = remote.version_id

        response = s3_client.head_object(**kwargs)

        version_id = response.get("VersionId", "")
        if version_id == "null":
            version_id = ""
        return {
            "etag": response["ETag"].strip('"'),
            "size": response["ContentLength"],
            "version_id": version_id,
            "encryption": response.get("ServerSideEncryption", ""),
            "sse_customer_algorithm": response.get("SSECustomerAlgorithm", ""),
        }

    def _build_new_data_manifest_record(self, key, fname_to_add, notes):
        _validate_tsv_safe(notes, "notes")
        # find the file's file size and calculate the checksum
        logger.info(f"Calculating md5sum for '{fname_to_add}'")
        md5sum = calc_md5sum_from_fname(fname_to_add)
        logger.info(f"Calculated md5sum '{md5sum}' for '{fname_to_add}'.")
        fsize = int(os.path.getsize(fname_to_add))
        logger.info(f"Calculated filesize '{fsize}' for '{fname_to_add}'.")

        return DataManifestRecord(
            key=key,
            md5sum=md5sum,
            s3_hash="",
            size=fsize,
            notes=notes,
            path=self._build_checkout_path(self.checkout_prefix, key),
            remote_uri=self._build_remote_datastore_uri(
                self.remote_datastore_uri, key
            ),
            source_uri="",
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
        if check_md5sum and record.md5sum:
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
            if self._data[key].is_external:
                raise MissingFileError(
                    f"External record '{key}' has not been synced yet. "
                    f"Run 'dm sync' to download the file before validating."
                )
            raise MissingFileError(f"Can not find '{key}' at '{local_abs_path}'")

        return self._verify_record_matches_file(
            self._data[key], local_abs_path, check_md5sum=check_md5sum
        )

    def _check_remote_etag(self, key):
        """For unversioned external records, verify the remote ETag hasn't changed.

        Versioned references (non-empty s3_version_id) are pinned to a specific
        object version, so the check is skipped for them.
        """
        record = self._data[key]
        if not record.is_external:
            return
        if record.s3_version_id:
            return

        if record.remote_uri.scheme == "s3":
            current_metadata = self._get_s3_object_metadata(record.remote_uri.uri)
            if current_metadata["etag"] != record.s3_hash:
                raise FileMismatchError(
                    f"Remote ETag for external record '{key}' has changed: "
                    f"expected '{record.s3_hash}', got '{current_metadata['etag']}'. "
                    f"The upstream file may have been replaced."
                )
        elif record.remote_uri.scheme in ("http", "https"):
            if not record.s3_hash:
                # No ETag stored — cannot do drift check via HEAD.
                # Full download + md5 verify will happen in _update_local_cache.
                return
            # HEAD request to check ETag
            req = urllib.request.Request(record.source_uri, method='HEAD')
            try:
                resp = urllib.request.urlopen(req, timeout=30)
            except (urllib.error.HTTPError, urllib.error.URLError) as e:
                # HEAD failure is non-fatal: log warning and let sync proceed
                logger.warning(f"HEAD request failed for {key}: {e}. Skipping ETag drift check.")
                return
            raw_etag = resp.headers.get('ETag', '')
            live_etag = _normalize_http_etag(raw_etag) if raw_etag else ''
            if live_etag and live_etag != record.s3_hash:
                logger.warning(
                    f"ETag changed for {key}: stored={record.s3_hash}, "
                    f"live={live_etag}. Will re-download and verify md5."
                )
                self._etag_drift_keys.add(key)

    def _update_local_cache(self, key, fast=False, retries=3, skip_remote_check=False):
        """Download key from the remote location to the local location."""
        # if the file already exists in the local cache, then verify it is the
        # same as the remote file
        local_cache_path = self.get_local_cache_path(key)
        logger.info(f"Setting local cache path to '{local_cache_path}'.")

        os.makedirs(
            os.path.dirname(local_cache_path), mode=DEFAULT_FOLDER_PERMISSIONS, exist_ok=True
        )

        # For unversioned external records, verify the remote hasn't been replaced
        if not skip_remote_check:
            self._check_remote_etag(key)

        # if local_path already exists and no ETag drift, validate cached file
        if os.path.exists(local_cache_path) and key not in self._etag_drift_keys:
            logger.debug(
                f"'{key}' already exists in the local cache -- validating that it matches the manifest."
            )
            self._verify_record_matches_file(
                self._data[key], local_cache_path, check_md5sum=not fast
            )
        else:
            record = self._data[key]
            if record.remote_uri.scheme in ("http", "https"):
                _download_http_to_file(
                    record.source_uri, local_cache_path, record, retries=retries
                )
                self._etag_drift_keys.discard(key)
            elif record.remote_uri.scheme == "s3":
                # download the file from S3 (using version ID)
                s3 = boto3.resource("s3")
                bucket = s3.Bucket(record.remote_uri.bucket)
                remote_key = record.remote_uri.path
                version_id = record.s3_version_id
                logger.info(f"Downloading '{remote_key}' (version: {version_id})")
                remote_object = bucket.Object(remote_key)
                if os.path.exists(local_cache_path):
                    raise RuntimeError(
                        f"local_cache_path '{local_cache_path}' already exists (this is unexpected)"
                    )
                extra_args = {'VersionId': version_id} if version_id else {}
                downloaded = False
                for rr in range(retries):
                    try:
                        remote_object.download_file(
                            str(local_cache_path),
                            ExtraArgs=extra_args
                        )
                        downloaded = True
                        break
                    except botocore.exceptions.ResponseStreamingError:
                        logger.error(
                            f"Error downloading '{remote_key}' to '{local_cache_path}'"
                            f"Retrying with retry number {rr+1} after a word from our sponsor..."
                        )
                        time.sleep(random.uniform(10, 60))

                if not downloaded:
                    raise RuntimeError(
                        f"Failed to download '{remote_key}' after {retries} retries"
                    )
                # set the permissions and group
                os.chmod(local_cache_path, DEFAULT_FILE_PERMISSIONS)
            else:
                raise ValueError(f"Unsupported scheme: {record.remote_uri.scheme}")

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
                if os.path.islink(old_local_cache_path):
                    raise RuntimeError(
                        f"Nested symlink detected at '{old_local_cache_path}' — "
                        "the data manifest should never create nested links"
                    )
                if old_local_cache_path != local_cache_path:
                    os.unlink(local_path)

            os.symlink(local_cache_path, local_path)

    @staticmethod
    def _build_datastore_suffix(key, file_hash):
        if not file_hash:
            raise ValueError(
                f"Cannot build cache path for key '{key}': no file hash available "
                "(both s3_hash and md5sum are empty)"
            )
        return os.path.join(
            os.path.dirname(key), f"./{file_hash}-" + os.path.basename(key)
        )

    @classmethod
    def _build_remote_datastore_uri(cls, remote_datastore_uri, key, version_id=""):
        """Build the remote S3 URI for a key.
        
        Note: S3 versioning is used instead of MD5 in the path.
        """
        return RemotePath(
            remote_datastore_uri.scheme,
            remote_datastore_uri.bucket,
            os.path.normpath(
                os.path.join(remote_datastore_uri.path, key)
            ),
            version_id,
        )

    def get_local_cache_path(self, key):
        record = self._data[key]
        file_hash = record.s3_hash if record.s3_hash else record.md5sum
        return os.path.normpath(
            os.path.join(
                self.local_cache_prefix,
                self._build_datastore_suffix(key, file_hash),
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
                    # use maxsplit=1 to handle values containing "="
                    parts = line.strip().split("=", maxsplit=1)
                    if len(parts) != 2:
                        raise ValueError(
                            f"Malformed config line {line_i + 1} in '{cls.local_config_path(manifest_path)}': "
                            f"expected 'KEY=VALUE' format, got '{line.strip()}'"
                        )
                    key, val = parts
                    config[key.strip()] = val.strip()
        except FileNotFoundError:
            raise MissingLocalConfigError(f"Could not find a local config file at '{cls.local_config_path(manifest_path)}'\nHint: You probably need to run checkout.")
        except OSError as e:
            raise MissingLocalConfigError(f"Could not read local config file at '{cls.local_config_path(manifest_path)}': {e}\nHint: You probably need to run checkout.")
        
        # validate version (after successfully reading the file)
        if "MANIFEST_VERSION" not in config:
            raise ValueError(
                f"MANIFEST_VERSION not found in local config file '{cls.local_config_path(manifest_path)}'"
            )
        if config["MANIFEST_VERSION"] not in SUPPORTED_MANIFEST_VERSIONS:
            raise ValueError(
                f"MANIFEST_VERSION mismatch in local config file '{cls.local_config_path(manifest_path)}': "
                f"expected one of {sorted(SUPPORTED_MANIFEST_VERSIONS)}, found '{config['MANIFEST_VERSION']}'"
            )
        return config

    @staticmethod
    def _load_header(fp):
        config = {}
        header = None
        line_i = -1
        for line_i, line in enumerate(fp):
            # skip empty lines
            if line.strip() == "":
                continue
            if line.startswith("#"):
                key, val = line[1:].strip().split("=", maxsplit=1)
                config[key.strip()] = val.strip()
            else:
                # assume that we're to the header now
                header = line.strip("\n").split("\t")
                break

        if header is None:
            raise ValueError("No header row found in data manifest file")

        # validate version
        if "MANIFEST_VERSION" not in config:
            raise ValueError(
                "MANIFEST_VERSION not found in data manifest header"
            )
        if config["MANIFEST_VERSION"] not in SUPPORTED_MANIFEST_VERSIONS:
            raise ValueError(
                f"MANIFEST_VERSION mismatch in data manifest: "
                f"expected one of {sorted(SUPPORTED_MANIFEST_VERSIONS)}, found '{config['MANIFEST_VERSION']}'"
            )

        fp.seek(0)
        return config, header, line_i

    def _read_records(self, header_offset):
        # read all of the file contents into memory
        data = {}
        is_v3 = len(self.header) >= len(self.default_header())
        for line_i, line in enumerate(self._fp):
            # skip until we are below the header
            if line_i <= header_offset:
                continue
            # skip commented and empty lines
            if line.startswith("#") or line.strip() == "":
                continue

            # parse and store this record to the ordered dict
            parts = line.strip("\n").split("\t")
            if is_v3:
                # v3 columns: key, s3_version_id, md5sum, s3_hash, size, source_uri, notes
                if len(parts) < 6:
                    raise ValueError(
                        f"Invalid record format in '{self.fname}' at line {line_i + 1}: "
                        f"expected at least 6 columns, got {len(parts)}"
                    )
                key = parts[0]
                s3_version_id = parts[1]
                md5sum = parts[2]
                s3_hash = parts[3]
                size = parts[4]
                source_uri = parts[5]
                notes = parts[6] if len(parts) > 6 else ""
            else:
                # v2 columns: key, s3_version_id, md5sum, size, notes
                if len(parts) < 4:
                    raise ValueError(
                        f"Invalid record format in '{self.fname}' at line {line_i + 1}: "
                        f"expected at least 4 columns (key, s3_version_id, md5sum, size), got {len(parts)}"
                    )
                key = parts[0]
                s3_version_id = parts[1]
                md5sum = parts[2]
                size = parts[3]
                notes = parts[4] if len(parts) > 4 else ""
                s3_hash = ""
                source_uri = ""

            # validate s3_version_id is not empty (only for regular records)
            if (not s3_version_id or not s3_version_id.strip()) and not source_uri:
                raise ValueError(
                    f"s3_version_id is required and cannot be empty for key '{key}' "
                    f"in '{self.fname}' at line {line_i + 1}"
                )

            # make sure the key follows the naming convention
            validate_key(key)

            # build remote_uri based on record type
            if source_uri:
                # External record: remote_uri from source_uri + version_id
                remote_uri = RemotePath.from_uri(source_uri, skip_validation=True)
                if remote_uri.version_id:
                    raise ValueError(
                        f"source_uri for key '{key}' contains '?versionId=...' at line {line_i + 1}. "
                        "Version IDs must be stored in the s3_version_id column, not embedded in source_uri."
                    )
                if s3_version_id:
                    remote_uri = dataclasses.replace(remote_uri, version_id=s3_version_id)
            else:
                # Regular record: remote_uri from manifest base + key
                remote_uri = self._build_remote_datastore_uri(
                    self.remote_datastore_uri, key, s3_version_id
                )

            record = DataManifestRecord(
                key=key,
                md5sum=md5sum,
                s3_hash=s3_hash,
                size=int(size),
                notes=notes,
                path=self._build_checkout_path(self.checkout_prefix, key),
                remote_uri=remote_uri,
                source_uri=source_uri,
            )
            if record.key in data:
                raise KeyAlreadyExistsError(
                    f"'{record.key}' is duplicated in '{self.fname}'"
                )
            data[record.key] = record

        self._fp.seek(0)

        return data

    def __init__(self, manifest_fname):
        self.fname = manifest_fname
        self._fp = open(manifest_fname, "r")

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
        try:
            self.checkout_prefix = local_config['CHECKOUT_PREFIX']
        except KeyError:
            raise MissingLocalConfigError(f"CHECKOUT_PREFIX not present in the local config file '{self.local_config_path(manifest_fname)}\nHint: May need to checkout again.'")

        try:
            self.local_cache_prefix = local_config['LOCAL_CACHE_PREFIX']
        except KeyError:
            raise MissingLocalConfigError(f"LOCAL_CACHE_PREFIX not present in the local config file '{self.local_config_path(manifest_fname)}\nHint: May need to checkout again.'")

        self._etag_drift_keys = set()
        self._data = self._read_records(header_offset)


    @staticmethod
    def _write_config(ofp, keys_and_values, prepend_hash):
        for key, val in keys_and_values.items():
            if "=" in key:
                raise ValueError(f"config key '{key}' contains a '='")
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
        return local_cache_prefix

    @staticmethod
    def _init_checkout_prefix(checkout_prefix):
        checkout_prefix = os.path.abspath(checkout_prefix)
        validate_local_prefix(checkout_prefix)

        if not os.path.exists(checkout_prefix):
            os.makedirs(checkout_prefix, exist_ok=True)

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

        return cls(manifest_fname)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return (
            f"DataManifest(fname='{self.fname}', "
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

    def sync_and_get(self, key, fast=True, skip_remote_check=False) -> DataManifestRecord:
        self.sync_record(key, fast=fast, skip_remote_check=skip_remote_check)
        return self.get(key, validate=False)  # validate was done in the sync

    def get(self, key, validate=True) -> DataManifestRecord:
        if validate:
            self.validate_record(key, check_md5sum=False)
        return self._data[key]

    def __iter__(self):
        return iter(self.values())

    def sync_record(self, key, fast=False, skip_remote_check=False):
        logger.debug(f"Syncing '{key}'")
        # if the file doesn't exist, then add it
        path = self._data[key].path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            self._update_local_cache(key, fast=fast, skip_remote_check=skip_remote_check)
            self._update_local_checkout(key)
        # if it does exist, verify that it matches the manifest
        else:
            # For already-synced external records, still check remote ETag for drift
            if not skip_remote_check:
                self._check_remote_etag(key)
            self.validate_record(key, check_md5sum=(not fast))
        return self._data[key]

    def sync(self, *, keys=None, fast=False, progress_bar=False, skip_remote_check=False, max_workers=8):
        """Sync data manifest records to local cache and checkout.

        Args:
            keys: If provided, sync only these keys. If None, sync all keys.
                An empty list is an explicit no-op (no error, no downloads).
                Keys not present in the manifest raise UnknownKeyError.
            fast: Skip md5sum verification (size-only check).
            progress_bar: Show tqdm progress bar.
            skip_remote_check: Skip ETag drift detection for externals.
            max_workers: Thread pool size for parallel downloads.

        Raises:
            UnknownKeyError: If any key in `keys` is not in the manifest.
                Raised before any downloads start, listing all unknown keys.
            RuntimeError: If any record fails to sync (after attempting all).

        Note:
            sync(keys=[]) is an explicit no-op — no error, no downloads.
            This is distinct from sync_prefix/sync_glob, where an empty
            match raises ValueError (a pattern matching nothing is likely
            a typo). The asymmetry is intentional.
        """
        if keys is None:
            target_keys = list(self.keys())
        else:
            target_keys = list(keys)
            unknown = set(target_keys) - set(self._data.keys())
            if unknown:
                sample = sorted(unknown)[:10]
                msg = f"{len(unknown)} key(s) not found in manifest '{self.fname}': {sample}"
                if len(unknown) > 10:
                    msg += f" (and {len(unknown) - 10} more)"
                raise UnknownKeyError(msg)

        if not target_keys:
            return

        errors = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.sync_record, key, fast=fast, skip_remote_check=skip_remote_check): key
                for key in target_keys
            }
            with tqdm(total=len(target_keys), disable=not progress_bar) as pbar:
                for future in as_completed(futures):
                    key = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        errors.append((key, e))
                    pbar.update(1)
        if errors:
            error_details = "\n".join(f"  {key}: {type(e).__name__}: {e}" for key, e in errors)
            raise RuntimeError(
                f"Failed to sync {len(errors)} of {len(target_keys)} record(s):\n{error_details}"
            )

    def sync_prefix(self, prefix, *, fast=False, progress_bar=False, skip_remote_check=False, max_workers=8):
        """Sync all keys starting with `prefix`.

        Uses str.startswith (NOT glob(prefix + "*")). Returns the list of
        matched keys.

        Args:
            prefix: Literal prefix string. Must be non-empty.

        Returns:
            List of matched keys.

        Raises:
            ValueError: If prefix is empty or no keys match.
            RuntimeError: If any matched record fails to sync.
        """
        matched = self.find_prefix(prefix)  # raises ValueError on empty prefix
        if not matched:
            raise ValueError(
                f"No keys match prefix '{prefix}' in manifest '{self.fname}' "
                f"({len(self._data)} total keys)"
            )
        self.sync(
            keys=matched, fast=fast, progress_bar=progress_bar,
            skip_remote_check=skip_remote_check, max_workers=max_workers,
        )
        return matched

    def sync_glob(self, pattern, *, fast=False, progress_bar=False, skip_remote_check=False, max_workers=8):
        """Sync all keys matching an fnmatch glob pattern.

        Equivalent to self.sync(keys=self.glob(pattern), ...).
        Returns the list of matched keys.

        Raises:
            ValueError: If the pattern matches zero keys (likely a typo).
            RuntimeError: If any matched record fails to sync.
        """
        matched = self.glob(pattern)
        if not matched:
            raise ValueError(
                f"No keys match pattern '{pattern}' in manifest '{self.fname}' "
                f"({len(self._data)} total keys)"
            )
        self.sync(
            keys=matched, fast=fast, progress_bar=progress_bar,
            skip_remote_check=skip_remote_check, max_workers=max_workers,
        )
        return matched

    def validate(self, fast=False):
        for key in self.keys():
            self.validate_record(key, check_md5sum=(not fast))

    def glob(self, pattern):
        return fnmatch.filter(self.keys(), pattern)

    def glob_records(self, pattern, validate=True):
        return [self.get(k, validate=validate) for k in self.glob(pattern)]

    def find_prefix(self, prefix):
        """Return all keys starting with `prefix` (literal str.startswith filter).

        This is NOT equivalent to glob(prefix + "*"). The two are kept separate
        so that a future change to glob()'s semantics cannot silently alter
        find_prefix().

        Args:
            prefix: Literal prefix string. Must be non-empty.

        Returns:
            List of matching keys.

        Raises:
            ValueError: If prefix is empty.
        """
        if not prefix:
            raise ValueError(
                "prefix must be non-empty (empty prefix matches all keys "
                "via str.startswith — almost certainly a bug)"
            )
        return [k for k in self.keys() if k.startswith(prefix)]


class DataManifestWriter(DataManifest):
    @staticmethod
    def _parse_manifest_records(content):
        """Parse manifest TSV content into a dict of key -> (version_id, md5sum, s3_hash, size, source_uri).

        Lightweight parser for conflict detection — does not require full instantiation.
        """
        records = {}
        header_seen = False
        for line in content.splitlines():
            line = line.strip('\n\r')
            if not line.strip() or line.startswith('#'):
                continue
            if not header_seen:
                header_seen = True
                continue
            parts = line.split('\t')
            if len(parts) < 4:
                continue
            key = parts[0]
            if len(parts) >= 6:
                records[key] = (parts[1], parts[2], parts[3], parts[4], parts[5])
            else:
                records[key] = (parts[1], parts[2], "", parts[3], "")
        return records

    def _record_to_tuple(self, record):
        return (record.s3_version_id, record.md5sum, record.s3_hash, str(record.size), record.source_uri)

    _save_counter = 0
    _save_counter_lock = threading.Lock()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._save_lock = threading.Lock()

        # Snapshot file content at open time for conflict detection
        self._fp.seek(0)
        self._last_known_content = self._fp.read()
        self._fp.seek(0)

        # Upgrade v2 header to v3 if needed
        if len(self.header) < len(self.default_header()):
            self.header = self.default_header()

        # Reopen in read-write mode for _save_to_disk
        self._fp.close()
        self._fp = open(self.fname, "r+")

    def sync_record(self, key, fast=False, skip_remote_check=False):
        """Sync a record with md5sum backfill for external records.

        After downloading an external file with empty md5sum, computes the
        content MD5 from the local file and persists it in the manifest.
        """
        record = super().sync_record(key, fast=fast, skip_remote_check=skip_remote_check)
        if record.is_external and not record.md5sum:
            local_cache_path = self.get_local_cache_path(key)
            if os.path.exists(local_cache_path):
                computed_md5 = calc_md5sum_from_fname(local_cache_path)
                self._data[key] = dataclasses.replace(record, md5sum=computed_md5)
                self._save_to_disk()
                logger.info(f"Backfilled md5sum '{computed_md5}' for external record '{key}'")
        return self._data[key]

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
        record = self._data[key]
        if record.is_external:
            raise ValueError(
                "Cannot delete external S3 objects from the datastore. "
                "External records reference objects we do not own. "
                "Use delete(key) without delete_from_datastore=True to remove the reference only."
            )
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

        # delete from s3 (deletes the specific version)
        s3_client = boto3.client("s3")
        bucket_name = self._data[key].remote_uri.bucket
        remote_key = self._data[key].remote_uri.path
        version_id = self._data[key].s3_version_id
        logger.debug(f"Deleting s3://{bucket_name}/{remote_key} (version: {version_id})")
        delete_kwargs = {"Bucket": bucket_name, "Key": remote_key}
        if version_id:
            delete_kwargs["VersionId"] = version_id
        s3_client.delete_object(**delete_kwargs)

    def _upload_to_s3(self, key, fname_to_add):
        """Upload file to S3 and return the version ID and ETag.

        Returns:
            tuple: (version_id, etag) where version_id is the S3 version ID
                and etag is the S3 ETag (quotes stripped).

        Raises:
            RuntimeError: If the bucket doesn't have versioning enabled (no version ID returned).
        """
        s3_client = boto3.client("s3")
        bucket_name = self._data[key].remote_uri.bucket
        remote_key = self._data[key].remote_uri.path
        logger.debug(f"Uploading to s3://{bucket_name}/{remote_key}")
        
        with open(fname_to_add, 'rb') as f:
            response = s3_client.put_object(
                Bucket=bucket_name,
                Key=remote_key,
                Body=f
            )
        
        version_id = response.get('VersionId')
        if not version_id:
            raise RuntimeError(
                f"S3 bucket '{bucket_name}' did not return a version ID. "
                f"Ensure versioning is enabled on the bucket."
            )
        
        etag = response["ETag"].strip('"')
        logger.debug(f"Uploaded '{remote_key}' with version ID '{version_id}', ETag '{etag}'")
        return version_id, etag

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
            row = [
                record.key,
                record.s3_version_id,
                record.md5sum,
                record.s3_hash,
                str(record.size),
                record.source_uri,
                record.notes,
            ]
            ofstream.write("\t".join(row) + "\n")

    def _save_to_disk(self):
        """Save the current data to disk atomically with conflict detection.

        Thread-safe: acquires _save_lock internally.

        1. Write new content to a temp file
        2. Backup current manifest to .bak.{unique_id}
        3. Atomic rename temp -> manifest
        4. Verify backup: check if another writer modified the file since we
           last read it. If so, rename backup to .CONFLICT.{unique_id} and raise.
        5. If clean, delete the backup.
        """
        with self._save_lock:
            self._save_to_disk_unlocked()

    def _save_to_disk_unlocked(self):
        """Internal save implementation. Must be called with _save_lock held."""
        dir_name = os.path.dirname(os.path.abspath(self.fname))
        with DataManifestWriter._save_counter_lock:
            DataManifestWriter._save_counter += 1
            counter = DataManifestWriter._save_counter
        unique_id = f"{int(time.time())}_{os.getpid()}_{counter}"
        bak_path = f"{self.fname}.bak.{unique_id}"
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as tmp_fp:
                self.write_tsv(tmp_fp)
                tmp_fp.flush()
                os.fsync(tmp_fp.fileno())
            shutil.copy2(self.fname, bak_path)
            os.rename(tmp_path, self.fname)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        # Reopen the renamed file for future writes
        self._fp.close()
        self._fp = open(self.fname, "r+")

        # Conflict detection: compare backup against last known state
        with open(bak_path) as f:
            bak_content = f.read()

        if bak_content != self._last_known_content:
            conflicts = self._detect_conflicts(bak_content)
            if conflicts:
                conflict_path = f"{self.fname}.CONFLICT.{unique_id}"
                os.rename(bak_path, conflict_path)
                raise RuntimeError(
                    f"CONFLICT DETECTED: Another writer modified '{self.fname}' "
                    f"while this DataManifestWriter held it open.\n"
                    f"The other writer's version has been saved to:\n"
                    f"  {conflict_path}\n"
                    f"Your changes have been written to the manifest, but the "
                    f"following data from the other writer may have been lost:\n"
                    + "\n".join(f"  - {c}" for c in conflicts)
                )

        # No conflicts — delete the backup
        os.unlink(bak_path)

        # Update last known content from in-memory state (not disk, to avoid race)
        sio = io.StringIO()
        self.write_tsv(sio)
        self._last_known_content = sio.getvalue()

    def _detect_conflicts(self, bak_content):
        """Compare backup content against our state to find lost data.

        Checks both directions:
        - Keys in backup but not in our data (other writer added them)
        - Keys in original but not in backup (other writer deleted them)
        - Keys modified by other writer vs. our modifications

        Returns a list of conflict descriptions, or empty list if clean.
        """
        bak_records = self._parse_manifest_records(bak_content)
        orig_records = self._parse_manifest_records(self._last_known_content)
        current_keys = set(self._data.keys())
        conflicts = []

        for key, bak_rec in bak_records.items():
            if key not in current_keys:
                # Key is in backup but not in our data
                if key not in orig_records:
                    # We never had it — another writer added it, we're losing it
                    conflicts.append(f"Key '{key}' was added by another writer and would be lost")
                # else: was in original and we deleted it — expected
            else:
                # Key exists in both backup and our data
                our_rec = self._record_to_tuple(self._data[key])
                orig_rec = orig_records.get(key)
                if bak_rec != orig_rec:
                    # Another writer modified this record
                    if our_rec != orig_rec:
                        conflicts.append(f"Key '{key}' was modified by both this writer and another writer")
                    else:
                        conflicts.append(f"Key '{key}' was modified by another writer and that change would be reverted")

        # Reverse check: keys that were in the original but deleted by another writer
        for key in orig_records:
            if key not in bak_records and key in current_keys:
                # Another writer deleted this key, but we still have it — our save would revert that delete
                our_rec = self._record_to_tuple(self._data[key])
                orig_rec = orig_records[key]
                if our_rec == orig_rec:
                    # We didn't modify it, so the other writer's delete is being silently reverted
                    conflicts.append(f"Key '{key}' was deleted by another writer and that deletion would be reverted")

        return conflicts

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

        Add a file to the manifest and upload the file to S3.
        """
        validate_key(key)

        if is_update and key in self._data and self._data[key].is_external:
            raise ValueError(
                "External S3 references are immutable. "
                "Delete and re-add instead."
            )

        if is_update:
            old_local_path = self._data[key].path
            if key not in self:
                raise ValueError(f"'{key}' is not present in '{self.fname}'")
        else:
            if key in self:
                raise KeyAlreadyExistsError(f"'{key}' is duplicated in '{self.fname}'")

        # add the data record into the object (version_id will be set after upload)
        self._data[key] = self._build_new_data_manifest_record(key, fname_to_add, notes)
        # Add the file to the remote datastore and get the version ID
        version_id, etag = self._upload_to_s3(key, fname_to_add)
        # Update the record's remote_uri with the version ID and s3_hash with the ETag
        old_remote_uri = self._data[key].remote_uri
        new_remote_uri = dataclasses.replace(old_remote_uri, version_id=version_id)
        self._data[key] = dataclasses.replace(self._data[key], remote_uri=new_remote_uri, s3_hash=etag)
        # Copy the file to the local cache
        self._copy_local_file_to_local_cache(key, fname_to_add)
        if is_update:
            # remove the symlink for the old file
            os.unlink(old_local_path)
        # Link the file from the local cache to the local datastore
        self._update_local_checkout(key)
        # update the data manifest on disk
        self._save_to_disk()

    def add(self, key, fname_to_add, notes="", exists_ok=False):
        """Add a file to the manifest and upload to S3."""
        try:
            self._add_or_update(key, fname_to_add, notes, is_update=False)
        except KeyAlreadyExistsError:
            if not exists_ok:
                raise
            if self._data[key].is_external:
                raise ValueError(
                    f"Key '{key}' exists as an external reference. "
                    "Cannot use exists_ok=True with external records. "
                    "Delete and re-add instead."
                )
            self._verify_record_matches_file(
                self._data[key], fname_to_add, check_md5sum=False
            )

    def add_external(self, key, uri=None, notes="", **kwargs):
        """Add an external resource to the manifest.

        Supports S3 (s3://), HTTP (http://), and HTTPS (https://) URIs.
        For S3: performs HEAD to capture metadata (no download).
        For HTTP(S): downloads the full file to compute md5 content pin.
        """
        # Backward compatibility: accept s3_uri= as keyword arg
        if uri is None and "s3_uri" in kwargs:
            import warnings
            warnings.warn(
                "add_external(s3_uri=...) is deprecated, use uri=... instead",
                DeprecationWarning, stacklevel=2,
            )
            uri = kwargs.pop("s3_uri")
        if uri is None:
            raise TypeError("add_external() missing required argument: 'uri'")

        validate_key(key)
        _validate_tsv_safe(notes, "notes")
        if key in self._data:
            raise KeyAlreadyExistsError(f"Key '{key}' already exists in '{self.fname}'. Delete first to re-add.")

        parsed = urlparse(uri)
        if parsed.scheme == "s3":
            metadata = self._get_s3_object_metadata(uri)
            etag = metadata["etag"]
            size = metadata["size"]
            version_id = metadata["version_id"]

            encryption = metadata["encryption"]
            is_opaque_etag = (
                is_multipart_etag(etag)
                or encryption in ("aws:kms", "aws:kms:dkek")
                or bool(metadata.get("sse_customer_algorithm"))
            )
            md5sum = "" if is_opaque_etag else etag

            remote_parsed = RemotePath.from_uri(uri, skip_validation=True)
            source_uri = f"s3://{remote_parsed.bucket}/{remote_parsed.path}"

            _validate_tsv_safe(source_uri, "source_uri")

            remote_uri = RemotePath.from_uri(source_uri, skip_validation=True)
            if version_id:
                remote_uri = dataclasses.replace(remote_uri, version_id=version_id)

            record = DataManifestRecord(
                key=key,
                md5sum=md5sum,
                s3_hash=etag,
                size=size,
                notes=notes,
                path=self._build_checkout_path(self.checkout_prefix, key),
                remote_uri=remote_uri,
                source_uri=source_uri,
            )

            self._data[key] = record
            self._save_to_disk()

        elif parsed.scheme in ("http", "https"):
            _validate_tsv_safe(uri, "source_uri")
            meta = _http_download_with_retry(
                uri, desc=os.path.basename(urlparse(uri).path) or key
            )
            try:
                record = DataManifestRecord(
                    key=key,
                    md5sum=meta["md5sum"],
                    s3_hash=meta["etag"],
                    size=meta["size"],
                    notes=notes,
                    path=self._build_checkout_path(self.checkout_prefix, key),
                    remote_uri=RemotePath.from_uri(uri, skip_validation=True),
                    source_uri=uri,
                )

                # Store the record first: get_local_cache_path() and
                # _update_local_checkout() both look up self._data[key].
                self._data[key] = record

                # Move downloaded file from temp to local cache
                cache_path = self.get_local_cache_path(key)
                os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                shutil.move(meta["local_path"], cache_path)
                os.chmod(cache_path, DEFAULT_FILE_PERMISSIONS)
                self._update_local_checkout(key)
                self._save_to_disk()
            except Exception:
                # Clean up temp file if anything fails after download
                if os.path.exists(meta["local_path"]):
                    os.unlink(meta["local_path"])
                raise
        else:
            raise ValueError(f"Unsupported URI scheme: {parsed.scheme}")

    def update(self, key, fname_to_add, notes=""):
        """Update a file that is in the manifest and upload to S3."""
        self._add_or_update(key, fname_to_add, notes, is_update=True)

    def delete(self, key, delete_from_datastore=False):
        """Remove a file from the manifest.

        Only set 'delete_from_datastore' if you *really* know what you're doing.
        """
        if key not in self:
            raise KeyError(f"'{key}' does not exist in '{self.fname}'")
        record = self._data[key]
        if delete_from_datastore and record.is_external:
            raise ValueError(
                "Cannot delete external S3 objects from the datastore. "
                "External records reference objects we do not own. "
                "Use delete(key) without delete_from_datastore=True to remove the reference only."
            )
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
