# Design: HTTP/HTTPS External Resources for datamanifest

**Status:** IMPLEMENTED — shipped in v1.2.0 (merge `2d0c714`, 2026-08-20).
Design review A- (2 rounds), implementation review A-, test audit found 0 wrong
assertions. 107 tests pass on master. Implementation matched this design with no
recorded divergences. Full gate log in `COORDINATION.http-external.md`.

**Deliberately not covered here:** archive-to-mirror backup, deferred to
`docs/pending/phase2_external_archive_backup.md`.

## Summary

This design extends `datamanifest`'s external resource support from S3-only to include `http://` and `https://` URIs. Public bioinformatics databases (JASPAR, HOCOMOCO, GENCODE) publish data over HTTP only, currently forcing users to mirror files to S3 or hardcode local paths. The change adds an `add-url` CLI command and extends the Python API so that `add_external()` accepts HTTP(S) URIs, downloads the file at add-time to compute an authoritative MD5 pin, and records it as an immutable external record in the existing v3 manifest format. No new dependencies are introduced; all HTTP operations use stdlib `urllib.request`. The manifest schema is unchanged — older datamanifest versions will fail clearly at `RemotePath.__post_init__` (L226) when encountering an `http`/`https` scheme, rather than silently mishandling the record.

## Affected Code

### `src/datamanifest/datamanifest.py`

| Function / Class | Line | Change |
|---|---|---|
| `RemotePath.__post_init__` | L226-230 | Relax scheme validation to accept `http` and `https` in addition to `s3` |
| `RemotePath.from_uri` | L201 | No change needed — `urlparse` already handles HTTP URIs correctly; `bucket` maps to netloc, `path` maps to URL path |
| `RemotePath.uri` property | L234 | No change needed — reconstructs `scheme://bucket/path` which produces valid HTTP URLs |
| `_get_s3_object_metadata` | L269 | Add an HTTP-aware sibling function (see below) |
| `add_external` | L1224 | Branch on URI scheme: S3 path unchanged, HTTP path calls new `_get_http_resource_metadata()` which downloads the file, computes md5, and captures ETag/size |
| `_update_local_cache` | L392-473 | Branch on `remote_uri.scheme`: existing S3 boto3 path for `s3`, new streaming HTTP download path for `http`/`https` |
| `_check_remote_etag` | L373-390 | Branch on scheme: S3 path unchanged, HTTP path issues a HEAD request comparing stored ETag (if any) |
| `calc_md5sum_from_remote_uri` | L134-151 | Add HTTP branch: streaming GET into tempfile with `hashlib.md5()` |
| `_build_new_data_manifest_record` | L290 | No change — only called for internal (owned) resources |
| `_upload_to_s3` | L1061 | No change — HTTP externals are never uploaded |
| `_update_local_checkout` | L475-503 | No change — symlink logic is protocol-agnostic |
| `_read_records` | L633 | No change — `source_uri` field already stores the full URI; `RemotePath.from_uri` will parse HTTP URIs once scheme validation is relaxed |
| `get_local_cache_path` | L531-539 | No change — uses `s3_hash or md5sum` for cache key; HTTP records will always have `md5sum` populated at add-time |
| `_verify_record_matches_file` | L320-348 | No change — md5/size checks are protocol-agnostic |
| `sync_record` (Writer) | L949 | No change — md5 backfill logic for opaque ETags applies identically; HTTP records with md5 already populated skip backfill |
| `is_multipart_etag` | L188-190 | No change — HTTP ETags containing `-` are handled the same way (treated as opaque) |

### `src/datamanifest/main.py`

| Function | Line | Change |
|---|---|---|
| `parse_args` | L155-225 | Add `add-url` subcommand parser |
| `main` | L248-294 | Add dispatch for `add-url` → `add_url_main()` |
| *(new)* `add_url_main` | — | Thin wrapper calling `DataManifestWriter.add_external(key, url, notes)` |

### `tests/test_datamanifest.py`

New test functions added (see Testing Plan below). No changes to existing tests.

### `pyproject.toml`

No change — `urllib.request` is stdlib. No new dependencies.

## Design Decisions

### 1. How to handle `RemotePath` for HTTP URLs

**Decision:** Extend `RemotePath` minimally — relax `__post_init__` to accept `http`/`https` schemes. Do not refactor or replace the class. Add a docstring documenting the dual semantics of `bucket`.

**Rationale:** The research (Summary C, "Option C") recommends keeping `source_uri` as the canonical string and branching on scheme in the three affected functions (`_get_s3_object_metadata`, `_update_local_cache`, `_check_remote_etag`). `RemotePath`'s field layout (`scheme`/`bucket`/`path`/`version_id`) maps naturally to HTTP: `bucket` = netloc (hostname + optional port), `path` = URL path, `version_id` = empty string. `RemotePath.from_uri` (L201) already uses `urlparse` internally, so HTTP URIs parse correctly without modification. The `uri` property (L234) reconstructs `scheme://bucket/path` which produces valid HTTP URLs. A deeper refactor (renaming `bucket` to `netloc`, introducing a protocol-dispatch ABC) would have high blast radius for no functional benefit.

**Important:** The `RemotePath` class docstring must be updated to document that `bucket` means "S3 bucket name" for `s3://` URIs and "hostname[:port]" for `http(s)://` URIs. Code that accesses `remote_uri.bucket` must always check `remote_uri.scheme` first — passing a hostname to `s3.Bucket()` would be a silent bug.

**The only code change in `RemotePath`** is in `__post_init__` (L226-230): replace the hard-coded `scheme == "s3"` check with `scheme in ("s3", "http", "https")`. This also serves as the backward-compatibility guard — older versions that still enforce `s3`-only will raise a clear `ValueError` when reading a manifest containing HTTP external records.

`version_id` will always be empty for HTTP records, since HTTP has no native object versioning.

### 2. `urllib.request` vs `requests` (dependency policy)

**Decision:** Use stdlib `urllib.request` exclusively. Do not add `requests` or `httpx` as a dependency.

**Rationale:** The requirements document explicitly mandates this: "no new dependencies — must use stdlib `urllib.request`". The current dependency list (`boto3`, `lockfile`, `tqdm`) is minimal by design, and the package must work in constrained environments (containers, AWS Batch) where installing extra packages is costly. `urllib.request` provides everything needed: `urlopen()` for streaming GET, `Request` for HEAD with custom headers, redirect following (enabled by default via `HTTPRedirectHandler`), and access to response headers for `Content-Length` and `ETag`.

**Implementation notes:**
- Streaming downloads: `urllib.request.urlopen(url)` returns a file-like object supporting `.read(chunk_size)`.
- HEAD requests: `urllib.request.Request(url, method='HEAD')`.
- Redirects: `urllib.request` follows redirects by default (up to a reasonable limit). The *original* user-provided URL is stored in `source_uri`, not the final redirected URL.
- Error handling: catch `urllib.error.HTTPError` (for 4xx/5xx) and `urllib.error.URLError` (for DNS/connection failures).
- Timeouts: pass `timeout=` to `urlopen()` to prevent hangs on unresponsive servers.

### 3. CLI surface: new `add-url` command

**Decision:** Add a new `add-url` subcommand. Do not rename or modify `add-s3`.

**Rationale:** Following the existing naming convention (kebab-case subcommands: `add-s3`, `add-multiple`), a new `add-url` command is the cleanest approach. Renaming `add-s3` to `add-external` with scheme auto-detection would break backward compatibility for scripts that invoke `dm add-s3`. The two commands have different semantics at add-time (S3: HEAD-only metadata fetch; HTTP: full download + md5 computation), so separate commands communicate intent clearly.

**CLI signature:**
```
dm add-url <manifest-path> <key> <url> [--notes NOTES]
```

Arguments mirror `add-s3` (L207-214): positional `manifest-path`, `key`, `url`; optional `--notes`.

### 4. Cache path strategy for HTTP resources

**Decision:** Use the existing cache-path scheme (`<md5sum>-<basename>`) with `md5sum` as the hash component. No changes to `get_local_cache_path` (L531-539) or `_build_datastore_suffix` (L505-514).

**Rationale:** The cache path is computed as `os.path.join(dirname(key), f"./{file_hash}-{basename(key)}")` where `file_hash = record.s3_hash or record.md5sum` (L531-539). For HTTP records, `md5sum` is always populated at add-time (it is the authoritative content pin). `s3_hash` (ETag) may or may not be present depending on the server. Two scenarios:

1. **Server provides ETag:** `s3_hash` is populated, cache path uses ETag as the hash component (consistent with S3 behavior). If content changes and md5 verification fails during sync, `FileMismatchError` is raised.
2. **Server provides no ETag:** `s3_hash` is empty, cache path uses `md5sum` as the hash component. This is the same fallback used for S3 multipart/KMS-encrypted objects whose ETags are opaque (L188-190).

The "chicken-and-egg" problem (needing to download before knowing the cache key) is resolved by the requirement to download at add-time: by the time the record is created, md5sum is known and the cache path is deterministic.

### 5. Handling servers without ETag

**Decision:** ETag is captured opportunistically but is not required. The authoritative integrity check is always md5sum content verification.

**Rationale:** HTTP ETags are far less standardized than S3 ETags. Many servers omit them entirely; others use weak ETags (`W/"..."`) that indicate semantic equivalence rather than byte-identity. The design handles all cases:

| Server behavior | `s3_hash` field | Drift detection during sync |
|---|---|---|
| Strong ETag present | Stored | HEAD first: matching ETag + cached file exists → skip download. Mismatched ETag → download + md5 verify. |
| Weak ETag present | Stored (stripped of `W/` prefix) | Same as strong, but ETag mismatch is expected more often. md5 is the authoritative check. |
| No ETag | Empty string | No HEAD optimization possible. Always download + md5 verify (unless already cached and `skip_remote_check=True`). |

This matches the existing pattern for S3 externals with opaque ETags — md5 is backfilled on first sync (L949) and becomes the ground truth. The difference is that HTTP records have md5 populated from the start (add-time download), so there is no backfill step.

### 6. Handling redirects

**Decision:** Follow redirects transparently during download. Store the *original* user-provided URL as `source_uri`, not the final resolved URL.

**Rationale:** Many bioinformatics databases use URL shorteners, CDN redirects, or versioned path redirections (e.g., GENCODE release URLs redirect to specific file servers). Storing the original URL:
- Preserves user intent and readability in the manifest.
- Avoids brittleness if CDN endpoints change but the canonical URL remains stable.
- `urllib.request` follows redirects by default, so no special handling is needed.

HEAD requests for drift detection also follow redirects, ensuring consistency.

### 7. Download-at-add-time semantics

**Decision:** `add_external()` for HTTP URIs downloads the full file at add-time, computes md5, and stores the file in the local cache. This differs from S3 external behavior (HEAD-only at add-time).

**Rationale:** HTTP lacks reliable server-side metadata for integrity:
- `Content-Length` is unreliable ("servers lie" — per requirements).
- ETags are optional and may not reflect content identity.
- There is no equivalent to S3's `version_id` for pinning.

The only reliable way to establish a content pin is to download and hash the file. This also eliminates the md5 backfill complexity that exists for S3 externals with opaque ETags. The downloaded file is retained in the local cache, so `sync()` can serve it immediately without re-downloading.

**Trade-off:** `add-url` is slower than `add-s3` for large files (full download vs HEAD request). This is acceptable because:
- The operation is infrequent (one-time registration).
- Correctness requires it (no other reliable content-pinning mechanism).
- The alternative (trusting Content-Length + ETag) would produce records that could fail on first sync.

## Implementation Plan

### Phase 1: Core library changes (`datamanifest.py`)

#### Step 1: Relax `RemotePath` scheme validation

Modify `RemotePath.__post_init__` (L226-232) to accept `http` and `https`:

```python
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
```

Note: the scheme check runs unconditionally (as in the current code), regardless of `_skip_validation` — only the *path* validation is skippable. `_validate_prefix` (L154) is S3-specific: it enforces a restrictive alphanumeric/`,_-/.`-only regex and requires a normalized path, both of which are inappropriate for HTTP URL paths (which may contain `%`, `?`, `=`, `~`, and other characters). The `_validate_prefix` call must remain gated to `scheme == "s3"` only — it must not be silently dropped for all schemes.

#### Step 2: Add HTTP metadata fetcher

Add a new function `_get_http_resource_metadata(url)` alongside `_get_s3_object_metadata` (L269). This function:

1. Opens a streaming GET to `url` via `urllib.request.urlopen()`.
2. Reads the response in chunks, writing to a `NamedTemporaryFile` while computing `hashlib.md5()` incrementally and tracking byte count.
3. Extracts `ETag` and `Content-Length` from response headers (opportunistic, not required). The `ETag` value is normalized before storage: strip a leading `W/` (weak-validator prefix) first, *then* strip surrounding quotes. The HTTP spec format is `W/"value"` (prefix outside quotes), so the `W/` prefix must be removed before quote-stripping to avoid leaving a stray leading quote. The normalization logic is:
   ```python
   raw = resp.headers.get('ETag', '')
   if raw.startswith('W/'):
       raw = raw[2:]
   etag = raw.strip('"')
   ```
   Normalizing at add-time ensures the value stored in `s3_hash` is directly comparable to the normalized value computed during `_check_remote_etag` drift checks (Step 5), rather than comparing a raw/quoted value against a stripped one.
4. Returns a dict: `{"md5sum": hex_digest, "size": actual_byte_count, "etag": etag_or_empty, "local_path": temp_file_path}`.
5. Wraps errors in clear exceptions: `urllib.error.HTTPError` for 4xx/5xx, `urllib.error.URLError` for network failures.
6. Implements retry with jittered backoff for 5xx and timeout errors (matching the S3 retry pattern at L453-466: up to 3 retries, `time.sleep(random.uniform(10, 60))`).
7. Uses `tqdm` for progress bar (consistent with existing S3 download UX), using `Content-Length` for total if available.

#### Step 3: Extend `add_external` to handle HTTP URIs

Modify `DataManifestWriter.add_external` (L1224) to branch on URI scheme. The parameter is renamed from `s3_uri` to `uri` to reflect that it now accepts multiple schemes, with backward compatibility via `**kwargs`:

```python
def add_external(self, key, uri=None, notes="", **kwargs):
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
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        # existing S3 external logic (unchanged)
        ...
    elif parsed.scheme in ("http", "https"):
        # new HTTP external logic
        _validate_tsv_safe(uri, "source_uri")
        meta = _get_http_resource_metadata(uri)
        record = DataManifestRecord(
            key=key,
            md5sum=meta["md5sum"],
            s3_hash=meta["etag"],
            size=meta["size"],
            notes=notes,
            path=self._build_checkout_path(self.checkout_prefix, key),
            remote_uri=RemotePath.from_uri(uri, skip_validation=True),
            source_uri=uri,
            # version_id will be empty since HTTP URL has no versionId query parameter
        )

        # Store the record first: get_local_cache_path() and
        # _update_local_checkout() both look up self._data[key].
        self._data[key] = record

        # Move downloaded file from temp to local cache
        cache_path = self.get_local_cache_path(key)  # uses md5sum since s3_hash may be empty
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        shutil.move(meta["local_path"], cache_path)
        self._update_local_checkout(key)
        self._save_to_disk()
    else:
        raise ValueError(f"Unsupported URI scheme: {parsed.scheme}")
```

`path=self._build_checkout_path(...)` is required — `DataManifestRecord.path` has no default (L249), and the existing S3 external path builds it the same way (L1259).

`RemotePath.from_uri(uri, skip_validation=True)` mirrors the existing S3 external path (L1244): `skip_validation=True` is required because HTTP URL paths can contain characters (`%`, `?`, `=`, `~`, etc.) that `_validate_prefix` rejects — see Step 1 note above.

`_validate_tsv_safe(uri, "source_uri")` mirrors the existing S3 external path (L1247), which validates `source_uri` before it is written to the TSV.

Key differences from the S3 path:
- Downloads the full file (not just HEAD).
- `md5sum` is populated immediately (no backfill needed).
- `version_id` (derived from `remote_uri`) is always empty.
- The downloaded file is moved directly into the local cache.
- `self._data[key] = record` must happen *before* `get_local_cache_path(key)` is called, since that method reads `self._data[key]` (L532) to determine the hash component of the cache path.
- `_update_local_checkout(key)` takes only `key` (L475) — it derives both the cache path and checkout path internally via `self._data[key]`, not a separate `cache_path` argument.

**Error handling:** `_get_http_resource_metadata(uri)` performs the download and is expected to raise on failure. `urllib.error.HTTPError` (4xx/5xx) and `urllib.error.URLError` (DNS/connection failures) should be allowed to propagate out of `add_external` with a clear error message (e.g. including the URI and status code), rather than being silently swallowed. Optionally these can be wrapped in a datamanifest-specific exception (e.g. `ExternalResourceFetchError`) for a more consistent error surface, but propagating the underlying `urllib.error` is acceptable for v1.

#### Step 4: Add HTTP download path to `_update_local_cache`

Modify `_update_local_cache` (L392-473) to branch on `remote_uri.scheme`:

```python
def _update_local_cache(self, key, ...):
    record = self._data[key]
    local_cache_path = self.get_local_cache_path(key)

    if os.path.exists(local_cache_path):
        return  # already cached

    if record.remote_uri.scheme == "s3":
        # existing boto3 download logic (L441-471, unchanged)
        ...
    elif record.remote_uri.scheme in ("http", "https"):
        _download_http_to_file(record.source_uri, local_cache_path, record)
    else:
        raise ValueError(f"Unsupported scheme: {record.remote_uri.scheme}")
```

Note: this is simplified for illustration. The actual `_update_local_cache` (L432-438) does not just check `os.path.exists` and return — when the cache file already exists it calls `self._verify_record_matches_file(self._data[key], local_cache_path, check_md5sum=not fast)` to validate the cached file's size/md5 against the record before trusting it. This existing integrity check must be preserved for the HTTP branch as well, not bypassed by an early return.

The new `_download_http_to_file` helper:
1. Streams GET to a temp file in the same directory (atomic rename pattern, matching existing S3 temp-file logic).
2. Computes md5 during download.
3. Verifies md5 matches `record.md5sum` — raises `FileMismatchError` on mismatch (upstream content mutation detected).
4. Renames temp file to `local_cache_path` on success.
5. Retries on 5xx/timeout with jittered backoff (3 attempts).
6. Shows `tqdm` progress bar.

#### Step 5: Add HTTP branch to `_check_remote_etag`

Modify `_check_remote_etag` (L373-390) to handle HTTP:

```python
def _check_remote_etag(self, key):
    record = self._data[key]
    if not record.is_external:
        return
    if record.remote_uri.scheme == "s3":
        # existing S3 HEAD logic (unchanged)
        ...
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
            # to download + md5 verify (the authoritative check).
            logger.warning(f"HEAD request failed for {key}: {e}. Skipping ETag drift check.")
            return
        raw_etag = resp.headers.get('ETag', '')
        if raw_etag.startswith('W/'):
            raw_etag = raw_etag[2:]
        live_etag = raw_etag.strip('"')
        if live_etag and live_etag != record.s3_hash:
            # ETag changed — flag for re-download, but don't error yet.
            # md5 verification after download is the authoritative check.
            #
            # IMPORTANT: do NOT delete the existing cached file here. If the
            # re-download fails (network error, server down), we'd lose the
            # only good copy. Instead, set a flag so _update_local_cache
            # downloads to a temp file and replaces the cache only after
            # md5 verification succeeds.
            logger.warning(
                f"ETag changed for {key}: stored={record.s3_hash}, "
                f"live={live_etag}. Will re-download and verify md5."
            )
            self._etag_drift_keys.add(key)  # checked by _update_local_cache
```

The `_etag_drift_keys` set (initialized in `__init__`) tracks keys whose ETag has changed. `_update_local_cache` checks this set: if the key is present, it downloads to a temp file and verifies md5 before replacing the cache — preserving the existing cache if the download fails or md5 doesn't match.

Semantics differ from S3: an ETag mismatch for HTTP is a *warning* triggering re-download, not an immediate error. The authoritative check is md5 verification after download. If the md5 also mismatches, `_download_http_to_file` raises `FileMismatchError` and the original cached file is preserved.

#### Step 6: Extend `calc_md5sum_from_remote_uri`

The current signature (L134) is `calc_md5sum_from_remote_uri(remote_path)`, where `remote_path` is a `RemotePath` object (not a URI string) — it asserts `isinstance(remote_path, RemotePath)` and requires `remote_path.version_id` for the S3 case. Callers (including `tests/test_datamanifest.py`) pass `record.remote_uri` directly. To avoid an undocumented signature/type change, keep the parameter as a `RemotePath` and branch on `remote_path.scheme` rather than re-parsing a URI string:

```python
def calc_md5sum_from_remote_uri(remote_path):
    assert isinstance(remote_path, RemotePath)
    if remote_path.scheme == "s3":
        # existing boto3 download-and-hash logic (unchanged, requires version_id)
        ...
    elif remote_path.scheme in ("http", "https"):
        md5 = hashlib.md5()
        with urllib.request.urlopen(remote_path.uri, timeout=300) as resp:
            while chunk := resp.read(8192):
                md5.update(chunk)
        return md5.hexdigest()
    else:
        raise ValueError(f"Unsupported scheme: {remote_path.scheme}")
```

This preserves the existing call sites unchanged — only the branch inside the function is new.

### Phase 2: CLI changes (`main.py`)

#### Step 7: Add `add-url` subcommand

In `parse_args()` (L155-225), add after the `add-s3` block (L207-214):

```python
add_url_parser = subparsers.add_parser("add-url", help="Add an HTTP/HTTPS URL as an external resource")
add_url_parser.add_argument("manifest-path")
add_url_parser.add_argument("key", help="Key for the resource in the manifest")
add_url_parser.add_argument("url", help="HTTP or HTTPS URL to the resource")
add_url_parser.add_argument("--notes", default="", help="Optional notes")
```

In `main()` dispatch (L248-294), add:

```python
elif args.command == "add-url":
    add_url_main(
        getattr(args, "manifest-path"),
        args.key,
        args.url,
        args.notes,
    )
```

#### Step 8: Add `add_url_main` function

Following the pattern of `add_s3_main` (L119-121):

```python
def add_url_main(manifest_fname, key, url, notes):
    dm = DataManifestWriter(manifest_fname)
    dm.add_external(key, url, notes=notes)
    record = dm.get(key)
    print(f"Added {key}: md5={record.md5sum} size={record.size} etag={record.s3_hash or '(none)'}")
```

The print statement fulfills the success-criteria requirement of printing md5/size/ETag on success.

## API Examples

### Python API

```python
from datamanifest import DataManifestWriter

# Add an HTTP external resource (downloads file, computes md5)
dm = DataManifestWriter("manifest.tsv")
dm.add_external(
    key="motifs/JASPAR2024_CORE_vertebrates.pfm",
    uri="https://jaspar.elixir.no/download/data/2024/CORE/JASPAR2024_CORE_vertebrates_non-redundant_pfms_jaspar.txt",
    notes="JASPAR 2024 core vertebrate PFMs"
)

# Retrieve the record
record = dm.get("motifs/JASPAR2024_CORE_vertebrates.pfm")
print(record.md5sum)      # "a1b2c3d4..."  — computed at add-time
print(record.source_uri)   # "https://jaspar.elixir.no/download/..."
print(record.is_external)  # True
print(record.s3_hash)      # ETag if server provided one, else ""

# Sync and get local path (downloads if not cached, verifies md5)
record = dm.sync_and_get("motifs/JASPAR2024_CORE_vertebrates.pfm")
print(record.path)  # "/path/to/checkout/motifs/JASPAR2024_CORE_vertebrates.pfm" (symlink)

# External records are immutable — these raise ValueError:
dm.update("motifs/JASPAR2024_CORE_vertebrates.pfm", "new_file.txt")      # ValueError
dm.delete("motifs/JASPAR2024_CORE_vertebrates.pfm", delete_from_datastore=True)  # ValueError
```

### CLI

```bash
# Add an HTTP external resource
dm add-url manifest.tsv \
    motifs/JASPAR2024_CORE_vertebrates.pfm \
    "https://jaspar.elixir.no/download/data/2024/CORE/JASPAR2024_CORE_vertebrates_non-redundant_pfms_jaspar.txt" \
    --notes "JASPAR 2024 core vertebrate PFMs"
# Output: Added motifs/JASPAR2024_CORE_vertebrates.pfm: md5=a1b2c3d4... size=1234567 etag=...

# Sync all records (HTTP externals verify md5 on download)
dm sync manifest.tsv

# Existing commands unchanged
dm add-s3 manifest.tsv ref/gencode.gtf s3://bucket/gencode.gtf?versionId=abc123
```

### Manifest TSV (after adding HTTP external)

```tsv
#VERSION=3
#REMOTE_DATASTORE_URI=s3://my-bucket/data
key	s3_version_id	md5sum	s3_hash	size	source_uri	notes
motifs/JASPAR2024.pfm		a1b2c3d4e5f6...	"etag-value"	1234567	https://jaspar.elixir.no/download/...	JASPAR 2024
ref/existing_s3.bed	ver123	abcdef012345...	abcdef012345...-1	5678	s3://other-bucket/file.bed	S3 external
```

Note: `s3_version_id` is empty for HTTP records. `md5sum` is always populated (not deferred). `s3_hash` contains the HTTP ETag if the server provided one.

## Testing Plan

### Unit Tests (mocked HTTP, no network)

These tests mock `urllib.request.urlopen` using `unittest.mock.patch`, following the existing pattern for boto3 mocking (L1644, L1718 in `test_datamanifest.py`).

1. **`test_add_external_http_basic`** — Mock `urlopen` returning a response with known content, Content-Length, and ETag headers. Verify:
   - Record is created with correct `md5sum`, `size`, `s3_hash`, `source_uri`.
   - `is_external` returns `True`.
   - `s3_version_id` is empty.
   - File exists in local cache with correct content.
   - Checkout symlink points to cache.

2. **`test_add_external_http_no_etag`** — Mock response without ETag header. Verify:
   - `s3_hash` is empty string.
   - `md5sum` and `size` are still correctly populated.
   - Cache path uses `md5sum` as the hash component.

3. **`test_add_external_http_weak_etag`** — Mock response with `W/"abc123"` weak ETag. Verify `s3_hash` stores the stripped value.

4. **`test_add_external_http_duplicate_key`** — Verify `KeyAlreadyExistsError` on duplicate key (same as S3 external behavior).

5. **`test_add_external_http_immutable`** — Verify `update()` raises `ValueError` on HTTP external key. Verify `delete(key, delete_from_datastore=True)` raises `ValueError`.

6. **`test_sync_http_external_md5_match`** — Remove cached file, mock `urlopen` for sync download, verify download succeeds and md5 matches.

7. **`test_sync_http_external_md5_mismatch`** — Mock `urlopen` returning different content than what was recorded. Verify `FileMismatchError` is raised.

8. **`test_check_remote_etag_http_changed`** — Mock HEAD response with changed ETag. Verify cache is invalidated and re-download is triggered.

9. **`test_check_remote_etag_http_no_etag_stored`** — Record has no `s3_hash`. Verify `_check_remote_etag` returns without error (no drift check possible).

10. **`test_http_download_retry_on_5xx`** — Mock `urlopen` raising `HTTPError(500)` twice then succeeding. Verify retry behavior and eventual success.

11. **`test_http_download_network_error`** — Mock `urlopen` raising `URLError`. Verify clear error message.

12. **`test_remote_path_accepts_http`** — Verify `RemotePath.from_uri("https://example.com/file.txt")` succeeds, with `scheme="https"`, `bucket="example.com"`, `path="/file.txt"`, `version_id=""`.

13. **`test_remote_path_rejects_ftp`** — Verify `RemotePath.from_uri("ftp://example.com/file.txt")` raises `ValueError`.

14. **`test_read_records_roundtrip_http`** — Write a manifest with an HTTP external record, re-open it, verify all fields are preserved correctly through TSV serialization/deserialization.

### Integration Tests (real HTTP, network required)

Following the project convention of testing against real infrastructure (like the S3 tests using `S3_TEST_BUCKET`), add a small number of integration tests that fetch real public URLs:

15. **`test_add_url_integration`** — Use a small, stable public file (e.g., a JASPAR matrix file or a small GENCODE annotation). Verify full add-url → sync → get cycle. Mark with `@pytest.mark.network` so it can be skipped in CI environments without network access.

### CLI Tests

16. **`test_cli_add_url`** — Invoke `dm add-url` via subprocess or by calling `main()` directly with mocked HTTP. Verify exit code and output format.

### Mock Strategy

```python
# Example mock pattern (following existing conventions at L1644):
from unittest.mock import patch, MagicMock
import io

def _mock_http_response(content, etag=None, content_length=None):
    """Create a mock urllib response."""
    resp = MagicMock()
    resp.read = io.BytesIO(content).read
    resp.headers = http.client.HTTPMessage()
    if etag:
        resp.headers['ETag'] = f'"{etag}"'
    if content_length is not None:
        resp.headers['Content-Length'] = str(content_length)
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp

@patch("urllib.request.urlopen")
def test_add_external_http_basic(mock_urlopen, manifest_fname):
    content = b"test file content"
    mock_urlopen.return_value = _mock_http_response(
        content, etag="abc123", content_length=len(content)
    )
    dm = DataManifestWriter(manifest_fname)
    dm.add_external("test/file.txt", "https://example.com/file.txt")
    record = dm.get("test/file.txt")
    assert record.is_external
    assert record.md5sum == hashlib.md5(content).hexdigest()
    assert record.s3_hash == "abc123"
    assert record.source_uri == "https://example.com/file.txt"
```

Note: mock response headers should use `http.client.HTTPMessage()` (or `MagicMock(spec=http.client.HTTPMessage)`) rather than a plain `dict`, since real `urllib` response headers are case-insensitive (`resp.headers.get('etag')` and `resp.headers.get('ETag')` are equivalent) — a plain dict mock would not catch a case-sensitivity bug in the implementation.

## Risks and Mitigations

### 1. Upstream content mutation (silent data corruption)

**Risk:** HTTP resources can change without notice — no versioning, ETags optional, Content-Length unreliable. A sync could download different content than what was originally registered.

**Mitigation:** md5 content pinning at add-time is the authoritative integrity check. Every sync download verifies the computed md5 against the stored `md5sum`. Mismatch raises `FileMismatchError` with a clear message indicating the upstream resource has changed. This is strictly stronger than the S3 external pattern (which may defer md5 computation for opaque ETags).

### 2. URL impermanence (broken links)

**Risk:** HTTP URLs can go offline, return 404, or change structure over time. Unlike S3 (where the bucket is under organizational control), public HTTP URLs are outside the user's control.

**Mitigation:** Download-at-add-time ensures the file is immediately cached locally. Once synced, the local cache serves the file without network access. The requirements document notes that a Phase 2 "archive-to-mirror-bucket" feature could copy HTTP externals to S3 for long-term durability, but this is out of scope.

### 3. Large file downloads at add-time

**Risk:** `add_external` for HTTP must download the entire file to compute md5, which could be slow or fail for very large files (multi-GB reference genomes).

**Mitigation:**
- `tqdm` progress bar provides UX feedback during download.
- Streaming download with temp-file-then-rename prevents partial/corrupt cache entries.
- Retry with jittered backoff handles transient network failures (3 attempts, matching S3 retry logic at L453-466).
- Timeout parameter on `urlopen()` prevents indefinite hangs.
- The download artifact is retained in the local cache, so sync never re-downloads unless the cache is invalidated.

### 4. `urllib.request` limitations vs `requests`

**Risk:** `urllib.request` has a less ergonomic API than `requests` for streaming, error handling, and header parsing.

**Mitigation:** The HTTP operations needed are straightforward (GET, HEAD, read headers). A small set of helper functions (`_get_http_resource_metadata`, `_download_http_to_file`) encapsulates the `urllib` details, keeping the rest of the codebase clean. If `urllib` proves insufficient for a future phase (e.g., HTTP auth, proxy support), `requests` can be added then.

### 5. Backward compatibility (older datamanifest versions)

**Risk:** An older `datamanifest` version reading a manifest with HTTP external records could behave unexpectedly.

**Mitigation:** The existing `RemotePath.__post_init__` (L226) hard-codes `scheme == "s3"` and raises `ValueError` for anything else. This means older versions will fail fast with a clear error message when encountering an HTTP `source_uri` — they cannot silently misinterpret the record. The manifest schema version remains v3 (no column changes), so the TSV structure is readable; only the URI scheme in `source_uri` triggers the failure. This is the ideal backward-compatibility behavior: fail loudly, fail early.

### 6. HTTP-specific edge cases

**Risk:** Various HTTP servers behave differently — chunked transfer encoding (no Content-Length), compressed responses (Content-Length doesn't match decompressed size), connection resets mid-download.

**Mitigation:**
- Size is computed from actual bytes read, not `Content-Length` header (per requirements: "servers lie").
- `urllib.request` handles chunked transfer encoding transparently.
- For compressed responses: `urlopen` does NOT set `Accept-Encoding: gzip` by default, so responses are typically uncompressed. If a server forces compression, the md5 is computed on the decompressed content (what `read()` returns), which is the correct behavior — the md5 pins the logical content, not the wire encoding.
- Connection resets during download are caught by the retry logic; partial temp files are cleaned up.
