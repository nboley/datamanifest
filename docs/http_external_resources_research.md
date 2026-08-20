# Research: HTTP/HTTPS URL Support for External Resources

## Summary

Adding HTTP/HTTPS support to `datamanifest`'s external resources feature is **moderate** in complexity. The current external resource system has a relatively clean separation from the "managed S3" path, and most changes would be confined to 2-3 functions in a single file. However, the `RemotePath` dataclass is deeply coupled to S3 semantics, which creates the biggest friction.

---

## 1. Current External Resource Implementation

### How `add_external()` works

**File:** `src/datamanifest/datamanifest.py:1224-1265`

```python
def add_external(self, key, s3_uri, notes=""):
```

Steps:
1. Validates key and checks for duplicates
2. Calls `_get_s3_object_metadata(s3_uri)` (line 1231) — performs `head_object` via boto3 to get ETag, size, version_id, encryption info
3. Determines if ETag is "opaque" (multipart upload, KMS-encrypted, or SSE-C) — if opaque, md5sum is left empty
4. Constructs `source_uri` as `s3://{bucket}/{path}` (strips any `?versionId=`)
5. Builds a `RemotePath` from `source_uri` with `skip_validation=True`
6. Attaches `version_id` to the `RemotePath` if one was returned by HEAD
7. Creates a `DataManifestRecord` and saves to disk — **no download occurs**

**S3 API calls made:** `s3_client.head_object(Bucket=..., Key=..., [VersionId=...])` — exactly one call.

### How `sync_record()` handles external records during download

**File:** `src/datamanifest/datamanifest.py:889-905` (reader) and `949-963` (writer)

The sync path is **identical for external and regular records**. The `_update_local_cache()` method (line 392-473) performs the download:

1. Takes a lock on the local cache path
2. Optionally calls `_check_remote_etag(key)` for unversioned external records (line 429)
3. If file doesn't exist locally, downloads via boto3:
   ```python
   s3 = boto3.resource("s3")
   bucket = s3.Bucket(self._data[key].remote_uri.bucket)
   remote_object = bucket.Object(remote_key)
   remote_object.download_file(str(local_cache_path), ExtraArgs=extra_args)
   ```
4. Then `_update_local_checkout()` creates a symlink from checkout path to cache

**Key insight:** The download mechanism uses `record.remote_uri.bucket` and `record.remote_uri.path` directly with boto3. This is the primary blocker for HTTP support.

### How external records are validated (ETag checks)

**File:** `src/datamanifest/datamanifest.py:373-390`

```python
def _check_remote_etag(self, key):
```

- Only runs for external records (`record.is_external`)
- Skipped if record has a `s3_version_id` (versioned references are pinned)
- Calls `_get_s3_object_metadata(record.remote_uri.uri)` and compares ETag to stored `s3_hash`
- Raises `FileMismatchError` if they differ

### Assumptions about `source_uri` being S3

The `source_uri` field is:
- **Stored** as a plain string in `DataManifestRecord` (line 251)
- **Parsed** via `RemotePath.from_uri(source_uri, skip_validation=True)` during `_read_records()` (line 689)
- The `RemotePath.__post_init__` (line 226-232) enforces `self.scheme != "s3"` raises ValueError — but **only when `skip_validation=False`**
- When `skip_validation=True`, the scheme check is bypassed, BUT the resulting `RemotePath` object still has `.bucket` and `.path` attributes that are used with boto3

### Does external skip versioning?

**No — external records DO participate in versioning when available.** The `add_external()` method captures the `version_id` from `head_object` response (line 1234). If the source bucket has versioning enabled, the reference is pinned to a specific version. If versioning is not enabled on the source bucket, `version_id` will be empty, and the ETag-based drift detection (`_check_remote_etag`) kicks in instead.

The user's assumption that external records "aren't versioned" is only partially correct: they aren't versioned *by datamanifest* (no upload to the managed bucket), but they CAN be pinned to a specific S3 version of the source object.

---

## 2. What Would Need to Change for HTTP Support

### Every place `source_uri` is parsed or used

| Location | Line | What it does | HTTP impact |
|----------|------|--------------|-------------|
| `_read_records()` | 689 | `RemotePath.from_uri(source_uri, skip_validation=True)` | **BLOCKER** — `RemotePath` is S3-specific |
| `add_external()` | 1231 | `_get_s3_object_metadata(s3_uri)` | **BLOCKER** — uses boto3 `head_object` |
| `add_external()` | 1244-1249 | Parses URI, constructs `source_uri` string | Needs HTTP path |
| `_update_local_cache()` | 441-458 | Downloads via `s3.Bucket(...).Object(...).download_file()` | **BLOCKER** — needs HTTP download |
| `_check_remote_etag()` | 384 | Calls `_get_s3_object_metadata(record.remote_uri.uri)` | **BLOCKER** — needs HTTP HEAD |
| `get_local_cache_path()` | 531-539 | Uses `record.s3_hash` or `record.md5sum` for cache path | Works if we provide one of these |
| `write_tsv()` | 1113 | Writes `record.source_uri` as string | Works as-is |
| `DataManifestRecord.is_external` | 254-255 | `bool(self.source_uri)` | Works as-is |

### Functions that assume S3 vs could work with HTTP

**Must change:**
1. `_get_s3_object_metadata()` (line 269-288) — entirely boto3-based
2. `_update_local_cache()` (line 392-473) — download logic is boto3
3. `_check_remote_etag()` (line 373-390) — calls `_get_s3_object_metadata`

**Work as-is:**
- `_verify_record_matches_file()` — operates on local files only
- `_update_local_checkout()` — creates symlinks, protocol-agnostic
- `get_local_cache_path()` — needs a hash, but doesn't care where it came from
- `validate_record()` — checks local files
- All TSV serialization — `source_uri` is just a string

### The `RemotePath` problem

`RemotePath` (line 194-239) is the biggest structural issue:
- It enforces `scheme == "s3"` in `__post_init__` (line 227)
- It has `.bucket` and `.path` attributes — semantically S3-specific
- It's used as `record.remote_uri` for ALL records, including external
- The download path (`_update_local_cache`) accesses `remote_uri.bucket` and `remote_uri.path`

For HTTP support, either:
- (A) Relax `RemotePath` to support HTTP (rename/generalize `.bucket`) — high blast radius
- (B) Create a separate abstraction for HTTP URLs — cleaner but more code
- (C) Don't use `RemotePath` for HTTP external records — store the URL directly and branch on scheme in the download/validation functions

**Option (C) is likely best:** Keep `source_uri` as the canonical string, and add an `if source_uri.startswith("http")` branch in the 3 functions that need it.

### Download path for external records — how to swap

The download in `_update_local_cache()` (line 440-471) would need a branch:

```python
if record.source_uri.startswith("http"):
    # Use requests/urllib to download
    _download_http(url, local_cache_path, version_id_or_etag)
else:
    # Existing S3 download path
    s3 = boto3.resource("s3")
    ...
```

### How would validation work for HTTP?

HTTP `HEAD` responses provide:
- `ETag` header — often an MD5 of the content (for simple uploads), but not always. Some servers return opaque ETags, quoted strings, or weak ETags (`W/"..."`)
- `Content-Length` — reliable on most servers
- `Last-Modified` — useful but not a content hash

**Proposed mapping:**
| DataManifest field | HTTP equivalent |
|---|---|
| `s3_hash` | HTTP `ETag` header (stripped of quotes) |
| `size` | HTTP `Content-Length` |
| `md5sum` | Empty initially; backfilled from content after download (same as multipart S3) |
| `s3_version_id` | Empty (HTTP has no versioning concept) |

### How would `get_local_cache_path` work?

`get_local_cache_path()` (line 531-539) uses `record.s3_hash` or `record.md5sum` to build the cache path:
```python
file_hash = record.s3_hash if record.s3_hash else record.md5sum
```

For HTTP: if the server provides an ETag, use it as `s3_hash`. If not, we'd need to download first, compute md5, and use that. This creates a chicken-and-egg problem for the first sync — we need the hash to know WHERE to cache it, but we need to download to compute the hash.

**Solution:** Use the HTTP ETag (even if opaque) as `s3_hash` for cache path construction. If no ETag is available, compute an md5 of the URL itself as a stable cache key, or require downloading to a temp location first.

---

## 3. Scope of Changes

### Files that would need modification

| File | Changes needed |
|------|----------------|
| `src/datamanifest/datamanifest.py` | 3-4 functions modified, ~60-100 lines added |
| `src/datamanifest/main.py` | Add `add-http` CLI subcommand (~15 lines) |
| `tests/test_datamanifest.py` | New test functions (~100-150 lines) |

**Total: 2 source files + tests. Roughly 150-250 lines of new/modified code.**

### Is it a clean abstraction boundary?

**Partially.** The download path (`_update_local_cache`) and metadata fetching (`_get_s3_object_metadata`) are the only places that call boto3 for external records. But they're not abstracted behind an interface — they inline the boto3 calls directly.

The cleanest approach would be:
1. Extract a `_get_external_metadata(uri)` dispatcher that calls either `_get_s3_object_metadata` or a new `_get_http_metadata`
2. Extract a `_download_external(record, local_path)` dispatcher that uses boto3 or requests/urllib
3. Modify `_check_remote_etag` to call the dispatcher

### Could it be backward-compatible?

**Yes.** HTTP URLs in `source_uri` would be a backward-compatible extension:
- The TSV format doesn't change (source_uri is already a free-form string)
- Existing S3 external records continue working unchanged
- The manifest version doesn't need to change (v3 already supports `source_uri`)
- Older versions of the tool would fail on HTTP records with a clear error from `RemotePath.from_uri()` ("DataManifest currently only supports s3")

### Estimated complexity

**Moderate.** Not trivial (there are real S3 assumptions to work around), but not a major refactor either. The changes are localized to well-defined functions. The hardest part is deciding the right semantics for HTTP resources that lack ETags or content-length.

---

## 4. Edge Cases and Gotchas

### HTTP servers may not support ETag or Content-Length

- **No ETag:** Can't do drift detection. Options: (a) skip drift check for HTTP, (b) compute md5 on download and use that, (c) require ETag and reject URLs that don't provide one
- **No Content-Length:** Can't pre-validate size before download. The size in the manifest would need to be filled after download.
- **Recommendation:** At `add_external` time, do `HEAD` request. If ETag and Content-Length are present, record them. If not, download the file immediately to compute md5 and size.

### HTTP resources can change without notice

Unlike S3 versioned buckets, HTTP URLs can serve different content at any time. Mitigations:
- Record ETag at add time; on sync, re-check ETag via HEAD
- If ETag changed → raise an error (same as current `_check_remote_etag` behavior)
- If server doesn't support ETag → no drift detection possible; document this risk
- Consider an `--immutable` flag that skips drift checks (for known-stable URLs like versioned releases)

### How to handle redirects?

- `requests` follows redirects by default (up to 30)
- Store the **original** URL as `source_uri`, not the final redirected URL
- On each sync, follow redirects transparently
- Consider storing the final URL after redirect as a note/debug field

### Rate limiting on public servers

- Add configurable retry with exponential backoff (similar to existing S3 retry logic at line 452-467)
- Respect `Retry-After` header if present
- Consider a user-agent string to be a good HTTP citizen

### Should HTTP resources be treated as immutable after first download?

**Proposed behavior:**
- Default: check ETag on each sync (same as unversioned S3 external records)
- If `--skip-remote-check` is passed to sync, skip the HTTP HEAD (already supported flag)
- If no ETag available from server: after first download, treat as immutable (only validate by md5sum of local file)

### Authentication

- Public URLs: no auth needed
- Private URLs: would need Authorization header support (Bearer token, Basic auth)
- **Recommendation for v1:** Support only public URLs. Auth support can be added later via a credential provider pattern.

### Large file downloads

- HTTP downloads should stream to disk (not load into memory)
- `requests` with `stream=True` + chunked writes handles this
- Progress reporting via `tqdm` (consistent with existing UX)

---

## 5. Proposed Implementation Plan

### Phase 1: Minimal HTTP support (public URLs only)

1. Add `_get_http_metadata(url)` — performs HEAD, returns `{etag, size, supports_etag}`
2. Add `_download_http(url, local_path)` — streams GET to file
3. Modify `add_external()` to detect `http://` or `https://` scheme and use HTTP metadata
4. Modify `_update_local_cache()` to branch on scheme for download
5. Modify `_check_remote_etag()` to branch on scheme for drift detection
6. Handle `RemotePath` — either use `skip_validation=True` with http scheme (works today for storage, breaks for `.bucket`/`.path` access) or store a placeholder `RemotePath` and use `source_uri` string directly in HTTP paths
7. Add `add-http` CLI command (or reuse `add-s3` renamed to `add-external`)

### Phase 2: Polish
- Auth support
- Configurable timeout/retries
- Handle servers without ETag gracefully
- Proxy support

### New dependency

Would require adding `requests` to dependencies (currently only `boto3`, `tqdm`, `lockfile` are used for network operations). Alternatively, use `urllib.request` from stdlib to avoid adding a dependency.

---

## 6. Key Decision Points for the Implementer

1. **New dependency?** `requests` (cleaner API, redirect handling, streaming) vs `urllib` (no new dep, more boilerplate)
2. **What to do when server has no ETag?** Error at add time, or allow and skip drift detection?
3. **`RemotePath` refactor?** Quick hack (branch on scheme in 3 places) vs proper abstraction (new base class/protocol)?
4. **CLI surface:** New `add-http` command, or extend `add-s3` → `add-external` accepting both?
5. **Cache path for ETag-less resources:** md5 of URL? md5 of content (requires download-first)?
