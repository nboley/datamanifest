# Design: Add from S3

Add files to a manifest by referencing an existing S3 object directly, without downloading and re-uploading.

## 1. Problem & Motivation

Bioinformatics workflows routinely produce large files (BAMs, references, indices) that already reside in S3. Today, `dm add` requires a local file path, downloads the file (if not local), uploads it to the manifest's remote datastore, and records the new version ID. For multi-gigabyte files that are already in S3, this round-trip is wasteful and slow.

**Who benefits:**
- Pipeline developers who produce outputs in S3 and want to catalog them in a manifest without re-uploading.
- Researchers who want to reference shared reference data (e.g., genome FASTAs, annotation databases) stored in a central S3 bucket.
- Teams consolidating data from multiple S3 locations into a single versioned manifest.

**What this unlocks:**
- Zero-copy registration of existing S3 objects into a manifest.
- Cross-bucket references: a manifest backed by `s3://my-bucket/` can reference files in `s3://shared-refs/`.
- Lazy local caching: external files are only downloaded when `sync` is called, not at add time.

## 2. User Interface

### New CLI subcommand

```
dm add-s3 <manifest-path> <key> <s3-uri> [--notes NOTES]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `manifest-path` | Path to the `.data_manifest.tsv` file |
| `key` | Manifest key (relative path within the manifest namespace) |
| `s3-uri` | Full S3 URI, e.g. `s3://bucket/path/to/file.bam` |
| `--notes` | Optional annotation string (default: `""`) |

**S3 URI semantics:**
- May include `?versionId=<id>` to pin a specific S3 object version.
- If `versionId` is omitted and the bucket has versioning enabled, the current version ID is captured automatically via `head_object`.
- If the bucket is unversioned (versioning never enabled), no version ID is stored (empty string).

**Example invocations:**

```bash
# Add a reference genome from a shared bucket
dm add-s3 my.data_manifest.tsv reference/hg38.fa s3://shared-refs/hg38.fa

# Pin a specific version
dm add-s3 my.data_manifest.tsv samples/K1-100.bam \
    "s3://other-bucket/output/K1-100.bam?versionId=abc123" \
    --notes "from pipeline run 2026-04-01"
```

**Immutability rule:** External references are immutable. There is no `update` path for external records. To change the target, `delete` the key and `add-s3` again. Additionally, `add(..., exists_ok=True)` must not silently skip over an existing external record — see Section 6.11.

## 3. Data Model Changes

### 3.1 New manifest TSV columns

Two new columns are added to the TSV format:

| Column | Type | Description |
|--------|------|-------------|
| `s3_hash` | `str` | S3 ETag captured at add/upload time. Present for all records (regular and external). |
| `source_uri` | `str` | Full S3 URI of the external object (without `?versionId`). Empty string for regular files. |

**Full column order (v3):**

```
key    s3_version_id    md5sum    s3_hash    size    source_uri    notes
```

### 3.2 MANIFEST_VERSION bump: 2 -> 3

**Why a version bump is required:**
- A v2 writer encountering a v3 manifest would silently strip `s3_hash` and `source_uri` columns when rewriting the TSV, causing data loss.
- The version bump forces old tools to reject v3 manifests with a clear error rather than silently corrupting them.

**Backward compatibility:**
- **v3 reader opens v2 manifest**: Accepted. Missing `s3_hash` and `source_uri` columns are treated as empty strings. All records are implicitly regular files with no recorded ETag.
- **v3 writer opens v2 manifest for mutation**: The writer upgrades the manifest to v3 on first save. This happens automatically because `write_tsv` (called by `_save_to_disk`) always writes the `MANIFEST_VERSION` constant from `config.py` — this is the pre-existing mechanism, not a new v3-specific behavior. Any `DataManifestWriter` operation that calls `_save_to_disk()` (add, update, delete) will upgrade the manifest version. The `MANIFEST_VERSION` line in the TSV header is updated to `"3"`. The local config file is NOT updated — the individual range checks in `_load_header` and `_read_local_config` accept any supported version independently. Existing records get empty `s3_hash` and `source_uri` values.
- **v2 reader opens v3 manifest**: Rejected with `ValueError` (existing behavior for version mismatches).
- **v2 writer opens v3 manifest**: Rejected with `ValueError`.

**Exact version-check relaxation (v3 readers accept both "2" and "3"):**

`_load_header` and `_read_local_config` both currently do strict equality:
```python
if config["MANIFEST_VERSION"] != MANIFEST_VERSION:
    raise ValueError(...)
```
Change both to accept either `"2"` or `"3"`:
```python
# config.py
SUPPORTED_MANIFEST_VERSIONS = {"2", "3"}
```

```python
if config["MANIFEST_VERSION"] not in SUPPORTED_MANIFEST_VERSIONS:
    raise ValueError(
        f"MANIFEST_VERSION mismatch: expected one of {sorted(SUPPORTED_MANIFEST_VERSIONS)}, "
        f"found '{config['MANIFEST_VERSION']}'"
    )
```

Define `SUPPORTED_MANIFEST_VERSIONS` in `config.py` alongside the existing `MANIFEST_VERSION` constant.

This allows v3 readers to open v2 manifests (and v2 local configs) without error.

**Cross-validation removal (manifest version vs. local config version):**

The current code cross-validates that the manifest TSV version and the local config version are strictly equal (`DataManifest.__init__`, line 637). With v2/v3 compatibility, this check becomes dead code: `_load_header` already independently validates `manifest_version in SUPPORTED_MANIFEST_VERSIONS`, and `_read_local_config` independently validates `local_config_version in SUPPORTED_MANIFEST_VERSIONS`. If both individual checks pass, the cross-validation can never fail — any combination of supported versions is valid.

Two scenarios motivate the range-based individual checks:

1. **Fresh checkout of a v2 manifest with v3 code**: `checkout()` calls `_load_header(fp)` first (line 719). With a strict equality check (`config["MANIFEST_VERSION"] != MANIFEST_VERSION`), this rejects v2 manifests when the code constant is `"3"`. The range-based check in `_load_header` (`config["MANIFEST_VERSION"] in SUPPORTED_MANIFEST_VERSIONS`) allows v2 manifests to be read by v3 code.
2. **Opening with a v2 local config after tool upgrade**: `_read_local_config` with a strict equality check rejects `"2"` when the code constant is `"3"`. The range-based check allows v2 local configs to be accepted by v3 code.

**Fix**: Remove the cross-validation block entirely. The individual range checks in `_load_header` and `_read_local_config` are sufficient.

`_save_to_disk()` does NOT need to update the local config file — the individual range checks handle the mixed-version state transparently.

**Checkout creates mixed-version state:** When v3 code checks out a v2 manifest, `checkout()` writes `MANIFEST_VERSION=3` to the new local config (it always writes the current code constant at line 728). The manifest TSV still says `MANIFEST_VERSION=2`. This intentionally creates a mixed-version state (v2 TSV + v3 local config), which the range-based individual checks in `_load_header` and `_read_local_config` accept. This is the most common upgrade scenario and validates the need for removing the cross-validation.

> **Downgrade scenario:** Once v3 code has checked out a v2 manifest, the local config file contains `MANIFEST_VERSION=3`. If a user subsequently attempts to open this checkout with v2 code, `_read_local_config` will reject the local config (v2 code only accepts `MANIFEST_VERSION=2`), even though the manifest TSV is still in v2 format. This is expected and intentional: once v3 code has touched a checkout, the checkout is considered upgraded. Users who need v2 compatibility must re-checkout with v2 tools. This is consistent with the one-way upgrade design — the local config upgrade is irreversible, just as a manifest TSV upgrade (on first write) is irreversible.

**`self.header` upgrade on v2 open (for DataManifestWriter):**

`write_tsv` writes `self.header` as the TSV column header row. When opening a v2 manifest, `self.header` is the 5-column v2 header. `DataManifestWriter.__init__` (after calling `super().__init__()`) must update `self.header` to `default_header()` whenever the loaded header has fewer columns than the current default:
```python
if len(self.header) < len(self.default_header()):  # v2 manifest — upgrade header
    self.header = self.default_header()
```
This ensures the written file gets the 7-column v3 header on any mutation. (`manifest_config` is a local variable inside `DataManifest.__init__` and is not accessible here; comparing against `len(self.default_header())` is the correct v2 detection.)

**Format detection in `_read_records` (v2 vs v3):**

v2 TSV has 5 columns: `key, s3_version_id, md5sum, size, notes`.
v3 TSV has 7 columns: `key, s3_version_id, md5sum, s3_hash, size, source_uri, notes`.
Detection: use `len(self.header)` (set during `_load_header`) to determine the manifest format. If header has >= 7 columns, parse as v3; else parse as v2 (with `s3_hash=""` and `source_uri=""` defaulting to empty string). Using the header rather than per-record column counts avoids misparsing records with trailing whitespace or tabs. See Section 6.2 for the full parsing logic.

### 3.3 DataManifestRecord changes

```python
@dataclasses.dataclass
class DataManifestRecord:
    key: str
    md5sum: str           # Content MD5; validation only. Empty for external multipart files.
    s3_hash: str          # S3 ETag at add/upload time; used for cache path naming. Empty for legacy v2 records.
    size: int
    notes: str
    path: str             # Derived: checkout_prefix + key (not stored in TSV)
    remote_uri: RemotePath  # Derived (not stored in TSV)
    source_uri: str = ""  # Stored in TSV; empty for regular files

    @property
    def is_external(self) -> bool:
        """True if this record references an external S3 object."""
        return bool(self.source_uri)

    @property
    def s3_version_id(self) -> str:
        """Delegates to remote_uri.version_id. May be empty for unversioned external files."""
        return self.remote_uri.version_id
```

**Field order note:** The dataclass field order differs from the TSV column order (`source_uri` appears last with a default of `""`) because Python dataclasses require fields with defaults to follow fields without defaults. The TSV column order is defined by `header()` and `default_header()`, not by the dataclass field order.

**Key changes from current implementation:**
- New stored fields: `s3_hash` (str), `source_uri` (str, default `""`).
- `s3_version_id` remains a derived property (delegates to `remote_uri.version_id`), not a stored field.
- `is_external` is a new computed property.
- `header()` static method updated to include new columns (see Section 6.1).

## 4. Two Hash Fields

| Field | Source | Purpose | When populated |
|-------|--------|---------|----------------|
| `s3_hash` | S3 ETag from API response (`put_object` or `head_object`) | **Local cache path naming** (`{s3_hash}-{filename}`), remote change detection | **Regular files**: at upload time (from `put_object` response). **External files**: at `add-s3` time (from `head_object` response). **Legacy v2 records**: empty (fall back to `md5sum` for cache path). |
| `md5sum` | Computed from file bytes (`md5sum` subprocess) | Local file integrity validation (`validate` command) | **Regular files**: at add/upload time. **External single-part files**: at `add-s3` time (ETag equals content MD5). **External multipart files**: empty (can be computed on-demand after sync). |

### Why `s3_hash` is the cache key

The previous design used `md5sum` for cache path naming (`{md5sum}-{filename}`). This created a chicken-and-egg problem for external files with multipart ETags: the content MD5 is unknown until the file is downloaded, but the cache path (where to download to) requires the MD5. That forced a complex deferred-state machine with writer overrides, reader guards, and auto-upgrade logic.

**Solution:** Use `s3_hash` (ETag) as the cache key instead. The ETag is always known at registration time — returned by `put_object` for regular files and `head_object` for external files — so the cache path is always computable without downloading.

**`get_local_cache_path` change:**

```python
def get_local_cache_path(self, key):
    record = self._data[key]
    # s3_hash is the primary cache key (v3 records).
    # Fall back to md5sum for legacy v2 records (s3_hash is empty).
    file_hash = record.s3_hash if record.s3_hash else record.md5sum
    return os.path.normpath(
        os.path.join(
            self.local_cache_prefix,
            self._build_datastore_suffix(key, file_hash),
        )
    )
```

**Note:** Rename the `_build_datastore_suffix` parameter from `md5sum` to `file_hash` to reflect that it now accepts either `s3_hash` or `md5sum`.

**Backward compatibility:** For all existing regular files, `s3_hash` (captured from `put_object`) equals `md5sum` because `put_object` is a single-part upload. So `{s3_hash}-{filename}` produces the same cache path as the old `{md5sum}-{filename}`. No cache migration needed.

> **KMS upgrade caveat:** When a v3 writer opens a v2 manifest and later `update`s an existing record on a KMS-encrypted bucket, the updated record's `s3_hash` (from the KMS-encrypted `put_object` response) will differ from the original `md5sum`. The old cache file (named `{md5sum}-{filename}`) becomes orphaned. This is a minor storage leak, not a correctness issue — the new cache file is correctly named `{s3_hash}-{filename}`. No cleanup is performed automatically.

> **Caveat:** This equivalence holds for unencrypted objects and SSE-S3. For buckets using SSE-KMS or SSE-C default encryption, `s3_hash` from `put_object` will differ from the locally-computed `md5sum`. In this case, new v3 records will use `{s3_hash}-{filename}` cache paths that differ from what v2 code would have used (`{md5sum}-{filename}`). Since v2 code cannot open v3 manifests, this is not a compatibility problem — but the cache will not be shared between the v2 and v3 cache paths for records created after the upgrade.

**Trade-off:** Two identical files uploaded via different multipart chunk sizes produce different ETags, so they won't deduplicate in the cache. This is negligible in practice — bioinformatics reference files are not duplicated across keys with different upload strategies.

### `md5sum` role after this change

`md5sum` becomes a **validation-only field**:
- Used by `validate_record` and `_verify_record_matches_file` for local integrity checks.
- Populated at add time for regular files (computed from local file bytes) and for external single-part files (ETag equals content MD5).
- Empty for external multipart files. Validation falls back to size-only checks. Content MD5 can be computed on-demand after sync as a future enhancement.
- Never used for cache path construction.

### Multipart ETag detection

An S3 ETag contains a hyphen (`-`) if and only if it was produced by a multipart upload:

```python
def is_multipart_etag(etag: str) -> bool:
    return "-" in etag
```

At `add-s3` time:
- If `is_multipart_etag(etag)` is `False`: set `md5sum = etag` (the ETag is the content MD5).
- If `is_multipart_etag(etag)` is `True`: set `md5sum = ""` (content MD5 unknown without downloading).

### SSE-KMS / SSE-C encryption caveat

For objects encrypted with SSE-KMS or SSE-C, the S3 ETag is **not** the content MD5 even for single-part uploads — it is an opaque value. The `is_multipart_etag` check alone is insufficient to determine whether `etag == md5sum`.

At `add-s3` time, `_get_s3_object_metadata` must also check the `ServerSideEncryption` header from the `head_object` response:

- If `ServerSideEncryption` is absent or `"AES256"` (SSE-S3): single-part ETag equals content MD5.
- If `ServerSideEncryption` is `"aws:kms"` or `"aws:kms:dkek"`, or if `SSECustomerAlgorithm` is present (SSE-C): treat the ETag as opaque — set `md5sum = ""` regardless of whether the ETag contains a hyphen.

**Updated `_get_s3_object_metadata` return dict:**

```python
return {
    "etag": response["ETag"].strip('"'),
    "size": response["ContentLength"],
    "version_id": version_id,
    "encryption": response.get("ServerSideEncryption", ""),
}
```

**Updated logic in `add_external`** (replaces the simple `is_multipart_etag` check):

```python
encryption = metadata["encryption"]
is_opaque_etag = (
    is_multipart_etag(etag)
    or encryption in ("aws:kms", "aws:kms:dkek")
    or bool(metadata.get("sse_customer_algorithm"))
)
md5sum = "" if is_opaque_etag else etag
```

> **Note:** SSE-C encrypted objects are effectively unsupported: `head_object` requires the caller to provide the encryption key, which `_get_s3_object_metadata` does not accept. The `sse_customer_algorithm` guard exists only as defense-in-depth; in practice, `head_object` will fail with a 400 error before the guard triggers.

## 5. RemotePath Changes

### Problem

`RemotePath.__post_init__` validates paths against `r"^[A-Za-z0-9,_\-/\.]+$"`. External S3 paths may contain characters outside this set (spaces, special characters, etc.), since they are not under our control.

### Solution: skip validation for external URIs

Add `_skip_validation` as a proper dataclass field on `RemotePath`. Making it a real field (not a plain attribute set via `object.__new__`) is critical: `dataclasses.replace(remote_uri, version_id=...)` creates a new instance via the dataclass `__init__`, which would silently drop any plain attribute set outside the field list. With `_skip_validation` as a field, `dataclasses.replace` carries it forward automatically.

```python
@dataclasses.dataclass
class RemotePath:
    scheme: str
    bucket: str
    path: str
    version_id: str = ""
    _skip_validation: bool = dataclasses.field(default=False, repr=False, compare=False)

    def __post_init__(self):
        if self.scheme != "s3":
            raise ValueError(f"Unsupported scheme: {self.scheme}")
        # Path validation is skipped for external URIs that may contain
        # arbitrary S3-legal characters outside our usual restricted set.
        if not self._skip_validation:
            _validate_prefix(self.path, InvalidPrefix)

    @classmethod
    def from_uri(cls, uri: str, skip_validation: bool = False) -> "RemotePath":
        parsed = urlparse(uri)
        if not skip_validation:
            # Keep existing safety checks for regular (internal) URIs
            assert parsed.params == "", f"Unexpected params in URI: {uri}"
            assert parsed.fragment == "", f"Unexpected fragment in URI: {uri}"
        # For external URIs (skip_validation=True), these checks are skipped
        # since external S3 URIs may include non-standard components.
        version_id = parse_qs(parsed.query).get("versionId", [""])[0]
        return cls(
            scheme=parsed.scheme,
            bucket=parsed.netloc,
            path=parsed.path.lstrip("/"),
            version_id=version_id,
            _skip_validation=skip_validation,
        )
```

Because `_skip_validation` is a dataclass field, all subsequent `dataclasses.replace(remote_uri, version_id=...)` calls in `_read_records` and `add_external` will propagate `_skip_validation=True` to the new instance without any extra handling.

**Trade-off:** Making `_skip_validation` a dataclass field leaks a construction-time concern into the data model (it appears in `dataclasses.asdict()` and `dataclasses.fields()`). The `repr=False, compare=False` settings mitigate most exposure. An alternative would be a factory classmethod (e.g., `RemotePath.external(uri)`) that bypasses validation without storing the flag, but this would require custom handling in every `dataclasses.replace()` call — the field approach is simpler given the existing codebase's reliance on `dataclasses.replace`.

**Import note:** `parse_qs` must be added to the existing top-level import: change `from urllib.parse import urlparse` to `from urllib.parse import urlparse, parse_qs`. The existing lazy `from urllib.parse import parse_qs` inside `from_uri` is removed in favor of the top-level import.

**Note on intentional behavioral changes to `RemotePath`:**
- The scheme error changes from `NotImplementedError` to `ValueError` to be more idiomatic for bad-input conditions. This is a breaking change for callers catching `NotImplementedError` specifically. Mention in release notes.
- The `assert parsed_uri.params == ""` and `assert parsed_uri.fragment == ""` checks in `from_uri` are retained for `skip_validation=False` (regular URIs) and only skipped for `skip_validation=True` (external URIs that may contain non-standard components).

**Where to use `skip_validation=True`:** Only when constructing a `RemotePath` from a `source_uri` value in `_read_records` (for external records). All regular-file `RemotePath` construction continues to validate.

## 6. Method-by-Method Changes in `datamanifest.py`

### 6.1 DataManifestRecord

**`header()` static method:**

```python
@staticmethod
def header():
    return ["key", "s3_version_id", "md5sum", "s3_hash", "size", "source_uri", "notes", "path", "remote_uri"]
```

Note: `path` and `remote_uri` remain in the header for programmatic access (e.g., iteration) but are not written to the TSV. The TSV columns are the first 7.

### 6.2 `_read_records`

Current behavior: expects columns `[key, s3_version_id, md5sum, size, notes]`, requires `s3_version_id` non-empty, derives `remote_uri` from `manifest.remote_datastore_uri + key + s3_version_id`.

**Changes:**

1. **Column parsing**: Accept both v2 (5 columns) and v3 (7 columns) layouts. Use `len(self.header)` (set by `_load_header`) to detect the manifest format once, not per-record column counts.

```python
# Determined once from self.header (set by _load_header):
is_v3 = len(self.header) >= len(self.default_header())
# v3 columns: key, s3_version_id, md5sum, s3_hash, size, source_uri, notes
# v2 columns: key, s3_version_id, md5sum, size, notes

# Validate minimum columns per record
min_cols = 6 if is_v3 else 4  # v3: key..source_uri required (notes optional); v2: key..size required
if len(parts) < min_cols:
    raise ValueError(
        f"Invalid record format in '{self.fname}' at line {line_i + 1}: "
        f"expected at least {min_cols} columns, got {len(parts)}"
    )
```

For v2 manifests, all records get:
- `s3_hash` defaults to `""`
- `source_uri` defaults to `""`

2. **Relax `s3_version_id` non-empty check**: Currently raises `ValueError` if `s3_version_id` is empty or whitespace-only. Change to: only require non-empty `s3_version_id` for regular (non-external) records. External records from unversioned buckets may have empty `s3_version_id`. The existing whitespace-only check (`.strip()`) is preserved for regular records to catch corrupted TSV entries.

```python
if (not s3_version_id or not s3_version_id.strip()) and not source_uri:
    raise ValueError(
        f"Record for key '{key}' has empty s3_version_id. "
        "This is only allowed for external S3 references."
    )
```

3. **`remote_uri` derivation**: Branch on `source_uri`:

```python
if source_uri:
    # External record: remote_uri from source_uri + version_id
    remote_uri = RemotePath.from_uri(source_uri, skip_validation=True)
    # Reject source_uri that embeds ?versionId — version is stored in s3_version_id column
    if remote_uri.version_id:
        raise ValueError(
            f"source_uri for key '{key}' contains '?versionId=...' at line {line_i + 1}. "
            "Version IDs must be stored in the s3_version_id column, not embedded in source_uri."
        )
    if s3_version_id:
        remote_uri = dataclasses.replace(remote_uri, version_id=s3_version_id)
else:
    # Regular record: remote_uri from manifest base + key (unchanged)
    remote_uri = _build_remote_datastore_uri(self.remote_datastore_uri, key, s3_version_id)
```

**Note on dual construction paths:** After this change, `_read_records` has two distinct `remote_uri` construction paths: `RemotePath.from_uri(source_uri, skip_validation=True)` for external records, and `_build_remote_datastore_uri(self.remote_datastore_uri, key, s3_version_id)` for regular records. This divergence is intentional — external records derive their remote location from `source_uri` (an arbitrary S3 path), while regular records derive it from the manifest's base URI + key (a controlled path). Both paths must be maintained going forward.

4. **Construct `DataManifestRecord`** with new fields:

```python
DataManifestRecord(
    key=key,
    md5sum=md5sum,
    s3_hash=s3_hash,
    size=int(size),
    notes=notes,
    path=self._build_checkout_path(self.checkout_prefix, key),
    remote_uri=remote_uri,
    source_uri=source_uri,
)
```

### 6.3 `write_tsv` / `default_header`

**`default_header()`:**

```python
def default_header():
    full_header = DataManifestRecord.header()
    # Remove non-TSV fields: ["path", "remote_uri"]
    assert full_header[-1] == "remote_uri", full_header
    assert full_header[-2] == "path", full_header
    return full_header[:-2]
```

This preserves the existing derivation pattern (line 260-265) where `default_header()` derives from `DataManifestRecord.header()` by stripping the non-TSV fields.

**`write_tsv(ofstream)`:**

Update the record row to include new columns:

```python
for record in self.values():
    row = [
        record.key,
        record.s3_version_id,  # from remote_uri.version_id
        record.md5sum,
        record.s3_hash,
        str(record.size),
        record.source_uri,
        record.notes,
    ]
    ofstream.write("\t".join(row) + "\n")
```

The 3 `#`-prefixed config lines remain unchanged. The `MANIFEST_VERSION` in the config line is always written from the `config.py` constant (this is the existing `write_tsv` behavior, not a new change), so it will be `"3"` after the constant is bumped.

### 6.4 `_update_local_cache`

Current behavior: always uses `self.remote_datastore_uri.bucket` for the S3 bucket, always passes `VersionId` in `ExtraArgs`, and uses `{md5sum}-{filename}` for the cache path.

**Changes** (all in `DataManifest._update_local_cache` — no writer override needed):

1. **Cache path**: `get_local_cache_path` now uses `s3_hash` (see Section 4). Since `s3_hash` is always populated for v3 records (both regular and external), the cache path is always computable. No guards or special cases needed.

2. **Bucket selection**: Use the record's own `remote_uri.bucket`, not `self.remote_datastore_uri.bucket`. This is backward-compatible for regular records (same bucket) and necessary for external records (different bucket). Note: this also resolves a pre-existing inconsistency where `_update_local_cache` used the datastore-level bucket (`self.remote_datastore_uri.bucket`) while `_delete_from_s3_and_cache` already used the per-record bucket (`self._data[key].remote_uri.bucket`). After this change, both methods consistently use the per-record bucket.

```python
bucket = s3.Bucket(self._data[key].remote_uri.bucket)
```

3. **Conditional VersionId in ExtraArgs**: Only include `VersionId` when it is non-empty.

```python
extra_args = {}
version_id = self._data[key].remote_uri.version_id
if version_id:
    extra_args['VersionId'] = version_id

# In the download call:
remote_object.download_file(str(local_cache_path), ExtraArgs=extra_args)
```

4. **S3 path**: Already uses the record's `remote_uri.path` (line 362) — no change needed. (Note: the bucket assignment `self.remote_datastore_uri.bucket` is at line 361.) For regular records this resolves to `manifest_base + key`; for external records it resolves to the external S3 path.

```python
remote_key = self._data[key].remote_uri.path
```

**No `DataManifestWriter` override is needed.** Because `s3_hash` is always known at add time, the cache path is always computable, and the existing reader `_update_local_cache` handles all records uniformly — regular and external, single-part and multipart. This eliminates the reader/writer split, the `NotImplementedError` guard, and the `sync_main` auto-upgrade logic from the previous design.

**Interaction with `_verify_record_matches_file`:** The existing cache-hit path (line 355) calls `_verify_record_matches_file(self._data[key], local_cache_path, check_md5sum=not fast)`. For external multipart files with empty `md5sum`, this call depends on the Section 6.5 guard to skip the MD5 check. Without that guard, re-syncing a cached external multipart file with `fast=False` would raise a spurious `FileMismatchError`.

### 6.5 `_verify_record_matches_file`

Current behavior: always checks size and (optionally) md5sum.

**Change**: Guard against empty `md5sum`. External multipart files have empty `md5sum` (see Section 4). When `record.md5sum` is empty, skip the MD5 check and only verify size.

```python
@staticmethod
def _verify_record_matches_file(record, fpath, check_md5sum=True):
    actual_size = os.path.getsize(fpath)
    if actual_size != record.size:
        raise FileMismatchError(
            f"Size mismatch for {record.key}: expected {record.size}, got {actual_size}"
        )
    if check_md5sum and record.md5sum:
        actual_md5 = calc_md5sum_from_fname(fpath)
        if actual_md5 != record.md5sum:
            raise FileMismatchError(
                f"MD5 mismatch for {record.key}: expected {record.md5sum}, got {actual_md5}"
            )
```

### 6.5.1 `validate_record` — guard for unsynced external records

The current `validate_record` (line 297-313 in datamanifest.py) raises `MissingFileError` if the local path doesn't exist. For external records that haven't been synced yet, this is expected behavior, not an error. Add a guard that distinguishes "file not synced" from "file missing after sync":

```python
def validate_record(self, key, check_md5sum=True):
    validate_key(key)
    local_abs_path = self._data[key].path
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
```

### 6.6 `delete`

**Change**: Block `delete_from_datastore=True` for external records. Match the actual code order: check key exists first, then get record, then check `is_external`.

```python
def delete(self, key, delete_from_datastore=False):
    if key not in self:
        raise KeyError(f"'{key}' does not exist in '{self.fname}'")
    record = self._data[key]
    if delete_from_datastore and record.is_external:
        raise ValueError(
            "Cannot delete external S3 objects from the datastore. "
            "External records reference objects we do not own. "
            "Use delete(key) without delete_from_datastore=True to remove the reference only."
        )
    # ... rest of existing delete logic unchanged ...
```

**`_delete_from_s3_and_cache` defense-in-depth:**

Although `delete()` blocks `delete_from_datastore=True` for external records, add a guard inside `_delete_from_s3_and_cache` itself to prevent accidental deletion of external objects if the method is ever called directly:

```python
def _delete_from_s3_and_cache(self, key):
    record = self._data[key]
    if record.is_external:
        raise ValueError(
            "Cannot delete external S3 objects from the datastore. "
            "External records reference objects we do not own. "
            "Use delete(key) without delete_from_datastore=True to remove the reference only."
        )
    # ... rest of existing method unchanged ...
```

**Conditional `VersionId` in S3 delete call:** For consistency with the `_update_local_cache` change (Section 6.4), also make the `VersionId` parameter conditional in the existing `delete_object` call:

```python
delete_kwargs = {"Bucket": bucket_name, "Key": remote_key}
if version_id:
    delete_kwargs["VersionId"] = version_id
s3_client.delete_object(**delete_kwargs)
```

> Note: This conditional is defense-in-depth only. In practice, `_delete_from_s3_and_cache` is unreachable for external records (blocked by the guard above) and regular records always have non-empty `version_id`.

### 6.7 `_add_or_update`

**Change**: Block update for external records (when the key already exists and the existing record is external). Use the actual parameter names: `fname_to_add` and `is_update`.

```python
def _add_or_update(self, key, fname_to_add, notes, is_update):
    if is_update and key in self._data and self._data[key].is_external:
        raise ValueError(
            "External S3 references are immutable. "
            "Delete and re-add instead."
        )
    # ... rest of existing logic unchanged ...
```

Also update the `dataclasses.replace` chain in `_add_or_update` to propagate `s3_hash` from the upload response. After `_upload_to_s3` returns `(version_id, etag)` (depends on the Section 6.8 change to return a tuple), set `s3_hash` alongside `remote_uri`:

```python
version_id, etag = self._upload_to_s3(key, fname_to_add)
old_remote_uri = self._data[key].remote_uri
new_remote_uri = dataclasses.replace(old_remote_uri, version_id=version_id)
self._data[key] = dataclasses.replace(self._data[key], remote_uri=new_remote_uri, s3_hash=etag)
```

> **Ordering invariant:** `_copy_local_file_to_local_cache` (called next in the method body) depends on `s3_hash` being set on the record, because `get_local_cache_path` uses `s3_hash` as the primary cache key. The `dataclasses.replace(..., s3_hash=etag)` call MUST happen before `_copy_local_file_to_local_cache`. If this ordering is disrupted, the cache file would be stored under the wrong name (`md5sum` fallback instead of `s3_hash`). For unencrypted single-part uploads `s3_hash == md5sum` so the bug would be masked — it would only manifest with KMS-encrypted buckets where the two values differ.

Additionally, `_build_new_data_manifest_record` (defined on `DataManifest`, the base class, not on `DataManifestWriter`) must be updated. The current implementation uses mixed positional and keyword constructor args — `key`, `md5sum`, and `fsize` are positional. Adding `s3_hash` before `size` in the dataclass field order would cause the positional `fsize` (an `int`) to be passed as `s3_hash` (a `str`), silently corrupting the record. Change to keyword args and add `s3_hash=""` (filled in after upload). Also remove the vestigial `s3_version_id=""` parameter from the method signature — it is never passed by callers and the version_id is always set later via `dataclasses.replace` in `_add_or_update`:

```python
record = DataManifestRecord(
    key=key,
    md5sum=md5sum,
    s3_hash="",        # filled in by dataclasses.replace after _upload_to_s3
    size=fsize,
    notes=notes,
    path=path,
    remote_uri=remote_uri,
    source_uri="",     # regular files have no external source
)
```

### 6.8 `_upload_to_s3`

Current signature: `_upload_to_s3(self, key, fname_to_add)`. It creates its own `s3_client`, derives `bucket` and `remote_key` from `self._data[key].remote_uri`, opens the file with `open(fname_to_add, 'rb')`, calls `put_object`, and returns the `VersionId` string.

**Change**: Also capture and return the ETag. This is a minimal addition — change only the return value:

```python
# Add after extracting version_id:
etag = response["ETag"].strip('"')
# Change the return from:
return version_id
# To:
return version_id, etag
```

The full method signature and body remain unchanged except for this addition. The caller in `_add_or_update` must be updated to unpack both values (see Section 6.7).

> **SSE-KMS note:** If the manifest's bucket uses SSE-KMS default encryption, the ETag returned by `put_object` will not equal the locally-computed `md5sum`. This is handled correctly: `s3_hash` stores the ETag (used for cache paths), and `md5sum` stores the locally-computed MD5 (used for validation). The two fields serve different purposes and are not required to match.

### 6.9 New: `_get_s3_object_metadata(s3_uri)`

**Class placement:** This method belongs on `DataManifest` (the base class), not exclusively on `DataManifestWriter`. Both reader and writer may need to call `head_object` for remote validation in the future, and placing it on the base class avoids duplication.

```python
@staticmethod
def _get_s3_object_metadata(s3_uri: str) -> dict:
    """Call head_object on an S3 URI. Returns dict with keys: etag, size, version_id, encryption, sse_customer_algorithm.

    Args:
        s3_uri: Full S3 URI, optionally with ?versionId=... query param.

    Returns:
        {
            "etag": str,                    # ETag with quotes stripped
            "size": int,                    # ContentLength
            "version_id": str,              # VersionId (empty string if unversioned)
            "encryption": str,              # ServerSideEncryption value (empty if absent)
            "sse_customer_algorithm": str,  # SSECustomerAlgorithm value (empty if absent)
        }

    Raises:
        botocore.exceptions.ClientError: if the object does not exist or access is denied.
    """
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
```

Note: AWS returns `VersionId: "null"` (the literal string) for objects in versioning-suspended buckets. This is treated the same as absent — stored as empty string.

### 6.10 New: `add_external(key, s3_uri, notes="")`

Public method on `DataManifestWriter`.

```python
def add_external(self, key, s3_uri, notes=""):
    """Add an external S3 object to the manifest without downloading it.

    Args:
        key: Manifest key (relative path).
        s3_uri: Full S3 URI (optionally with ?versionId=...).
        notes: Optional annotation.

    Raises:
        ValueError: If key already exists in the manifest.
        ValueError: If key fails validation.
        botocore.exceptions.ClientError: If head_object fails (access denied, not found).
    """
    # 1. Validate key
    validate_key(key)
    if key in self._data:
        raise ValueError(f"Key '{key}' already exists. Delete first to re-add.")

    # 2. Get metadata from S3
    metadata = self._get_s3_object_metadata(s3_uri)
    etag = metadata["etag"]
    size = metadata["size"]
    version_id = metadata["version_id"]

    # 3. Determine md5sum (validation only — not used for cache path)
    encryption = metadata["encryption"]
    is_opaque_etag = (
        is_multipart_etag(etag)
        or encryption in ("aws:kms", "aws:kms:dkek")
        or bool(metadata.get("sse_customer_algorithm"))
    )
    md5sum = "" if is_opaque_etag else etag

    # 4. Build source_uri (strip versionId query param for storage)
    parsed = RemotePath.from_uri(s3_uri, skip_validation=True)
    source_uri = f"s3://{parsed.bucket}/{parsed.path}"

    # 4b. Validate source_uri is TSV-safe
    if '\t' in source_uri or '\n' in source_uri or '\r' in source_uri:
        raise ValueError(
            f"source_uri contains characters that would corrupt the TSV format: {source_uri!r}"
        )
```

> S3 keys can technically contain tab and newline characters. Since the manifest is stored as TSV, these characters would corrupt the file format. This validation rejects such keys at add time with a clear error.

> **Note:** Any query parameters other than `versionId` (e.g., `x-amz-*` presigned URL parameters) are silently stripped from `source_uri`. Only the `s3://bucket/path` form is stored.

```python
    # 5. version_id comes from head_object, which already respects ?versionId in the URI.
    # No override needed — metadata["version_id"] is always correct.

    # 6. Build remote_uri
    remote_uri = RemotePath.from_uri(source_uri, skip_validation=True)
    if version_id:
        remote_uri = dataclasses.replace(remote_uri, version_id=version_id)

    # 7. Build record
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

    # 8. Store and save (no download, no cache population, no symlink)
    self._data[key] = record
    self._save_to_disk()
```

> **Note:** `_get_s3_object_metadata` passes the parsed `?versionId` to `head_object`, which returns the version ID of the requested version. There is no need to separately override `version_id` from the parsed URI — `metadata["version_id"]` already reflects the pinned version if one was specified.

**Important**: `add_external` does NOT download the file, populate the local cache, or create a symlink. The file only becomes locally available after `sync`.

### 6.11 `DataManifestWriter.add` — guard `exists_ok=True` for external records

The existing `DataManifestWriter.add(key, fname_to_add, notes="", exists_ok=False)` method catches `KeyAlreadyExistsError` when `exists_ok=True` and silently verifies the existing record matches the file (size-only check, `check_md5sum=False`). This creates a subtle gap: if the existing record is external, the size check runs against the local file but the external record may reference a completely different S3 object. Add a guard:

```python
def add(self, key, fname_to_add, notes="", exists_ok=False):
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
```

## 7. `main.py` Changes

### 7.1 New subparser

```python
add_s3_parser = subparsers.add_parser(
    "add-s3",
    help="Add an existing S3 object to the manifest without re-uploading",
)
add_s3_parser.add_argument("manifest-path", help="Path to the manifest TSV file")
add_s3_parser.add_argument("key", help="Manifest key (relative path)")
add_s3_parser.add_argument("s3-uri", help="Full S3 URI (optionally with ?versionId=...)")
add_s3_parser.add_argument("--notes", default="", help="Optional annotation")
```

### 7.2 New handler function

```python
def add_s3_main(manifest_fname, key, s3_uri, notes=""):
    dm = DataManifestWriter(manifest_fname)
    dm.add_external(key, s3_uri, notes=notes)
```

Note: all existing handlers use direct instantiation (no `with` statement), consistent with how `DataManifestWriter` manages its own file lock lifecycle.

### 7.3 Dispatch branch

Add to the `if/elif` chain in `main()`:

```python
elif args.command == "add-s3":
    add_s3_main(
        manifest_fname=getattr(args, "manifest-path"),
        key=args.key,
        s3_uri=getattr(args, "s3-uri"),
        notes=args.notes,
    )
```

### 7.4 `sync_main` — no changes needed

Because `s3_hash` is always known at add time and is used as the cache key (see Section 4), `sync` works transparently through the reader (`DataManifest`) for all record types — regular and external, single-part and multipart. No auto-upgrade to `DataManifestWriter` is needed. The existing `sync_main` implementation is unchanged.

## 8. `config.py` Changes

```python
MANIFEST_VERSION = "3"
```

Single-line change. All downstream code already reads this constant.

> **Import note:** The `SUPPORTED_MANIFEST_VERSIONS` constant must also be imported in `datamanifest.py`. Update the existing import block: `from .config import (..., SUPPORTED_MANIFEST_VERSIONS)`.

## 9. Testing Strategy

### 9.1 Integration tests (require S3 bucket `nboley-test-data-manifest`)

| Test | Description |
|------|-------------|
| `test_add_external_basic` | `add_external` a file already in the test bucket. Verify record stored, `is_external` is True, `source_uri` populated, `s3_hash` populated, no local cache file, no symlink. |
| `test_add_external_with_version_id` | `add_external` with explicit `?versionId=...`. Verify the pinned version_id is recorded, not the latest. |
| `test_add_external_sync_single_part` | Add external file (single-part upload), then `sync`. Verify: file downloaded to cache with correct `{s3_hash}-{filename}` name, symlink created, md5sum matches ETag. |
| `test_add_external_sync_multipart` | Upload a file via multipart to the test bucket (use `boto3.s3.transfer.TransferConfig(multipart_threshold=1)` to force multipart on a small file), then `add_external` + `sync` via reader (`DataManifest`). Verify: md5sum is empty, `s3_hash` contains a multipart ETag (has `-`), cache file is named `{s3_hash}-{filename}`, symlink created, file content is correct, sync completes successfully without `NotImplementedError` or any writer upgrade. |
| `test_add_external_resync_multipart` | Add external multipart file, sync once, then sync again with `fast=False`. Verify: re-sync succeeds (cache-hit path in `_update_local_cache` correctly skips MD5 check when `md5sum` is empty). |
| `test_add_external_validate_pre_sync` | Add external, call `validate_record` without syncing. Verify: raises `MissingFileError` (the local symlink does not exist yet). |
| `test_add_external_validate_post_sync_single_part` | Add external (single-part), sync, validate with `check_md5sum=True`. Verify: full md5sum + size check passes. |
| `test_add_external_validate_post_sync_multipart` | Add external (multipart), sync, validate with `check_md5sum=True`. Verify: size check passes, md5sum check is skipped (md5sum is empty). |
| `test_add_external_delete_reference` | Add external, then `delete(key)`. Verify: record removed from manifest. |
| `test_add_external_delete_from_datastore_blocked` | Add external, then `delete(key, delete_from_datastore=True)`. Verify: raises `ValueError` with message about external objects. |
| `test_add_external_update_blocked` | Add external, then attempt `update(key, new_path)`. Verify: raises `ValueError` with immutability message. |
| `test_add_external_duplicate_key` | Add a regular file, then `add_external` with the same key. Verify: raises `ValueError` about duplicate key. |
| `test_add_exists_ok_regular` | Add a regular file, then call `add(key, same_path, exists_ok=True)`. Verify: no error raised, record unchanged. Establishes baseline behavior before the external-record guard. |
| `test_add_exists_ok_blocked_for_external` | `add_external` a file, then call `add(key, local_path, exists_ok=True)` with the same key. Verify: raises `ValueError` about external records and `exists_ok`. |
| `test_add_external_unversioned_bucket` | Requires a second unversioned test bucket. If unavailable, rely on the mocked unit test `test_add_external_unversioned_bucket_mocked` below. If a second bucket is available: add external from an unversioned bucket. Verify: empty `s3_version_id`, download works without `VersionId` in ExtraArgs. |
| `test_s3_hash_captured_on_regular_add` | `add` a regular file. Verify: `s3_hash` is populated from the upload response ETag. |
| `test_manifest_round_trip_v3` | Create manifest, add regular + external files, close, reopen, verify all fields preserved. |
| `test_v2_to_v3_upgrade_on_write` | Create a v2 manifest with a regular file. Open with `DataManifestWriter` (v3 code), add a new file. Verify: TSV is rewritten with 7-column format, `MANIFEST_VERSION=3` in header, original record has empty `s3_hash` and `source_uri`, new record has populated `s3_hash`. Reopen and verify all records parse correctly. |
| `test_regular_add_cache_path_uses_s3_hash` | `add` a regular file. Verify: the local cache file is stored at `{s3_hash}-{filename}` path (which equals `{md5sum}-{filename}` for single-part unencrypted uploads). Verify the symlink points to this cache path. |
| `test_validate_all_with_unsynced_external` | Create manifest with one regular file (synced) and one external file (unsynced). Call `validate()`. Verify: raises `MissingFileError` with message mentioning "not been synced". |

### 9.2 Unit tests (no S3 required)

| Test | Description |
|------|-------------|
| `test_is_multipart_etag` | `is_multipart_etag("abc123")` -> False, `is_multipart_etag("abc123-3")` -> True. |
| `test_source_uri_parsing` | Parse a TSV row with `source_uri` column. Verify `DataManifestRecord.is_external` and `remote_uri` derived from `source_uri`. |
| `test_v2_manifest_read_by_v3_reader` | Read a v2-format TSV (5 columns). Verify: records loaded with empty `s3_hash` and `source_uri`, no errors. |
| `test_v3_manifest_rejected_by_v2_version_check` | Simulate v2 version check against v3 MANIFEST_VERSION. Verify: `ValueError` raised. |
| `test_external_record_remote_uri_construction` | Given a `source_uri` and `s3_version_id`, verify `remote_uri` has the external bucket/path, not the manifest's base URI. |
| `test_remote_path_skip_validation` | `RemotePath.from_uri("s3://bucket/path with spaces/file.txt", skip_validation=True)` succeeds. Without `skip_validation`, raises `ValueError`. |
| `test_default_header_v3` | `default_header()` returns 7 columns in correct order. |
| `test_write_tsv_v3_columns` | Write a manifest with regular + external records, verify TSV has 7 tab-separated columns per row. |
| `test_verify_record_empty_md5sum` | Call `_verify_record_matches_file` with a record that has empty `md5sum` and `check_md5sum=True`. Verify: only size is checked, no error from missing md5. |
| `test_get_local_cache_path_uses_s3_hash` | Create a record with `s3_hash="abc123"` and `md5sum="def456"`. Verify `get_local_cache_path` uses `s3_hash`, not `md5sum`, in the cache filename. |
| `test_get_local_cache_path_falls_back_to_md5sum` | Create a v2 record with `s3_hash=""` and `md5sum="def456"`. Verify `get_local_cache_path` uses `md5sum` as fallback. |
| `test_add_external_sse_kms_opaque_etag` | Mock `head_object` returning `ServerSideEncryption: "aws:kms"` with a non-multipart ETag. Verify `md5sum` is set to empty (ETag treated as opaque). |
| `test_source_uri_rejects_embedded_version_id` | Write a v3 TSV record with `source_uri` containing `?versionId=...`. Verify `_read_records` raises `ValueError` about embedded version IDs. |
| `test_validate_record_unsynced_external_message` | Create a record with `is_external=True` whose path doesn't exist. Call `validate_record`. Verify the error message distinguishes "not synced" from generic "can not find". |
| `test_add_external_unversioned_bucket_mocked` | Mock `_get_s3_object_metadata` to return `version_id=""` (simulating an unversioned bucket). Call `add_external`, verify record has empty `s3_version_id`. Then mock `_update_local_cache` to verify it omits `VersionId` from `ExtraArgs` when syncing. This test is required because the integration test for unversioned buckets may not be feasible with the existing test bucket. |
| `test_read_records_empty_version_id_external` | Write a v3 TSV record with non-empty `source_uri` and empty `s3_version_id`. Verify `_read_records` loads successfully. Write a v3 TSV record with empty `source_uri` and empty `s3_version_id`. Verify `_read_records` raises `ValueError`. |

### 9.3 Existing test requiring update

The test `test_version_mismatch_between_manifest_and_local_config` currently creates a manifest with `MANIFEST_VERSION=2` and a local config with `MANIFEST_VERSION=3`, expecting a `ValueError`. Note: this test currently fails at `_read_local_config`'s strict version check (rejecting `"3"` when the code constant is `"2"`), NOT at the cross-validation between manifest and local config. After the v3 change, `_read_local_config` will accept `"3"` via the range check, so this test will no longer fail at all. It must be rewritten to test the individual version range checks instead:

- **`_load_header` rejects unsupported versions**: Create a manifest TSV with `MANIFEST_VERSION=1` (or `"99"`). Verify `_load_header` raises `ValueError` mentioning `SUPPORTED_MANIFEST_VERSIONS`.
- **`_read_local_config` rejects unsupported versions**: Create a local config with `MANIFEST_VERSION=99`. Verify `_read_local_config` raises `ValueError` mentioning `SUPPORTED_MANIFEST_VERSIONS`.
- **Mixed supported versions are accepted**: Create a manifest with `MANIFEST_VERSION=2` and a local config with `MANIFEST_VERSION=3`. Verify: opens successfully without error (this combination is now valid).

The test `test_empty_version_id_in_manifest` (line 831) creates a record with an empty `s3_version_id` and expects `ValueError("s3_version_id is required and cannot be empty")`. After the v3 change, empty `s3_version_id` is allowed for external records (those with non-empty `source_uri`). This test must be preserved for regular records (empty `source_uri`) and a new companion test added for external records where empty `s3_version_id` is accepted:

- **Empty `s3_version_id` rejected for regular records**: Keep the existing test but ensure the record has no `source_uri` column (or empty `source_uri` in v3 format).
- **Empty `s3_version_id` accepted for external records**: Create a v3 record with non-empty `source_uri` and empty `s3_version_id`. Verify: record loads successfully without error.

### 9.4 CLI tests

| Test | Description |
|------|-------------|
| `test_add_s3_cli_basic` | Run `dm add-s3 ...` via subprocess. Verify exit code 0 and record present in manifest. |
| `test_add_s3_cli_missing_args` | Run `dm add-s3` with missing arguments. Verify argparse error. |

## 10. Edge Cases

### 10.1 Cross-account S3 access

The caller's AWS credentials must have `s3:GetObject` (and `s3:GetObjectVersion` if pinning a version) on the external bucket. If access is denied, `head_object` raises `botocore.exceptions.ClientError` with code `403`.

**Error handling:** Let the `ClientError` propagate from `_get_s3_object_metadata`. The boto3 error message is clear ("Access Denied"). No wrapping needed.

### 10.2 Unversioned external bucket

If the external bucket has never had versioning enabled, `head_object` does not return a `VersionId` key. `_get_s3_object_metadata` returns `version_id=""`.

**Consequence:** The record has an empty `s3_version_id`. The `_read_records` validation for non-empty `s3_version_id` is relaxed for external records (see Section 6.2). `_update_local_cache` omits `VersionId` from `ExtraArgs` (see Section 6.4).

**Integrity:** Without a version ID, there is no guarantee the file hasn't been replaced. Size serves as a basic integrity check. For single-part files, `md5sum` (which equals the ETag) provides content verification. For multipart files, `s3_hash` comparison via a subsequent `head_object` can detect changes cheaply.

### 10.3 External file deleted or modified upstream

If the external S3 object is deleted or replaced after being added to the manifest:
- **`sync`** will fail with a `ClientError` (404 or 403) or, if the object was replaced and the version was pinned, a `NoSuchVersion` error.
- **Error message**: Let the boto3 error propagate. It naturally indicates the S3 key and version.
- **Detection without downloading**: `head_object` on the external URI and compare the returned ETag to the stored `s3_hash`. If they differ, the object has been modified or replaced. This check can be added to `validate` in a future enhancement.

### 10.4 Suspended versioning on external bucket

S3 buckets can have versioning "Suspended" (was enabled, now paused). In this state, `head_object` may return `VersionId: "null"`. Treat `"null"` the same as empty string — do not store it.

```python
version_id = response.get("VersionId", "")
if version_id == "null":
    version_id = ""
```

### 10.5 Key collision between regular and external

A key can only exist once in a manifest. Attempting to `add_external` a key that already exists (regular or external) raises `ValueError`. Attempting to `add` a key that exists as an external record also raises `ValueError`. The user must `delete` first.

### 10.6 `calc_md5sum_from_remote_uri` limitations

The existing `calc_md5sum_from_remote_uri` utility function requires a non-empty `version_id` and always passes `VersionId` to the S3 download call. This function is incompatible with external records from unversioned buckets (where `version_id` is empty). It is not called in the main sync/validate code path, so this does not block the feature. Users calling this utility directly on external records from unversioned buckets will get a `ValueError`. A future enhancement could relax the `version_id` requirement.

## 11. Migration

### Existing v2 manifests

| Scenario | Behavior |
|----------|----------|
| Read-only access (`DataManifest`) | v3 reader accepts v2 manifests. Missing columns default to empty. |
| Write access (`DataManifestWriter`) | On first `_save_to_disk()`, the manifest is rewritten as v3. `MANIFEST_VERSION` updated in the TSV header only (local config is NOT rewritten — the individual range checks in `_load_header` and `_read_local_config` handle mixed-version state). Existing records get empty `s3_hash` and `source_uri`. |
| v2 tool encounters v3 manifest | Rejected with existing `ValueError("MANIFEST_VERSION mismatch...")`. |

### Upgrade path

No manual migration script is needed. The v3 writer automatically upgrades v2 manifests on first write. Users update the `datamanifest` package and continue working. The only irreversible step is the first write: once a manifest is v3, v2 tools cannot open it.

**Recommendation:** Users should commit their manifest TSV before upgrading the tool, so the v2 -> v3 rewrite is a clean, reviewable diff.

### Package version

Bump `pyproject.toml` version from `1.0.1` to `1.1.0` (new feature, minor behavioral change: `RemotePath.__post_init__` now raises `ValueError` instead of `NotImplementedError` for unsupported schemes).

## 12. Implementation Order

### Phase 1: Data model foundation

**Goal:** v3 manifest format works for regular files; no external file support yet.

> **Atomicity note:** Steps 1-12 must be applied as a single commit. Intermediate states (e.g., bumping `MANIFEST_VERSION` before updating `_read_records` to parse 7 columns, or updating `write_tsv` to emit 7 columns before the header upgrade in `DataManifestWriter.__init__`) would break existing tests and corrupt manifests.

1. Add `s3_hash` and `source_uri` fields to `DataManifestRecord` (with defaults).
2. Add `is_external` property.
3. Update `default_header()` and `header()`.
4. Update `write_tsv` to emit 7 columns.
5. Update `_read_records` to parse v3 (7 columns) with v2 backward compat (5 columns), using `len(self.header)` for detection.
6. Add `SUPPORTED_MANIFEST_VERSIONS = {"2", "3"}` constant; change `_load_header` version check to range-based.
7. Change `_read_local_config` version check to range-based.
8. Remove cross-validation between manifest and local config versions in `DataManifest.__init__` (line 637).
9. Add header upgrade in `DataManifestWriter.__init__`: `if len(self.header) < len(self.default_header()): self.header = self.default_header()`.
10. Bump `MANIFEST_VERSION` to `"3"` in `config.py`.
11. Update `_upload_to_s3` to capture and return ETag; populate `s3_hash` in `_add_or_update`.
12. Update `get_local_cache_path` to use `s3_hash` with `md5sum` fallback (Section 4).
13. Write unit tests for v3 parsing, v2 backward compat, cache path logic, round-trip.

**Verification:** All existing tests pass (cache paths are identical since `s3_hash == md5sum` for single-part uploads). New v3 manifests are written correctly. v2 manifests are read and upgraded.

### Phase 2: External file support + sync

**Goal:** `add_external` and `sync` work end-to-end for external files.

1. Add `RemotePath.from_uri` `skip_validation` parameter and `_skip_validation` field.
2. Add `_get_s3_object_metadata` helper.
3. Add `is_multipart_etag` helper.
4. Add `add_external` method on `DataManifestWriter`.
5. Update `_read_records` to build `remote_uri` from `source_uri` for external records.
6. Relax `s3_version_id` non-empty check for external records.
7. Update `_update_local_cache`: bucket from record, conditional VersionId, S3 path from record.
8. Update `_verify_record_matches_file`: guard empty `md5sum`.
9. Update `delete`: block `delete_from_datastore=True` for external records.
10. Update `_add_or_update`: block update for external records.
11. Update `add`: guard `exists_ok=True` for external records.
12. Write integration tests for add_external, sync (single-part + multipart), validate, delete, update.

**Verification:** Full lifecycle: add-s3 -> sync -> validate -> delete. Both reader and writer can sync external files (no writer override needed).

### Phase 3: CLI

**Goal:** `dm add-s3` works from the command line.

1. Add `add-s3` subparser to `main.py`.
2. Add `add_s3_main` handler.
3. Add dispatch branch.
4. Write CLI integration test.

**Verification:** `dm add-s3 manifest.tsv key s3://bucket/file` works end-to-end.

## 13. Files Changed

| File | Method / Section | Change |
|------|-----------------|--------|
| `config.py` | `MANIFEST_VERSION` | `"2"` -> `"3"` |
| `config.py` | `SUPPORTED_MANIFEST_VERSIONS` | **New**: `{"2", "3"}` constant |
| `datamanifest.py` | `DataManifestRecord` | Add `s3_hash`, `source_uri` fields; add `is_external` property; update `header()` |
| `datamanifest.py` | `RemotePath.from_uri` | Add `skip_validation` parameter |
| `datamanifest.py` | `RemotePath.__post_init__` | Check `_skip_validation` attribute before path validation |
| `datamanifest.py` | `_read_records` | Parse 7 columns (v3) or 5 (v2); relax `s3_version_id` check; branch `remote_uri` derivation |
| `datamanifest.py` | `default_header` | Return 7-column list |
| `datamanifest.py` | `write_tsv` | Emit 7 columns per record row |
| `datamanifest.py` | `get_local_cache_path` | Use `s3_hash` with `md5sum` fallback for cache path naming |
| `datamanifest.py` | `_build_datastore_suffix` | Rename `md5sum` parameter to `file_hash` |
| `datamanifest.py` | `DataManifest._update_local_cache` | Conditional VersionId; bucket from record; S3 path from record |
| `datamanifest.py` | `_verify_record_matches_file` | Guard empty `md5sum` |
| `datamanifest.py` | `validate_record` | Guard for unsynced external records with distinct error message |
| `datamanifest.py` | `delete` | Block `delete_from_datastore=True` for external records |
| `datamanifest.py` | `DataManifest.__init__` | Remove cross-validation between manifest and local config versions |
| `datamanifest.py` | `_add_or_update` | Block update for external records; unpack ETag from `_upload_to_s3` |
| `datamanifest.py` | `add` | Guard `exists_ok=True` for external records |
| `datamanifest.py` | `_upload_to_s3` | Capture and return ETag alongside VersionId |
| `datamanifest.py` | `_get_s3_object_metadata` | **New**: `head_object` wrapper returning etag, size, version_id |
| `datamanifest.py` | `is_multipart_etag` | **New**: module-level helper, checks for `-` in ETag |
| `datamanifest.py` | `add_external` | **New**: public method on `DataManifestWriter` |
| `main.py` | `add-s3` subparser | **New**: argparse subcommand definition |
| `main.py` | `add_s3_main` | **New**: handler function |
| `main.py` | `sync_main` | No changes needed (sync works via reader for all record types) |
| `main.py` | `main()` dispatch | Add `elif args.command == "add-s3"` branch |
| `pyproject.toml` | `version` | `1.0.1` -> `1.1.0` |
| `tests/test_datamanifest.py` | Multiple | New integration + unit tests (see Section 9); update `test_version_mismatch_between_manifest_and_local_config` |
| `src/datamanifest/__init__.py` | Exports | Review whether `add_external`, `is_multipart_etag`, or new exception types need re-exporting from the package top level |

## 14. Follow-up: Remote integrity check and md5sum backfill on sync

### Problem

External references from unversioned buckets have no protection against silent upstream replacement. The stored `s3_hash` (ETag) can detect changes, but nothing in the current sync or validate path checks it against the remote.

Additionally, external multipart files have empty `md5sum` after `add-s3`, so local integrity validation is limited to size-only checks until the content MD5 is computed.

### Proposed behavior

**Pre-download ETag check in `_update_local_cache`:**

Before downloading an external file, call `head_object` on the remote URI and compare the returned ETag against the stored `s3_hash`. If they differ, raise an error:

```python
# In _update_local_cache, before the download:
if self._data[key].is_external:
    current_metadata = self._get_s3_object_metadata(self._data[key].remote_uri.uri)
    if current_metadata["etag"] != self._data[key].s3_hash:
        raise FileMismatchError(
            f"Remote ETag for external record '{key}' has changed: "
            f"expected '{self._data[key].s3_hash}', got '{current_metadata['etag']}'. "
            f"The upstream file may have been replaced."
        )
```

For versioned references (non-empty `s3_version_id`), this check is redundant -- the version ID pins the exact object -- so it can be skipped as an optimization.

**Post-download md5sum backfill:**

After downloading an external file with empty `md5sum`, compute the content MD5 from the downloaded file and update the record. This requires `sync` to upgrade from reader to writer (or accept a writer) so it can persist the updated `md5sum`:

```python
# After successful download, if md5sum was empty:
if not self._data[key].md5sum:
    computed_md5 = calc_md5sum_from_fname(local_cache_path)
    self._data[key] = dataclasses.replace(self._data[key], md5sum=computed_md5)
    self._save_to_disk()
```

After backfill, subsequent `validate` calls (even with `check_md5sum=True`) will have a content hash to verify against, closing the integrity gap for multipart files.

### Design considerations

- **Reader vs writer for sync:** Currently `sync_main` uses `DataManifest` (reader). The md5sum backfill requires write access. Options: (a) upgrade `sync_main` to use `DataManifestWriter` when external records with empty `md5sum` are present, (b) add a `backfill_md5sum` post-sync command, (c) accept that md5sum backfill is best-effort and only happens when opened via writer. Option (a) is cleanest but changes the locking behavior of sync.
- **Cost:** The `head_object` pre-check adds one API call per external record per sync. For manifests with many external records, this could be noticeable. Consider making it opt-out (`sync --skip-remote-check`) rather than opt-in.
- **Idempotency:** Once `md5sum` is backfilled, subsequent syncs skip the computation. The backfill is a one-time cost per external record.
