# Phase 1: HTTP/HTTPS External Resources

## Problem

DataManifest external resources only support S3 URIs. Public bioinformatics
databases (JASPAR, HOCOMOCO, GENCODE) are available via stable HTTP/HTTPS URLs
but can't be registered as external resources. Users must either upload copies
to their own S3 mirror or hardcode local filesystem paths.

## Solution

Extend `add_external` to accept `http://` and `https://` URLs in `source_uri`.
Content-hash pinning (md5) is the version mechanism — HTTP has no native
versioning, so the md5 computed at add time is the source of truth.

## Functional Requirements

### 1. `add_external` accepts HTTP/HTTPS URLs
- `source_uri` accepts `http://` and `https://` in addition to `s3://`
- At add time: download the file, compute md5, record size
- HTTP ETag captured if server provides one (stored in `s3_hash` for drift
  detection), but md5 is the authoritative version pin
- Public URLs only — no authentication support required

### 2. Manifest record fields for HTTP resources
- `source_uri`: the HTTP/HTTPS URL — documents provenance
- `s3_version_id`: empty (HTTP has no versioning)
- `md5sum`: content hash computed at add time — the version pin
- `s3_hash`: HTTP ETag if available, empty otherwise
- `size`: file size in bytes (from download, not Content-Length — servers lie)

### 3. Sync behavior
- `sync_and_get`: download from `source_uri` (HTTP GET, streaming)
- Verify downloaded content md5 matches stored `md5sum`
- If md5 doesn't match: raise `FileMismatchError` — source has mutated
- Cache locally as normal, keyed by md5sum

### 4. Drift detection
- On sync, if record has an `s3_hash` (HTTP ETag): do HEAD request first,
  compare ETag to stored value
- ETag match → skip download if file already cached locally
- ETag mismatch or no ETag → download and verify by md5
- If md5 matches despite ETag change: benign (server config change)
- If md5 doesn't match: raise error

### 5. CLI
- Add `add-url` subcommand: `dm add-url <manifest> <key> <url> [--notes ...]`
- Or extend existing `add-s3` to auto-detect scheme (rename to `add-external`)
- Prints md5, size, and ETag (if available) after successful add

### 6. Download robustness
- Stream to temp file, rename on success (no partial files in cache)
- Retry with backoff on transient HTTP errors (5xx, timeout)
- Follow redirects (store original URL, not final redirect target)
- Progress bar via tqdm (consistent with existing S3 download UX)

## Constraints

- Backward-compatible: existing S3 external records work unchanged
- Manifest version stays at v3 (no schema change)
- No new dependencies: use `urllib.request` from stdlib
- Must work in containers (AWS Batch, Docker)
- Older datamanifest versions fail clearly on HTTP records (`RemotePath`
  validation error names the unsupported scheme)

## Out of Scope

- HTTP authentication (Bearer, Basic, API keys)
- Archive backup to mirror bucket (see Phase 2)
- Proxy support
- Modifying regular (non-external) record behavior

## Success Criteria

- `dm add-url manifest.tsv genome_agnostic/motif_databases/jaspar.txt https://jaspar.elixir.no/download/data/2024/CORE/JASPAR2024_CORE_vertebrates_redundant_pfms_jaspar.txt`
  succeeds, prints md5 and size
- `dm.sync_and_get("genome_agnostic/motif_databases/jaspar.txt")` downloads
  from URL, verifies md5, returns local cached path
- Second `sync_and_get` call uses local cache, no re-download
- If URL content changes: `FileMismatchError` on next sync
