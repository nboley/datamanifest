# Phase 2: External Resource Archive Backup

**Depends on:** Phase 1 (HTTP external resources)

## Problem

HTTP external resources can disappear or change without notice. Unlike S3
versioned buckets, there is no upstream guarantee of immutability or
availability. If a public server goes down or reorganizes its URLs, the
pinned version is lost — the manifest points to a dead link, and the md5
can't be satisfied.

## Solution

When `add_external` registers an HTTP resource, upload a backup copy to the
manifest's mirror bucket under an `_archive/` prefix. This gives every HTTP
external resource a versioned backup in infrastructure we control. On sync,
try the source URL first; fall back to the archive if the source is
unavailable.

## Functional Requirements

### 1. Archive on add
- After downloading an HTTP external resource (Phase 1), upload the file
  to `<REMOTE_DATA_MIRROR_URI>/_archive/<key>`
- Capture the `s3_version_id` from the upload response
- Store this version ID in the manifest record (repurposes the currently-empty
  `s3_version_id` field for HTTP records)

### 2. Updated manifest record fields
- `source_uri`: HTTP/HTTPS URL (provenance — unchanged from Phase 1)
- `s3_version_id`: version ID of the archive copy in the mirror bucket
- `md5sum`: content hash (unchanged — still the authoritative pin)
- `s3_hash`: HTTP ETag if available (unchanged)
- `size`: file size (unchanged)

### 3. Fallback sync behavior
- `sync_and_get`: try source URL first (HTTP GET)
- If source fails (connection error, 404, timeout after retries):
  fall back to archive copy in mirror bucket, log a warning
- If source succeeds but md5 doesn't match: fall back to archive copy,
  raise a warning that the source has mutated
- Archive copy is always verified against stored md5sum
- If archive copy also fails: raise error (data is truly lost)

### 4. Archive integrity
- At add time: verify the uploaded archive copy md5 matches the downloaded
  content md5 (round-trip verification)
- Archive copy uses the mirror bucket's standard storage class (not Glacier)
  so it's immediately accessible for fallback

### 5. CLI changes
- `add-url` gains `--no-archive` flag to skip the backup (for testing or
  when the mirror bucket isn't available)
- `dm verify` checks that archive copies exist and match md5 for all
  HTTP external records

## Constraints

- Only applies to HTTP external resources (S3 externals have their own
  versioning)
- Archive uploads go to the existing `REMOTE_DATA_MIRROR_URI` — no new
  buckets or infrastructure
- `_archive/` prefix chosen to avoid key collisions with regular records
- Backward-compatible: records without `s3_version_id` (Phase 1 records
  created before Phase 2) work without fallback — sync just skips the
  archive path

## Out of Scope

- Glacier/Deep Archive storage class (standard S3 is fine for now)
- Archive for S3 external resources
- Periodic re-archiving or archive refresh
- Restoring mutated sources from archive (manual process)

## Success Criteria

- `dm add-url manifest.tsv my/key https://example.com/data.txt` downloads,
  archives to `s3://.../external_data_resources/_archive/my/key`, records
  version ID
- Source URL goes down → `sync_and_get` transparently falls back to archive,
  logs warning
- Source URL mutates → `sync_and_get` detects md5 mismatch, falls back to
  archive, warns that source has changed
- `dm verify` confirms archive copies exist and match
