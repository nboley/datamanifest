# DataManifest

A lightweight Python tool and library for storing and versioning collections of bioinformatics data on S3. Files are managed as a local file tree with symlinks, backed by S3 with native versioning for safety and deduplication.

## Project Goals

- Provide a simple, file-tree-based interface to versioned data collections
- Enable fast checkouts via a shared local cache with symlink-based checkout directories
- Ensure data safety through MD5 verification and S3 versioning
- Support multiple concurrent checkouts sharing the same cache (e.g., Docker containers with a shared mount)

## Repository Layout

```
src/datamanifest/
  __init__.py          # Exports DataManifest, DataManifestWriter
  datamanifest.py      # Core classes: DataManifest (read), DataManifestWriter (read/write),
                       #   RemotePath, DataManifestRecord, S3/HTTP helpers, validation,
                       #   conflict detection
  config.py            # Constants: MANIFEST_VERSION, SUPPORTED_MANIFEST_VERSIONS,
                       #   default file/folder permissions
  main.py              # CLI entry point (`dm` command) and subcommand dispatch
  _logging.py          # Custom logging setup, FileDescriptorLogger utility
tests/
  test_datamanifest.py # Integration tests and unit tests for parsing/validation.
                       #   WARNING: tests use S3_TEST_BUCKET = "karius-biomarker-data-assets",
                       #   which is the live production assets bucket (~0.28 TB of real
                       #   reference data). Tests create and delete temporary objects keyed
                       #   by git hash + random string, but `pytest --verbose .` touches
                       #   production infrastructure — there is no dedicated test bucket.
  test_conflict_detection.py  # Tests for concurrent-writer conflict detection
  test_parallel_sync.py       # Tests for parallel sync
  data/
    data/              # Small test data files (BAM, BAI)
    genome/            # Reference FASTA
notebooks/
  tutorial.ipynb       # Usage tutorial
```

## Key Concepts

### Two-File System
Each manifest consists of two files:
1. **Manifest TSV** (`*.data_manifest.tsv`) — contains the header config (remote URI, cache suffix, version), column header, and records. The v3 TSV columns are: `key`, `s3_version_id`, `md5sum`, `s3_hash`, `size`, `source_uri`, `notes`. This file is portable/shareable.
2. **Local config** (`*.data_manifest.tsv.local_config`) — machine-specific: checkout prefix, local cache prefix, manifest version. Created by `checkout`.

### Three-Tier Storage
- **S3 remote** — canonical store; files stored at plain paths, versioned via S3 native versioning. Each upload returns a version ID recorded in the manifest.
- **Local cache** — files at `<local_cache_prefix>/<dirname(key)>/<file_hash>-<basename(key)>`, where `file_hash` is `s3_hash` when available, falling back to `md5sum`. The `s3_hash` is the S3 ETag, which for multipart uploads is *not* an MD5 — so code that assumes `md5sum` in the cache path will look up the wrong file for any row whose `s3_hash` differs from its `md5sum`. Multiple checkouts share the same cache.
- **Checkout directory** — symlinks into the local cache, providing a normal-looking file tree.

### Classes
- **`DataManifest`** — read-only access. Key methods: `sync(fast, progress_bar, skip_remote_check, max_workers=8)` (parallel sync via ThreadPoolExecutor), `sync_record(key)`, `sync_and_get(key)`, `validate(fast)`, `validate_record(key)`, `get(key, validate)`, `glob(pattern)` (fnmatch on keys), `glob_records(pattern, validate)` (returns list of `DataManifestRecord`), `keys()`, `values()`, `__iter__()`, `__contains__()`, `__len__()`. Context manager (`with DataManifest(...) as dm:`).
- **`DataManifestWriter(DataManifest)`** — read-write: `add`, `update`, `delete` files (uploads to S3); `add_external(key, uri, notes)` for referencing existing S3, HTTP, or HTTPS resources without re-uploading. Rewrites manifest TSV on each mutation with conflict detection (backup, compare, `.CONFLICT` file on concurrent-writer collision). Context manager.
- **`RemotePath`** — dataclass for remote URIs. Supports `s3`, `http`, and `https` schemes, with optional `versionId` query parameter (for S3).
- **`DataManifestRecord`** — dataclass: `key`, `md5sum`, `s3_hash`, `size` (int), `notes`, `path`, `remote_uri` (RemotePath), `source_uri` (str, default `""`). Properties: `is_external` (`bool(source_uri)` — true for records added via `add_external`), `s3_version_id` (delegates to `remote_uri.version_id`).

## CLI (`dm`)

Entry point: `dm` (defined in `pyproject.toml` as `datamanifest.main:main`).

Subcommands: `create`, `checkout`, `sync`, `add`, `update`, `delete`, `add-s3`, `add-url`, `add-multiple`.

Global flags (`--verbose`, `--quiet`, `--debug`) must appear before the subcommand.

## Dependencies

Runtime: `boto3`, `tqdm`
Test: `pytest` (used but not declared in pyproject.toml)
Python: >= 3.7

## Running Tests

```bash
pytest --verbose .
```

Tests require:
- AWS credentials with access to the `karius-biomarker-data-assets` S3 bucket (**this is the live production assets bucket**, not a throwaway test bucket — tests create temporary S3 objects keyed by git hash + random string and clean them up, but failures or interruptions leave orphaned objects in production)
- The bucket must have versioning enabled
- Tests create temporary S3 objects (keyed by git hash + random string) and clean them up

## Manifest Version

Current version: `3` (defined in `config.py`). Supported versions: `{"2", "3"}` (`SUPPORTED_MANIFEST_VERSIONS` in `config.py`). Both the manifest TSV header and local config must declare a version in the supported set. `DataManifestWriter` auto-upgrades v2 headers to v3 on open.

## Conventions

- Linting: flake8 with max line length 120 (see `.flake8`)
- Keys must be relative, normalized paths using `[A-Za-z0-9,_\-/\.]` characters
- Manifest filenames must end with `.data_manifest.tsv`
- File permissions default to `0660`, directory permissions to `2770` (setgid)
