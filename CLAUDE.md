# DataManifest

A lightweight Python tool and library for storing and versioning collections of bioinformatics data on S3. Files are managed as a local file tree with symlinks, backed by S3 with native versioning for safety and deduplication.

## Project Goals

- Provide a simple, file-tree-based interface to versioned data collections
- Enable fast checkouts via a shared local cache with symlink-based checkout directories
- Ensure data safety through file-system locks (`fcntl` + `lockfile`), MD5 verification, and S3 versioning
- Support multiple concurrent checkouts sharing the same cache (e.g., Docker containers with a shared mount)

## Repository Layout

```
src/datamanifest/
  __init__.py          # Exports DataManifest, DataManifestWriter
  datamanifest.py      # Core classes: DataManifest (read), DataManifestWriter (read/write),
                       #   RemotePath, DataManifestRecord, S3 helpers, validation
  config.py            # Constants: MANIFEST_VERSION, default file/folder permissions
  main.py              # CLI entry point (`dm` command) and subcommand dispatch
  _logging.py          # Custom logging setup, FileDescriptorLogger utility
tests/
  test_datamanifest.py # Integration tests (require S3 bucket `nboley-test-data-manifest`
                       #   with versioning enabled) and unit tests for parsing/validation
  data/                # Small test data files (BAM, BAI, reference FASTA)
notebooks/
  tutorial.ipynb       # Usage tutorial
```

## Key Concepts

### Two-File System
Each manifest consists of two files:
1. **Manifest TSV** (`*.data_manifest.tsv`) — contains the header config (remote URI, cache suffix, version), column header, and records (key, s3_version_id, md5sum, size, notes). This file is portable/shareable.
2. **Local config** (`*.data_manifest.tsv.local_config`) — machine-specific: checkout prefix, local cache prefix, manifest version. Created by `checkout`.

### Three-Tier Storage
- **S3 remote** — canonical store; files stored at plain paths, versioned via S3 native versioning. Each upload returns a version ID recorded in the manifest.
- **Local cache** — files named `{md5sum}-{filename}` for deduplication across versions. Multiple checkouts share this.
- **Checkout directory** — symlinks into the local cache, providing a normal-looking file tree.

### Locking
- `DataManifest` (reader) takes a shared `fcntl` lock — multiple readers allowed, no writer.
- `DataManifestWriter` upgrades to an exclusive `fcntl` lock — blocks all other access.
- `lockfile.LockFile` used for cache file downloads and sync operations.

### Classes
- **`DataManifest`** — read-only access: sync, validate, get, glob, iterate records.
- **`DataManifestWriter(DataManifest)`** — read-write: add, update, delete files; uploads to S3; rewrites manifest TSV on each mutation.
- **`RemotePath`** — dataclass for S3 URIs with optional `versionId` query parameter.
- **`DataManifestRecord`** — dataclass for a single file entry (key, md5sum, size, notes, path, remote_uri).

## CLI (`dm`)

Entry point: `dm` (defined in `pyproject.toml` as `datamanifest.main:main`).

Subcommands: `create`, `checkout`, `sync`, `add`, `update`, `delete`, `add-multiple`.

Global flags (`--verbose`, `--quiet`, `--debug`) must appear before the subcommand.

## Dependencies

Runtime: `boto3`, `lockfile`, `tqdm`
Test: `pytest`
Python: >= 3.6

## Running Tests

```bash
pytest --verbose .
```

Tests require:
- AWS credentials with access to the `nboley-test-data-manifest` S3 bucket
- The bucket must have versioning enabled
- Tests create temporary S3 objects (keyed by git hash + random string) and clean them up

## Manifest Version

Current version: `2` (defined in `config.py`). Both the manifest TSV header and local config must declare matching `MANIFEST_VERSION` values. Mismatches produce clear `ValueError` messages.

## Conventions

- Linting: flake8 with max line length 120 (see `.flake8`)
- Keys must be relative, normalized paths using `[A-Za-z0-9,_\-/\.]` characters
- Manifest filenames must end with `.data_manifest.tsv`
- File permissions default to `0660`, directory permissions to `2770` (setgid)
