# Design: Prefix-Scoped Sync for `datamanifest`

**Date:** 2026-09-28
**Package version:** 1.3.0 — designed against `756bc19`, implemented against `a174183`.
Both are tagged `v1.3.0`, so the tag does not distinguish them; `a174183` is the one with the
`max_workers` thread pool in `sync()`.
**Status:** IMPLEMENTED — `bd1caf2` (feature), `961068a` (call-site fix), `ee5bdaf` (this doc)

---

## 1. Current API (read from source)

All signatures below are from `src/datamanifest/datamanifest.py` at v1.3.0.

### `sync()` — line 975

```python
def sync(self, fast=False, progress_bar=False, skip_remote_check=False, max_workers=8):
```

Iterates **all keys** in the manifest. Uses `ThreadPoolExecutor(max_workers)` to call
`sync_record()` per key. Collects errors and raises a single `RuntimeError` listing
every failed key. No key-filtering parameter exists.

### `sync_record()` — line 959

```python
def sync_record(self, key, fast=False, skip_remote_check=False):
```

Syncs a single key: downloads to cache if missing, creates checkout symlink, validates
if already present. Returns the `DataManifestRecord`. This is the leaf operation that
`sync()` and `sync_and_get()` both delegate to.

### `sync_and_get()` — line 947

```python
def sync_and_get(self, key, fast=True, skip_remote_check=False) -> DataManifestRecord:
```

Calls `sync_record(key, ...)` then returns `self.get(key, validate=False)`.

### `keys()` — line 941

```python
def keys(self):
    return self._data.keys()
```

Returns `dict_keys` of all manifest keys (strings). No filtering.

### `glob()` — line 1007

```python
def glob(self, pattern):
    return fnmatch.filter(self.keys(), pattern)
```

Returns a list of keys matching an `fnmatch` glob pattern. Uses `fnmatch.filter` which
supports `*`, `?`, `[seq]`, `[!seq]`.

### `glob_records()` — line 1010

```python
def glob_records(self, pattern, validate=True):
    return [self.get(k, validate=validate) for k in self.glob(pattern)]
```

Returns `DataManifestRecord` objects for all keys matching a glob. Optionally validates
each (size check, no md5 by default).

### `checkout()` — line 901

```python
@classmethod
def checkout(cls, manifest_fname, checkout_prefix, local_cache_prefix=None, force=False):
```

Writes the `.local_config` file (CHECKOUT_PREFIX, LOCAL_CACHE_PREFIX). Does NOT sync any
data. Returns a `DataManifest` instance.

### `DataManifestRecord` — line 331

```python
@dataclasses.dataclass
class DataManifestRecord:
    key: str
    md5sum: str
    s3_hash: str
    size: int
    notes: str
    path: str              # checkout symlink path
    remote_uri: RemotePath
    source_uri: str = ""   # non-empty → is_external

    @property
    def is_external(self) -> bool:
        return bool(self.source_uri)
```

---

## 2. Does the existing API already solve prefix sync?

**Almost, but not quite.** The following one-liner syncs a prefix today:

```python
with DataManifest(manifest_path) as dm:
    for key in dm.glob("my_prefix/*"):
        dm.sync_record(key, fast=True)
```

This works correctly for small sets. However, it is **strictly serial** — it calls
`sync_record` in a loop, not via the `ThreadPoolExecutor` that `sync()` uses. For the
IBD sidecar case (763 small files), serial iteration is ~10x slower than parallel.

A user could replicate the parallelism themselves:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

with DataManifest(manifest_path) as dm:
    keys = dm.glob("sidecars/*")
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(dm.sync_record, k, fast=True): k for k in keys}
        errors = []
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                errors.append((futures[f], e))
    if errors:
        raise RuntimeError(...)
```

But this is **15 lines of boilerplate** that exactly duplicates `sync()`'s body with a
filter prepended. The value of a library is absorbing that boilerplate.

**Conclusion: existing API provides the building blocks (`glob` + `sync_record`) but
does not compose them into a single parallel, error-collecting call. A `prefix=` or
`pattern=` parameter on `sync()` is warranted.**

---

## 3. Locking analysis

### Within a single `DataManifest` instance

`DataManifest` (the read-only class) has **no locks at all**. No `threading.Lock`,
no `flock`, no `filelock`. The only locking is in `DataManifestWriter`:

- `_save_lock` (line 1048): `threading.Lock()` — guards `_save_to_disk` writes.
- `_save_counter_lock` (line 1044): class-level `threading.Lock()` — guards a
  monotonic counter for unique backup filenames.

Since `sync()` uses `ThreadPoolExecutor`, the question is whether `sync_record()` is
thread-safe on the read-only `DataManifest` class. Examining the shared mutable state:

| State | Mutated by `sync_record`? | Thread-safe? |
|-------|--------------------------|--------------|
| `self._data` (dict) | No — read only | Yes (GIL + no mutation) |
| `self._etag_drift_keys` (set) | Yes — `.add()` and `.discard()` in `_check_remote_etag` / `_update_local_cache` | **No** — `set.add()` is atomic under CPython GIL but `.discard()` interleaved with `key not in` check (line 520) is a TOCTOU race. In practice benign because each key is synced by exactly one thread, but not formally safe. |
| Filesystem (cache files, symlinks) | Yes | Safe — each key maps to a unique cache path and unique checkout path, so no two threads touch the same files. |

**Bottom line:** `sync()` is already parallel at `max_workers=8` (line 975). There is no
cross-process file lock. Concurrent processes syncing the same manifest are safe **as long
as they operate on disjoint keys** — the cache path includes the content hash, so two
processes downloading the same key to the same cache directory would race on the same file,
but `os.rename` of a fully-downloaded temp file is atomic on POSIX, and the existing code
uses `download_file` directly to the final path (not atomic). This is a pre-existing issue,
not introduced by prefix sync.

### Across processes

No `flock`/`filelock` anywhere in `DataManifest`. Two processes can open the same
manifest and sync concurrently. This is safe for reads because:

1. Each process loads `_data` at `__init__` time and never re-reads the TSV.
2. Cache files are keyed by content hash — identical content writes are idempotent.
3. Symlinks are created atomically by `os.symlink`.

**Implication for prefix sync:** Parallel sync is already the default (`max_workers=8`).
Prefix filtering simply reduces the set of keys fed to the existing parallel machinery.
No new concurrency concerns are introduced.

---

## 4. Cache path derivation

From `get_local_cache_path()` (line 630) and `_build_datastore_suffix()` (line 604):

```
<local_cache_prefix>/<dirname(key)>/<hash>-<basename(key)>
```

Where `hash` = `s3_hash` if non-empty, else `md5sum`. For a key like
`zhao_atac/C1-AC-Control.h5ad` with s3_hash `abc123`:

```
/efs/.dm_cache/zhao_atac/abc123-C1-AC-Control.h5ad
```

The checkout path (`record.path`) is:

```
<checkout_prefix>/<key>
```

And `_update_local_checkout` creates a symlink from checkout path → cache path.

**Prefix-scoped checkout:** Since `_update_local_checkout` is already called per-key
inside `sync_record`, a prefix-filtered sync automatically produces prefix-filtered
checkout symlinks. No separate "prefix checkout" method is needed.

---

## 5. External record handling

`is_external` = `bool(record.source_uri)` (line 343). The sync dispatch in
`_update_local_cache()` (line 504) branches on `record.remote_uri.scheme`:

| Scheme | Download method | Hash verification |
|--------|----------------|-------------------|
| `s3` | `boto3 download_file` with `VersionId` | Size check + optional md5 |
| `http`/`https` | `_http_download_with_retry` → `_download_http_to_file` | md5 if `record.md5sum` is set; otherwise download proceeds unchecked |

External records with **empty `s3_hash`**: `get_local_cache_path()` falls through to
`md5sum`. If `md5sum` is also empty, `_build_datastore_suffix` raises `ValueError`
("no file hash available"). This means external records MUST have at least one of
`s3_hash` or `md5sum` to be syncable at all.

**ETag drift detection** (`_check_remote_etag`, line 462): For http(s) externals, a HEAD
request checks the ETag. If it differs, the key is added to `_etag_drift_keys`, which
forces re-download. If HEAD fails, it logs a warning and continues (non-fatal).

**Risk for prefix sync:** The 71 GEO http externals in `shared_data` are precisely the
use case. Without prefix scoping, `sync()` attempts all 7,288 keys including those 71
NCBI downloads. With prefix scoping, a user can sync only `zhao_atac/*` (the 71 files)
or exclude them by syncing a different prefix. The design must not change external
handling — it just filters which keys enter `sync_record`.

---

## 6. Proposed API

### New exception

```python
class UnknownKeyError(KeyError):
    """Raised when requested keys are not present in the manifest."""
    pass
```

Placed alongside the other exception classes (near L74–97). Subclasses `KeyError`
so existing `except KeyError` callers keep working.

### New query method: `find_prefix()`

```python
def find_prefix(self, prefix: str) -> List[str]:
    """Return all keys starting with `prefix`.

    This is a literal str.startswith filter — NOT equivalent to
    glob(prefix + "*"). The two are kept separate so that a future
    change to glob()'s semantics cannot silently alter find_prefix().

    Args:
        prefix: Literal prefix to match. Must be non-empty.

    Returns:
        List of matching keys, in manifest order.

    Raises:
        ValueError: If prefix is empty (matches everything — almost
            certainly a bug in programmatic prefix construction).
    """
```

### Primitive: Add `keys=` parameter to `sync()`

```python
def sync(
    self,
    *,
    keys: Optional[Iterable[str]] = None,
    fast: bool = False,
    progress_bar: bool = False,
    skip_remote_check: bool = False,
    max_workers: int = 8,
) -> None:
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
```

### Convenience methods (on `DataManifest`)

```python
def sync_glob(
    self,
    pattern: str,
    *,
    fast: bool = False,
    progress_bar: bool = False,
    skip_remote_check: bool = False,
    max_workers: int = 8,
) -> List[str]:
    """Sync all keys matching an fnmatch glob pattern.

    Equivalent to self.sync(keys=self.glob(pattern), ...).
    Returns the list of matched keys.

    Raises ValueError if the pattern matches zero keys (likely a typo).
    """

def sync_prefix(
    self,
    prefix: str,
    *,
    fast: bool = False,
    progress_bar: bool = False,
    skip_remote_check: bool = False,
    max_workers: int = 8,
) -> List[str]:
    """Sync all keys starting with `prefix`.

    This is the common case: sync everything under a directory-like prefix.
    Uses str.startswith (NOT glob(prefix + "*")).
    Returns the list of matched keys.

    Raises:
        ValueError: If prefix is empty or no keys match.
    """
```

### Why `keys=` on `sync()` instead of `pattern=` or `prefix=`

1. **Composability.** Users already have `glob()`, `keys()`, and list comprehensions.
   Passing the result directly to `sync(keys=...)` is natural and doesn't require
   learning a new pattern syntax.

2. **Explicitness.** A `prefix=` parameter on `sync()` is ambiguous: does `"foo"` match
   `"foobar"` or only `"foo/..."?` With `keys=`, the user controls the semantics.

3. **Convenience methods are sugar.** `sync_prefix()` and `sync_glob()` are one-liners
   over `sync(keys=...)`. They exist to make the 90% case (`dm.sync_prefix("sidecars/")`)
   a single call.

### Why glob patterns, not regex

`glob()` already exists and uses `fnmatch`. Users of bioinformatics tools expect shell
glob patterns (`*`, `?`, `[0-9]`), not regex. No change needed.

### `sync_and_get` stays unchanged

`sync_and_get(key)` already operates on a single key. No prefix variant is needed — a
user who wants multiple records after sync can call `glob_records(pattern, validate=False)`
since the records are already synced.

### CLI extension

```
dm sync <manifest-path> [--fast] [--skip-remote-check] [--prefix PREFIX] [--glob PATTERN]
```

`--prefix` and `--glob` are mutually exclusive. If neither is given, sync all (backward
compatible). Maps to `sync_prefix()` / `sync_glob()` respectively.

---

## 7. Backwards compatibility

| Existing call | Behavior after change |
|--------------|----------------------|
| `dm.sync()` | Unchanged — `keys=None` means all keys |
| `dm.sync(fast=True, progress_bar=True)` | Unchanged |
| `dm.sync(max_workers=4)` | Unchanged |
| `dm sync manifest.tsv` (CLI) | Unchanged — no `--prefix`/`--glob` → syncs all |

The only change to `sync()`'s signature is adding `keys=` as a keyword-only argument
with default `None`. Since `sync()` currently has no positional-only parameters and all
existing callers use keyword arguments (verified in `main.py` line 146 and test files),
this is fully backward compatible.

**Note on keyword-only enforcement:** The current signature uses positional parameters:
`sync(self, fast=False, progress_bar=False, ...)`. The proposed signature uses `*` to
force keyword-only. This is a **minor breaking change** if any caller passes `fast` or
`progress_bar` positionally (e.g., `dm.sync(True, True)`). Checking all known call sites:

- `main.py:146` — `dm.sync(fast=fast, progress_bar=progress_bar, skip_remote_check=skip_remote_check)` — keyword ✓
- `main.py:157` — `dm.sync(fast=fast, progress_bar=progress_bar)` — keyword ✓
- `test_parallel_sync.py:96` — `dm.sync(fast=True, max_workers=4)` — keyword ✓
- `test_parallel_sync.py:117` — `dm.sync(fast=True, max_workers=1)` — keyword ✓
- `test_parallel_sync.py:167` — `dm.sync(fast=True)` — keyword ✓

All existing callers use keywords. The `*` enforcement is safe.

**Unbundling consideration:** The `*` enforcement is a separate concern from the `keys=`
parameter. It could be a prior commit so the feature commit is a pure addition. All 5
known call sites are keyword-only already (`main.py:146`, `main.py:157`,
`test_parallel_sync.py:96/117/167`). **Decision:** unbundle into a prior commit for
cleaner git history.

---

## 8. Parallelism

### Current state

`sync()` already uses `ThreadPoolExecutor(max_workers=8)` (line 984). This is
thread-based parallelism — appropriate for I/O-bound S3/HTTP downloads where the GIL
is released during network and file I/O.

### What prefix sync changes

Nothing about the parallelism model. Prefix sync simply reduces the `keys` list that
is fed to the same `ThreadPoolExecutor`. The `max_workers` parameter is already exposed.

### Thread safety of `sync_record`

Each call to `sync_record(key)` operates on:
- A unique cache path (content-hash in filename)
- A unique checkout symlink path (key-derived)
- Read-only access to `self._data[key]`

The only shared mutable state is `self._etag_drift_keys` (a `set`). Under CPython,
`set.add()` and `set.discard()` are atomic due to the GIL, and each key is processed by
exactly one thread, so the TOCTOU window at line 520 (`key not in self._etag_drift_keys`)
is benign. This is a pre-existing condition, not introduced by this proposal.

### What could go wrong

1. **Partial writes to cache:** `_update_local_cache` downloads S3 objects directly to the
   final cache path via `download_file` (line 550). If the process is killed mid-download,
   a partial file is left at the cache path. On next sync, the size check will catch it
   (unless `fast=True` and the partial file happens to match the expected size). This is a
   pre-existing issue. **Mitigation (out of scope for this design):** download to a temp
   file and `os.rename` atomically.

2. **Two processes syncing the same key concurrently:** Both will attempt to download to the
   same cache path. The second writer will find the file exists mid-download from the first
   and either: (a) if the first finished, validation passes; (b) if the first is
   mid-download, `os.path.exists` returns True on a partial file, and `_verify_record_matches_file`
   raises `FileMismatchError`. This is also pre-existing. Prefix sync does not worsen it.

3. **`_etag_drift_keys` race in `DataManifestWriter.sync_record`:** The writer's override
   (line 1063) calls `super().sync_record()` then conditionally calls `_save_to_disk()`.
   If two threads trigger backfill simultaneously, `_save_to_disk` acquires `_save_lock`
   and serializes correctly. No new risk from prefix sync.

---

## 9. Interaction with externals

The design intentionally does not change how external records are handled. The only
effect is that `keys=` / `sync_prefix()` / `sync_glob()` filter which keys are passed
to `sync_record()`. The behavior of `sync_record()` for externals is unchanged:

- S3 externals: download with `VersionId`, ETag drift check via HEAD.
- HTTP/HTTPS externals: download via `urllib`, md5 verification if available.

**The motivating use case:** `shared_data` manifest with 71 GEO http externals (~90.3 GB).
A user who wants only the annotation files can do:

```python
dm.sync_prefix("annotations/")  # skips the 90 GB GEO downloads entirely
```

Or, to sync only the GEO files:

```python
dm.sync_glob("zhao_atac/*.h5ad")
```

**No special handling of externals in the filtering logic.** The filter operates on keys
(strings); it has no knowledge of whether a key is external. This is the correct design —
the user knows what prefix they want, and the library should not second-guess them.

---

## 10. Failure semantics

### Current behavior

`sync()` (line 975–1001) uses **continue-and-report**: all keys are attempted, errors are
collected, and a single `RuntimeError` is raised at the end listing every failed key and
its exception. This is the right model for bulk sync.

### Proposed behavior for prefix sync

**Same: continue-and-report.** The `RuntimeError` at the end includes the full list of
failed keys and their exceptions. The caller can inspect the exception message to
determine what failed.

Rationale:
- All-or-nothing (rollback) is impractical for downloads — you can't "un-download" a
  file, and the cached files are correct (just incomplete).
- Silent partial success is a known hazard (see: Nextflow `errorStrategy 'ignore'`
  masking a 27% drop). The exception makes partial failure visible.
- The existing pattern is well-understood by users of the library.

### Additional guarantees

1. **Empty match raises immediately.** `sync_prefix("nonexistent/")` and
   `sync_glob("*.xyz")` raise `ValueError("No keys match prefix/pattern '...'")` before
   starting any downloads. This catches typos early. To intentionally sync zero keys,
   pass `keys=[]` explicitly.

2. **Unknown keys raise immediately.** If `keys=` contains a key not in the manifest,
   `UnknownKeyError` (a subclass of `KeyError`) is raised before any downloads start,
   listing all unknown keys. This is a fast-fail for programming errors. The entire
   requested key set is validated up front via set-difference — not one-by-one inside
   the thread pool.

3. **Return values.** `sync()` returns `None` (preserved). `sync_prefix()` and
   `sync_glob()` return the matched key list — it's free (already computed) and
   saves callers a redundant `find_prefix()`/`glob()` call.

4. **Empty prefix guard.** `sync_prefix("")` and `find_prefix("")` raise `ValueError`
   because `"".startswith("")` is `True` for all strings, degenerating to a full sync
   with no warning. An empty prefix is almost certainly a bug in programmatic prefix
   construction.

---

## 11. Implementation sketch

```python
# New exception — near L74-97 with other exception classes
class UnknownKeyError(KeyError):
    """Raised when requested keys are not present in the manifest."""
    pass


# In DataManifest class

def find_prefix(self, prefix: str) -> List[str]:
    if not prefix:
        raise ValueError("prefix must be non-empty")
    return [k for k in self.keys() if k.startswith(prefix)]


def sync(
    self,
    *,
    keys: Optional[Iterable[str]] = None,
    fast: bool = False,
    progress_bar: bool = False,
    skip_remote_check: bool = False,
    max_workers: int = 8,
) -> None:
    if keys is None:
        target_keys = list(self.keys())
    else:
        target_keys = list(keys)
        # Up-front validation: set-difference all requested keys against manifest
        unknown = set(target_keys) - set(self._data.keys())
        if unknown:
            sample = sorted(unknown)[:10]
            msg = f"{len(unknown)} key(s) not found in manifest '{self.fname}': {sample}"
            if len(unknown) > 10:
                msg += f" (and {len(unknown) - 10} more)"
            raise UnknownKeyError(msg)

    if not target_keys:
        return  # explicit no-op for keys=[]

    errors = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                self.sync_record, key, fast=fast, skip_remote_check=skip_remote_check
            ): key
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
        error_details = "\n".join(
            f"  {key}: {type(e).__name__}: {e}" for key, e in errors
        )
        raise RuntimeError(
            f"Failed to sync {len(errors)} of {len(target_keys)} record(s):\n{error_details}"
        )


def sync_prefix(
    self,
    prefix: str,
    *,
    fast: bool = False,
    progress_bar: bool = False,
    skip_remote_check: bool = False,
    max_workers: int = 8,
) -> List[str]:
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


def sync_glob(
    self,
    pattern: str,
    *,
    fast: bool = False,
    progress_bar: bool = False,
    skip_remote_check: bool = False,
    max_workers: int = 8,
) -> List[str]:
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
```

---

## 12. Testing strategy

### Existing test conventions (from `tests/`)

- Tests use a real S3 bucket (`karius-biomarker-data-assets`) with versioning enabled.
- `cleandir` fixtures create temp directories, clean up with `shutil.rmtree`.
- `s3_cleanup` fixture purges S3 prefixes after tests.
- `test_conflict_detection.py` demonstrates **no-S3 testing**: creates manifests directly
  on disk with synthetic records, tests `DataManifestWriter` behavior without network.

### Proposed tests

#### No-network tests (unit, fast)

These follow the `test_conflict_detection.py` pattern of creating manifests on disk
with synthetic records.

1. **`test_sync_keys_filters_correctly`** — Create a manifest with 5 keys in 2 prefixes.
   Mock `sync_record` to track which keys are called. Call `sync(keys=[...])` with a
   subset. Assert only the specified keys were synced.

2. **`test_sync_prefix_matches`** — Same setup, call `sync_prefix("prefix_a/")`. Assert
   correct keys matched.

3. **`test_sync_glob_matches`** — Call `sync_glob("prefix_a/*.txt")`. Assert correct keys.

4. **`test_sync_keys_unknown_key_raises`** — Pass a key not in the manifest. Assert
   `KeyError` raised before any `sync_record` calls.

5. **`test_sync_prefix_no_match_raises`** — `sync_prefix("nonexistent/")` raises
   `ValueError`.

6. **`test_sync_glob_no_match_raises`** — `sync_glob("*.xyz")` raises `ValueError`.

7. **`test_sync_keys_empty_list`** — `sync(keys=[])` succeeds with no work (no error,
   no downloads). This is the explicit "sync nothing" escape hatch.

8. **`test_sync_keys_none_syncs_all`** — `sync(keys=None)` and `sync()` behave
   identically (all keys synced).

9. **`test_sync_prefix_error_collection`** — Some keys in prefix fail. Assert the
   `RuntimeError` includes only the failing keys and that successful keys within the
   prefix were still synced.

#### S3 integration tests (slow)

10. **`test_sync_prefix_downloads_subset`** — Create a manifest with files in two
    prefixes. Sync only one prefix. Verify only those files exist locally.

11. **`test_sync_glob_with_externals`** — Add a mix of regular and external (S3)
    records. Glob-sync a pattern that includes some of each. Verify correct behavior.

#### CLI integration tests

12. **`test_cli_prefix_flag`** — Invoke `sync_main` with `prefix=` argument. Assert
    `sync_prefix` is called with the correct prefix.

13. **`test_cli_glob_flag`** — Invoke `sync_main` with `glob_pattern=` argument. Assert
    `sync_glob` is called with the correct pattern.

14. **`test_cli_prefix_and_glob_mutual_exclusion`** — Parse args with both `--prefix`
    and `--glob` flags. Assert argparse rejects the combination.

#### Test file location

New tests go in `tests/test_prefix_sync.py`, following the naming pattern of
`test_parallel_sync.py` and `test_conflict_detection.py`.

**Note:** The existing test suite has 124 tests (101 in `test_datamanifest.py`,
19 in `test_conflict_detection.py`, 4 in `test_parallel_sync.py`), not ~105 as
previously estimated.

---

## 13. CLI changes

### `dm sync` command

Add two mutually exclusive options using `argparse.add_mutually_exclusive_group()`:

```
dm sync <manifest-path> [--fast] [--skip-remote-check] [--prefix PREFIX] [--glob PATTERN]
```

Implementation in `main.py`:

```python
filter_group = sync_subparser.add_mutually_exclusive_group()
filter_group.add_argument("--prefix", default=None, help="Sync only keys starting with PREFIX")
filter_group.add_argument("--glob", default=None, help="Sync only keys matching GLOB pattern")
```

Using `add_mutually_exclusive_group()` ensures argparse rejects `--prefix` and `--glob`
passed together, rather than silently dropping one via an `if/elif` chain.

In `sync_main()`:

```python
def sync_main(manifest_fname, fast, progress_bar=True, skip_remote_check=False,
              prefix=None, glob_pattern=None):
    # ... existing writer-check logic ...
    with dm:
        if prefix:
            dm.sync_prefix(prefix, fast=fast, progress_bar=progress_bar,
                           skip_remote_check=skip_remote_check)
        elif glob_pattern:
            dm.sync_glob(glob_pattern, fast=fast, progress_bar=progress_bar,
                         skip_remote_check=skip_remote_check)
        else:
            dm.sync(fast=fast, progress_bar=progress_bar,
                    skip_remote_check=skip_remote_check)
```

---

## 14. Migration

**None required.** This is purely additive:

- No schema changes to manifest files.
- No changes to cache layout.
- No changes to `DataManifestRecord`.
- `sync()` signature change is backward-compatible (new keyword-only arg with default `None`).
- `sync_prefix()` and `sync_glob()` are new methods, not modifications.
- CLI gains optional `--prefix` and `--glob` flags with no default behavior change.

The `*` (keyword-only) enforcement on `sync()` is technically a minor API change, but
all known callers already use keyword arguments (verified above). If this is a concern,
it can be omitted.

---

## 15. Risk assessment

### Biggest risk: the `*` keyword-only change

If any external caller passes `fast` positionally to `sync()` (e.g., `dm.sync(True)`),
this will break. Mitigation: grep all known downstream repos (`biomarker-projects`,
`biomarker`, `biomarker-pipeline`) for `.sync(` calls. If any use positional args,
keep the current positional signature and add `keys` after the existing parameters.

### Pre-existing risk: non-atomic cache writes

`_update_local_cache()` downloads S3 objects directly to the final cache path (line 550:
`remote_object.download_file(str(local_cache_path), ...)`). A crash mid-download leaves
a partial file. This is not introduced by prefix sync, but prefix sync makes parallel
downloads more likely, which increases the window for this issue. Consider a follow-up
to download to a temp file and `os.rename`.

### Pre-existing risk: `_etag_drift_keys` is a plain `set`

Used from multiple threads in `sync()` without a lock. Safe under CPython GIL for single
operations (`add`, `discard`, `in`), but the check-then-act at line 520 is technically a
TOCTOU race. Again, pre-existing and benign (one thread per key), but worth noting.

### Low risk: empty-match `ValueError` could surprise

A user who calls `sync_prefix("data/")` expecting it to silently do nothing when no keys
match will get a `ValueError`. This is intentional — silent no-ops on prefix typos are
worse than loud failures. Users who want the silent behavior can use
`sync(keys=dm.glob("data/*"))` which accepts an empty list.

---

## 16. Summary

| Question | Answer |
|----------|--------|
| Does existing API solve it? | Partially — `glob()` + `sync_record()` loop works but is serial and requires 15 lines of boilerplate |
| Proposed solution | `keys=` param on `sync()`, plus `sync_prefix()` and `sync_glob()` convenience methods |
| Locking | `DataManifest` has NO file locks. `DataManifestWriter` has thread locks for save. Concurrent processes on disjoint keys are safe. |
| Parallelism | Already exists (`max_workers=8`). Prefix sync reuses it. |
| Externals | No change — filter operates on keys, not record types |
| Failure semantics | Continue-and-report (same as current `sync()`) |
| Backward compatibility | Fully preserved — `sync()` with no args syncs all |
| Migration | None |
| Biggest risk | The `*` keyword-only enforcement; mitigate by checking downstream callers |

---

## Review Notes (2026-09-28)

**Verdict**: APPROVED WITH CONDITIONS
**Grade**: A-

### Factual Verification

All line-number citations and signatures verified correct against source at v1.3.0.
Every claim independently confirmed: `sync()` L975, `sync_record()` L959,
`sync_and_get()` L947, `keys()` L941, `glob()` L1007, `glob_records()` L1010,
`checkout()` L901, `DataManifestRecord` L331, locks at L1044/L1048,
`_etag_drift_keys` at L502/L520/L533/L852, `is_external` at L343,
sync dispatch on `remote_uri.scheme` at L529/L534.

### Risks (all addressed in reconciliation)
1. ~~§6 docstrings say `RuntimeError` for empty match but §10/§11 say `ValueError`~~ → FIXED: §6 now says `ValueError`.
2. ~~CLI `--prefix` + `--glob` passed together silently drops `--glob`~~ → FIXED: §13 uses `add_mutually_exclusive_group()`.
3. ~~`sync_prefix("")` matches all keys without error~~ → FIXED: `find_prefix("")` raises `ValueError`.

### Conditions (all met)
1. ✓ §6 `sync_prefix`/`sync_glob` docstrings say `ValueError`.
2. ✓ §13 uses `argparse.add_mutually_exclusive_group()`.

### CLAUDE.md Staleness (out of scope but flagged)
- `MANIFEST_VERSION` is `3`, not `2` (`config.py` L3).
- Cache path is `{s3_hash or md5sum}-{basename(key)}`, not `{md5sum}-{filename}`.
- Test bucket is `karius-biomarker-data-assets`, not `nboley-test-data-manifest`.
- `DataManifestRecord` description omits `s3_hash` and `source_uri` fields (v3 additions).

### Key Tradeoffs
- **`keys=` on `sync()` vs. `prefix=`/`pattern=`**: Chose `keys=` for composability — users already have `glob()` and list comprehensions. Convenience methods (`sync_prefix`, `sync_glob`) are sugar. This is the right call: one primitive + two wrappers > two special-purpose parameters.
- **`*` keyword-only enforcement**: NOT unbundled — it lives on the same signature line as
  `keys=`, so splitting it would need an artificial intermediate commit. It ships inside
  `bd1caf2`.
  On call sites, this doc originally claimed "all 5 known call sites verified keyword-only".
  That count was wrong twice over. The repo actually has **20** `sync()` call sites, and the
  one the original enumeration missed — `test_datamanifest.py:621`, `dm.sync(fast)` — was the
  **only positional one in the repo**, so the incomplete audit missed precisely the case that
  mattered. Fixed in `961068a`.
  Two lessons worth keeping: enumerate by searching for the *absence* of `kwarg=` rather than
  listing sites you can think of; and note this was invisible to the test suite because
  `test_datamanifest.py` is the file we deliberately never run (it writes to the production
  bucket). A test that cannot be run cannot report that it is broken.
  All 20 sites are now confirmed keyword-only. Downstream repos were checked separately:
  0 positional callers in `biomarker-projects`, `biomarker`, `fragments_h5`.
- **Empty-match `ValueError` with `keys=[]` escape hatch**: Well-designed asymmetry — explicit empty list is a conscious no-op, empty glob/prefix match is likely a typo. Documented in the §6 docstring.
- **Return values**: `sync_prefix`/`sync_glob` return the matched key list. Free (already computed) and useful for callers.
- **`UnknownKeyError(KeyError)`**: New exception subclass. Up-front set-difference validation before any thread starts.
- **`find_prefix` vs `glob`**: `find_prefix` uses `str.startswith`, NOT `glob(prefix + "*")`. Each name means what it says.
- **Existing `KeyError` sites** (`get()`, `sync_record()`, etc.): NOT routed through `UnknownKeyError` — that would be scope creep. The up-front validation in `sync()` is the right place for the new exception.
- **Proportionality**: ~80 lines of production code for "filtered + parallel + error-collected in one call" with `find_prefix` + `UnknownKeyError` — appropriate for the narrowed gap. Not over-built.
