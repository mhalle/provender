# provender

Content-addressed blobs on an object store (S3, GCS, Azure, R2), for caches that several
machines share. Extracted from `haversack.objectcache` on 2026-09-20 so that a second
project could use the same store rather than write a parallel one.

```python
from provender import Blobs, check_store, open_store

store, prefix = open_store("s3://bucket/myproject")   # credentials from the environment
check_store(store, prefix, updates=False)             # blobs need create-if-absent only
blobs = Blobs(store, prefix)

blob = blobs.put_file("field.rkf")          # {"digest": "sha256:...", "size": 24_117_248}
blobs.has(blob["digest"])                   # True
blobs.fetch(blob["digest"], "copy.rkf")     # True; False if it is gone or was wrong
blobs.entries()                             # every blob under this prefix, with sizes
blobs.sweep(keep=live_digests)              # deletes unreferenced blobs over a day old
```

`check_store(store, prefix)` — without `updates=False` — also demands
replace-if-unchanged, which only the mutable half (0.2) needs. Ask for it if you will want
that half: S3, GCS, Azure, R2 and provender's own `DiskStore` all pass, while obstore's
`LocalStore` passes the blobs-only check and fails the other.

## On a local disk: `DiskStore`

`open_store("file:///path")` returns a `DiskStore`: a directory that honors **both**
conditional writes, so the same protocol runs on a laptop with no network as on a bucket,
and a copy between the two is a sync rather than a translation.

```python
from provender import DiskStore, check_store, ops

store = DiskStore("/var/cache/myproject")
check_store(store)                                    # passes, updates included
ops.put(store, "refs/k.json", b"{...}", mode="create")
meta = ops.head(store, "refs/k.json")
ops.put(store, "refs/k.json", b"{...v2}", mode={"e_tag": meta["e_tag"]})  # or PreconditionError
```

`provender.ops` has obstore's six calls - `get`, `put`, `head`, `copy`, `list`, `delete` -
with obstore's arguments, return shapes and exceptions, answered by a `DiskStore` itself
and passed to obstore for anything else. Code written against a bucket through `ops` runs
unchanged on a directory; `Blobs` is written that way.

How it keeps its promises:

- **Objects appear whole**: written to a temporary file, flushed, renamed into place. A
  reader holds the file it opened, whatever replaces it meanwhile.
- **Conditional writes are decided under an exclusive `flock`** (one of 1,024 lock files,
  chosen by the path's hash); only the comparison and the rename happen under it. A process
  that dies holding a lock releases it with its descriptors.
- **An ETag is the SHA-256 of the content** (up to 16 MB; S3's is the MD5), so it changes
  exactly when the bytes do - not through a reused inode or a coarse clock, both of which
  exFAT has.

Its limits: **one host** (`flock` is not trusted over a network filesystem - share through
a bucket); a path cannot be both an object and a prefix; names are case-sensitive only where
the filesystem is; and macOS's `._*` and `.DS_Store` files are neither listed nor
addressable. Tested on APFS and on a real exFAT volume, where obstore's `LocalStore` cannot
create-if-absent at all (its exclusive rename is unsupported there).

## What it is

A blob is named by the SHA-256 of its own content and written **only if absent**, so:

- identical bytes are stored once, whoever writes them;
- a blob never changes, so a reader holds nothing and no lock, lease or claim is needed;
- a fetch verifies what it read against the name, because the store does not.

## What it deliberately does not do

- **Look anything up by name.** A blob is addressed by its digest, so a client needs a map
  from its own names to digests before it can fetch anything, and a listing does not help:
  it lists hashes. That map is an index, and an index is the mutable half — see "Until 0.2"
  below.
- **Decide what is garbage.** `sweep` deletes what the CALLER says is unreferenced, and
  only under its own prefix. Knowing which blobs are live belongs to whoever writes the
  index — see "Prefixes" below. Two things are dangerous only on purpose: an empty live
  set is REFUSED (`EmptyKeepSet`) unless `allow_empty=True`, because "nothing is live" and
  "I could not read my index" arrive as the same value; and blobs younger than `grace_s`
  (a day by default) are spared, because every client writes a blob before the index entry
  that names it, and in that window a live blob looks exactly like garbage.
- **Delete a blob that failed verification.** A blob belongs to every result whose bytes
  were identical, and one client's bad read (a truncated stream reads the same as a corrupt
  object) is not grounds to remove it for everyone. It is remembered as suspect instead:
  this process stops deduplicating onto it, so the next write of those bytes replaces it,
  and until then reads of it fail cleanly.
- **Interpret your data.** Nothing here reads a blob's contents.

## Prefixes

One `Blobs` owns `<prefix>blobs/` and nothing else. A sweep lists only within that prefix,
so two projects sharing a bucket cannot account for - or collect - each other's bytes. The
cost is that they do not share identical bytes either, which is the right trade: a rule that
holds structurally beats one that holds by care.

## Errors

`fetch` answers **False** for a blob that is absent or does not match its name - a miss, not
an exception. Anything else the store raises (credentials, network, a 503) is raised, and
the caller decides: a read usually degrades to a miss, a write usually must not.

## Until 0.2: keep a manifest

A blobs-only client holds its own map from names to digests — a small JSON file, in git if
it carries no derived data. It does two jobs: the lookup, and the live set handed to
`sweep`. Give it the shape the 0.2 pointer body will have, and the migration is one upload:

```json
{"format": 1,
 "files": {"<your name>": {"digest": "sha256:...", "size": 24117248}},
 "body":  {"provenance": {"...": "whatever identifies this export"},
           "members": ["..."]}}
```

`files` is the part 0.2 reads, because garbage collection has to know which blobs an entry
references. `body` is yours and is never inspected.

## Deduplication is a bonus, not a property

Identical bytes are stored once, but do not design around it. Output that is deterministic
on one GPU and library stack can differ on another, so the "same" data may get two digests.
That is harmless — an index records whichever copy it published — as long as nothing
assumes a name maps to a digest it can predict.

## Status

0.1 is the blob half, plus (since 2026-09-23) the local-filesystem backend, `DiskStore`.
The mutable half - a pointer per key, replaced by compare-and-swap, with bounded history,
and an envelope whose `body` the package never reads - follows in 0.2. It exists in
haversack, and now has the second backend its interface was waiting to be cut against.
