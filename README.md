# provender

Content-addressed blobs on an object store (S3, GCS, Azure, R2), for caches that several
machines share. Extracted from `haversack.objectcache` on 2026-09-20 so that a second
project could use the same store rather than write a parallel one.

```python
from provender import Blobs, check_store, open_store

store, prefix = open_store("s3://bucket/myproject")   # credentials from the environment
check_store(store, prefix)                            # refuse a store that cannot do this
blobs = Blobs(store, prefix)

blob = blobs.put_file("field.rkf")          # {"digest": "sha256:...", "size": 24_117_248}
blobs.has(blob["digest"])                   # True
blobs.fetch(blob["digest"], "copy.rkf")     # True; False if it is gone or was wrong
list(blobs.iter())                          # every blob under this prefix, with sizes
blobs.sweep(keep={blob["digest"]})          # delete what the caller says is unreferenced
```

## What it is

A blob is named by the SHA-256 of its own content and written **only if absent**, so:

- identical bytes are stored once, whoever writes them;
- a blob never changes, so a reader holds nothing and no lock, lease or claim is needed;
- a fetch verifies what it read against the name, because the store does not.

## What it deliberately does not do

- **Decide what is garbage.** `sweep` deletes what the CALLER says is unreferenced, and
  only under its own prefix. Knowing which blobs are live belongs to whoever writes the
  index — see "Prefixes" below.
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

## Status

0.1 is the blob half. The mutable half - a pointer per key, replaced by compare-and-swap,
with bounded history - follows in 0.2, together with a local-filesystem backend behind the
same interfaces. Both already exist in haversack and move here once their interfaces are
cut against a second backend.
