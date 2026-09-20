"""Bytes named by the SHA-256 of their own content.

The whole of the concurrency story is in the naming. A blob never changes, so a reader
holds nothing and needs no lease; two writers of the same bytes write the same object, so
neither needs a claim; and deleting one is at worst a miss, never the loss of something
someone was reading. Everything hard about a shared cache is in the MUTABLE index that
names blobs - which is a separate half, and not in this module.
"""
from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

#: Read and hashed in chunks of this size; also the streaming chunk for a fetch.
CHUNK = 1 << 20
#: How old a blob must be before a sweep may take it, by default. Covers the window every
#: client has between writing a blob and writing the index entry that names it.
GRACE_S = 24 * 3600


class EmptyKeepSet(ValueError):
    """``sweep`` was asked to keep nothing. See :meth:`Blobs.sweep`."""


def digest_file(path) -> str:
    """``sha256:<hex>`` of a file's contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def _hex(digest: str) -> str:
    algo, _, hexd = (digest or "").partition(":")
    if (algo != "sha256" or len(hexd) != 64
            or any(c not in "0123456789abcdef" for c in hexd)):
        raise ValueError(f"not a sha256 digest: {digest!r}")
    return hexd


class Blobs:
    """The blobs under ``<prefix>blobs/sha256/``. One instance owns exactly that, so two
    projects sharing a bucket can neither read nor collect each other's bytes."""

    def __init__(self, store, prefix: str = ""):
        self.store = store
        self.prefix = prefix
        self.base = f"{prefix}blobs/sha256/"
        #: Digests this process read and found wrong: never deduplicated onto again, so
        #: the next write of those bytes replaces the stored object. See `fetch`.
        self.suspect: set[str] = set()

    def path(self, digest: str) -> str:
        """Where a digest lives. Validated, so a digest that came from a document written
        by another machine cannot address anything but a blob under this prefix."""
        return f"{self.base}{_hex(digest)}"

    def put_file(self, src) -> dict:
        """Store a file's bytes unless they are already here; ``{"digest", "size"}``.

        Create-if-absent, so a concurrent write of the same bytes is not a conflict - it is
        the same object by definition, and whichever landed first is kept. A digest this
        process has found WRONG in the store is overwritten instead of deduplicated onto.

        The file is hashed and THEN uploaded, so a file rewritten in between is stored
        under the name of what it used to be. A later ``fetch`` catches that and suspects
        the blob, but the cheap fix is on the caller's side: write to a temporary name and
        rename it into place, then hand this the finished file.
        """
        import obstore
        from obstore.exceptions import AlreadyExistsError
        src = Path(src)
        blob = {"digest": digest_file(src), "size": src.stat().st_size}
        if blob["digest"] in self.suspect:
            obstore.put(self.store, self.path(blob["digest"]), src)
            self.suspect -= {blob["digest"]}
        elif not self.has(blob["digest"]):
            try:
                obstore.put(self.store, self.path(blob["digest"]), src, mode="create")
            except AlreadyExistsError:
                pass                           # someone else wrote the same bytes first
        return blob

    def put_bytes(self, data: bytes) -> dict:
        import obstore
        from obstore.exceptions import AlreadyExistsError
        blob = {"digest": f"sha256:{hashlib.sha256(data).hexdigest()}", "size": len(data)}
        if blob["digest"] in self.suspect:
            obstore.put(self.store, self.path(blob["digest"]), data)
            self.suspect -= {blob["digest"]}
        elif not self.has(blob["digest"]):
            try:
                obstore.put(self.store, self.path(blob["digest"]), data, mode="create")
            except AlreadyExistsError:
                pass
        return blob

    def has(self, digest: str) -> bool:
        import obstore
        try:
            obstore.head(self.store, self.path(digest))
        except FileNotFoundError:
            return False
        return True

    def fetch(self, digest: str, dest) -> bool:
        """Write the blob to ``dest``, verified against its name. False when it is gone or
        did not match; anything else the store raises comes out.

        Verified because the name is a promise the store does not keep for you: a truncated
        or corrupted object would otherwise be handed to the caller as its data.

        NOT deleted when it fails to match. This client cannot tell a corrupt object from a
        stream that ended early, and a blob belongs to every writer whose bytes were
        identical - deleting it here took unrelated entries down with it (haversack review,
        2026-09-19). It is remembered as suspect instead, which makes the next write of
        those bytes replace it.

        Written to a temporary name beside ``dest`` and renamed, so a reader of ``dest``
        sees either nothing or the whole verified blob.
        """
        import obstore
        dest = Path(dest)
        tmp = dest.with_name(f".{dest.name}.{uuid.uuid4().hex[:8]}.part")
        h = hashlib.sha256()
        try:
            with open(tmp, "wb") as f:
                for chunk in obstore.get(self.store, self.path(digest)).stream(
                        min_chunk_size=CHUNK):
                    h.update(chunk)
                    f.write(chunk)
            if f"sha256:{h.hexdigest()}" != digest:
                self.suspect.add(digest)
                return False
            tmp.replace(dest)
            return True
        except FileNotFoundError:
            return False
        finally:
            tmp.unlink(missing_ok=True)

    def entries(self, *, older_than: float | None = None):
        """Every blob under this prefix as ``{"digest", "size", "modified"}``, oldest first
        where the store reports a time.

        ``older_than`` (a unix time) keeps only blobs last modified before it - what a
        sweep wants, since a blob written moments ago may belong to an index entry that has
        not been written yet.
        """
        import datetime as _dt

        import obstore
        out = []
        for batch in obstore.list(self.store, self.base):
            for obj in batch:
                name = obj["path"][len(self.base):]
                if "/" in name or len(name) != 64:
                    continue                   # not one of ours; leave it alone
                modified = obj.get("last_modified")
                if isinstance(modified, _dt.datetime):
                    modified = modified.timestamp()
                if older_than is not None and (modified is None or modified >= older_than):
                    continue
                out.append({"digest": f"sha256:{name}", "size": obj.get("size"),
                            "modified": modified})
        out.sort(key=lambda b: (b["modified"] is None, b["modified"]))
        return out

    def sweep(self, *, keep, grace_s: float = GRACE_S, now: float | None = None,
              allow_empty: bool = False) -> dict:
        """Delete blobs under this prefix that ``keep`` does not contain and that are older
        than ``grace_s``.

        ``keep`` is the caller's set of live digests (or blob records). This module has no
        idea which blobs an index references, and guessing is how a garbage collector eats
        live data.

        **The grace is not optional decoration.** Every client writes the blob first and
        its index afterwards, so between those two a live blob is indistinguishable from
        garbage. A day is far longer than that window and costs only storage; sweeping
        without one deletes a blob uploaded a second ago (observed from the feldglas side,
        2026-09-20, when the extraction kept the mechanism and dropped the default).
        ``grace_s=0`` is allowed and means what it says.

        An EMPTY ``keep`` is refused unless ``allow_empty`` says so, because "nothing is
        live" and "I could not read my index" arrive here as the same value - an unreadable
        manifest, a listing that failed, a client whose index has not been written yet.
        Deleting everything is a legitimate request and a catastrophic accident, so it has
        to be said on purpose (``EmptyKeepSet``). Both defaults follow one rule: cleanup
        refuses what it cannot account for, and anything dangerous is done on purpose.
        """
        import time as _time

        import obstore
        live = {d if isinstance(d, str) else d["digest"] for d in keep}
        if not live and not allow_empty:
            raise EmptyKeepSet(
                "sweep was given no live digests: pass allow_empty=True to mean "
                "'delete every blob under this prefix', or fix the caller that could "
                "not read its index")
        cutoff = (_time.time() if now is None else now) - grace_s
        deleted, spared = 0, 0
        for blob in self.entries(older_than=cutoff):
            if blob["digest"] in live:
                continue
            try:
                obstore.delete(self.store, self.path(blob["digest"]))
            except FileNotFoundError:
                spared += 1                    # already gone: not this sweep's doing
                continue
            deleted += 1
        return {"deleted": deleted, "already_gone": spared}
