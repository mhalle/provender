"""A store on the local filesystem that honors BOTH conditional writes.

obstore's own ``LocalStore`` can create-if-absent but cannot replace-if-unchanged
(``check_store`` refuses it, measured), so the mutable half of a cache - one pointer per key
replaced by compare-and-swap - could only live on a bucket. That made a network necessary
for the one configuration that must never need one: a single machine, offline. This is the
backend that closes it, so a directory and a bucket carry the SAME protocol and a copy
between them is a sync, not a translation.

It answers obstore's function API (``provender.ops`` dispatches to it): ``get``, ``put``
with ``mode`` ``"overwrite"``/``"create"``/``{"e_tag": ...}``, ``head``, ``copy``,
``list`` and ``delete``, in the same shapes and raising the same exceptions
(``FileNotFoundError``, obstore's ``AlreadyExistsError`` and ``PreconditionError``). Code
written against a bucket runs here unchanged.

How the guarantees are made:

- **Every object appears whole.** A write goes to a temporary file beside its target, is
  flushed to disk, and is renamed into place; a reader opens either the old file or the new
  one, never a mixture. An open file survives its replacement, so a reader needs no lock.
- **Conditional writes are decided under a lock.** Every write to a path - including an
  unconditional one and a delete, either of which could otherwise slip between another
  writer's comparison and its rename - takes an exclusive ``flock`` on one of
  :data:`LOCK_STRIPES` lock files chosen by the path's hash. The expensive part, writing the
  bytes, happens BEFORE the lock; only the comparison and the rename happen under it. A
  process that dies holding a lock releases it with its descriptors, so there is no stale
  lock to prove dead.
- **An ETag is the content's SHA-256** for any object up to :data:`HASHED_ETAG_MAX` - the
  semantics S3's own ETag has (the MD5 of the bytes). It changes whenever the bytes do and
  cannot suffer ABA through a reused inode number or a coarse clock - exFAT keeps
  timestamps to 10 ms and synthesizes inode numbers, and "inode-mtime-size" (what
  ``LocalStore`` uses) could repeat there. Larger objects get "inode-mtime-size": they are
  blobs, which are never replaced conditionally, and hashing them on every ``head`` would
  cost a read of the whole file.

What it does not do:

- **One host.** ``flock`` is not trusted across a network filesystem; machines share through
  a bucket, never through one directory mounted twice.
- **A path cannot be both an object and a prefix** ("a" and "a/b"): a filesystem cannot hold
  a file and a directory under one name, where a bucket can. Nothing in the protocol does it.
- **Names are case-sensitive only where the filesystem is.** On APFS or exFAT (case-folding)
  "A" and "a" are one object. The protocol names everything in lowercase hex.
- **A write interrupted by a crash leaves a temporary file** (``*.provender-tmp``). It is
  never listed, read or served; removing old ones is housekeeping, not correctness.
- **macOS's own files are not objects**: ``._<name>`` (AppleDouble, written beside every
  file on exFAT and FAT32) and ``.DS_Store``. See :func:`_os_metadata`.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import os
import stat as _stat
import uuid
from contextlib import contextmanager
from pathlib import Path

#: How many lock files a store's writes are spread over. A fixed count, so a store of a
#: million blobs does not grow a million lock files; two paths sharing a stripe only wait
#: for each other's rename, which is microseconds.
LOCK_STRIPES = 1024
#: Objects up to this size carry a content ETag (see the module docstring). A pointer is a
#: few kilobytes; this is three orders of magnitude of margin.
HASHED_ETAG_MAX = 16 << 20
#: A write in progress, or one a crash interrupted. Never listed, read or addressable.
TMP_SUFFIX = ".provender-tmp"
#: The store's own state (its lock files), at the root and outside every object path.
STATE_DIR = ".provender"
_CHUNK = 1 << 20


def _os_metadata(name: str) -> bool:
    """A file the operating system put there, not an object. macOS writes ``._<name>``
    (AppleDouble: the extended attributes of ``<name>``) beside every file on a filesystem
    without native xattrs - exFAT, FAT32, SMB shares - and ``.DS_Store`` wherever Finder
    looks. Listed as objects, a ``._<key>.json`` read as an unreadable pointer, and a sweep
    that refuses to run while any pointer is unreadable then never ran on exFAT (found
    running this suite on a real exFAT volume, 2026-09-23). Neither listed nor addressable."""
    return name.startswith("._") or name == ".DS_Store"


class DiskStore:
    """A directory holding objects under slash-separated paths, as a bucket holds keys."""

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._locks = self.root / STATE_DIR / "locks"

    def __repr__(self) -> str:
        return f"DiskStore({str(self.root)!r})"

    # -- paths and locks -----------------------------------------------------------------

    def _file(self, path: str) -> Path:
        """The file for an object path, refusing any path that could leave the root, reach
        the store's own state, or name a write in progress."""
        if (not isinstance(path, str) or not path or path.startswith("/")
                or "\\" in path or "\0" in path):
            raise ValueError(f"not an object path: {path!r}")
        parts = path.split("/")
        if (any(p in ("", ".", "..") or _os_metadata(p) for p in parts)
                or parts[0] == STATE_DIR or parts[-1].endswith(TMP_SUFFIX)):
            raise ValueError(f"not an object path: {path!r}")
        return self.root.joinpath(*parts)

    @contextmanager
    def _locked(self, path: str):
        import fcntl
        stripe = int.from_bytes(hashlib.sha256(path.encode("utf-8")).digest()[:4], "big")
        self._locks.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._locks / f"{stripe % LOCK_STRIPES:04d}", os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)     # released when fd closes, or its process dies
            yield
        finally:
            os.close(fd)

    # -- reading -------------------------------------------------------------------------

    def _open(self, path: str) -> tuple[int, os.stat_result]:
        """``(fd, stat)`` of the object, or FileNotFoundError - also for a directory, which
        a bucket would call "no such object"."""
        fd = os.open(self._file(path), os.O_RDONLY)
        st = os.fstat(fd)
        if not _stat.S_ISREG(st.st_mode):
            os.close(fd)
            raise FileNotFoundError(f"no object at {path!r}")
        return fd, st

    def _meta(self, path: str, fd: int, st: os.stat_result) -> dict:
        return {"path": path, "last_modified": _when(st), "size": st.st_size,
                "e_tag": _etag(fd, st), "version": None}

    def head(self, path: str) -> dict:
        fd, st = self._open(path)
        try:
            return self._meta(path, fd, st)
        finally:
            os.close(fd)

    def get(self, path: str) -> "_Got":
        fd, st = self._open(path)
        try:
            return _Got(fd, self._meta(path, fd, st))
        except BaseException:
            os.close(fd)
            raise

    def list(self, prefix: str | None = None, *, offset: str | None = None,
             chunk_size: int = 50) -> "_Listing":
        """Every object under ``prefix``, in path order, in batches of ``chunk_size``.

        ``prefix`` matches whole path segments, as obstore's does: "a" lists "a/b" and never
        "ab/c". Listings carry no ETag (``e_tag`` is None) - hashing every object to list it
        would read the whole store; ``head`` one object for its ETag."""
        base = self.root
        if prefix:
            base = self._file(prefix.rstrip("/"))
        found: list[dict] = []
        if base.is_dir():
            for dirpath, dirnames, filenames in os.walk(base):
                if Path(dirpath) == self.root and STATE_DIR in dirnames:
                    dirnames.remove(STATE_DIR)
                dirnames[:] = [d for d in dirnames if not _os_metadata(d)]
                for name in filenames:
                    if name.endswith(TMP_SUFFIX) or _os_metadata(name):
                        continue
                    full = Path(dirpath) / name
                    try:
                        st = full.stat()
                    except FileNotFoundError:
                        continue               # deleted while we walked: not listed
                    if not _stat.S_ISREG(st.st_mode):
                        continue
                    rel = full.relative_to(self.root).as_posix()
                    if offset is not None and rel <= offset:
                        continue
                    found.append({"path": rel, "last_modified": _when(st),
                                  "size": st.st_size, "e_tag": None, "version": None})
        found.sort(key=lambda o: o["path"])
        return _Listing(found, max(1, int(chunk_size)))

    # -- writing -------------------------------------------------------------------------

    def put(self, path: str, file, *, mode=None) -> dict:
        """Write an object; ``{"e_tag", "version"}``.

        ``mode``: None or ``"overwrite"`` replaces whatever is there; ``"create"`` raises
        ``AlreadyExistsError`` if anything is; ``{"e_tag": tag}`` replaces only while the
        current ETag is ``tag`` and raises ``PreconditionError`` otherwise - including when
        the object is absent, as a bucket does.

        ``file`` is bytes (or any buffer), a path (``os.PathLike``) whose contents are
        copied, or an object with ``read()``. A plain ``str`` is refused: it could mean
        either of the first two."""
        from obstore.exceptions import AlreadyExistsError, PreconditionError
        if not (mode is None or mode in ("overwrite", "create")
                or (isinstance(mode, dict) and isinstance(mode.get("e_tag"), str))):
            raise ValueError(f"unsupported put mode: {mode!r}")
        target = self._file(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.{uuid.uuid4().hex[:12]}{TMP_SUFFIX}")
        try:
            etag = _write(tmp, file)           # the slow part, outside the lock
            with self._locked(path):
                if mode == "create":
                    if target.exists():
                        raise AlreadyExistsError(f"object exists: {path!r}")
                elif isinstance(mode, dict):
                    current = self._current_etag(path)
                    if current != mode["e_tag"]:
                        raise PreconditionError(
                            f"{path!r}: expected ETag {mode['e_tag']}, found "
                            f"{current if current is not None else 'no object'}")
                os.replace(tmp, target)
        finally:
            tmp.unlink(missing_ok=True)
        return {"e_tag": etag, "version": None}

    def _current_etag(self, path: str) -> str | None:
        try:
            fd, st = self._open(path)
        except FileNotFoundError:
            return None
        try:
            return _etag(fd, st)
        finally:
            os.close(fd)

    def copy(self, from_: str, to: str, *, overwrite: bool = True) -> None:
        """Copy an object. Onto ITSELF it refreshes the modification time and moves no bytes
        - what ``Blobs.touch`` asks of a bucket's server-side copy."""
        if from_ == to:
            target = self._file(to)
            with self._locked(to):
                fd, st = self._open(to)
                os.close(fd)
                if not overwrite:
                    from obstore.exceptions import AlreadyExistsError
                    raise AlreadyExistsError(f"object exists: {to!r}")
                os.utime(target)
            return
        src = self._file(from_)
        fd, _ = self._open(from_)              # FileNotFoundError for a missing source
        os.close(fd)
        self.put(to, src, mode=None if overwrite else "create")

    def delete(self, paths) -> None:
        """Remove objects. An absent one is not an error, as on a bucket."""
        for path in ([paths] if isinstance(paths, str) else list(paths)):
            target = self._file(path)
            with self._locked(path):
                try:
                    if not _stat.S_ISREG(target.lstat().st_mode):
                        continue               # a prefix, not an object
                    target.unlink()
                except FileNotFoundError:
                    pass


class _Got:
    """``obstore.get``'s result: ``meta``, ``bytes()``, ``stream(min_chunk_size=)``. It
    holds the file OPEN, so what it returns is the object as it was when opened, whatever
    replaces it meanwhile."""

    def __init__(self, fd: int, meta: dict):
        self._fd = fd
        self.meta = meta

    def _close(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    def __del__(self):
        self._close()

    def bytes(self) -> bytes:
        try:
            return b"".join(self._chunks(_CHUNK))
        finally:
            self._close()

    def stream(self, min_chunk_size: int = _CHUNK):
        try:
            yield from self._chunks(max(1, int(min_chunk_size)))
        finally:
            self._close()

    def _chunks(self, size: int):
        if self._fd is None:
            raise ValueError("this result has already been read")
        offset = 0
        while True:
            chunk = os.pread(self._fd, size, offset)
            if not chunk:
                return
            offset += len(chunk)
            yield chunk


class _Listing:
    """``obstore.list``'s result: iterable in batches, and ``collect()`` for all of it."""

    def __init__(self, items: list, chunk_size: int):
        self._items, self._chunk = items, chunk_size

    def __iter__(self):
        for i in range(0, len(self._items), self._chunk):
            yield self._items[i:i + self._chunk]

    def collect(self) -> list:
        return list(self._items)


def _when(st: os.stat_result) -> _dt.datetime:
    return _dt.datetime.fromtimestamp(st.st_mtime_ns / 1e9, tz=_dt.timezone.utc)


def _etag(fd: int, st: os.stat_result) -> str:
    if st.st_size > HASHED_ETAG_MAX:
        return f'"{st.st_ino:x}-{st.st_mtime_ns:x}-{st.st_size:x}"'
    h = hashlib.sha256()
    offset = 0
    while True:
        chunk = os.pread(fd, _CHUNK, offset)
        if not chunk:
            break
        h.update(chunk)
        offset += len(chunk)
    return f'"{h.hexdigest()}"'


def _write(tmp: Path, file) -> str:
    """Write ``file`` to ``tmp``, flushed to disk; the ETag it will have once renamed (a
    rename changes neither the bytes, the inode nor the modification time)."""
    if isinstance(file, str):
        raise TypeError("put: pass bytes, a pathlib.Path, or a readable file - a str could "
                        "be either of the first two")
    fd = os.open(tmp, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o644)   # read back for the ETag
    try:
        if isinstance(file, os.PathLike):
            with open(file, "rb") as src:
                for chunk in iter(lambda: src.read(_CHUNK), b""):
                    _write_all(fd, chunk)
        elif hasattr(file, "read"):
            for chunk in iter(lambda: file.read(_CHUNK), b""):
                _write_all(fd, chunk)
        else:
            _write_all(fd, memoryview(file).cast("B"))
        os.fsync(fd)
        return _etag(fd, os.fstat(fd))
    finally:
        os.close(fd)


def _write_all(fd: int, data) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]
