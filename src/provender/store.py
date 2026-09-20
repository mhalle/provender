"""Opening an object store, and refusing one that cannot carry a shared cache.

``obstore`` is imported inside the functions, never at module scope: a consumer whose core
imports only numpy should be able to ``import provender`` without a store client, its
credential chain or its sockets coming along.
"""
from __future__ import annotations

import uuid
from pathlib import Path


class StoreUnsuitable(ValueError):
    """The store cannot carry the protocol - it does not honor conditional writes."""


def open_store(url: str):
    """``(store, prefix)`` for ``s3://bucket/prefix``, ``gs://...``, ``az://...``,
    ``file:///path`` or ``memory://``.

    Credentials come from the environment, as obstore reads them (``AWS_*``, ``GOOGLE_*``,
    ``AZURE_*``); nothing here takes a secret as an argument. ``memory://`` is
    process-local and exists for tests.

    The HOST is whatever the caller names. That is deliberate: this is a store you
    configured, not a URL from a request, and nothing in here should ever be handed one.
    """
    from urllib.parse import urlparse
    u = urlparse(url)
    scheme = u.scheme.lower()
    if scheme == "memory":
        from obstore.store import MemoryStore
        return MemoryStore(), _prefix(u.netloc + u.path)
    if scheme == "file":
        # the whole path is the ROOT, and there is no prefix: a directory already is one.
        # An earlier version read a prefix out of the URL fragment, which nobody would
        # guess and nothing tested (feldglas review, 2026-09-20). Pass a prefix to `Blobs`
        # if you want to namespace inside a directory.
        from obstore.store import LocalStore
        root = Path(u.path)
        root.mkdir(parents=True, exist_ok=True)
        return LocalStore(root), ""
    if scheme in ("s3", "s3a", "gs", "az", "abfs", "abfss") and u.netloc:
        from obstore.store import from_url
        return from_url(f"{scheme}://{u.netloc}"), _prefix(u.path)
    raise ValueError(f"store {url!r}: expected s3://bucket[/prefix], gs://..., az://..., "
                     "file:///path or memory://")


def _prefix(path: str) -> str:
    p = (path or "").strip("/")
    return f"{p}/" if p else ""


def check_store(store, prefix: str = "", *, updates: bool = True) -> None:
    """Refuse a store on which this protocol would silently lose writes.

    Asked of the store rather than assumed, because the answer varies and the docs do not
    say: obstore's own local-filesystem store cannot replace an object conditionally, and
    S3-compatible services differ (Cloudflare R2 and AWS honor both; some ignore the
    headers entirely, which would let two writers each believe they had won).

    - ``blobs`` need create-if-absent, and a store that OVERWRITES on it is refused.
    - ``updates`` (the mutable half: one pointer replaced by compare-and-swap) additionally
      needs replace-if-unchanged to succeed with the current etag and to FAIL with a stale
      one. Pass False if you only ever write blobs.
    """
    import obstore
    from obstore.exceptions import (AlreadyExistsError, NotSupportedError,
                                    PreconditionError)
    path = f"{prefix}.probe/{uuid.uuid4().hex}"
    name = type(store).__name__

    def refuse(why: str) -> StoreUnsuitable:
        return StoreUnsuitable(
            f"store ({name}) {why}: a shared cache needs create-if-absent"
            + (" and replace-if-unchanged writes" if updates else " writes")
            + "; S3, GCS, Azure and R2 do this, a local filesystem store does not")
    try:
        try:
            obstore.put(store, path, b"0", mode="create")
        except (NotImplementedError, NotSupportedError, TypeError) as e:
            raise refuse(f"cannot create-if-absent ({e})") from None
        try:
            obstore.put(store, path, b"1", mode="create")
        except AlreadyExistsError:
            pass
        except (NotImplementedError, NotSupportedError, TypeError) as e:
            raise refuse(f"cannot create-if-absent ({e})") from None
        else:
            raise refuse("overwrote an existing object on create-if-absent")
        if not updates:
            return
        stale = update_mode(obstore.head(store, path))
        try:
            obstore.put(store, path, b"2", mode=stale)
        except (NotImplementedError, NotSupportedError, TypeError) as e:
            raise refuse(f"cannot replace-if-unchanged ({e})") from None
        except PreconditionError:
            raise refuse("refused a replace carrying the current etag") from None
        try:
            obstore.put(store, path, b"3", mode=stale)
        except PreconditionError:
            pass
        else:
            raise refuse("accepted a replace carrying a stale etag")
    finally:
        try:
            obstore.delete(store, path)
        except Exception:                      # noqa: BLE001 - a probe left behind is litter
            pass


def update_mode(meta) -> dict:
    """The replace-if-unchanged mode for an object whose metadata is ``meta``.

    ``version`` only when the store reports one: obstore refuses None for it, and a store
    without object versions (most) reports none.
    """
    mode = {"e_tag": meta["e_tag"]}
    if meta.get("version") is not None:
        mode["version"] = meta["version"]
    return mode
