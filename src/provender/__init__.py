"""Content-addressed blobs on an object store, for caches several machines share.

Extracted from ``haversack.objectcache`` (2026-09-20) so a second project could use the
store rather than grow a parallel copy of it. 0.1 is the blob half; the mutable half - one
pointer per key replaced by compare-and-swap, with bounded history - follows once its
interface has been cut against a local-filesystem backend as well as this one.

That backend is :class:`DiskStore` (2026-09-23): a directory that honors both conditional
writes, answering obstore's function API through :mod:`provender.ops`, so the same code
runs over a bucket and over a directory. ``open_store("file:///path")`` returns one.

Importing this module pulls the standard library only: obstore is imported inside the
functions that talk to a store.
"""
from . import ops
from .blobs import CHUNK, GRACE_S, Blobs, EmptyKeepSet, digest_file
from .disk import DiskStore
from .store import StoreUnsuitable, check_store, open_store, update_mode

__version__ = "0.1.5"
__all__ = ["Blobs", "CHUNK", "DiskStore", "GRACE_S", "EmptyKeepSet", "StoreUnsuitable",
           "check_store", "digest_file", "ops", "open_store", "update_mode", "__version__"]
