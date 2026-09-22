"""Content-addressed blobs on an object store, for caches several machines share.

Extracted from ``haversack.objectcache`` (2026-09-20) so a second project could use the
store rather than grow a parallel copy of it. 0.1 is the blob half; the mutable half - one
pointer per key replaced by compare-and-swap, with bounded history - follows once its
interface has been cut against a local-filesystem backend as well as this one.

Importing this module pulls the standard library only: obstore is imported inside the
functions that talk to a store.
"""
from .blobs import CHUNK, GRACE_S, Blobs, EmptyKeepSet, digest_file
from .store import StoreUnsuitable, check_store, open_store, update_mode

__version__ = "0.1.5"
__all__ = ["Blobs", "CHUNK", "GRACE_S", "EmptyKeepSet", "StoreUnsuitable", "check_store",
           "digest_file", "open_store", "update_mode", "__version__"]
