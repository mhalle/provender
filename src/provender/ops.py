"""obstore's six calls, answered by whichever backend a store is.

The same names, arguments, return shapes and exceptions as obstore's functions of those
names: a :class:`~provender.disk.DiskStore` answers itself, anything else goes to obstore.
So code written as ``ops.put(store, path, data, mode=...)`` runs unchanged over a bucket
and over a directory - which is the point of having one protocol.

obstore is imported inside each call, never at module scope (see ``provender.store``).
"""
from __future__ import annotations


def _disk(store) -> bool:
    from .disk import DiskStore
    return isinstance(store, DiskStore)


def get(store, path, *, options=None):
    if _disk(store):
        if options:
            raise NotImplementedError("DiskStore.get takes no options")
        return store.get(path)
    import obstore
    return obstore.get(store, path, options=options)


def put(store, path, file, *, mode=None, **kwargs):
    if _disk(store):
        return store.put(path, file, mode=mode)
    import obstore
    return obstore.put(store, path, file, mode=mode, **kwargs)


def head(store, path):
    if _disk(store):
        return store.head(path)
    import obstore
    return obstore.head(store, path)


def copy(store, from_, to, *, overwrite=True):
    if _disk(store):
        return store.copy(from_, to, overwrite=overwrite)
    import obstore
    return obstore.copy(store, from_, to, overwrite=overwrite)


def list(store, prefix=None, *, offset=None, chunk_size=50):   # noqa: A001 - obstore's name
    if _disk(store):
        return store.list(prefix, offset=offset, chunk_size=chunk_size)
    import obstore
    return obstore.list(store, prefix, offset=offset, chunk_size=chunk_size)


def delete(store, paths):
    if _disk(store):
        return store.delete(paths)
    import obstore
    return obstore.delete(store, paths)
