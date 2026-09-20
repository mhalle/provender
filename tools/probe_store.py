"""Check a real bucket: conditional writes, then a blob round trip under a throwaway prefix.

    AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \\
    AWS_ENDPOINT=https://<account>.r2.cloudflarestorage.com AWS_REGION=auto \\
    python tools/probe_store.py s3://BUCKET/provender-probe

Whether an S3-compatible service honors create-if-absent is exactly what its marketing does
not say, so this asks it. Everything is written under `<prefix>/run-<id>/` and deleted at
the end, whatever happened.
"""
from __future__ import annotations

import sys
import tempfile
import uuid
from pathlib import Path


def main(url: str) -> int:
    import obstore

    from provender import Blobs, check_store, open_store

    store, prefix = open_store(url)
    prefix = f"{prefix}run-{uuid.uuid4().hex[:8]}/"
    print(f"store {type(store).__name__}, prefix {prefix}")
    try:
        check_store(store, prefix, updates=False)
        print("ok   create-if-absent honored")
        check_store(store, prefix)
        print("ok   replace-if-unchanged honored (the mutable half will work here too)")
        blobs = Blobs(store, prefix)
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            src = d / "payload"
            src.write_bytes(b"provender probe " + uuid.uuid4().bytes * 1000)
            blob = blobs.put_file(src)
            assert blobs.has(blob["digest"])
            again = blobs.put_file(src)
            assert again == blob
            print(f"ok   put and dedupe ({blob['size']} bytes)")
            out = d / "copy"
            assert blobs.fetch(blob["digest"], out) and out.read_bytes() == src.read_bytes()
            print("ok   fetch verified against the digest")
            listed = blobs.iter()
            assert [b["digest"] for b in listed] == [blob["digest"]], listed
            print("ok   list")
            assert blobs.sweep(keep=set()) == {"deleted": 1}
            assert not blobs.has(blob["digest"])
            print("ok   sweep deletes what the caller does not keep")
        return 0
    finally:
        n = 0
        for batch in obstore.list(store, prefix):
            for obj in batch:
                obstore.delete(store, obj["path"])
                n += 1
        print(f"cleaned {n} object(s) under {prefix}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1]))
