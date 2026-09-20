"""Check a real bucket: conditional writes, then blob round trips under a throwaway prefix.

    AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \\
    AWS_ENDPOINT=https://<account>.r2.cloudflarestorage.com AWS_REGION=auto \\
    python tools/probe_store.py s3://BUCKET/provender-probe

Whether an S3-compatible service honors create-if-absent is exactly what its marketing does
not say, so this asks it - and asks it at a REALISTIC SIZE too, because a put large enough
to become a multipart upload is a different code path in the store and in obstore, and no
documentation I could find states how a conditional put interacts with one. Everything is
written under `<prefix>/run-<id>/` and deleted at the end, whatever happened.

``--size-mb 0`` skips the large blob (the in-memory store in the tests uses this).
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path


def probe(url: str, *, size_mb: int = 32, say=print) -> int:
    import obstore

    from provender import Blobs, check_store, open_store

    store, prefix = open_store(url)
    prefix = f"{prefix}run-{uuid.uuid4().hex[:8]}/"
    say(f"store {type(store).__name__}, prefix {prefix}")
    try:
        check_store(store, prefix, updates=False)
        say("ok   create-if-absent honored (all a blobs-only client needs)")
        try:
            check_store(store, prefix)
            say("ok   replace-if-unchanged honored (the mutable half will work here too)")
        except Exception as e:                 # noqa: BLE001 - blobs still work without it
            say(f"note no replace-if-unchanged: {e}")
        blobs = Blobs(store, prefix)
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            src = d / "payload"
            src.write_bytes(b"provender probe " + uuid.uuid4().bytes * 1000)
            blob = blobs.put_file(src)
            assert blobs.has(blob["digest"])
            assert blobs.put_file(src) == blob
            say(f"ok   put and dedupe ({blob['size']} bytes)")
            out = d / "copy"
            assert blobs.fetch(blob["digest"], out) and out.read_bytes() == src.read_bytes()
            say("ok   fetch verified against the digest")
            listed = blobs.entries()
            assert [b["digest"] for b in listed] == [blob["digest"]], listed
            say("ok   list")

            if size_mb:
                big = d / "field"
                big.write_bytes(os.urandom(size_mb << 20))   # incompressible, like a field
                t = time.perf_counter()
                rec = blobs.put_file(big)
                up = time.perf_counter() - t
                say(f"ok   create-if-absent at {size_mb} MB "
                    f"({up:.1f}s, {size_mb / up:.1f} MB/s) - multipart territory")
                t = time.perf_counter()
                assert blobs.put_file(big) == rec, "a second put must dedupe, not conflict"
                say(f"ok   re-put dedupes at that size ({time.perf_counter() - t:.2f}s)")
                back = d / "field-back"
                t = time.perf_counter()
                assert blobs.fetch(rec["digest"], back)
                down = time.perf_counter() - t
                assert back.read_bytes() == big.read_bytes()
                say(f"ok   verified fetch at {size_mb} MB "
                    f"({down:.1f}s, {size_mb / down:.1f} MB/s)")

            live = {b["digest"] for b in blobs.entries()}
            assert blobs.sweep(keep=live, grace_s=0)["deleted"] == 0, "live blobs stay"
            say("ok   sweep spares what the caller keeps")
            assert blobs.sweep(keep=live)["deleted"] == 0, "and the default grace spares all"
            say("ok   the default grace spares a blob written moments ago")
            gone = blobs.sweep(keep=set(), grace_s=0, allow_empty=True)["deleted"]
            assert gone == len(live), (gone, live)
            assert blobs.entries() == []
            say(f"ok   sweep deletes what the caller does not keep ({gone})")
        return 0
    finally:
        n = 0
        for batch in obstore.list(store, prefix):
            for obj in batch:
                obstore.delete(store, obj["path"])
                n += 1
        say(f"cleaned {n} object(s) under {prefix}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("url", help="s3://bucket/prefix, gs://..., az://..., memory://...")
    ap.add_argument("--size-mb", type=int, default=32,
                    help="the field-sized blob, in MB; 0 skips it (default: 32)")
    a = ap.parse_args(argv)
    return probe(a.url, size_mb=a.size_mb)


if __name__ == "__main__":
    sys.exit(main())
