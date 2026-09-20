"""The blob half, against obstore's in-memory store.

Every test here came from haversack's `test_objectcache.py`, where these behaviours were
found - several of them by an adversarial review round on 2026-09-19 - and each pins one
property the extraction must not lose.
"""
from __future__ import annotations

import hashlib
import tempfile
import time
import unittest
import unittest.mock
from pathlib import Path

import pytest

obstore = pytest.importorskip("obstore")
from obstore.store import LocalStore, MemoryStore  # noqa: E402

from provender import (Blobs, EmptyKeepSet, StoreUnsuitable,  # noqa: E402
                       check_store, digest_file, open_store)


class _Fixture(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.store = MemoryStore()
        self.blobs = Blobs(self.store, "proj/")

    def tearDown(self):
        self._tmp.cleanup()

    def file(self, name: str, data: bytes) -> Path:
        p = self.tmp / name
        p.write_bytes(data)
        return p


class TestNaming(_Fixture):
    def test_a_blob_is_named_by_its_own_bytes(self):
        blob = self.blobs.put_file(self.file("a", b"hello"))
        self.assertEqual(f"sha256:{hashlib.sha256(b'hello').hexdigest()}", blob["digest"])
        self.assertEqual(5, blob["size"])
        self.assertEqual(blob["digest"], digest_file(self.file("a", b"hello")))

    def test_identical_bytes_are_one_object(self):
        self.blobs.put_file(self.file("a", b"same"))
        self.blobs.put_file(self.file("b", b"same"))
        self.assertEqual(1, len(self.blobs.iter()))

    def test_a_digest_may_not_address_anything_but_a_blob(self):
        for bad in ("sha256:../../elsewhere" + "a" * 44, "md5:" + "a" * 64, "", "sha256:zz",
                    "sha256:" + "A" * 64):
            with self.subTest(digest=bad), self.assertRaises(ValueError):
                self.blobs.path(bad)

    def test_a_prefix_owns_only_its_own_blobs(self):
        other = Blobs(self.store, "another/")
        mine = self.blobs.put_file(self.file("a", b"mine"))
        theirs = other.put_file(self.file("b", b"theirs"))
        self.assertEqual([mine["digest"]], [b["digest"] for b in self.blobs.iter()])
        self.blobs.sweep(keep=set(), allow_empty=True)   # takes everything of ITS own
        self.assertEqual([], self.blobs.iter())
        self.assertTrue(other.has(theirs["digest"]), "another project's bytes are not ours")


class TestFetch(_Fixture):
    def test_a_fetch_verifies_what_it_read(self):
        blob = self.blobs.put_file(self.file("a", b"payload"))
        dest = self.tmp / "out"
        self.assertTrue(self.blobs.fetch(blob["digest"], dest))
        self.assertEqual(b"payload", dest.read_bytes())

    def test_a_missing_blob_is_false_not_an_error(self):
        absent = f"sha256:{hashlib.sha256(b'never stored').hexdigest()}"
        self.assertFalse(self.blobs.fetch(absent, self.tmp / "out"))

    def test_a_blob_that_does_not_match_its_name_is_false_and_leaves_nothing(self):
        blob = self.blobs.put_file(self.file("a", b"payload"))
        obstore.put(self.store, self.blobs.path(blob["digest"]), b"tampered")
        dest = self.tmp / "out"
        self.assertFalse(self.blobs.fetch(blob["digest"], dest))
        self.assertFalse(dest.exists(), "a verified fetch never leaves unverified bytes")

    def test_a_mismatch_is_suspected_not_deleted_and_the_next_write_replaces_it(self):
        """One client's bad read is not grounds to delete bytes every writer of that
        content shares - a truncated stream reads exactly like a corrupt object."""
        blob = self.blobs.put_file(self.file("a", b"payload"))
        obstore.put(self.store, self.blobs.path(blob["digest"]), b"tampered")
        self.blobs.fetch(blob["digest"], self.tmp / "out")
        self.assertTrue(self.blobs.has(blob["digest"]), "not deleted")
        self.assertIn(blob["digest"], self.blobs.suspect)
        self.blobs.put_file(self.file("a", b"payload"))
        self.assertNotIn(blob["digest"], self.blobs.suspect)
        self.assertTrue(self.blobs.fetch(blob["digest"], self.tmp / "out2"))

    def test_a_store_fault_is_raised_not_swallowed(self):
        """A miss and "the bucket is unreachable" are different answers; only the CALLER
        knows whether its read may degrade."""
        from obstore.exceptions import PermissionDeniedError
        blob = self.blobs.put_file(self.file("a", b"payload"))
        with unittest.mock.patch.object(obstore, "get",
                                        side_effect=PermissionDeniedError("403")):
            with self.assertRaises(PermissionDeniedError):
                self.blobs.fetch(blob["digest"], self.tmp / "out")


class TestListingAndSweep(_Fixture):
    def test_iter_reports_size_and_time(self):
        blob = self.blobs.put_file(self.file("a", b"12345"))
        got = self.blobs.iter()
        self.assertEqual([blob["digest"]], [b["digest"] for b in got])
        self.assertEqual(5, got[0]["size"])
        self.assertLessEqual(got[0]["modified"], time.time() + 1)

    def test_older_than_excludes_the_young(self):
        self.blobs.put_file(self.file("a", b"fresh"))
        self.assertEqual([], self.blobs.iter(older_than=time.time() - 60))
        self.assertEqual(1, len(self.blobs.iter(older_than=time.time() + 60)))

    def test_sweep_deletes_only_what_the_caller_does_not_keep(self):
        live = self.blobs.put_file(self.file("a", b"live"))
        dead = self.blobs.put_file(self.file("b", b"dead"))
        got = self.blobs.sweep(keep={live["digest"]})
        self.assertEqual({"deleted": 1}, got)
        self.assertTrue(self.blobs.has(live["digest"]))
        self.assertFalse(self.blobs.has(dead["digest"]))

    def test_sweep_accepts_blob_records_as_well_as_digests(self):
        live = self.blobs.put_file(self.file("a", b"live"))
        self.blobs.sweep(keep=[live])
        self.assertTrue(self.blobs.has(live["digest"]))

    def test_sweep_honors_older_than(self):
        blob = self.blobs.put_file(self.file("a", b"young"))
        self.assertEqual({"deleted": 0}, self.blobs.sweep(
            keep=set(), allow_empty=True, older_than=time.time() - 60))
        self.assertTrue(self.blobs.has(blob["digest"]))

    def test_sweep_refuses_an_empty_live_set_unless_it_is_meant(self):
        """"Nothing is live" and "I could not read my index" arrive as the same value: an
        unreadable manifest, a failed listing, a client whose pointers do not exist yet.
        A client that stores its map of names to digests outside the store - which is what
        a blobs-only consumer does until the pointer half lands - would otherwise sweep
        its entire dataset the first time that map failed to load."""
        blob = self.blobs.put_file(self.file("a", b"precious"))
        with self.assertRaises(EmptyKeepSet):
            self.blobs.sweep(keep=set())
        with self.assertRaises(EmptyKeepSet):
            self.blobs.sweep(keep=[])
        self.assertTrue(self.blobs.has(blob["digest"]), "nothing was deleted")
        self.assertEqual({"deleted": 1}, self.blobs.sweep(keep=set(), allow_empty=True))

    def test_a_foreign_object_under_blobs_is_left_alone(self):
        obstore.put(self.store, "proj/blobs/sha256/notadigest", b"x")
        obstore.put(self.store, "proj/blobs/sha256/nested/thing", b"x")
        self.assertEqual([], self.blobs.iter())
        self.assertEqual({"deleted": 0}, self.blobs.sweep(keep=set(), allow_empty=True))


class TestStoreChecks(unittest.TestCase):
    def test_memory_store_passes_both_halves(self):
        check_store(MemoryStore(), "p/")
        check_store(MemoryStore(), "p/", updates=False)

    def test_a_local_filesystem_store_can_create_but_not_replace(self):
        """obstore's LocalStore implements create-if-absent and not replace-if-unchanged
        (measured 2026-09-19, obstore 0.11.1), so a blobs-only consumer may use it."""
        with tempfile.TemporaryDirectory() as d:
            check_store(LocalStore(d), updates=False)
            with self.assertRaises(StoreUnsuitable) as cm:
                check_store(LocalStore(d))
            self.assertIn("replace-if-unchanged", str(cm.exception))
            self.assertEqual([], [p for p in Path(d).rglob("*") if p.is_file()],
                             "the probe object goes even when the store is refused")

    def test_a_store_that_ignores_the_etag_is_refused(self):
        real_put = obstore.put

        def put(s, path, data, *, mode=None, **kw):
            return real_put(s, path, data, mode=None if isinstance(mode, dict) else mode, **kw)
        with unittest.mock.patch.object(obstore, "put", put):
            with self.assertRaises(StoreUnsuitable) as cm:
                check_store(MemoryStore())
        self.assertIn("stale etag", str(cm.exception))

    def test_a_store_that_overwrites_on_create_is_refused_even_blobs_only(self):
        real_put = obstore.put

        def put(s, path, data, *, mode=None, **kw):
            return real_put(s, path, data, mode=None if mode == "create" else mode, **kw)
        with unittest.mock.patch.object(obstore, "put", put):
            with self.assertRaises(StoreUnsuitable) as cm:
                check_store(MemoryStore(), updates=False)
        self.assertIn("create-if-absent", str(cm.exception))

    def test_open_store_parses_bucket_and_prefix(self):
        # memory:// has no bucket to name, so the whole path is prefix - which lets one
        # in-memory store hold several namespaces in a test
        self.assertEqual("bucket/p/q/", open_store("memory://bucket/p/q")[1])
        self.assertEqual("p/q/", open_store("s3://bucket/p/q")[1])
        self.assertEqual("", open_store("s3://bucket")[1])
        with self.assertRaises(ValueError):
            open_store("https://example.org/x")


class TestImportIsLight(unittest.TestCase):
    def test_importing_provender_pulls_no_store_client(self):
        """A consumer whose core imports only numpy must not get a credential chain and a
        socket library behind its back."""
        import subprocess
        import sys
        code = ("import sys, provender; "
                "assert 'obstore' not in sys.modules, sorted(m for m in sys.modules "
                "if 'obstore' in m); "
                "assert 'numpy' not in sys.modules; print('light')")
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
        self.assertEqual(0, out.returncode, out.stderr)
        self.assertIn("light", out.stdout)
