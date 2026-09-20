"""The blob half, against obstore's in-memory store.

Every test here came from haversack's `test_objectcache.py`, where these behaviours were
found - several of them by an adversarial review round on 2026-09-19 - and each pins one
property the extraction must not lose.
"""
from __future__ import annotations

import hashlib
import sys
import tempfile
import time
import unittest
import unittest.mock
from pathlib import Path

import pytest

obstore = pytest.importorskip("obstore")
from obstore.store import LocalStore, MemoryStore  # noqa: E402

from provender import (GRACE_S, Blobs, EmptyKeepSet, StoreUnsuitable,  # noqa: E402
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
        self.assertEqual(1, len(self.blobs.entries()))

    def test_a_digest_may_not_address_anything_but_a_blob(self):
        for bad in ("sha256:../../elsewhere" + "a" * 44, "md5:" + "a" * 64, "", "sha256:zz",
                    "sha256:" + "A" * 64):
            with self.subTest(digest=bad), self.assertRaises(ValueError):
                self.blobs.path(bad)

    def test_a_prefix_owns_only_its_own_blobs(self):
        other = Blobs(self.store, "another/")
        mine = self.blobs.put_file(self.file("a", b"mine"))
        theirs = other.put_file(self.file("b", b"theirs"))
        self.assertEqual([mine["digest"]], [b["digest"] for b in self.blobs.entries()])
        self.blobs.sweep(keep=set(), grace_s=0, allow_empty=True)   # everything of ITS own
        self.assertEqual([], self.blobs.entries())
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
        got = self.blobs.entries()
        self.assertEqual([blob["digest"]], [b["digest"] for b in got])
        self.assertEqual(5, got[0]["size"])
        self.assertLessEqual(got[0]["modified"], time.time() + 1)

    def test_older_than_excludes_the_young(self):
        self.blobs.put_file(self.file("a", b"fresh"))
        self.assertEqual([], self.blobs.entries(older_than=time.time() - 60))
        self.assertEqual(1, len(self.blobs.entries(older_than=time.time() + 60)))

    def test_sweep_deletes_only_what_the_caller_does_not_keep(self):
        live = self.blobs.put_file(self.file("a", b"live"))
        dead = self.blobs.put_file(self.file("b", b"dead"))
        got = self.blobs.sweep(keep={live["digest"]}, grace_s=0)
        self.assertEqual({"deleted": 1, "already_gone": 0, "refreshed": 0}, got)
        self.assertTrue(self.blobs.has(live["digest"]))
        self.assertFalse(self.blobs.has(dead["digest"]))

    def test_sweep_accepts_blob_records_as_well_as_digests(self):
        live = self.blobs.put_file(self.file("a", b"live"))
        self.blobs.sweep(keep=[live], grace_s=0)
        self.assertTrue(self.blobs.has(live["digest"]))

    def test_sweep_spares_a_blob_inside_the_grace(self):
        blob = self.blobs.put_file(self.file("a", b"young"))
        self.assertEqual({"deleted": 0, "already_gone": 0, "refreshed": 0},
                         self.blobs.sweep(keep=set(), allow_empty=True, grace_s=60))
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
        self.assertEqual({"deleted": 1, "already_gone": 0, "refreshed": 0},
                         self.blobs.sweep(keep=set(), grace_s=0, allow_empty=True))

    def test_a_foreign_object_under_blobs_is_left_alone(self):
        obstore.put(self.store, "proj/blobs/sha256/notadigest", b"x")
        obstore.put(self.store, "proj/blobs/sha256/nested/thing", b"x")
        self.assertEqual([], self.blobs.entries())
        self.assertEqual({"deleted": 0, "already_gone": 0, "refreshed": 0},
                         self.blobs.sweep(keep=set(), grace_s=0, allow_empty=True))


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


# -- what the feldglas review found (2026-09-20) ----------------------------------------


class TestSweepGrace(_Fixture):
    """A client writes the blob first and its index afterwards. Between those two a live
    blob is indistinguishable from garbage, so the sweep has a grace period BY DEFAULT -
    the extraction kept the mechanism and dropped the default, and a blob uploaded a second
    ago was deleted."""

    def test_the_default_grace_spares_a_blob_written_moments_ago(self):
        blob = self.blobs.put_file(self.file("a", b"just uploaded"))
        got = self.blobs.sweep(keep={"sha256:" + "0" * 64})
        self.assertEqual(0, got["deleted"])
        self.assertTrue(self.blobs.has(blob["digest"]))

    def test_an_explicit_zero_grace_means_what_it_says(self):
        blob = self.blobs.put_file(self.file("a", b"just uploaded"))
        self.assertEqual(1, self.blobs.sweep(keep={"sha256:" + "0" * 64},
                                             grace_s=0)["deleted"])
        self.assertFalse(self.blobs.has(blob["digest"]))

    def test_a_blob_older_than_the_grace_goes(self):
        blob = self.blobs.put_file(self.file("a", b"old"))
        later = time.time() + GRACE_S + 60
        self.assertEqual(1, self.blobs.sweep(keep={"sha256:" + "0" * 64},
                                             now=later)["deleted"])
        self.assertFalse(self.blobs.has(blob["digest"]))

    def test_a_blob_already_gone_is_not_counted_as_deleted(self):
        """Two sweepers, or a client deleting as one runs: `deleted` is what THIS sweep
        removed, so an operator reading it is not told work happened twice."""
        self.blobs.put_file(self.file("a", b"vanishing"))
        # the other sweeper's delete landed between this one's listing and its own delete.
        # (MemoryStore does not raise on deleting what is absent, so the store's answer is
        # produced here rather than by racing it for real.)
        def already_gone(store, path, *a, **kw):
            raise FileNotFoundError(path)
        with unittest.mock.patch.object(obstore, "delete", already_gone):
            got = self.blobs.sweep(keep={"sha256:" + "0" * 64}, grace_s=0)
        self.assertEqual({"deleted": 0, "already_gone": 1, "refreshed": 0}, got,
                         "`deleted` is what THIS sweep removed")


class TestPutBytes(_Fixture):
    def test_put_bytes_matches_put_file(self):
        by_bytes = self.blobs.put_bytes(b"content")
        by_file = self.blobs.put_file(self.file("a", b"content"))
        self.assertEqual(by_file, by_bytes)
        self.assertEqual(1, len(self.blobs.entries()))

    def test_put_bytes_replaces_a_suspect_blob(self):
        blob = self.blobs.put_bytes(b"content")
        obstore.put(self.store, self.blobs.path(blob["digest"]), b"tampered")
        self.assertFalse(self.blobs.fetch(blob["digest"], self.tmp / "out"))
        self.assertIn(blob["digest"], self.blobs.suspect)
        self.blobs.put_bytes(b"content")
        self.assertNotIn(blob["digest"], self.blobs.suspect)
        self.assertTrue(self.blobs.fetch(blob["digest"], self.tmp / "out2"))


class TestConcurrentCreate(_Fixture):
    """The actual concurrency claim: two writers of the same bytes, where the `has` check
    said absent and the write then lost the race. The precheck normally hides this branch,
    so it is forced."""

    def test_losing_a_create_race_is_not_an_error(self):
        data = b"written twice at once"
        other = Blobs(self.store, self.blobs.prefix)
        other.put_file(self.file("a", data))          # the winner, already stored
        with unittest.mock.patch.object(Blobs, "has", return_value=False):
            blob = self.blobs.put_file(self.file("b", data))
        self.assertEqual(digest_file(self.file("a", data)), blob["digest"])
        dest = self.tmp / "out"
        self.assertTrue(self.blobs.fetch(blob["digest"], dest))
        self.assertEqual(data, dest.read_bytes(), "the winner's bytes, which are the same")

    def test_put_bytes_losing_a_create_race_is_not_an_error(self):
        data = b"also written twice"
        Blobs(self.store, self.blobs.prefix).put_bytes(data)
        with unittest.mock.patch.object(Blobs, "has", return_value=False):
            self.blobs.put_bytes(data)
        self.assertEqual(1, len(self.blobs.entries()))


class TestFileUrls(unittest.TestCase):
    def test_a_file_url_is_a_root_with_no_prefix(self):
        """A prefix read out of the URL fragment is nobody's guess: pass one to Blobs."""
        with tempfile.TemporaryDirectory() as d:
            store, prefix = open_store(f"file://{d}/store")
            self.assertEqual("", prefix)
            self.assertTrue(Path(d, "store").is_dir())
            blobs = Blobs(store, "mine/")
            src = Path(d) / "x"
            src.write_bytes(b"local")
            blob = blobs.put_file(src)
            self.assertTrue(Path(d, "store", "mine", "blobs", "sha256").is_dir())
            out = Path(d) / "back"
            self.assertTrue(blobs.fetch(blob["digest"], out))
            self.assertEqual(b"local", out.read_bytes())


class TestTheProbeRuns(unittest.TestCase):
    """tools/ is where this family's code rots: nothing imports it, so it drifts. The
    empty-keep guard broke the probe the day it was added, and no test noticed."""

    def test_the_probe_passes_against_an_in_memory_store(self):
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
        try:
            import probe_store
        finally:
            sys.path.pop(0)
        said = []
        self.assertEqual(0, probe_store.probe("memory://probe", size_mb=0, say=said.append))
        self.assertTrue(any("sweep deletes" in line for line in said), said)
        self.assertTrue(any("default grace spares" in line for line in said), said)

    def test_the_probes_argument_parsing_works(self):
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
        try:
            import probe_store
        finally:
            sys.path.pop(0)
        self.assertEqual(0, probe_store.main(["memory://probe2", "--size-mb", "0"]))


class TestPreListedCandidates(_Fixture):
    """A caller that lists blobs BEFORE reading its index is safe from its own clock: a
    blob written after the listing was never a candidate. Listing inside `sweep` left only
    an age comparison between the caller's clock and the store's, and S3 truncates
    last-modified to whole seconds (found reviewing haversack's use, 2026-09-20)."""

    def test_a_blob_written_after_the_listing_is_not_swept(self):
        old = self.blobs.put_file(self.file("a", b"already here"))
        candidates = self.blobs.entries(older_than=time.time() + 60)
        # the index is read here, and a second writer publishes in the meantime
        fresh = self.blobs.put_file(self.file("b", b"published mid-sweep"))
        got = self.blobs.sweep(keep={old["digest"]}, candidates=candidates, grace_s=0)
        self.assertEqual({"deleted": 0, "already_gone": 0, "refreshed": 0}, got)
        self.assertTrue(self.blobs.has(fresh["digest"]), "it was never a candidate")
        self.assertTrue(self.blobs.has(old["digest"]))

    def test_listing_inside_the_sweep_is_what_loses_it(self):
        """The same interleaving without pre-listed candidates, to show the difference is
        real rather than asserted."""
        old = self.blobs.put_file(self.file("a", b"already here"))
        fresh = self.blobs.put_file(self.file("b", b"published mid-sweep"))
        self.blobs.sweep(keep={old["digest"]}, grace_s=0)
        self.assertFalse(self.blobs.has(fresh["digest"]))

    def test_candidates_are_used_as_given(self):
        a = self.blobs.put_file(self.file("a", b"one"))
        b = self.blobs.put_file(self.file("b", b"two"))
        got = self.blobs.sweep(keep=set(), allow_empty=True,
                               candidates=[c for c in self.blobs.entries(
                                   older_than=time.time() + 60)
                                   if c["digest"] == a["digest"]])
        self.assertEqual(1, got["deleted"])
        self.assertFalse(self.blobs.has(a["digest"]))
        self.assertTrue(self.blobs.has(b["digest"]), "not offered, not deleted")

    def test_an_empty_candidate_list_still_refuses_an_empty_keep_set(self):
        with self.assertRaises(EmptyKeepSet):
            self.blobs.sweep(keep=set(), candidates=[])


class TestDedupRefreshesTheTimestamp(_Fixture):
    """A sweep's one rule is that a blob an index entry names was written no earlier than
    that entry. Deduplication broke it: a recomputation that produces identical bytes
    uploads nothing, so the blob kept a timestamp older than the entry about to name it -
    and was swept out from under it (reproduced in haversack, 2026-09-20)."""

    def test_storing_bytes_that_are_already_here_makes_them_young_again(self):
        blob = self.blobs.put_file(self.file("a", b"identical output"))
        before = self.blobs.entries()[0]["modified"]
        time.sleep(0.05)
        again = self.blobs.put_file(self.file("b", b"identical output"))
        self.assertEqual(blob, again)
        self.assertGreater(self.blobs.entries()[0]["modified"], before)

    def test_the_refreshed_blob_is_no_longer_an_old_candidate(self):
        self.blobs.put_file(self.file("a", b"identical output"))
        cutoff = time.time()
        time.sleep(0.05)
        self.blobs.put_file(self.file("b", b"identical output"))   # the dedup publication
        candidates = self.blobs.entries(older_than=cutoff)
        self.assertEqual([], candidates, "it is as young as the entry that names it")

    def test_put_bytes_refreshes_too(self):
        self.blobs.put_bytes(b"identical output")
        before = self.blobs.entries()[0]["modified"]
        time.sleep(0.05)
        self.blobs.put_bytes(b"identical output")
        self.assertGreater(self.blobs.entries()[0]["modified"], before)

    def test_touch_says_when_a_blob_is_not_there(self):
        self.assertFalse(self.blobs.touch(f"sha256:{'0' * 64}"))

    def test_a_store_that_refuses_the_copy_still_reports_presence(self):
        blob = self.blobs.put_file(self.file("a", b"payload"))
        with unittest.mock.patch.object(obstore, "copy",
                                        side_effect=NotImplementedError("no copy here")):
            self.assertTrue(self.blobs.touch(blob["digest"]))
            self.assertEqual(blob, self.blobs.put_file(self.file("b", b"payload")))


class TestCandidatesAreRecheckedBeforeDeletion(_Fixture):
    """Pre-listing cannot save a DEDUPLICATED write: the object is genuinely old while the
    reference to it is new, so it sits in the candidate list looking like garbage. The
    re-check before each delete is what closes that (haversack review, 2026-09-20)."""

    def test_a_blob_refreshed_after_the_listing_is_spared(self):
        orphan = self.blobs.put_file(self.file("a", b"identical output"))
        candidates = self.blobs.entries(older_than=time.time() + 60)
        keeper = self.blobs.put_file(self.file("b", b"something else"))
        time.sleep(0.02)
        self.blobs.put_file(self.file("c", b"identical output"))   # dedupes: touch only
        got = self.blobs.sweep(keep={keeper["digest"]}, candidates=candidates, grace_s=0)
        self.assertEqual(1, got["refreshed"])
        self.assertEqual(0, got["deleted"])
        self.assertTrue(self.blobs.has(orphan["digest"]))

    def test_a_blob_nobody_touched_still_goes(self):
        orphan = self.blobs.put_file(self.file("a", b"garbage"))
        keeper = self.blobs.put_file(self.file("b", b"live"))
        candidates = self.blobs.entries(older_than=time.time() + 60)
        got = self.blobs.sweep(keep={keeper["digest"]}, candidates=candidates, grace_s=0)
        self.assertEqual(1, got["deleted"])
        self.assertFalse(self.blobs.has(orphan["digest"]))

    def test_a_candidate_whose_state_cannot_be_read_is_spared(self):
        orphan = self.blobs.put_file(self.file("a", b"garbage"))
        keeper = self.blobs.put_file(self.file("b", b"live"))
        candidates = self.blobs.entries(older_than=time.time() + 60)
        from obstore.exceptions import PermissionDeniedError
        with unittest.mock.patch.object(obstore, "head",
                                        side_effect=PermissionDeniedError("403")):
            got = self.blobs.sweep(keep={keeper["digest"]}, candidates=candidates,
                                   grace_s=0)
        self.assertEqual(0, got["deleted"], "a sweep that cannot tell does not delete")
        self.assertTrue(self.blobs.has(orphan["digest"]))

    def test_a_candidate_with_no_timestamp_is_spared(self):
        self.blobs.put_file(self.file("a", b"garbage"))
        keeper = self.blobs.put_file(self.file("b", b"live"))
        candidates = [{**c, "modified": None}
                      for c in self.blobs.entries(older_than=time.time() + 60)]
        got = self.blobs.sweep(keep={keeper["digest"]}, candidates=candidates, grace_s=0)
        self.assertEqual(0, got["deleted"])
