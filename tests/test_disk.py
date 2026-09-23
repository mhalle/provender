"""DiskStore: a directory that honors both conditional writes (2026-09-23).

What these hold it to:

- it answers obstore's function API in obstore's shapes - the SAME behavioral checks pass
  against obstore's in-memory store and against a directory, so a difference is a finding;
- it passes the probe that refuses obstore's own LocalStore;
- compare-and-swap loses no update under contention from separate PROCESSES and from
  threads, and exactly one of several concurrent creators wins;
- a writer that dies holding the lock does not wedge the store;
- nothing internal (lock files, an interrupted write) is listed, read or addressable;
- every Blobs test in ``test_blobs.py`` passes on it too.
"""
from __future__ import annotations

import datetime as _dt
import multiprocessing
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path

import pytest

obstore = pytest.importorskip("obstore")
from obstore.exceptions import AlreadyExistsError, PreconditionError  # noqa: E402
from obstore.store import LocalStore, MemoryStore  # noqa: E402

import test_blobs  # noqa: E402
from provender import DiskStore, check_store, open_store, ops, update_mode  # noqa: E402
from provender import disk as disk_mod  # noqa: E402


class _Backends:
    """Each behavioral check runs against both backends; ``backend`` names which."""

    def stores(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return [("memory", MemoryStore()), ("disk", DiskStore(Path(tmp.name) / "s"))]


class TestTheSameAnswersAsABucket(_Backends, unittest.TestCase):
    def test_put_get_head_round_trip_in_obstores_shapes(self):
        for backend, s in self.stores():
            with self.subTest(backend=backend):
                put = ops.put(s, "a/b", b"hello")
                self.assertEqual({"e_tag", "version"}, set(put))
                meta = ops.head(s, "a/b")
                self.assertEqual({"path", "last_modified", "size", "e_tag", "version"},
                                 set(meta))
                self.assertEqual(("a/b", 5), (meta["path"], meta["size"]))
                self.assertIsInstance(meta["last_modified"], _dt.datetime)
                self.assertEqual(put["e_tag"], meta["e_tag"])
                got = ops.get(s, "a/b")
                self.assertEqual(meta["e_tag"], got.meta["e_tag"])
                self.assertEqual(b"hello", bytes(got.bytes()))
                self.assertEqual(b"hello", b"".join(
                    bytes(c) for c in ops.get(s, "a/b").stream(min_chunk_size=2)))

    def test_a_missing_object_is_file_not_found(self):
        for backend, s in self.stores():
            with self.subTest(backend=backend):
                for call in (lambda: ops.head(s, "nope"), lambda: ops.get(s, "nope"),
                             lambda: ops.copy(s, "nope", "nope")):
                    with self.assertRaises(FileNotFoundError):
                        call()

    def test_create_if_absent_refuses_an_existing_object_and_keeps_it(self):
        for backend, s in self.stores():
            with self.subTest(backend=backend):
                ops.put(s, "k", b"first", mode="create")
                with self.assertRaises(AlreadyExistsError):
                    ops.put(s, "k", b"second", mode="create")
                self.assertEqual(b"first", bytes(ops.get(s, "k").bytes()))

    def test_replace_if_unchanged_takes_the_current_tag_and_refuses_a_stale_one(self):
        for backend, s in self.stores():
            with self.subTest(backend=backend):
                ops.put(s, "k", b"v1")
                stale = update_mode(ops.head(s, "k"))
                ops.put(s, "k", b"v2", mode=stale)
                with self.assertRaises(PreconditionError):
                    ops.put(s, "k", b"v3", mode=stale)
                self.assertEqual(b"v2", bytes(ops.get(s, "k").bytes()))

    def test_replace_if_unchanged_on_an_absent_object_is_a_precondition_failure(self):
        for backend, s in self.stores():
            with self.subTest(backend=backend):
                with self.assertRaises(PreconditionError):
                    ops.put(s, "k", b"v", mode={"e_tag": '"whatever"'})
                with self.assertRaises(FileNotFoundError):
                    ops.head(s, "k")

    def test_a_listing_prefix_matches_whole_segments(self):
        for backend, s in self.stores():
            with self.subTest(backend=backend):
                for p in ("a/1", "a/2", "a/sub/3", "ab/4", "b"):
                    ops.put(s, p, b"x")
                self.assertEqual(["a/1", "a/2", "a/sub/3"],
                                 sorted(o["path"] for o in ops.list(s, "a").collect()))
                self.assertEqual(["a/1", "a/2", "a/sub/3"],
                                 sorted(o["path"] for o in ops.list(s, "a/").collect()))
                self.assertEqual(5, len(ops.list(s).collect()))
                self.assertEqual([], ops.list(s, "nothing").collect())

    def test_a_listing_comes_in_batches_and_carries_size_and_time(self):
        for backend, s in self.stores():
            with self.subTest(backend=backend):
                for i in range(5):
                    ops.put(s, f"p/{i}", b"x" * i)
                batches = list(ops.list(s, "p", chunk_size=2))
                self.assertEqual(5, sum(len(b) for b in batches))
                self.assertGreater(len(batches), 1)
                for o in ops.list(s, "p").collect():
                    self.assertEqual(int(o["path"][-1]), o["size"])
                    self.assertIsInstance(o["last_modified"], _dt.datetime)

    def test_deleting_an_absent_object_is_not_an_error(self):
        for backend, s in self.stores():
            with self.subTest(backend=backend):
                ops.put(s, "k", b"v")
                ops.delete(s, "k")
                ops.delete(s, "k")
                with self.assertRaises(FileNotFoundError):
                    ops.head(s, "k")

    def test_copy_onto_itself_keeps_the_bytes(self):
        for backend, s in self.stores():
            with self.subTest(backend=backend):
                ops.put(s, "k", b"v")
                ops.copy(s, "k", "k")
                self.assertEqual(b"v", bytes(ops.get(s, "k").bytes()))

    def test_copy_elsewhere_and_its_create_if_absent(self):
        for backend, s in self.stores():
            with self.subTest(backend=backend):
                ops.put(s, "a", b"one")
                ops.put(s, "b", b"two")
                with self.assertRaises(AlreadyExistsError):
                    ops.copy(s, "a", "b", overwrite=False)
                ops.copy(s, "a", "c")
                self.assertEqual(b"one", bytes(ops.get(s, "c").bytes()))


class TestTheProbe(unittest.TestCase):
    def test_a_disk_store_passes_the_probe_that_refuses_localstore(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(Exception):
                check_store(LocalStore(d))
            check_store(DiskStore(Path(d) / "disk"), "p/")
            check_store(DiskStore(Path(d) / "disk"), updates=False)

    def test_a_file_url_opens_a_disk_store_that_passes(self):
        with tempfile.TemporaryDirectory() as d:
            store, prefix = open_store(f"file://{d}/s")
            self.assertIsInstance(store, DiskStore)
            self.assertEqual("", prefix)
            check_store(store)
            self.assertEqual([], ops.list(store).collect(), "the probe leaves nothing")


class _Disk(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name) / "store"
        self.s = DiskStore(self.root)


class TestTheDiskItself(_Disk):
    def test_an_etag_is_the_contents_so_equal_bytes_agree_and_a_change_shows(self):
        a = ops.put(self.s, "a", b"same")["e_tag"]
        b = ops.put(self.s, "b", b"same")["e_tag"]
        self.assertEqual(a, b)
        self.assertNotEqual(a, ops.put(self.s, "a", b"different")["e_tag"])

    def test_touching_moves_the_time_and_not_the_tag(self):
        ops.put(self.s, "k", b"v")
        old = ops.head(self.s, "k")
        os.utime(self.root / "k", (1_000_000_000, 1_000_000_000))
        ops.copy(self.s, "k", "k")
        new = ops.head(self.s, "k")
        self.assertGreater(new["last_modified"].timestamp(), time.time() - 60,
                           "set back to 2001, the touch must bring it to now")
        self.assertEqual(old["e_tag"], new["e_tag"])

    def test_paths_that_could_leave_the_root_or_reach_its_state_are_refused(self):
        for bad in ("", "/abs", "a//b", "../x", "a/../b", "./a", "a/.", "a\\b", "a\0b",
                    ".provender/locks/0000", f"x{disk_mod.TMP_SUFFIX}", "a/"):
            with self.subTest(path=bad):
                with self.assertRaises(ValueError):
                    ops.put(self.s, bad, b"x")
                with self.assertRaises(ValueError):
                    ops.head(self.s, bad)

    def test_macos_metadata_files_are_not_objects(self):
        """On exFAT macOS writes ``._<name>`` beside EVERY file; listed, a ``._<key>.json``
        read as an unreadable pointer and froze the sweep. Planted here, so an APFS run
        holds it too; the suite has also run whole on a real exFAT volume."""
        ops.put(self.s, "refs/k.json", b"{}")
        (self.root / "refs" / "._k.json").write_bytes(b"\0\5\26\7appledouble")
        (self.root / "._refs").write_bytes(b"x")
        (self.root / "refs" / ".DS_Store").write_bytes(b"x")
        (self.root / "._dir").mkdir()
        (self.root / "._dir" / "inside").write_bytes(b"x")
        self.assertEqual(["refs/k.json"], [o["path"] for o in ops.list(self.s).collect()])
        for bad in ("refs/._k.json", ".DS_Store", "._dir/inside"):
            with self.subTest(path=bad), self.assertRaises(ValueError):
                ops.head(self.s, bad)

    def test_a_str_is_not_taken_as_data_or_as_a_path(self):
        with self.assertRaises(TypeError):
            ops.put(self.s, "k", "some text")

    def test_nothing_internal_is_listed_and_an_interrupted_write_is_invisible(self):
        ops.put(self.s, "real", b"x")
        (self.root / f".real.dead{disk_mod.TMP_SUFFIX}").write_bytes(b"half")
        self.assertTrue((self.root / disk_mod.STATE_DIR).is_dir(), "locks were taken")
        self.assertEqual(["real"], [o["path"] for o in ops.list(self.s).collect()])

    def test_a_reader_keeps_what_it_opened_while_the_object_is_replaced(self):
        ops.put(self.s, "k", b"before" * 1000)
        got = ops.get(self.s, "k")
        ops.put(self.s, "k", b"after")
        ops.delete(self.s, "k")
        self.assertEqual(b"before" * 1000, bytes(got.bytes()))

    def test_a_large_object_still_swaps_conditionally(self):
        """Above HASHED_ETAG_MAX the tag is inode-mtime-size: blobs are never swapped, but
        the mode must still be honored, not waved through."""
        with unittest.mock.patch.object(disk_mod, "HASHED_ETAG_MAX", 4):
            ops.put(self.s, "big", b"123456789")
            stale = update_mode(ops.head(self.s, "big"))
            ops.put(self.s, "big", b"abcdefghij", mode=stale)
            with self.assertRaises(PreconditionError):
                ops.put(self.s, "big", b"lost", mode=stale)

    def test_it_reads_what_obstores_localstore_wrote(self):
        """``file://`` used to open a LocalStore: a directory it wrote must still read."""
        LocalStore(self.root)
        obstore.put(LocalStore(self.root), "old/blob", b"from before")
        self.assertEqual(b"from before", bytes(ops.get(self.s, "old/blob").bytes()))
        self.assertEqual(["old/blob"], [o["path"] for o in ops.list(self.s, "old").collect()])

    def test_an_object_and_a_prefix_cannot_share_a_name(self):
        ops.put(self.s, "a/b", b"x")
        with self.assertRaises(OSError):
            ops.put(self.s, "a", b"y")
        with self.assertRaises(FileNotFoundError):
            ops.head(self.s, "a")              # a directory is not an object
        ops.delete(self.s, "a")                # and deleting it removes nothing
        self.assertEqual(b"x", bytes(ops.get(self.s, "a/b").bytes()))


# -- contention ---------------------------------------------------------------------------

def _increment(root: str, times: int) -> int:
    """Read a counter, add one, write it back conditionally, retrying on every lost race.
    Returns how many races it lost - a test with none proves nothing."""
    s = DiskStore(root)
    lost = 0
    for _ in range(times):
        while True:
            got = ops.get(s, "counter")
            mode = update_mode(got.meta)
            n = int(bytes(got.bytes()))
            try:
                ops.put(s, "counter", str(n + 1).encode(), mode=mode)
                break
            except PreconditionError:
                lost += 1
    return lost


def _create(root: str, me: int) -> bool:
    try:
        ops.put(DiskStore(root), "once", str(me).encode(), mode="create")
        return True
    except AlreadyExistsError:
        return False


def _die_holding_the_lock(root: str) -> None:
    s = DiskStore(root)
    with s._locked("k"):
        os._exit(0)                            # no cleanup: the kernel's release only


class TestContention(_Disk):
    WORKERS, TIMES = 6, 40

    def test_no_update_is_lost_across_processes(self):
        ops.put(self.s, "counter", b"0")
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(self.WORKERS) as pool:
            lost = pool.starmap(_increment, [(str(self.root), self.TIMES)] * self.WORKERS)
        self.assertEqual(str(self.WORKERS * self.TIMES).encode(),
                         bytes(ops.get(self.s, "counter").bytes()))
        self.assertGreater(sum(lost), 0, "no race was lost, so the check proved nothing")

    def test_no_update_is_lost_across_threads(self):
        """flock is per open file description, so threads contend as processes do."""
        ops.put(self.s, "counter", b"0")
        lost = []
        threads = [threading.Thread(target=lambda: lost.append(
            _increment(str(self.root), self.TIMES))) for _ in range(self.WORKERS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(str(self.WORKERS * self.TIMES).encode(),
                         bytes(ops.get(self.s, "counter").bytes()))

    def test_exactly_one_concurrent_creator_wins(self):
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(self.WORKERS) as pool:
            won = pool.starmap(_create, [(str(self.root), i) for i in range(self.WORKERS)])
        self.assertEqual(1, sum(won))
        self.assertEqual(str(won.index(True)).encode(),
                         bytes(ops.get(self.s, "once").bytes()))

    def test_a_writer_that_dies_holding_the_lock_does_not_wedge_the_store(self):
        ctx = multiprocessing.get_context("spawn")
        p = ctx.Process(target=_die_holding_the_lock, args=(str(self.root),))
        p.start()
        p.join(30)
        done = threading.Event()
        threading.Thread(target=lambda: (ops.put(self.s, "k", b"after"), done.set()),
                         daemon=True).start()
        self.assertTrue(done.wait(10), "a write waited on a dead process's lock")
        self.assertEqual(b"after", bytes(ops.get(self.s, "k").bytes()))


# -- every Blobs test, again on a DiskStore -------------------------------------------------

#: These fake a store fault by patching one of obstore's own functions: they hold Blobs'
#: handling of the fault, which no backend changes, and a DiskStore never calls obstore.
_PATCH_OBSTORE = {"test_a_store_fault_is_raised_not_swallowed",
                  "test_a_blob_already_gone_is_not_counted_as_deleted",
                  "test_a_store_that_refuses_the_copy_still_reports_presence",
                  "test_a_candidate_whose_state_cannot_be_read_is_spared"}


def _on_disk(base):
    def make_store(self):
        return DiskStore(self.tmp / "disk-store")
    attrs = {"make_store": make_store}
    for name in dir(base):
        if name in _PATCH_OBSTORE:
            attrs[name] = unittest.skip("patches obstore itself; see _PATCH_OBSTORE")(
                getattr(base, name))
    return type(f"{base.__name__}OnDisk", (base,), attrs)


for _name in dir(test_blobs):
    _base = getattr(test_blobs, _name)
    if (isinstance(_base, type) and issubclass(_base, test_blobs._Fixture)
            and _base is not test_blobs._Fixture):
        globals()[f"{_name}OnDisk"] = _on_disk(_base)
del _name, _base


def test_the_blobs_suite_really_runs_on_disk():
    """A rerun that silently stayed in memory would prove nothing."""
    runs = [v for k, v in globals().items() if k.endswith("OnDisk")]
    assert len(runs) >= 8, runs
    case = runs[0]("setUp")
    case.setUp()
    try:
        assert isinstance(case.store, DiskStore)
    finally:
        case.tearDown()
