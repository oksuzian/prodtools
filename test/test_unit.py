#!/usr/bin/env python3
"""
Unit tests for prodtools core modules.

Tests run without SAM/grid access by using in-memory tarballs and mocked
samweb_client. This provides a regression baseline before adding new features
(e.g., stash support).

Run with:  python -m pytest test/test_unit.py -v
       or: python test/test_unit.py
"""

import atexit
import contextlib
import copy
import hashlib
import io
import json
import os
import shutil
import sqlite3
import subprocess
import importlib.machinery
import importlib.util
import sys
import types
import tarfile
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

# Make the package root importable when running from any directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Every temp dir this suite makes is removed at interpreter exit. Bare
# tempfile.mkdtemp() leaks one directory per test; at ~1000 tests a run,
# that walked /tmp into the ext4 65000-subdirectory ceiling on the gpvms,
# after which NOTHING on the node could mkdir in /tmp — including
# production submission (2026-08-13). Cleanup lives here, once, rather
# than in 31 separate setUp methods that each have to remember.
_TMPDIRS = []


def _mkdtemp():
    """tempfile.mkdtemp() that is cleaned up when the process exits."""
    d = tempfile.mkdtemp()
    _TMPDIRS.append(d)
    return d


@atexit.register
def _cleanup_tmpdirs():
    for d in _TMPDIRS:
        shutil.rmtree(d, ignore_errors=True)

# The MCP server package lives outside utils/; add its src root so the
# server's tools are testable in this suite without MCP machinery.
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'mcp', 'src'))

# samweb_client and other Fermilab-specific modules are not available outside
# the Mu2e environment. Stub them before any utils import occurs so that the
# test suite runs standalone.
_STUB_MODULES = [
    'samweb_client',
    'ifdh',
]
for _mod in _STUB_MODULES:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

from utils.job_common import Mu2eName, remove_storage_prefix, Mu2eJobBase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tarball(jobpars: dict, fcl_content: str = "#include \"base.fcl\"\n") -> str:
    """
    Build an in-memory tarball containing jobpars.json + mu2e.fcl and write
    it to a temporary file.  Returns the path to the .tar file.
    fcl_content=None omits the mu2e.fcl member entirely (code-tarball cnfs).

    The file is placed in /tmp and must be removed by the caller if desired.
    """
    import tempfile
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w') as tar:
        # Add jobpars.json
        jp_bytes = json.dumps(jobpars).encode()
        ti = tarfile.TarInfo(name='jobpars.json')
        ti.size = len(jp_bytes)
        tar.addfile(ti, io.BytesIO(jp_bytes))
        if fcl_content is not None:
            # Add mu2e.fcl
            fcl_bytes = fcl_content.encode()
            ti2 = tarfile.TarInfo(name='mu2e.fcl')
            ti2.size = len(fcl_bytes)
            tar.addfile(ti2, io.BytesIO(fcl_bytes))
    buf.seek(0)

    tmp = tempfile.NamedTemporaryFile(suffix='.tar', delete=False)
    tmp.write(buf.read())
    tmp.close()
    return tmp.name


def _root_input_jobpars(files, merge=1, run=1430, owner='mu2e', dsconf='TestConf'):
    """Return a jobpars.json dict suitable for a RootInput job."""
    return {
        "code": "",
        "setup": "/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/TestConf/setup.sh",
        "tbs": {
            "seed": "services.SeedService.baseSeed",
            "subrunkey": "",
            "event_id": {"source.maxEvents": 2147483647},
            "outfiles": {
                "outputs.PrimaryOutput.fileName":
                    f"sim.{owner}.TestDesc.{dsconf}.sequencer.art"
            },
            "inputs": {
                "source.fileNames": [merge, files]
            },
            "sequential_aux": False,
        },
        "jobname": f"cnf.{owner}.TestDesc.{dsconf}.0.tar",
        "owner": owner,
        "dsconf": dsconf,
    }


def _empty_event_jobpars(run=1430, events=1000, owner='mu2e', dsconf='TestConf'):
    """Return a jobpars.json dict suitable for an EmptyEvent job."""
    return {
        "code": "",
        "setup": "/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/TestConf/setup.sh",
        "tbs": {
            "seed": "services.SeedService.baseSeed",
            "subrunkey": "source.firstSubRun",
            "event_id": {
                "source.firstRun": run,
                "source.maxEvents": events,
            },
            "outfiles": {
                "outputs.PrimaryOutput.fileName":
                    f"sim.{owner}.TestDesc.{dsconf}.sequencer.art"
            },
        },
        "jobname": f"cnf.{owner}.TestDesc.{dsconf}.0.tar",
        "owner": owner,
        "dsconf": dsconf,
    }


# ---------------------------------------------------------------------------
# 1. Mu2eName Perl-parity contract (job_common.py, formerly Mu2eName)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Fake prodtools release for submit/enqueue tests. Both boundaries require
# an entry's prodtools_dir to be a real release tree (bin/setup.sh,
# bin/runjob.sh, utils/runmu2e.py), checked on the submit host.
# ---------------------------------------------------------------------------
def _make_fake_prodtools_dir():
    import atexit, shutil, tempfile
    d = tempfile.mkdtemp(prefix='fake-prodtools-')
    for rel in ('bin/setup.sh', 'bin/runjob.sh', 'utils/runmu2e.py'):
        f = Path(d) / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text('# fake\nMU2EGRID_PRODTOOLS_DIR\n')
    atexit.register(shutil.rmtree, d, True)
    return d


FAKE_PRODTOOLS_DIR = _make_fake_prodtools_dir()

class TestMu2eNameParity(unittest.TestCase):

    def test_parse_standard_filename(self):
        fn = Mu2eName("dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art")
        self.assertEqual(fn.tier, "dts")
        self.assertEqual(fn.owner, "mu2e")
        self.assertEqual(fn.description, "CeEndpoint")
        self.assertEqual(fn.dsconf, "Run1Bab")
        self.assertEqual(fn.sequencer, "001440_00001234")
        self.assertEqual(fn.extension, "art")

    def test_parse_sim_filename(self):
        fn = Mu2eName("sim.mu2e.MuminusStopsCat.MDC2025ac.001430_00000000.art")
        self.assertEqual(fn.tier, "sim")
        self.assertEqual(fn.sequencer, "001430_00000000")
        self.assertEqual(fn.dsconf, "MDC2025ac")

    def test_parse_nts_filename(self):
        fn = Mu2eName("nts.mu2e.CosmicCRYExtracted.MDC2020av.001205_00000000.root")
        self.assertEqual(fn.tier, "nts")
        self.assertEqual(fn.extension, "root")

    def test_str_returns_filename(self):
        name = "dig.mu2e.CosmicCRYAllMix1BB.MDC2025af.001430_00000076.art"
        fn = Mu2eName(name)
        self.assertEqual(str(fn), name)

    def test_invalid_filename_raises(self):
        with self.assertRaises(ValueError):
            Mu2eName("too.few.parts")

    def test_invalid_filename_seven_parts_raises(self):
        with self.assertRaises(ValueError):
            Mu2eName("a.b.c.d.e.f.g")  # only 5 or 6 fields are valid

    def test_parse_six_parts_ok(self):
        fn = Mu2eName("a.b.c.d.e.f")
        self.assertEqual(fn.tier, "a")
        self.assertEqual(fn.extension, "f")

    def test_dataset_derivation(self):
        """Dataset name can be derived from filename by dropping sequencer."""
        fn = Mu2eName("dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art")
        self.assertEqual(str(fn.dataset), "dts.mu2e.CeEndpoint.Run1Bab.art")

    def test_dataset_derivation_sim(self):
        fn = Mu2eName("sim.mu2e.MuminusStopsCat.MDC2025ac.001430_00000007.art")
        self.assertEqual(str(fn.dataset), "sim.mu2e.MuminusStopsCat.MDC2025ac.art")


# ---------------------------------------------------------------------------
# 1b. Mu2eName extended interface (job_common.py)
# ---------------------------------------------------------------------------

class TestMu2eName(unittest.TestCase):
    """Exercise the unified parse/build/derivation surface of Mu2eName.

    TestMu2eNameParity above pins the historical (Perl Mu2eName)
    contract; this class pins the extended interface.
    """

    # parse / discriminators

    def test_parse_dataset_five_fields(self):
        from utils.job_common import Mu2eName
        n = Mu2eName.parse("dts.mu2e.CeEndpoint.Run1Bab.art")
        self.assertTrue(n.is_dataset)
        self.assertFalse(n.is_file)
        self.assertFalse(n.is_tarball)
        self.assertIsNone(n.sequencer)
        self.assertEqual(n.extension, "art")

    def test_parse_file_six_fields(self):
        from utils.job_common import Mu2eName
        n = Mu2eName.parse("dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art")
        self.assertTrue(n.is_file)
        self.assertFalse(n.is_dataset)
        self.assertFalse(n.is_tarball)
        self.assertEqual(n.sequencer, "001440_00001234")

    def test_parse_tarball(self):
        from utils.job_common import Mu2eName
        n = Mu2eName.parse("cnf.mu2e.CeEndpoint.MDC2025af_best_v1_3.42.tar")
        self.assertTrue(n.is_tarball)
        self.assertFalse(n.is_file)
        self.assertFalse(n.is_dataset)
        self.assertEqual(n.index, 42)

    def test_reject_four_fields(self):
        from utils.job_common import Mu2eName
        with self.assertRaises(ValueError):
            Mu2eName.parse("a.b.c.d")

    def test_reject_seven_fields(self):
        from utils.job_common import Mu2eName
        with self.assertRaises(ValueError):
            Mu2eName.parse("a.b.c.d.e.f.g")

    # sub-fields

    def test_dsconf_base_with_version_suffix(self):
        from utils.job_common import Mu2eName
        n = Mu2eName.parse("mcs.mu2e.CeEndpoint.MDC2025af_best_v1_3.001440_00001234.art")
        self.assertEqual(n.dsconf_base, "MDC2025af")

    def test_dsconf_base_plain(self):
        from utils.job_common import Mu2eName
        n = Mu2eName.parse("dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art")
        self.assertEqual(n.dsconf_base, "Run1Bab")

    def test_campaign_extracts_mdc(self):
        from utils.job_common import Mu2eName
        n = Mu2eName.parse("mcs.mu2e.X.MDC2025af_best_v1_3.001440_00001234.art")
        self.assertEqual(n.campaign, "MDC2025af")

    def test_campaign_extracts_run1b(self):
        from utils.job_common import Mu2eName
        n = Mu2eName.parse("dts.mu2e.X.Run1Bab.001440_00001234.art")
        self.assertEqual(n.campaign, "Run1Bab")

    def test_index_raises_on_non_tarball(self):
        from utils.job_common import Mu2eName
        n = Mu2eName.parse("dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art")
        with self.assertRaises(ValueError):
            _ = n.index

    # tier_class parity with the existing module-level map

    def test_tier_class_matches_legacy_map(self):
        """Pin the tier_class umbrella mapping. The legacy module-level
        dict was deleted from jobsub_argv as part of unification; the
        expected values below are the verified Phase-2 list (sim chain,
        ancillary, data, MC ntuples)."""
        from utils.job_common import Mu2eName
        legacy = {
            "sim": "sim", "dig": "sim", "dts": "sim", "mcs": "sim", "mix": "sim",
            "log": "etc", "etc": "etc", "cnf": "etc", "bck": "etc",
            "rec": "dat", "ntd": "dat",
            "nts": "nts",
        }
        for tier, expected in legacy.items():
            n = Mu2eName.build(tier=tier, owner="mu2e", description="X",
                               dsconf="MDC2025af", extension="art")
            self.assertEqual(n.tier_class, expected, f"tier_class mismatch for {tier}")

    def test_tier_class_unknown_passes_through(self):
        from utils.job_common import Mu2eName
        n = Mu2eName.build(tier="zzz", owner="mu2e", description="X",
                           dsconf="MDC2025af", extension="art")
        self.assertEqual(n.tier_class, "zzz")

    # derivations

    def test_dataset_idempotent(self):
        from utils.job_common import Mu2eName
        ds = Mu2eName.parse("dts.mu2e.CeEndpoint.Run1Bab.art")
        self.assertEqual(ds.dataset, ds)

    def test_with_sequencer_and_extension_and_as_tier(self):
        from utils.job_common import Mu2eName
        n = Mu2eName.parse("dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art")
        self.assertEqual(str(n.with_sequencer("999999_00000001")),
                         "dts.mu2e.CeEndpoint.Run1Bab.999999_00000001.art")
        self.assertEqual(str(n.with_extension("root")),
                         "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.root")
        self.assertEqual(str(n.as_tier("log").with_extension("log")),
                         "log.mu2e.CeEndpoint.Run1Bab.001440_00001234.log")

    def test_log_dataset_from_tarball(self):
        from utils.job_common import Mu2eName
        n = Mu2eName.parse("cnf.mu2e.FlatMuMinus.MDC2025ab.0.tar")
        self.assertEqual(str(n.log_dataset()), "log.mu2e.FlatMuMinus.MDC2025ab.log")

    def test_log_dataset_matches_legacy_helper(self):
        """Pinned against the published output of the legacy
        db_builder._jobdef_to_log_dataset helper (deleted with the POMS
        monitoring toolchain; expected values listed inline).
        """
        from utils.job_common import Mu2eName
        cases = [
            ("cnf.mu2e.FlatMuMinus.MDC2025ab.0.tar",
             "log.mu2e.FlatMuMinus.MDC2025ab.log"),
            ("cnf.mu2e.CeEndpoint.MDC2025af_best_v1_3.42.tar",
             "log.mu2e.CeEndpoint.MDC2025af_best_v1_3.log"),
            ("cnf.mu2e.CosmicCRYAll.Run1Bag.123456.tar",
             "log.mu2e.CosmicCRYAll.Run1Bag.log"),
        ]
        for tarball, expected in cases:
            self.assertEqual(
                str(Mu2eName.parse(tarball).log_dataset()),
                expected,
                f"log_dataset mismatch for {tarball}",
            )

    # round-trip

    def test_roundtrip_file(self):
        from utils.job_common import Mu2eName
        s = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
        self.assertEqual(str(Mu2eName.parse(s)), s)

    def test_roundtrip_dataset(self):
        from utils.job_common import Mu2eName
        s = "dts.mu2e.CeEndpoint.Run1Bab.art"
        self.assertEqual(str(Mu2eName.parse(s)), s)

    def test_roundtrip_tarball(self):
        from utils.job_common import Mu2eName
        s = "cnf.mu2e.CeEndpoint.MDC2025af_best_v1_3.42.tar"
        self.assertEqual(str(Mu2eName.parse(s)), s)

    # build validation

    def test_build_rejects_dot_in_field(self):
        from utils.job_common import Mu2eName
        with self.assertRaises(ValueError):
            Mu2eName.build(tier="dts", owner="mu2e", description="X.Y",
                           dsconf="MDC2025af", extension="art")

    def test_build_rejects_dot_in_sequencer(self):
        from utils.job_common import Mu2eName
        with self.assertRaises(ValueError):
            Mu2eName.build(tier="dts", owner="mu2e", description="X",
                           dsconf="MDC2025af", sequencer="00.00", extension="art")


# ---------------------------------------------------------------------------
# 1c. Submission-map entry accessors (jobdesc.py)
# ---------------------------------------------------------------------------

class TestMapEntry(unittest.TestCase):
    """Pin the fail-loud / sentinel-default contract of utils.jobdesc."""

    GOOD = {
        "tarball": "cnf.mu2e.RMCFlatGamma.MDC2025ag.0.tar",
        "outputs": [{"dataset": "sim.mu2e.RMCFlatGamma.MDC2025ag.art",
                     "location": "tape"}],
        "njobs": 50,
        "inloc": "tape",
    }

    def test_tarball_of_happy_path(self):
        from utils.jobdesc import tarball_of
        self.assertEqual(tarball_of(self.GOOD), self.GOOD["tarball"])

    def test_tarball_of_missing_raises(self):
        from utils.jobdesc import tarball_of
        with self.assertRaises(ValueError):
            tarball_of({})

    def test_tarball_of_rejects_non_cnf(self):
        from utils.jobdesc import tarball_of
        with self.assertRaises(ValueError):
            tarball_of({"tarball": "sim.mu2e.X.MDC2025ag.001430_00000000.art"})

    def test_tarball_of_rejects_unparseable(self):
        from utils.jobdesc import tarball_of
        with self.assertRaises(ValueError):
            tarball_of({"tarball": "not-a-mu2e-name.txt"})

    def test_outputs_of_happy_path(self):
        from utils.jobdesc import outputs_of
        self.assertEqual(outputs_of(self.GOOD), self.GOOD["outputs"])

    def test_outputs_of_missing_raises(self):
        from utils.jobdesc import outputs_of
        with self.assertRaises(ValueError):
            outputs_of({"tarball": self.GOOD["tarball"]})

    def test_njobs_of_present(self):
        from utils.jobdesc import njobs_of
        self.assertEqual(njobs_of(self.GOOD), 50)

    def test_njobs_of_absent_returns_default(self):
        from utils.jobdesc import njobs_of
        self.assertIsNone(njobs_of({}))
        self.assertEqual(njobs_of({}, default=0), 0)
        self.assertEqual(njobs_of({}, default="?"), "?")

    def test_inloc_of_present(self):
        from utils.jobdesc import inloc_of
        self.assertEqual(inloc_of(self.GOOD), "tape")

    def test_inloc_of_absent_returns_none_sentinel(self):
        from utils.jobdesc import inloc_of
        self.assertEqual(inloc_of({}), "none")


# ---------------------------------------------------------------------------
# 2. remove_storage_prefix (job_common.py)
# ---------------------------------------------------------------------------

class TestRemoveStoragePrefix(unittest.TestCase):

    def test_enstore_prefix(self):
        path = "enstore:/pnfs/mu2e/tape/phy-sim/dts/mu2e/CeEndpoint/Run1Bab/art"
        self.assertEqual(
            remove_storage_prefix(path),
            "/pnfs/mu2e/tape/phy-sim/dts/mu2e/CeEndpoint/Run1Bab/art"
        )

    def test_dcache_prefix(self):
        path = "dcache:/pnfs/mu2e/persistent/datasets/phy-sim/dts/mu2e"
        self.assertEqual(remove_storage_prefix(path), "/pnfs/mu2e/persistent/datasets/phy-sim/dts/mu2e")

    def test_no_prefix_passthrough(self):
        path = "/pnfs/mu2e/tape/phy-sim/something"
        self.assertEqual(remove_storage_prefix(path), path)

    def test_empty_string(self):
        self.assertEqual(remove_storage_prefix(""), "")


# ---------------------------------------------------------------------------
# 3. Mu2eJobBase._my_random (job_common.py)
# ---------------------------------------------------------------------------

class TestMyRandom(unittest.TestCase):
    """_my_random is accessed via Mu2eJobBase (parent of Mu2eJobFCL)."""

    def setUp(self):
        # Use a minimal concrete subclass to access the method
        class _Stub(Mu2eJobBase):
            def _extract_json(self):
                return {}
        # _Stub needs a real (dummy) tarball path; we just test the hash method
        self._stub = object.__new__(_Stub)

    def _rand(self, *args):
        return Mu2eJobBase._my_random(self._stub, *args)

    def test_deterministic(self):
        a = self._rand(5, "file1.art", "file2.art")
        b = self._rand(5, "file1.art", "file2.art")
        self.assertEqual(a, b)

    def test_different_index(self):
        a = self._rand(0, "file1.art", "file2.art")
        b = self._rand(1, "file1.art", "file2.art")
        self.assertNotEqual(a, b)

    def test_different_files(self):
        a = self._rand(0, "file1.art")
        b = self._rand(0, "file2.art")
        self.assertNotEqual(a, b)

    def test_returns_integer(self):
        self.assertIsInstance(self._rand(0, "x"), int)


# ---------------------------------------------------------------------------
# 4. Mu2eJobFCL: path location and formatting
# ---------------------------------------------------------------------------

class TestLocateFile(unittest.TestCase):
    """Tests for resolver.locate without SAM (uses dir: prefix)."""

    def setUp(self):
        from utils.jobfcl import Mu2eJobFCL
        files = ["sim.mu2e.Test.MDC2025ac.001430_00000000.art"]
        jp = _root_input_jobpars(files)
        self.tar = _make_tarball(jp, "#include \"base.fcl\"\nmodule_type : RootInput\n")
        self.Cls = Mu2eJobFCL

    def tearDown(self):
        os.unlink(self.tar)

    def test_dir_prefix_no_sam(self):
        job = self.Cls(self.tar, inloc='dir:/data/inputs', proto='file')
        path = job._resolver.locate("myfile.art")
        self.assertEqual(path, "/data/inputs/myfile.art")

    def test_dir_prefix_trailing_slash_stripped(self):
        job = self.Cls(self.tar, inloc='dir:/data/inputs/', proto='file')
        path = job._resolver.locate("myfile.art")
        self.assertEqual(path, "/data/inputs/myfile.art")

    def test_dir_prefix_with_subdirectory(self):
        job = self.Cls(self.tar, inloc='dir:/a/b/c', proto='file')
        path = job._resolver.locate("x.art")
        self.assertEqual(path, "/a/b/c/x.art")


class TestLocateDatasetArea(unittest.TestCase):
    """resolver.locate resolves a DATASET's area once, then computes every
    file path from the name. file_exists_at is the only probe, so these
    tests drive it directly rather than mocking SAM."""

    _FNAME = 'dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art'
    _DISK = ('/pnfs/mu2e/persistent/datasets/phy-sim/dts/mu2e/CeEndpoint/'
             'Run1Bab/art/dd/7e/' + _FNAME)
    _TAPE = ('/pnfs/mu2e/tape/phy-sim/dts/mu2e/CeEndpoint/Run1Bab/art/'
             'dd/7e/' + _FNAME)

    def setUp(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _root_input_jobpars([self._FNAME])
        self.tar = _make_tarball(jp, "#include \"base.fcl\"\nmodule_type : RootInput\n")
        self.Cls = Mu2eJobFCL

    def tearDown(self):
        os.unlink(self.tar)

    @staticmethod
    def _present(*paths):
        """Patch file_exists_at so only `paths` exist."""
        wanted = set(paths)
        return patch('utils.file_resolver.file_exists_at',
                     side_effect=lambda p: p in wanted)

    def test_declared_inloc_used_when_present(self):
        with self._present(self._TAPE, self._DISK):
            job = self.Cls(self.tar, inloc='tape', proto='file')
            self.assertEqual(job._resolver.locate(self._FNAME), self._TAPE)

    def test_disk_inloc_used_when_present(self):
        with self._present(self._TAPE, self._DISK):
            job = self.Cls(self.tar, inloc='disk', proto='file')
            self.assertEqual(job._resolver.locate(self._FNAME), self._DISK)

    def test_falls_back_to_disk_not_tape(self):
        """A dataset absent from the declared inloc but present on BOTH
        disk and tape must resolve to disk: reading the tape copy queues
        an Enstore recall that can outlast the job's lease."""
        with self._present(self._TAPE, self._DISK):
            job = self.Cls(self.tar, inloc='resilient', proto='file')
            self.assertEqual(job._resolver.locate(self._FNAME), self._DISK)

    def test_tape_used_when_only_copy(self):
        with self._present(self._TAPE):
            job = self.Cls(self.tar, inloc='resilient', proto='file')
            self.assertEqual(job._resolver.locate(self._FNAME), self._TAPE)

    def test_absent_everywhere_raises(self):
        with self._present():
            job = self.Cls(self.tar, inloc='tape', proto='file')
            with self.assertRaises(ValueError):
                job._resolver.locate(self._FNAME)

    def test_area_resolved_once_per_dataset(self):
        """The probe count is per dataset, not per file — the property
        that lets a 20,000-file merge resolve without a SAM batch."""
        other = 'dts.mu2e.CeEndpoint.Run1Bab.001440_00009999.art'
        with self._present(self._TAPE, self._DISK) as probe:
            job = self.Cls(self.tar, inloc='tape', proto='file')
            job._resolver.locate(self._FNAME)
            calls_after_first = probe.call_count
            job._resolver.locate(other)
            self.assertEqual(probe.call_count, calls_after_first)

    def test_no_sam_call(self):
        """locate() must not touch SAM at all — that is the whole point."""
        with self._present(self._TAPE), \
             patch('utils.samweb_wrapper.locate_file_strict') as mock_sam:
            job = self.Cls(self.tar, inloc='tape', proto='file')
            job._resolver.locate(self._FNAME)
        mock_sam.assert_not_called()


class TestFormatFilename(unittest.TestCase):
    """Tests for _format_filename protocol handling."""

    def setUp(self):
        from utils.jobfcl import Mu2eJobFCL
        files = ["sim.mu2e.Test.MDC2025ac.001430_00000000.art"]
        jp = _root_input_jobpars(files)
        self.tar = _make_tarball(jp, "#include \"base.fcl\"\nmodule_type : RootInput\n")
        self.Cls = Mu2eJobFCL

    def tearDown(self):
        os.unlink(self.tar)

    def test_file_proto_returns_physical_path(self):
        job = self.Cls(self.tar, inloc='dir:/pnfs/mu2e/tape/phy-sim', proto='file')
        result = job._format_filename("myfile.art")
        self.assertEqual(result, "/pnfs/mu2e/tape/phy-sim/myfile.art")

    def test_root_proto_converts_pnfs_to_xroot(self):
        job = self.Cls(self.tar, inloc='dir:/pnfs/mu2e/tape/phy-sim', proto='root')
        result = job._format_filename("myfile.art")
        self.assertTrue(result.startswith("xroot://fndcadoor.fnal.gov//pnfs/fnal.gov/usr/"))
        self.assertIn("myfile.art", result)

    def test_root_proto_non_pnfs_raises(self):
        """root protocol requires /pnfs/ paths; non-pnfs should raise."""
        job = self.Cls(self.tar, inloc='dir:/local/data', proto='root')
        with self.assertRaises(ValueError):
            job._format_filename("myfile.art")

    def test_root_proto_xroot_path_structure(self):
        job = self.Cls(self.tar, inloc='dir:/pnfs/mu2e/tape/phy-sim/dts', proto='root')
        result = job._format_filename("dts.mu2e.X.Y.000001_00000001.art")
        expected_prefix = "xroot://fndcadoor.fnal.gov//pnfs/fnal.gov/usr/mu2e/tape/phy-sim/dts/"
        self.assertTrue(result.startswith(expected_prefix),
                        f"Expected prefix: {expected_prefix}\nGot: {result}")

    def test_enstore_prefix_stripped_in_root_proto(self):
        """A storage prefix must be stripped before xroot conversion.

        Computed paths never carry `enstore:`, but url() still runs
        remove_storage_prefix for SAM-derived paths reaching it from
        other consumers, so the stripping stays covered here.
        """
        from utils.file_resolver import remove_storage_prefix, xroot_read_url
        stripped = remove_storage_prefix('enstore:/pnfs/mu2e/tape/phy-sim/f.art')
        self.assertEqual(stripped, '/pnfs/mu2e/tape/phy-sim/f.art')
        self.assertTrue(xroot_read_url(stripped).startswith(
            "xroot://fndcadoor.fnal.gov//pnfs/"))


# ---------------------------------------------------------------------------
# 5. Mu2eJobFCL: job inputs selection
# ---------------------------------------------------------------------------

class TestJobPrimaryInputs(unittest.TestCase):

    def setUp(self):
        from utils.jobfcl import Mu2eJobFCL
        self.files = [
            "sim.mu2e.Test.MDC2025ac.001430_%08d.art" % i for i in range(10)
        ]
        jp = _root_input_jobpars(self.files, merge=2)
        self.tar = _make_tarball(jp, "#include \"base.fcl\"\nmodule_type : RootInput\n")
        self.Cls = Mu2eJobFCL

    def tearDown(self):
        os.unlink(self.tar)

    def test_first_job_gets_first_merge_files(self):
        job = self.Cls(self.tar, inloc='dir:/tmp')
        result = job.job_primary_inputs(0)
        self.assertEqual(result['source.fileNames'], self.files[0:2])

    def test_second_job_gets_next_slice(self):
        job = self.Cls(self.tar, inloc='dir:/tmp')
        result = job.job_primary_inputs(1)
        self.assertEqual(result['source.fileNames'], self.files[2:4])

    def test_last_job(self):
        job = self.Cls(self.tar, inloc='dir:/tmp')
        result = job.job_primary_inputs(4)
        self.assertEqual(result['source.fileNames'], self.files[8:10])

    def test_out_of_range_raises(self):
        job = self.Cls(self.tar, inloc='dir:/tmp')
        with self.assertRaises(ValueError):
            job.job_primary_inputs(5)

    def test_njobs_correct(self):
        job = self.Cls(self.tar, inloc='dir:/tmp')
        self.assertEqual(job.njobs(), 5)


class TestJobPrimaryInputsMergeOne(unittest.TestCase):
    """Edge case: merge=1 (each job gets exactly 1 file)."""

    def setUp(self):
        from utils.jobfcl import Mu2eJobFCL
        self.files = ["sim.mu2e.T.MDC2025ac.001430_%08d.art" % i for i in range(3)]
        jp = _root_input_jobpars(self.files, merge=1)
        self.tar = _make_tarball(jp, "#include \"base.fcl\"\nmodule_type : RootInput\n")
        self.job = Mu2eJobFCL(self.tar, inloc='dir:/tmp')

    def tearDown(self):
        os.unlink(self.tar)

    def test_each_job_gets_one_file(self):
        for i, f in enumerate(self.files):
            result = self.job.job_primary_inputs(i)
            self.assertEqual(result['source.fileNames'], [f])

    def test_njobs_equals_file_count(self):
        self.assertEqual(self.job.njobs(), 3)


class TestJobAuxInputsRandom(unittest.TestCase):
    """Auxiliary inputs in random (default) mode."""

    def _make_job_with_aux(self, aux_files, nreq=2):
        from utils.jobfcl import Mu2eJobFCL
        jp = {
            "code": "",
            "setup": "/cvmfs/test/setup.sh",
            "tbs": {
                "seed": "services.SeedService.baseSeed",
                "subrunkey": "source.firstSubRun",
                "event_id": {"source.firstRun": 1430, "source.maxEvents": 1000},
                "outfiles": {"outputs.Out.fileName": "sim.mu2e.T.TC.sequencer.art"},
                "auxin": {
                    "physics.producers.gen.fileNames": [nreq, aux_files]
                },
                "sequential_aux": False,
            },
            "jobname": "cnf.mu2e.T.TC.0.tar",
            "owner": "mu2e",
            "dsconf": "TC",
        }
        tar = _make_tarball(jp, "module_type : EmptyEvent\n")
        return Mu2eJobFCL(tar, inloc='dir:/tmp'), tar

    def test_deterministic_selection(self):
        files = ["aux_%02d.art" % i for i in range(10)]
        job, tar = self._make_job_with_aux(files, nreq=3)
        try:
            r1 = job.job_aux_inputs(0)
            r2 = job.job_aux_inputs(0)
            self.assertEqual(r1, r2)
        finally:
            os.unlink(tar)

    def test_different_indices_different_selection(self):
        files = ["aux_%02d.art" % i for i in range(10)]
        job, tar = self._make_job_with_aux(files, nreq=3)
        try:
            r0 = job.job_aux_inputs(0)
            r1 = job.job_aux_inputs(1)
            self.assertNotEqual(r0, r1)
        finally:
            os.unlink(tar)

    def test_no_duplicates_in_selection(self):
        files = ["aux_%02d.art" % i for i in range(10)]
        job, tar = self._make_job_with_aux(files, nreq=5)
        try:
            result = job.job_aux_inputs(0)
            selected = result['physics.producers.gen.fileNames']
            self.assertEqual(len(selected), len(set(selected)))
        finally:
            os.unlink(tar)

    def test_correct_count_returned(self):
        files = ["aux_%02d.art" % i for i in range(10)]
        job, tar = self._make_job_with_aux(files, nreq=4)
        try:
            result = job.job_aux_inputs(0)
            self.assertEqual(len(result['physics.producers.gen.fileNames']), 4)
        finally:
            os.unlink(tar)


class TestJobAuxInputsSequential(unittest.TestCase):
    """Auxiliary inputs in sequential mode."""

    def _make_job_with_seq_aux(self, aux_files, nreq=2):
        from utils.jobfcl import Mu2eJobFCL
        jp = {
            "code": "",
            "setup": "/cvmfs/test/setup.sh",
            "tbs": {
                "seed": "services.SeedService.baseSeed",
                "subrunkey": "source.firstSubRun",
                "event_id": {"source.firstRun": 1430, "source.maxEvents": 1000},
                "outfiles": {"outputs.Out.fileName": "sim.mu2e.T.TC.sequencer.art"},
                "auxin": {
                    "physics.producers.gen.fileNames": [nreq, aux_files]
                },
                "sequential_aux": True,
            },
            "jobname": "cnf.mu2e.T.TC.0.tar",
            "owner": "mu2e",
            "dsconf": "TC",
        }
        tar = _make_tarball(jp, "module_type : EmptyEvent\n")
        return Mu2eJobFCL(tar, inloc='dir:/tmp'), tar

    def test_sequential_first_job(self):
        files = ["aux_%02d.art" % i for i in range(6)]
        job, tar = self._make_job_with_seq_aux(files, nreq=2)
        try:
            result = job.job_aux_inputs(0)
            self.assertEqual(result['physics.producers.gen.fileNames'], files[0:2])
        finally:
            os.unlink(tar)

    def test_sequential_second_job(self):
        files = ["aux_%02d.art" % i for i in range(6)]
        job, tar = self._make_job_with_seq_aux(files, nreq=2)
        try:
            result = job.job_aux_inputs(1)
            self.assertEqual(result['physics.producers.gen.fileNames'], files[2:4])
        finally:
            os.unlink(tar)

    def test_sequential_rollover(self):
        """When index * nreq >= nfiles, roll over from the beginning."""
        files = ["aux_%02d.art" % i for i in range(4)]
        job, tar = self._make_job_with_seq_aux(files, nreq=2)
        try:
            # Job 2: first=4, which == nf → rollover → first=0
            result = job.job_aux_inputs(2)
            self.assertEqual(result['physics.producers.gen.fileNames'], files[0:2])
        finally:
            os.unlink(tar)


# ---------------------------------------------------------------------------
# 6. Mu2eJobFCL: sequencer
# ---------------------------------------------------------------------------

class TestSequencer(unittest.TestCase):

    def test_sequencer_from_event_id(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _empty_event_jobpars(run=1430)
        tar = _make_tarball(jp, "module_type : EmptyEvent\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            seq = job.sequencer(5)
            self.assertEqual(seq, "001430_00000005")
        finally:
            os.unlink(tar)

    def test_sequencer_from_input_files(self):
        from utils.jobfcl import Mu2eJobFCL
        files = ["sim.mu2e.Test.MDC2025ac.001430_00000000.art",
                 "sim.mu2e.Test.MDC2025ac.001430_00000001.art"]
        jp = _root_input_jobpars(files, merge=2)
        tar = _make_tarball(jp, "module_type : RootInput\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            seq = job.sequencer(0)
            # First (sorted) sequencer from input files
            self.assertEqual(seq, "001430_00000000")
        finally:
            os.unlink(tar)

    def test_sequencer_different_indices_differ(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _empty_event_jobpars(run=1430)
        tar = _make_tarball(jp, "module_type : EmptyEvent\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            self.assertNotEqual(job.sequencer(0), job.sequencer(1))
        finally:
            os.unlink(tar)


# ---------------------------------------------------------------------------
# 7. Mu2eJobFCL: job outputs
# ---------------------------------------------------------------------------

class TestJobOutputs(unittest.TestCase):

    def test_output_sequencer_substituted(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _empty_event_jobpars(run=1430)
        tar = _make_tarball(jp, "module_type : EmptyEvent\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            outputs = job.job_outputs(7)
            out_file = outputs['outputs.PrimaryOutput.fileName']
            # Sequencer for index 7 with run 1430 = 001430_00000007
            self.assertIn("001430_00000007", out_file)
        finally:
            os.unlink(tar)

    def test_output_owner_substituted(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _empty_event_jobpars(run=1430, owner='oksuzian')
        tar = _make_tarball(jp, "module_type : EmptyEvent\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            outputs = job.job_outputs(0)
            out_file = outputs['outputs.PrimaryOutput.fileName']
            self.assertIn("oksuzian", out_file)
        finally:
            os.unlink(tar)

    def test_output_dsconf_substituted(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _empty_event_jobpars(run=1430, dsconf='MDC2025ac')
        tar = _make_tarball(jp, "module_type : EmptyEvent\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            outputs = job.job_outputs(0)
            out_file = outputs['outputs.PrimaryOutput.fileName']
            self.assertIn("MDC2025ac", out_file)
        finally:
            os.unlink(tar)

    def test_output_follows_mu2e_naming(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _empty_event_jobpars(run=1430, owner='mu2e', dsconf='TestConf')
        tar = _make_tarball(jp, "module_type : EmptyEvent\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            outputs = job.job_outputs(3)
            out_file = outputs['outputs.PrimaryOutput.fileName']
            parts = out_file.split('.')
            self.assertEqual(len(parts), 6, f"Expected 6 parts, got: {out_file}")
            self.assertEqual(parts[0], "sim")
        finally:
            os.unlink(tar)


# ---------------------------------------------------------------------------
# 8. Mu2eJobFCL: generate_fcl
# ---------------------------------------------------------------------------

class TestGenerateFCL(unittest.TestCase):

    def setUp(self):
        from utils.jobfcl import Mu2eJobFCL
        self.files = ["sim.mu2e.Test.MDC2025ac.001430_%08d.art" % i for i in range(4)]
        jp = _root_input_jobpars(self.files, merge=2)
        self.tar = _make_tarball(jp, "#include \"base.fcl\"\nmodule_type : RootInput\n")
        self.Cls = Mu2eJobFCL

    def tearDown(self):
        os.unlink(self.tar)

    def test_fcl_contains_header_comment(self):
        job = self.Cls(self.tar, inloc='dir:/pnfs/mu2e/tape/phy-sim', proto='file')
        fcl = job.generate_fcl(0)
        self.assertIn("Code added by mu2ejobfcl", fcl)

    def test_fcl_contains_input_files(self):
        job = self.Cls(self.tar, inloc='dir:/pnfs/mu2e/tape/phy-sim', proto='file')
        fcl = job.generate_fcl(0)
        self.assertIn(self.files[0], fcl)
        self.assertIn(self.files[1], fcl)

    def test_fcl_does_not_contain_other_job_files(self):
        job = self.Cls(self.tar, inloc='dir:/pnfs/mu2e/tape/phy-sim', proto='file')
        fcl = job.generate_fcl(0)
        self.assertNotIn(self.files[2], fcl)

    def test_fcl_contains_output_filename(self):
        job = self.Cls(self.tar, inloc='dir:/pnfs/mu2e/tape/phy-sim', proto='file')
        fcl = job.generate_fcl(1)
        outputs = job.job_outputs(1)
        for fname in outputs.values():
            self.assertIn(fname, fcl)

    def test_fcl_second_job_different_from_first(self):
        job = self.Cls(self.tar, inloc='dir:/pnfs/mu2e/tape/phy-sim', proto='file')
        fcl0 = job.generate_fcl(0)
        fcl1 = job.generate_fcl(1)
        self.assertNotEqual(fcl0, fcl1)

    def test_fcl_contains_source_file_names_key(self):
        job = self.Cls(self.tar, inloc='dir:/pnfs/mu2e/tape/phy-sim', proto='file')
        fcl = job.generate_fcl(0)
        self.assertIn("source.fileNames", fcl)

    def test_fcl_xroot_format_for_root_proto(self):
        job = self.Cls(self.tar, inloc='dir:/pnfs/mu2e/tape/phy-sim', proto='root')
        fcl = job.generate_fcl(0)
        self.assertIn("xroot://fndcadoor.fnal.gov//pnfs/", fcl)

    def test_empty_event_fcl_has_subrun(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _empty_event_jobpars(run=1430)
        tar = _make_tarball(jp, "module_type : EmptyEvent\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            fcl = job.generate_fcl(3)
            self.assertIn("source.firstSubRun: 3", fcl)
        finally:
            os.unlink(tar)


# ---------------------------------------------------------------------------
# 9. Mu2eDSName path building (datasetFileList.py)
# ---------------------------------------------------------------------------

class TestMu2eDSName(unittest.TestCase):
    """Path-building tests for the (deleted) Mu2eDSName, retargeted at
    `datasetFileList._dataset_dir`, which folds the logic onto
    Mu2eName.tier_class.
    """

    def setUp(self):
        from utils.datasetFileList import _dataset_dir
        self.dsdir = _dataset_dir

    def test_sim_tape_path(self):
        path = self.dsdir("sim.mu2e.MuminusStopsCat.MDC2025ac.art", 'tape')
        self.assertEqual(path, "/pnfs/mu2e/tape/phy-sim/sim/mu2e/MuminusStopsCat/MDC2025ac/art")

    def test_dts_tape_path(self):
        path = self.dsdir("dts.mu2e.CeEndpoint.Run1Bab.art", 'tape')
        self.assertEqual(path, "/pnfs/mu2e/tape/phy-sim/dts/mu2e/CeEndpoint/Run1Bab/art")

    def test_dts_disk_path(self):
        path = self.dsdir("dts.mu2e.CeEndpoint.Run1Bab.art", 'disk')
        self.assertEqual(path, "/pnfs/mu2e/persistent/datasets/phy-sim/dts/mu2e/CeEndpoint/Run1Bab/art")

    def test_nts_type(self):
        path = self.dsdir("nts.mu2e.CosmicCRY.MDC2025ac.root", 'tape')
        self.assertIn("phy-nts", path)

    def test_mcs_type(self):
        path = self.dsdir("mcs.mu2e.CosmicCRY.MDC2025ac.art", 'tape')
        self.assertIn("phy-sim", path)

    def test_unknown_type(self):
        path = self.dsdir("log.mu2e.Something.MDC2025ac.log", 'tape')
        self.assertIn("phy-etc", path)

    def test_scratch_path(self):
        path = self.dsdir("sim.mu2e.Test.MDC2025ac.art", 'scratch')
        self.assertIn("/pnfs/mu2e/scratch/datasets/", path)

    def test_unknown_location_returns_empty(self):
        path = self.dsdir("sim.mu2e.Test.MDC2025ac.art", 'stash')  # not yet implemented
        self.assertEqual(path, "")


# ---------------------------------------------------------------------------
# 10. datasetFileList Mu2eName hash paths
# ---------------------------------------------------------------------------

class TestDatasetFileListFilename(unittest.TestCase):

    def setUp(self):
        # datasetFileList no longer re-exports Mu2eName; pull directly
        # from job_common (where the alias still points at Mu2eName).
        from utils.job_common import Mu2eName
        self.Cls = Mu2eName

    def test_relpathname_has_three_parts(self):
        fn = self.Cls("dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art")
        relpath = fn.relpathname()
        parts = relpath.split('/')
        self.assertEqual(len(parts), 3, f"Expected 3 path parts, got: {relpath}")

    def test_relpathname_ends_with_filename(self):
        name = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
        fn = self.Cls(name)
        self.assertTrue(fn.relpathname().endswith(name))

    def test_relpathname_uses_sha256_prefix(self):
        name = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
        fn = self.Cls(name)
        h = hashlib.sha256(name.encode()).hexdigest()
        expected_prefix = f"{h[:2]}/{h[2:4]}"
        self.assertTrue(fn.relpathname().startswith(expected_prefix))

    def test_relpathname_deterministic(self):
        name = "sim.mu2e.Test.MDC2025ac.001430_00000000.art"
        fn1 = self.Cls(name)
        fn2 = self.Cls(name)
        self.assertEqual(fn1.relpathname(), fn2.relpathname())

    def test_different_filenames_different_hash(self):
        fn1 = self.Cls("sim.mu2e.A.MDC2025ac.001430_00000000.art")
        fn2 = self.Cls("sim.mu2e.B.MDC2025ac.001430_00000000.art")
        # Different files should generally hash differently (not guaranteed but
        # extremely likely for these inputs)
        self.assertNotEqual(fn1.relpathname(), fn2.relpathname())


# ---------------------------------------------------------------------------
# 11. Stash path derivation (prerequisite check for future implementation)
# ---------------------------------------------------------------------------

class TestStashPathDerivation(unittest.TestCase):
    """
    Tests for the stash path construction logic described in the StashCache
    plan. These tests specify the expected behavior for inloc='stash' so that
    the implementation can be validated against them.

    The formula is:
        STASH_READ_ROOT/datasets/<tier>/<owner>/<description>/<dsconf>/<ext>/<filename>
    derived purely from the filename via Mu2eName.
    """

    STASH_ROOT = "/cvmfs/mu2e.osgstorage.org/pnfs/fnal.gov/usr/mu2e/persistent/stash"

    def _stash_path(self, filename: str) -> str:
        """Reference implementation of stash path building (not yet in code)."""
        fn = Mu2eName(filename)
        dataset = f"{fn.tier}.{fn.owner}.{fn.description}.{fn.dsconf}.{fn.extension}"
        ds_path = dataset.replace('.', '/')
        return f"{self.STASH_ROOT}/datasets/{ds_path}/{filename}"

    def test_ce_endpoint_path(self):
        fname = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
        path = self._stash_path(fname)
        expected = (
            f"{self.STASH_ROOT}/datasets/dts/mu2e/CeEndpoint/Run1Bab/art/{fname}"
        )
        self.assertEqual(path, expected)

    def test_sim_file_path(self):
        fname = "sim.mu2e.MuminusStopsCat.MDC2025ac.001430_00000007.art"
        path = self._stash_path(fname)
        expected = (
            f"{self.STASH_ROOT}/datasets/sim/mu2e/MuminusStopsCat/MDC2025ac/art/{fname}"
        )
        self.assertEqual(path, expected)

    def test_different_owners(self):
        fname = "dts.oksuzian.CeEndpoint.Run1Bab.001440_00000001.art"
        path = self._stash_path(fname)
        self.assertIn("/oksuzian/", path)

    def test_path_contains_stash_root(self):
        fname = "nts.mu2e.CosmicCRY.MDC2025ac.001430_00000000.root"
        path = self._stash_path(fname)
        self.assertTrue(path.startswith(self.STASH_ROOT))

    def test_path_contains_datasets_prefix(self):
        fname = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
        path = self._stash_path(fname)
        self.assertIn("/datasets/", path)

    def test_filename_at_end_of_path(self):
        fname = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
        path = self._stash_path(fname)
        self.assertTrue(path.endswith(fname))


# ---------------------------------------------------------------------------
# 12. jobfcl stash integration (resolver.locate and _format_filename)
# ---------------------------------------------------------------------------

STASH_READ_DEFAULT = "/cvmfs/mu2e.osgstorage.org/pnfs/fnal.gov/usr/mu2e/persistent/stash"
STASH_WRITE_DEFAULT = "/pnfs/mu2e/persistent/stash"


class TestLocateFileStash(unittest.TestCase):
    """resolver.locate with inloc='stash' — path derived from filename (SAM only as fallback)."""

    def setUp(self):
        from utils.jobfcl import Mu2eJobFCL
        files = ["dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"]
        jp = _root_input_jobpars(files)
        self.tar = _make_tarball(jp, "module_type : RootInput\n")
        self.Cls = Mu2eJobFCL
        # Simulate files being present on stash CVMFS
        self._exists_patch = patch('os.path.exists', return_value=True)
        self._exists_patch.start()

    def tearDown(self):
        self._exists_patch.stop()
        os.unlink(self.tar)

    def test_stash_locate_no_sam_call(self):
        """SAM must not be contacted when inloc='stash'."""
        with patch('utils.samweb_wrapper.locate_file_strict') as mock_locate:
            from utils.jobfcl import Mu2eJobFCL
            job = Mu2eJobFCL(self.tar, inloc='stash', proto='file')
            job._resolver.locate("dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art")
        mock_locate.assert_not_called()

    def test_stash_path_structure(self):
        job = self.Cls(self.tar, inloc='stash', proto='file')
        fname = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
        path = job._resolver.locate(fname)
        expected = (
            f"{STASH_READ_DEFAULT}/datasets/dts/mu2e/CeEndpoint/Run1Bab/art/{fname}"
        )
        self.assertEqual(path, expected)

    def test_stash_path_sim_file(self):
        job = self.Cls(self.tar, inloc='stash', proto='file')
        fname = "sim.mu2e.MuminusStopsCat.MDC2025ac.001430_00000007.art"
        path = job._resolver.locate(fname)
        self.assertIn("/datasets/sim/mu2e/MuminusStopsCat/MDC2025ac/art/", path)
        self.assertTrue(path.endswith(fname))

    def test_stash_path_uses_env_var(self):
        custom_root = "/custom/stash/root"
        with patch.dict(os.environ, {"MU2E_STASH_READ": custom_root}):
            # Re-import to pick up new env var (module-level constant)
            import importlib
            import utils.jobfcl as jfcl_mod
            importlib.reload(jfcl_mod)
            job = jfcl_mod.Mu2eJobFCL(self.tar, inloc='stash', proto='file')
            fname = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
            path = job._resolver.locate(fname)
            self.assertTrue(path.startswith(custom_root))
            # Restore
            importlib.reload(jfcl_mod)


class TestFormatFilenameStash(unittest.TestCase):
    """_format_filename with inloc='stash' always returns plain path."""

    def setUp(self):
        from utils.jobfcl import Mu2eJobFCL
        files = ["dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"]
        jp = _root_input_jobpars(files)
        self.tar = _make_tarball(jp, "module_type : RootInput\n")
        self.Cls = Mu2eJobFCL
        self.fname = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
        # Simulate files being present on stash CVMFS
        self._exists_patch = patch('os.path.exists', return_value=True)
        self._exists_patch.start()

    def tearDown(self):
        self._exists_patch.stop()
        os.unlink(self.tar)

    def test_stash_file_proto_returns_cvmfs_path(self):
        job = self.Cls(self.tar, inloc='stash', proto='file')
        result = job._format_filename(self.fname)
        self.assertTrue(result.startswith(STASH_READ_DEFAULT))

    def test_stash_root_proto_still_returns_plain_path(self):
        """proto='root' must be ignored for stash — no xroot conversion."""
        job = self.Cls(self.tar, inloc='stash', proto='root')
        result = job._format_filename(self.fname)
        self.assertFalse(result.startswith("xroot://"),
                         f"Expected plain CVMFS path, got: {result}")
        self.assertTrue(result.startswith(STASH_READ_DEFAULT))

    def test_stash_fcl_contains_cvmfs_path(self):
        from utils.jobfcl import Mu2eJobFCL
        files = ["dts.mu2e.CeEndpoint.Run1Bab.001440_00000000.art",
                 "dts.mu2e.CeEndpoint.Run1Bab.001440_00000001.art"]
        jp = _root_input_jobpars(files, merge=2)
        tar = _make_tarball(jp, "module_type : RootInput\n")
        try:
            job = Mu2eJobFCL(tar, inloc='stash', proto='root')
            fcl = job.generate_fcl(0)
            self.assertIn(STASH_READ_DEFAULT, fcl)
            self.assertNotIn("xroot://", fcl)
        finally:
            os.unlink(tar)


# ---------------------------------------------------------------------------
# 13. stash_utils module
# ---------------------------------------------------------------------------

class TestStashUtils(unittest.TestCase):
    """Tests for utils/stash_utils.py path helpers."""

    def setUp(self):
        from utils import stash_utils
        self.su = stash_utils

    def test_read_root_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MU2E_STASH_READ", None)
            root = self.su.stash_read_root()
        self.assertEqual(root, STASH_READ_DEFAULT)

    def test_write_root_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MU2E_STASH_WRITE", None)
            root = self.su.stash_write_root()
        self.assertEqual(root, STASH_WRITE_DEFAULT)

    def test_read_root_from_env(self):
        with patch.dict(os.environ, {"MU2E_STASH_READ": "/my/read/root"}):
            root = self.su.stash_read_root()
        self.assertEqual(root, "/my/read/root")

    def test_write_root_from_env(self):
        with patch.dict(os.environ, {"MU2E_STASH_WRITE": "/my/write/root"}):
            root = self.su.stash_write_root()
        self.assertEqual(root, "/my/write/root")

    def test_read_path_for_file(self):
        fname = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MU2E_STASH_READ", None)
            path = self.su.read_path_for_file(fname)
        expected = f"{STASH_READ_DEFAULT}/datasets/dts/mu2e/CeEndpoint/Run1Bab/art/{fname}"
        self.assertEqual(path, expected)

    def test_write_path_for_file(self):
        fname = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MU2E_STASH_WRITE", None)
            path = self.su.write_path_for_file(fname)
        expected = f"{STASH_WRITE_DEFAULT}/datasets/dts/mu2e/CeEndpoint/Run1Bab/art/{fname}"
        self.assertEqual(path, expected)

    def test_read_and_write_paths_share_subpath(self):
        """The sub-path after the root must be identical for read and write."""
        fname = "sim.mu2e.MuminusStopsCat.MDC2025ac.001430_00000007.art"
        rp = self.su.read_path_for_file(fname)
        wp = self.su.write_path_for_file(fname)
        rp_sub = rp[len(self.su.stash_read_root()):]
        wp_sub = wp[len(self.su.stash_write_root()):]
        self.assertEqual(rp_sub, wp_sub)

    def test_read_path_ends_with_filename(self):
        fname = "dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art"
        path = self.su.read_path_for_file(fname)
        self.assertTrue(path.endswith(fname))

    def test_copy_dataset_dry_run(self):
        """dry_run=True must not copy or makedirs."""
        from utils import stash_utils

        mock_files = ["dts.mu2e.CeEndpoint.Run1Bab.001440_00000000.art",
                      "dts.mu2e.CeEndpoint.Run1Bab.001440_00000001.art"]
        mock_locations = [
            {'location_type': 'disk',
             'full_path': '/pnfs/mu2e/persistent/datasets/phy-sim/dts/mu2e/CeEndpoint/Run1Bab/art'}
        ]

        with patch('utils.stash_utils.files_in_dataset', return_value=mock_files), \
             patch('utils.stash_utils.locate_files_strict',
                   side_effect=lambda fns: {f: mock_locations for f in fns}), \
             patch('os.makedirs') as mock_mkdir, \
             patch('utils.stash_utils.shutil.copyfile') as mock_run:
            n = stash_utils.copy_dataset_to_stash(
                "dts.mu2e.CeEndpoint.Run1Bab.art",
                source_loc='disk',
                dry_run=True,
                verbose=False,
            )

        mock_mkdir.assert_not_called()
        mock_run.assert_not_called()
        self.assertEqual(n.copied, 2)

    def test_copy_dataset_calls_copyfile(self):
        """Copies via shutil.copyfile(src, dest) — not a `cp` subprocess.

        copyfile, not copy2/copy: the destination is dCache, where the
        metadata/permission copy those do is not reliably supported."""
        from utils import stash_utils

        mock_files = ["dts.mu2e.CeEndpoint.Run1Bab.001440_00000000.art"]
        mock_locations = [
            {'location_type': 'disk',
             'full_path': '/pnfs/mu2e/persistent/datasets/phy-sim/dts/mu2e/CeEndpoint/Run1Bab/art'}
        ]
        with patch('utils.stash_utils.files_in_dataset', return_value=mock_files), \
             patch('utils.stash_utils.locate_files_strict',
                   side_effect=lambda fns: {f: mock_locations for f in fns}), \
             patch('os.makedirs'), \
             patch('utils.stash_utils.shutil.copyfile') as mock_run:
            n = stash_utils.copy_dataset_to_stash(
                "dts.mu2e.CeEndpoint.Run1Bab.art",
                source_loc='disk',
                dry_run=False,
                verbose=False,
            )

        self.assertEqual(n.copied, 1)
        src, dest = mock_run.call_args[0]
        self.assertTrue(src.endswith(mock_files[0]))
        self.assertTrue(dest.endswith(mock_files[0]))

    def test_copy_dataset_counts_oserror_as_failure(self):
        """A copy that raises OSError is a failure, not a silent success.

        The old code read subprocess returncode; shutil raises instead, so
        the failure path must catch OSError or a failed copy would be
        counted as copied."""
        from utils import stash_utils

        mock_files = ["dts.mu2e.CeEndpoint.Run1Bab.001440_00000000.art",
                      "dts.mu2e.CeEndpoint.Run1Bab.001440_00000001.art"]
        mock_locations = [
            {'location_type': 'disk',
             'full_path': '/pnfs/mu2e/persistent/datasets/phy-sim/dts/mu2e/CeEndpoint/Run1Bab/art'}
        ]

        with patch('utils.stash_utils.files_in_dataset', return_value=mock_files), \
             patch('utils.stash_utils.locate_files_strict',
                   side_effect=lambda fns: {f: mock_locations for f in fns}), \
             patch('os.makedirs'), \
             patch('utils.stash_utils.shutil.copyfile',
                   side_effect=OSError(28, "No space left on device")):
            n = stash_utils.copy_dataset_to_stash(
                "dts.mu2e.CeEndpoint.Run1Bab.art",
                source_loc='disk', dry_run=False, verbose=False)

        self.assertEqual(n.copied, 0)

    def test_copy_dataset_limit(self):
        """--limit N should copy at most N files."""
        from utils import stash_utils

        mock_files = ["dts.mu2e.CeEndpoint.Run1Bab.001440_%08d.art" % i for i in range(10)]
        mock_locations = [
            {'location_type': 'disk',
             'full_path': '/pnfs/mu2e/persistent/datasets/phy-sim/dts/mu2e/CeEndpoint/Run1Bab/art'}
        ]
        with patch('utils.stash_utils.files_in_dataset', return_value=mock_files), \
             patch('utils.stash_utils.locate_files_strict',
                   side_effect=lambda fns: {f: mock_locations for f in fns}) as mock_loc, \
             patch('os.makedirs'), \
             patch('utils.stash_utils.shutil.copyfile') as mock_run:
            stash_utils.copy_dataset_to_stash(
                "dts.mu2e.CeEndpoint.Run1Bab.art",
                source_loc='disk',
                limit=3,
                dry_run=False,
                verbose=False,
            )
        # one batch locate for the (limited) copy list, not one per file
        mock_loc.assert_called_once()
        self.assertEqual(len(mock_loc.call_args[0][0]), 3)

        self.assertEqual(mock_run.call_count, 3)

    def test_copy_dataset_skips_on_locate_failure(self):
        """Files that cannot be located should be skipped, not crash."""
        from utils import stash_utils

        mock_files = ["dts.mu2e.CeEndpoint.Run1Bab.001440_00000000.art"]

        with patch('utils.stash_utils.files_in_dataset', return_value=mock_files), \
             patch('utils.samweb_wrapper.locate_file_strict', return_value=[]), \
             patch('os.makedirs'), \
             patch('utils.stash_utils.shutil.copyfile') as mock_run:
            n = stash_utils.copy_dataset_to_stash(
                "dts.mu2e.CeEndpoint.Run1Bab.art",
                source_loc='disk',
                dry_run=False,
                verbose=False,
            )

        mock_run.assert_not_called()
        self.assertEqual(n.copied, 0)


# ---------------------------------------------------------------------------
# 14. runmu2e: stash skips copy_input
# ---------------------------------------------------------------------------

class TestProcessJobdefStashSkipsCopyInput(unittest.TestCase):
    """
    When inloc='stash', process_jobdef must use streaming mode even when
    args.copy_input is True — CVMFS files need no local copying.
    """

    def test_stash_does_not_call_mdh_copy(self):
        from utils import runmu2e

        files = ["sim.mu2e.Test.TestConf.001440_00000000.art"]
        jp = _root_input_jobpars(files, merge=1)
        tar = _make_tarball(jp, "module_type : RootInput\n")

        args = MagicMock()
        args.copy_input = True   # would trigger mdh copy for tape/disk

        jobdesc = {
            'tarball': tar,
            'njobs': 1,
            'inloc': 'stash',
            'outputs': [],
        }

        mock_fcl = tar.replace('.tar', '.fcl')

        with patch('utils.runmu2e.write_fcl', return_value=mock_fcl) as mock_wfcl, \
             patch('utils.runmu2e.run') as mock_run, \
             patch('utils.jobquery.Mu2eJobPars') as mock_pars:

            mock_pars.return_value.setup.return_value = "/cvmfs/test/setup.sh"

            runmu2e.process_jobdef(
                jobdesc,
                fname="cnf.mu2e.Test.TestConf.0.fcl",
                args=args,
            )

        # write_fcl must be called with inloc='stash' (streaming), not 'dir:...'
        call_inloc = mock_wfcl.call_args[0][1]
        self.assertEqual(call_inloc, 'stash',
                         f"Expected inloc='stash' (streaming), got '{call_inloc}'")

        # mdh copy-file must NOT have been called
        for call in mock_run.call_args_list:
            cmd = str(call[0][0]) if call[0] else ''
            self.assertNotIn('mdh copy-file', cmd,
                             "mdh copy-file must not be called for stash inloc")

        os.unlink(tar)


class TestProcessJobdefCopyInputFlip(unittest.TestCase):
    """Streaming is the default (POMS-era parity: the launch template
    never passed --copy-input); an entry opts in to local staging with
    copy_input: true, and the entry key wins over the CLI flag."""

    FILE = "sim.mu2e.Test.TestConf.001440_00000000.art"

    def _run(self, *, entry_extra=None, cli_copy=False):
        from utils import runmu2e

        jp = _root_input_jobpars([self.FILE], merge=1)
        tar = _make_tarball(jp, "module_type : RootInput\n")
        args = MagicMock()
        args.copy_input = cli_copy
        jobdesc = {'tarball': tar, 'njobs': 1, 'inloc': 'tape',
                    'outputs': [], **(entry_extra or {})}
        located = {self.FILE: [{'location_type': 'tape'}]}
        try:
            with patch('utils.runmu2e.write_fcl',
                       return_value='x.fcl') as mock_wfcl, \
                 patch('utils.runmu2e.run'), \
                 patch('utils.runmu2e.locate_files_strict',
                       return_value=located), \
                 patch('utils.runmu2e._fetch_file_local') as mock_fetch, \
                 patch('utils.jobquery.Mu2eJobPars') as mock_pars:
                mock_pars.return_value.setup.return_value = "/cvmfs/s.sh"
                runmu2e.process_jobdef(
                    jobdesc, fname="cnf.mu2e.Test.TestConf.0.fcl",
                    args=args)
            return mock_wfcl.call_args[0], mock_fetch
        finally:
            os.unlink(tar)

    def test_default_streams_from_tape(self):
        (_, inloc, proto, _), fetch = self._run()
        self.assertEqual((inloc, proto), ('tape', 'root'))
        # Only the tarball itself may be fetched — never the inputs.
        for call in fetch.call_args_list:
            self.assertNotIn(self.FILE, str(call))

    def test_entry_opt_in_copies(self):
        (_, inloc, proto, _), fetch = self._run(
            entry_extra={'copy_input': True})
        self.assertTrue(inloc.startswith('dir:'), inloc)
        self.assertEqual(proto, 'file')
        fetched = [str(c) for c in fetch.call_args_list]
        self.assertTrue(any(self.FILE in c for c in fetched),
                        f"input not staged locally: {fetched}")

    def test_entry_false_wins_over_cli_flag(self):
        (_, inloc, proto, _), _ = self._run(
            entry_extra={'copy_input': False}, cli_copy=True)
        self.assertEqual((inloc, proto), ('tape', 'root'))

    def test_non_bool_copy_input_fails(self):
        with self.assertRaises(SystemExit):
            self._run(entry_extra={'copy_input': 'yes'})


class TestProcessJobdefCodeRoot(unittest.TestCase):
    """$INPUT_TAR_DIR_LOCAL / args.code_root is the entire mechanism by
    which a grid worker resolves a code-mode cnf's relative `setup`
    (utils/runmu2e.py:_code_root_from, wired into process_jobdef at
    `code_root=_code_root_from(args)`). Deleting that wiring breaks
    every code-mode grid job with a ValueError and costs zero test
    failures unless something exercises it with a REAL relative-setup
    jobpars and a real SimpleNamespace `args` — MagicMock() makes
    getattr(args, 'code_root', None) return a truthy Mock and never
    reaches this code at all."""

    def setUp(self):
        files = ["sim.mu2e.Test.TestConf.001440_00000000.art"]
        jp = _root_input_jobpars(files, merge=1)
        jp['setup'] = 'Code/setup.sh'
        self.tar = _make_tarball(jp, "module_type : RootInput\n")
        self.addCleanup(os.unlink, self.tar)
        self.jobdesc = {
            'tarball': self.tar,
            'njobs': 1,
            'inloc': 'stash',
            'outputs': [],
        }
        self.fname = "cnf.mu2e.Test.TestConf.0.fcl"

    def test_input_tar_dir_local_env_resolves_setup(self):
        from utils import runmu2e
        code_root = _mkdtemp()
        args = SimpleNamespace(copy_input=False)
        with patch.dict(os.environ, {'INPUT_TAR_DIR_LOCAL': code_root}), \
             patch('utils.runmu2e.write_fcl', return_value='x.fcl'), \
             patch('utils.runmu2e.run'):
            fcl, simjob_setup, infiles, outputs, inloc = \
                runmu2e.process_jobdef(self.jobdesc, self.fname, args)
        self.assertEqual(simjob_setup,
                         os.path.join(code_root, 'Code/setup.sh'))

    def test_missing_code_root_raises(self):
        from utils import runmu2e
        args = SimpleNamespace(copy_input=False)
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('INPUT_TAR_DIR_LOCAL', None)
            with patch('utils.runmu2e.write_fcl', return_value='x.fcl'), \
                 patch('utils.runmu2e.run'):
                with self.assertRaises(ValueError):
                    runmu2e.process_jobdef(self.jobdesc, self.fname, args)


# ---------------------------------------------------------------------------
# 15. version field in tarball names
# ---------------------------------------------------------------------------

class TestVersionField(unittest.TestCase):
    """version field in config controls the version digit in tarball/FCL names."""

    def _cfg(self, **extra):
        return {'owner': 'mu2e', 'desc': 'TestDesc', 'dsconf': 'TestConf', **extra}

    # --- get_parfile_name ---

    def test_default_version_is_zero(self):
        from utils.json2jobdef import get_parfile_name
        self.assertEqual(get_parfile_name(self._cfg()), 'cnf.mu2e.TestDesc.TestConf.0.tar')

    def test_version_one(self):
        from utils.json2jobdef import get_parfile_name
        self.assertEqual(get_parfile_name(self._cfg(version=1)), 'cnf.mu2e.TestDesc.TestConf.1.tar')

    def test_version_five(self):
        from utils.json2jobdef import get_parfile_name
        self.assertEqual(get_parfile_name(self._cfg(version=5)), 'cnf.mu2e.TestDesc.TestConf.5.tar')

    # --- version + tarball_append ---

    def test_version_with_tarball_append(self):
        """version and tarball_append are independent: append modifies desc, version changes digit."""
        from utils.json2jobdef import get_parfile_name
        cfg = self._cfg(version=1, tarball_append='_ext1')
        self.assertEqual(get_parfile_name(cfg), 'cnf.mu2e.TestDesc_ext1.TestConf.1.tar')

    def test_tarball_append_without_version_stays_zero(self):
        from utils.json2jobdef import get_parfile_name
        cfg = self._cfg(tarball_append='_ext1')
        self.assertEqual(get_parfile_name(cfg), 'cnf.mu2e.TestDesc_ext1.TestConf.0.tar')


# ---------------------------------------------------------------------------
# 16. write_fcl FCL filename derivation
# ---------------------------------------------------------------------------

class TestWriteFclFilenameDerivation(unittest.TestCase):
    """write_fcl replaces the tarball version digit with the job index in the FCL filename."""

    def _run_write_fcl(self, tarball_basename, index):
        """Call write_fcl with a mocked Mu2eJobFCL and return the FCL filename created."""
        import tempfile
        from utils.prod_utils import write_fcl

        orig_dir = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                tarball_path = os.path.join(tmpdir, tarball_basename)
                mock_job = MagicMock()
                mock_job.find_index.return_value = index
                mock_job.generate_fcl.return_value = "# test fcl"
                with patch('utils.prod_utils.Mu2eJobFCL', return_value=mock_job):
                    write_fcl(tarball_path, inloc='tape', index=index)
                created = [f for f in os.listdir(tmpdir) if f.endswith('.fcl')]
                return created[0] if created else None
            finally:
                os.chdir(orig_dir)

    def test_version_zero_replaced_by_index(self):
        fcl = self._run_write_fcl("cnf.mu2e.TestDesc.TestConf.0.tar", 7)
        self.assertEqual(fcl, "cnf.mu2e.TestDesc.TestConf.7.fcl")

    def test_version_two_replaced_by_index(self):
        """Non-zero version digit is replaced by the job index, not appended."""
        fcl = self._run_write_fcl("cnf.mu2e.TestDesc.TestConf.2.tar", 7)
        self.assertEqual(fcl, "cnf.mu2e.TestDesc.TestConf.7.fcl")

    def test_multi_digit_index(self):
        fcl = self._run_write_fcl("cnf.mu2e.TestDesc.TestConf.1.tar", 12345)
        self.assertEqual(fcl, "cnf.mu2e.TestDesc.TestConf.12345.fcl")


# ---------------------------------------------------------------------------
# 17. stash SAM-fallback (file not on CVMFS)
# ---------------------------------------------------------------------------

class TestStashMiss(unittest.TestCase):
    """inloc='stash' with the file absent from CVMFS: the dataset resolves
    to another area (load-bearing — mixing jobs declare one inloc while
    their pileup and primaries sit in different areas)."""

    _FNAME = 'dts.mu2e.CeEndpoint.Run1Bab.001440_00001234.art'
    _TAPE = ('/pnfs/mu2e/tape/phy-sim/dts/mu2e/CeEndpoint/Run1Bab/art/'
             'dd/7e/' + _FNAME)

    def setUp(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _root_input_jobpars([self._FNAME])
        self.tar = _make_tarball(jp, "module_type : RootInput\n")
        self.Cls = Mu2eJobFCL

    def tearDown(self):
        os.unlink(self.tar)

    @staticmethod
    def _present(*paths):
        wanted = set(paths)
        return patch('utils.file_resolver.file_exists_at',
                     side_effect=lambda p: p in wanted)

    def test_stash_hit_returns_cvmfs_path(self):
        from utils.file_resolver import stash_read_path
        stash = stash_read_path(self._FNAME)
        with self._present(stash):
            job = self.Cls(self.tar, inloc='stash', proto='file')
            self.assertEqual(job._resolver.locate(self._FNAME), stash)

    def test_stash_miss_resolves_elsewhere(self):
        with self._present(self._TAPE):
            job = self.Cls(self.tar, inloc='stash', proto='file')
            self.assertEqual(job._resolver.locate(self._FNAME), self._TAPE)

    def test_stash_miss_warns_which_area_was_used(self):
        """The substitution must be announced: a silent area swap is how
        an unintended tape recall used to go unnoticed."""
        err = io.StringIO()
        with self._present(self._TAPE), contextlib.redirect_stderr(err):
            job = self.Cls(self.tar, inloc='stash', proto='file')
            job._resolver.locate(self._FNAME)
        self.assertIn('tape', err.getvalue())
        self.assertIn('dts.mu2e.CeEndpoint.Run1Bab.art', err.getvalue())

    def test_absent_everywhere_raises(self):
        with self._present():
            job = self.Cls(self.tar, inloc='stash', proto='file')
            with self.assertRaises(ValueError):
                job._resolver.locate(self._FNAME)

    def test_root_proto_converts_resolved_path_to_xroot(self):
        with self._present(self._TAPE):
            job = self.Cls(self.tar, inloc='stash', proto='root')
            result = job._format_filename(self._FNAME)
        self.assertTrue(result.startswith(
            "xroot://fndcadoor.fnal.gov//pnfs/"), result)

    def test_fallback_to_stash_under_root_proto_reads_cvmfs(self):
        """The read grammar follows the RESOLVED area, not the declared
        inloc. A job declaring `inloc: disk` whose dataset is only on
        stash used to ask xrootd for a /cvmfs path and raise, killing
        every job in the campaign before art started."""
        from utils.file_resolver import stash_read_path
        stash = stash_read_path(self._FNAME)
        with self._present(stash):
            job = self.Cls(self.tar, inloc='disk', proto='root')
            self.assertEqual(job._format_filename(self._FNAME), stash)

    def test_fallback_to_stash_under_file_proto_reads_cvmfs(self):
        from utils.file_resolver import stash_read_path
        stash = stash_read_path(self._FNAME)
        with self._present(stash):
            job = self.Cls(self.tar, inloc='tape', proto='file')
            self.assertEqual(job._format_filename(self._FNAME), stash)

    def test_resilient_inloc_falling_back_to_stash_reads_cvmfs(self):
        """Mixing entries declare `inloc: resilient` for their pileup
        Cats; a Cat staged to stash instead must still be readable."""
        from utils.file_resolver import stash_read_path
        stash = stash_read_path(self._FNAME)
        with self._present(stash):
            job = self.Cls(self.tar, inloc='resilient', proto='root')
            self.assertEqual(job._format_filename(self._FNAME), stash)

    def test_fallback_to_resilient_always_uses_xroot(self):
        """Resilient has no CVMFS mirror, so it streams even when the
        declared inloc's proto would have said 'file'."""
        from utils.file_resolver import resilient_path
        res = resilient_path(self._FNAME)
        with self._present(res):
            job = self.Cls(self.tar, inloc='disk', proto='file')
            result = job._format_filename(self._FNAME)
        self.assertTrue(result.startswith(
            "xroot://fndcadoor.fnal.gov//pnfs/"), result)
        self.assertIn('/resilient/', result)

    def test_disk_hit_still_streams_via_xroot(self):
        """Regression guard: the common no-fallback case is unchanged."""
        from utils.file_resolver import file_path_at
        disk = file_path_at(self._FNAME, 'disk')
        with self._present(disk):
            job = self.Cls(self.tar, inloc='disk', proto='root')
            result = job._format_filename(self._FNAME)
        self.assertTrue(result.startswith(
            "xroot://fndcadoor.fnal.gov//pnfs/"), result)
        self.assertIn('/persistent/datasets/', result)


class TestCreateInputsFileExclude(unittest.TestCase):
    """Verify that _create_inputs_file honours the exclude_files parameter."""

    def setUp(self):
        import tempfile
        self._orig_dir = os.getcwd()
        self._tmpdir = _mkdtemp()
        os.chdir(self._tmpdir)

    def tearDown(self):
        os.chdir(self._orig_dir)
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_no_exclusion_writes_all(self):
        from utils.json2jobdef import _create_inputs_file
        all_files = [f"sim.mu2e.Test.TC.00000{i}.art" for i in range(5)]
        config = {'input_data': {'sim.mu2e.Test.TC.art': 1}}
        with patch('utils.json2jobdef.list_files', return_value=all_files):
            _create_inputs_file(config)
        written = Path('inputs.txt').read_text().strip().split('\n')
        self.assertEqual(written, all_files)

    def test_exclusion_removes_files(self):
        from utils.json2jobdef import _create_inputs_file
        all_files = [f"sim.mu2e.Test.TC.00000{i}.art" for i in range(5)]
        exclude = {all_files[1], all_files[3]}
        config = {'input_data': {'sim.mu2e.Test.TC.art': 1}}
        with patch('utils.json2jobdef.list_files', return_value=all_files):
            _create_inputs_file(config, exclude_files=exclude)
        written = Path('inputs.txt').read_text().strip().split('\n')
        self.assertEqual(len(written), 3)
        for f in exclude:
            self.assertNotIn(f, written)

    def test_exclude_all_produces_empty(self):
        from utils.json2jobdef import _create_inputs_file
        all_files = ["sim.mu2e.Test.TC.000000.art"]
        config = {'input_data': {'sim.mu2e.Test.TC.art': 1}}
        with patch('utils.json2jobdef.list_files', return_value=all_files):
            _create_inputs_file(config, exclude_files=set(all_files))
        content = Path('inputs.txt').read_text().strip()
        self.assertEqual(content, '')

    def test_empty_exclude_set_writes_all(self):
        from utils.json2jobdef import _create_inputs_file
        all_files = ["a.art", "b.art"]
        config = {'input_data': {'sim.mu2e.Test.TC.art': 1}}
        with patch('utils.json2jobdef.list_files', return_value=all_files):
            _create_inputs_file(config, exclude_files=set())
        written = Path('inputs.txt').read_text().strip().split('\n')
        self.assertEqual(written, all_files)


# ---------------------------------------------------------------------------
# 19. _next_version auto-increment (json2jobdef.py)
# ---------------------------------------------------------------------------

class TestNextVersion(unittest.TestCase):

    def _cfg(self, **extra):
        return {'owner': 'mu2e', 'desc': 'TestDesc', 'dsconf': 'TC', **extra}

    def test_no_existing_files_returns_zero(self):
        from utils.json2jobdef import _next_version
        with patch('utils.json2jobdef.files_in_dataset', return_value=[]):
            self.assertEqual(_next_version(self._cfg()), 0)

    def test_single_version_zero_returns_one(self):
        from utils.json2jobdef import _next_version
        with patch('utils.json2jobdef.files_in_dataset',
                   return_value=['cnf.mu2e.TestDesc.TC.0.tar']):
            self.assertEqual(_next_version(self._cfg()), 1)

    def test_multiple_versions_returns_next(self):
        from utils.json2jobdef import _next_version
        files = ['cnf.mu2e.TestDesc.TC.0.tar',
                 'cnf.mu2e.TestDesc.TC.1.tar',
                 'cnf.mu2e.TestDesc.TC.2.tar']
        with patch('utils.json2jobdef.files_in_dataset', return_value=files):
            self.assertEqual(_next_version(self._cfg()), 3)

    def test_sam_exception_propagates(self):
        """A SAM failure must NOT read as version 0 — that collides with an
        existing cnf name. Fail loud instead."""
        from utils.json2jobdef import _next_version
        with patch('utils.json2jobdef.files_in_dataset', side_effect=Exception("SAM down")):
            with self.assertRaises(Exception):
                _next_version(self._cfg())

    def test_non_sequential_versions(self):
        from utils.json2jobdef import _next_version
        files = ['cnf.mu2e.TestDesc.TC.0.tar', 'cnf.mu2e.TestDesc.TC.5.tar']
        with patch('utils.json2jobdef.files_in_dataset', return_value=files):
            self.assertEqual(_next_version(self._cfg()), 6)


# ---------------------------------------------------------------------------
# 20. _compute_extend_exclusions integration (json2jobdef.py)
# ---------------------------------------------------------------------------

class TestComputeExtendExclusions(unittest.TestCase):
    """Test the full extend exclusion logic with mocked SAM and fhicl-get."""

    def _cfg(self, **extra):
        return {
            'owner': 'mu2e',
            'desc': 'TestDesc',
            'dsconf': 'TC',
            'fcl': 'Production/JobConfig/test.fcl',
            'fcl_overrides': {
                'outputs.Out.fileName': 'mcs.mu2e.{desc}.version.sequencer.art'
            },
            **extra,
        }

    def test_exclusion_set_populated(self):
        from utils.json2jobdef import _compute_extend_exclusions
        parents = ['input_a.art', 'input_b.art']

        with patch('utils.json2jobdef.get_output_dataset_names',
                   return_value=['mcs.mu2e.TestDesc.TC.art']), \
             patch('utils.json2jobdef.parents_of_dataset',
                   return_value=parents), \
             patch('utils.json2jobdef.files_in_dataset', return_value=[]):
            cfg = self._cfg()
            result = _compute_extend_exclusions(cfg)

        self.assertEqual(result, set(parents))
        self.assertEqual(cfg['version'], 0)

    def test_version_incremented(self):
        from utils.json2jobdef import _compute_extend_exclusions

        with patch('utils.json2jobdef.get_output_dataset_names',
                   return_value=['mcs.mu2e.TestDesc.TC.art']), \
             patch('utils.json2jobdef.parents_of_dataset',
                   return_value=['parent.art']), \
             patch('utils.json2jobdef.files_in_dataset',
                   return_value=['cnf.mu2e.TestDesc.TC.0.tar']):
            cfg = self._cfg()
            _compute_extend_exclusions(cfg)

        self.assertEqual(cfg['version'], 1)

    def test_no_output_datasets_exits(self):
        from utils.json2jobdef import _compute_extend_exclusions

        with patch('utils.json2jobdef.get_output_dataset_names',
                   return_value=[]):
            with self.assertRaises(SystemExit):
                _compute_extend_exclusions(self._cfg())

    def test_multiple_output_datasets_union(self):
        from utils.json2jobdef import _compute_extend_exclusions

        with patch('utils.json2jobdef.get_output_dataset_names',
                   return_value=['ds1.art', 'ds2.art']), \
             patch('utils.json2jobdef.parents_of_dataset', side_effect=[
                 ['a.art', 'b.art'],     # parents of ds1
                 ['b.art', 'c.art'],     # parents of ds2
             ]), \
             patch('utils.json2jobdef.files_in_dataset', return_value=[]):
            cfg = self._cfg()
            result = _compute_extend_exclusions(cfg)

        self.assertEqual(result, {'a.art', 'b.art', 'c.art'})


# ---------------------------------------------------------------------------
# 21. get_output_dataset_names (jobdef.py) - mocked fhicl-get
# ---------------------------------------------------------------------------

class TestGetOutputDatasetNames(unittest.TestCase):

    def _cfg(self, **extra):
        return {
            'owner': 'mu2e',
            'desc': 'TestDesc',
            'dsconf': 'TC',
            'fcl': 'base.fcl',
            'fcl_overrides': {},
            **extra,
        }

    def test_single_output_module(self):
        from utils.jobdef import get_output_dataset_names

        def mock_fhicl_get(path, cmd, key=''):
            if cmd == '--names-in' and key == 'outputs':
                return 'Out'
            if cmd == '--sequence-of' and key == 'physics.end_paths':
                return 'output_stream'
            if cmd == '--sequence-of' and key == 'physics.output_stream':
                return 'Out'
            if cmd == '--atom-as' and key == 'outputs.Out.fileName':
                return 'mcs.mu2e.{desc}.version.sequencer.art'
            return ''

        with patch('utils.jobdef._run_fhicl_get', side_effect=mock_fhicl_get), \
             patch('utils.prod_utils.write_fcl_template'), \
             patch('os.path.exists', return_value=True), \
             patch('os.unlink'):
            result = get_output_dataset_names(self._cfg())

        self.assertEqual(result, ['mcs.mu2e.TestDesc.TC.art'])

    def test_no_outputs_section(self):
        from utils.jobdef import get_output_dataset_names
        import subprocess as sp

        def mock_fhicl_get(path, cmd, key=''):
            if cmd == '--names-in' and key == 'outputs':
                raise sp.CalledProcessError(1, 'fhicl-get')
            return ''

        with patch('utils.jobdef._run_fhicl_get', side_effect=mock_fhicl_get), \
             patch('utils.prod_utils.write_fcl_template'), \
             patch('os.path.exists', return_value=True), \
             patch('os.unlink'):
            result = get_output_dataset_names(self._cfg())

        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# 22. validate_jobdesc — three-way mode detection (runmu2e.py)
# ---------------------------------------------------------------------------

class TestValidateJobdesc(unittest.TestCase):

    def test_direct_input_mode(self):
        from utils.runmu2e import validate_jobdesc
        jd = {'tarball': 'cnf.mu2e.Reco.MDC2025af.0.tar',
              'inloc': 'tape', 'outputs': []}
        self.assertEqual(validate_jobdesc(jd), 'direct_input')

    def test_normal_mode(self):
        from utils.runmu2e import validate_jobdesc
        jd = {'tarball': 'cnf.mu2e.T.TC.0.tar', 'njobs': 5,
              'inloc': 'tape', 'outputs': []}
        self.assertFalse(validate_jobdesc(jd))

    def test_direct_input_is_truthy(self):
        """'direct_input' string must be truthy for backward-compatible if-checks."""
        from utils.runmu2e import validate_jobdesc
        jd = {'tarball': 'cnf.mu2e.Reco.MDC2025af.0.tar',
              'inloc': 'tape', 'outputs': []}
        self.assertTrue(validate_jobdesc(jd))

    def test_normal_mode_is_falsy(self):
        from utils.runmu2e import validate_jobdesc
        jd = {'tarball': 'cnf.mu2e.T.TC.0.tar', 'njobs': 5,
              'inloc': 'tape', 'outputs': []}
        self.assertFalse(validate_jobdesc(jd))

    def test_direct_input_missing_outputs_exits(self):
        from utils.runmu2e import validate_jobdesc
        jd = {'tarball': 'cnf.mu2e.Reco.MDC2025af.0.tar', 'inloc': 'tape'}
        with self.assertRaises(SystemExit):
            validate_jobdesc(jd)

    def test_normal_mode_missing_njobs_exits(self):
        """Entry without tarball: falls through to normal-mode validation which requires njobs."""
        from utils.runmu2e import validate_jobdesc
        jd = {'inloc': 'tape', 'outputs': []}  # no tarball, no njobs
        with self.assertRaises(SystemExit):
            validate_jobdesc(jd)

    def test_empty_dict_exits(self):
        from utils.runmu2e import validate_jobdesc
        with self.assertRaises(SystemExit):
            validate_jobdesc({})


# ---------------------------------------------------------------------------
# 23. job_outputs() override_desc / override_seq (direct-input mode)
# ---------------------------------------------------------------------------

def _generic_reco_jobpars(owner='mu2e', dsconf='MDC2025af_best_v1_3'):
    """Jobpars for a generic reco tarball: {desc} deferred in outfiles."""
    return {
        "code": "",
        "setup": "/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/MDC2025af/setup.sh",
        "tbs": {
            "seed": "services.SeedService.baseSeed",
            "subrunkey": "",
            "event_id": {"source.maxEvents": 2147483647},
            "outfiles": {
                "outputs.LoopHelixOutput.fileName":
                    f"mcs.{owner}.{{desc}}.{dsconf}.sequencer.art"
            },
        },
        "jobname": f"cnf.{owner}.OnSpillTriggeredReco.{dsconf}.0.tar",
        "owner": owner,
        "dsconf": dsconf,
    }


class TestJobOutputsOverride(unittest.TestCase):

    def test_override_seq_used_instead_of_computed(self):
        """override_seq must appear in the output filename."""
        from utils.jobfcl import Mu2eJobFCL
        jp = _generic_reco_jobpars()
        tar = _make_tarball(jp, "#include \"OnSpill.fcl\"\n")
        try:
            job = Mu2eJobFCL(tar, inloc='tape')
            outputs = job.job_outputs(0, override_seq='001430_00000042')
            out_file = outputs['outputs.LoopHelixOutput.fileName']
            self.assertIn('001430_00000042', out_file)
        finally:
            os.unlink(tar)

    def test_override_desc_replaces_desc_placeholder(self):
        """{desc} in outfile template is replaced by override_desc."""
        from utils.jobfcl import Mu2eJobFCL
        jp = _generic_reco_jobpars()
        tar = _make_tarball(jp, "#include \"OnSpill.fcl\"\n")
        try:
            job = Mu2eJobFCL(tar, inloc='tape')
            outputs = job.job_outputs(
                0,
                override_desc='CeEndpointOnSpillTriggered',
                override_seq='001430_00000042'
            )
            out_file = outputs['outputs.LoopHelixOutput.fileName']
            self.assertIn('CeEndpointOnSpillTriggered', out_file)
            self.assertNotIn('{desc}', out_file)
        finally:
            os.unlink(tar)

    def test_different_override_desc_yields_different_output(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _generic_reco_jobpars()
        tar = _make_tarball(jp, "#include \"OnSpill.fcl\"\n")
        try:
            job = Mu2eJobFCL(tar, inloc='tape')
            out_a = job.job_outputs(0, override_desc='CeEndpoint', override_seq='001430_00000001')
            out_b = job.job_outputs(0, override_desc='CosmicSignal', override_seq='001430_00000001')
            self.assertNotEqual(
                out_a['outputs.LoopHelixOutput.fileName'],
                out_b['outputs.LoopHelixOutput.fileName']
            )
        finally:
            os.unlink(tar)

    def test_output_follows_six_part_mu2e_convention(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _generic_reco_jobpars()
        tar = _make_tarball(jp, "#include \"OnSpill.fcl\"\n")
        try:
            job = Mu2eJobFCL(tar, inloc='tape')
            outputs = job.job_outputs(
                0,
                override_desc='CeEndpointMix1BBTriggered',
                override_seq='001430_00000042'
            )
            out_file = outputs['outputs.LoopHelixOutput.fileName']
            parts = out_file.split('.')
            self.assertEqual(len(parts), 6)
            self.assertEqual(parts[0], 'mcs')
            self.assertEqual(parts[1], 'mu2e')
            self.assertEqual(parts[2], 'CeEndpointMix1BBTriggered')
            self.assertEqual(parts[4], '001430_00000042')
            self.assertEqual(parts[5], 'art')
        finally:
            os.unlink(tar)

    def test_no_overrides_backward_compatible(self):
        """Existing callers with no overrides must still work."""
        from utils.jobfcl import Mu2eJobFCL
        jp = _empty_event_jobpars(run=1430)
        tar = _make_tarball(jp, "module_type : EmptyEvent\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            outputs = job.job_outputs(3)
            out_file = outputs['outputs.PrimaryOutput.fileName']
            self.assertIn('001430_00000003', out_file)
        finally:
            os.unlink(tar)


class TestGenericTarballGuard(unittest.TestCase):
    """A generic tarball deliberately leaves {desc} (and sequencer) unresolved
    in its outfiles for runtime/direct-input substitution. The build-time
    validate_output_filenames guard must NOT be run against it — it would see
    the literal {desc}/sequencer and abort. These tests pin both halves: the
    guard does raise on a deferred cnf (so skipping it is load-bearing), and
    build_jobdef actually skips it when generic_tarball is set."""

    def test_guard_raises_on_deferred_desc(self):
        from utils.jobfcl import validate_output_filenames
        jp = _generic_reco_jobpars()  # outfiles keep literal {desc}
        tar = _make_tarball(jp, "#include \"OnSpill.fcl\"\n")
        try:
            with self.assertRaises(ValueError):
                validate_output_filenames(tar)
        finally:
            os.unlink(tar)

    def test_build_skips_guard_for_generic_tarball(self):
        """build_jobdef must NOT call validate_output_filenames when
        generic_tarball is set — the deferred {desc}/sequencer cannot resolve
        at build time, so running the guard would abort the build."""
        from unittest.mock import patch
        from utils import json2jobdef
        with patch.object(json2jobdef, 'validate_output_filenames') as guard, \
             patch.object(json2jobdef, 'create_jobdef'), \
             patch.object(json2jobdef, 'get_parfile_name', return_value='cnf.x.0.tar'):
            cfg = {'desc': 'reco', 'dsconf': 'D', 'owner': 'mu2e',
                   'simjob_setup': 's', 'inloc': 'tape', 'generic_tarball': True,
                   'fcl': 'f.fcl', 'outloc': {'*.art': 'tape'}}
            try:
                json2jobdef.build_jobdef(cfg, job_args=[])
            except Exception:
                pass  # downstream packaging is mocked/partial; we only assert the guard
            guard.assert_not_called()


def _perdesc_mcs_jobpars(desc='CeEndpoint', dsconf='TestConf'):
    """A normal (non-generic) reco-style cnf: concrete output desc, RootInput so
    the sequencer resolves from the input file."""
    return {
        "code": "", "setup": f"/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/{dsconf}/setup.sh",
        "tbs": {
            "seed": "services.SeedService.baseSeed", "subrunkey": "",
            "event_id": {"source.maxEvents": 2147483647},
            "outfiles": {"outputs.LoopHelixOutput.fileName":
                         f"mcs.mu2e.{desc}.{dsconf}.sequencer.art"},
            "inputs": {"source.fileNames":
                       [1, [f"dig.mu2e.{desc}.{dsconf}.001430_00000000.art"]]},
            "sequential_aux": False,
        },
        "jobname": f"cnf.mu2e.{desc}.{dsconf}.0.tar", "owner": "mu2e", "dsconf": dsconf,
    }


class TestGenericCnfDiscovery(unittest.TestCase):
    """A generic cnf (output desc deferred as {desc}) must be discoverable by
    the dataset->cnf matcher as a LAST resort: exact per-desc cnfs always win,
    a generic cnf in the candidate list must not crash the scan, and fcldump
    flags a generic match (is_generic_cnf) so it reports instead of generating."""

    def test_generic_desc_matches(self):
        from utils.jobdef_lookup import _generic_desc_matches
        self.assertTrue(_generic_desc_matches('{desc}-KL', 'CeEndpoint-KL'))
        self.assertTrue(_generic_desc_matches('{desc}', 'AnythingAtAll'))
        self.assertFalse(_generic_desc_matches('{desc}-KL', 'CeEndpoint-CH'))
        self.assertFalse(_generic_desc_matches('{desc}-KL', 'CeEndpoint'))

    def test_is_generic_cnf_true(self):
        from utils.jobdef_lookup import is_generic_cnf
        tar = _make_tarball(_generic_reco_jobpars(), "#include \"OnSpill.fcl\"\n")
        try:
            self.assertTrue(is_generic_cnf(tar))
        finally:
            os.unlink(tar)

    def test_is_generic_cnf_false_for_resolved(self):
        from utils.jobdef_lookup import is_generic_cnf
        tar = _make_tarball(_perdesc_mcs_jobpars(), "#include \"OnSpill.fcl\"\n")
        try:
            self.assertFalse(is_generic_cnf(tar))
        finally:
            os.unlink(tar)

    def test_generic_fallback_match(self):
        """Only a generic cnf in the list -> matched via the {desc} template."""
        from unittest.mock import patch
        from utils import jobdef_lookup
        tar = _make_tarball(_generic_reco_jobpars(), "#include \"OnSpill.fcl\"\n")
        try:
            with patch.object(jobdef_lookup, 'locate_tarball', return_value=tar):
                result = jobdef_lookup.find_matching_jobdef(
                    ['cnf.mu2e.reco.TestConf.0.tar'], 'CeEndpointOnSpill', 'mcs')
            self.assertEqual(result, tar)
        finally:
            os.unlink(tar)

    def test_exact_wins_and_generic_does_not_crash(self):
        """Per-desc cnf present alongside a generic one -> exact wins, and the
        generic cnf in the candidate list does not abort the scan."""
        from unittest.mock import patch
        from utils import jobdef_lookup
        perdesc = _make_tarball(_perdesc_mcs_jobpars(desc='CeEndpoint'),
                                "#include \"OnSpill.fcl\"\n")
        generic = _make_tarball(_generic_reco_jobpars(dsconf='TestConf'),
                                "#include \"OnSpill.fcl\"\n")
        mapping = {'cnf.mu2e.CeEndpoint.TestConf.0.tar': perdesc,
                   'cnf.mu2e.reco.TestConf.0.tar': generic}
        try:
            with patch.object(jobdef_lookup, 'locate_tarball',
                              side_effect=lambda j: mapping[j]):
                result = jobdef_lookup.find_matching_jobdef(
                    list(mapping.keys()), 'CeEndpoint', 'mcs')
            self.assertEqual(result, perdesc)
        finally:
            os.unlink(perdesc)
            os.unlink(generic)

    def test_generic_desc_capture(self):
        from utils.jobdef_lookup import _generic_desc_capture
        self.assertEqual(_generic_desc_capture('{desc}-KL', 'CeEndpoint-KL'), 'CeEndpoint')
        self.assertEqual(_generic_desc_capture('{desc}', 'FlatGamma'), 'FlatGamma')
        self.assertIsNone(_generic_desc_capture('{desc}-KL', 'CeEndpoint-CH'))
        self.assertIsNone(_generic_desc_capture('NoPlaceholder', 'NoPlaceholder'))

    def test_derive_generic_input_from_target(self):
        """--target output file -> input file: strip suffix to input desc, map
        tier (mcs->dig), find input in SAM by desc+seq (any dsconf)."""
        from unittest.mock import patch
        from utils import jobdef_lookup
        # generic cnf with a {desc}-KL mcs output template
        jp = _generic_reco_jobpars()
        jp['tbs']['outfiles'] = {"outputs.KinematicLineOutput.fileName":
                                 "mcs.mu2e.{desc}-KL.Run1Ban_best_v1_4-000.sequencer.art"}
        tar = _make_tarball(jp, "#include \"OnSpill.fcl\"\n")
        target = "mcs.mu2e.CeEndpoint-KL.Run1Ban_best_v1_4-000.001470_00000004.art"
        # input dig at a DIFFERENT dsconf than the output (the realistic case)
        dig = "dig.mu2e.CeEndpoint.Run1Bai_best_v1_4-000.001470_00000004.art"
        try:
            with patch('utils.samweb_wrapper.files_like', return_value=[dig]) as fl:
                got = jobdef_lookup.derive_generic_input(tar, target)
            self.assertEqual(got, dig)
            # queried the dig tier (mcs->dig) for the stripped desc + exact seq
            pattern = fl.call_args[0][0]
            self.assertIn("dig.mu2e.CeEndpoint.", pattern)
            self.assertEqual(fl.call_args[1].get('sequencer'), "001470_00000004")
        finally:
            os.unlink(tar)

    def test_derive_generic_input_no_match_raises(self):
        from unittest.mock import patch
        from utils import jobdef_lookup
        tar = _make_tarball(_generic_reco_jobpars(), "#include \"OnSpill.fcl\"\n")
        try:
            with patch('utils.samweb_wrapper.files_like', return_value=[]):
                with self.assertRaises(ValueError):
                    jobdef_lookup.derive_generic_input(
                        tar, "mcs.mu2e.X.MDC2025af_best_v1_3.001430_00000001.art")
        finally:
            os.unlink(tar)

    def test_sample_dataset_target_sorts_and_honors_index(self):
        """A bare --dataset against a generic cnf has no sequencer; the sampler
        picks one of the dataset's own files. SAM returns files in arbitrary
        order, so sort before indexing or the pick is not reproducible."""
        from unittest.mock import patch
        from utils import jobdef_lookup
        ds = "nts.mu2e.MuCap1809keVCaloOnSpill.MDC2025au_best_v1_5.root"
        files = [f"nts.mu2e.MuCap1809keVCaloOnSpill.MDC2025au_best_v1_5.001430_{i:08d}.root"
                 for i in (9, 0, 3)]
        with patch('utils.samweb_wrapper.list_files', return_value=files) as lf:
            self.assertEqual(jobdef_lookup.sample_dataset_target(ds), files[1])
            self.assertEqual(jobdef_lookup.sample_dataset_target(ds, index=2), files[0])
        self.assertIn(f"dh.dataset {ds}", lf.call_args[0][0])
        self.assertIn("with limit", lf.call_args[0][0])

    def test_sample_dataset_target_none_when_nothing_to_sample(self):
        """No files (nothing produced yet) or an index past the batch: return
        None so the caller prints its guidance instead of guessing."""
        from unittest.mock import patch
        from utils import jobdef_lookup
        ds = "nts.mu2e.X.MDC2025au_best_v1_5.root"
        with patch('utils.samweb_wrapper.list_files', return_value=[]):
            self.assertIsNone(jobdef_lookup.sample_dataset_target(ds))
        with patch('utils.samweb_wrapper.list_files', return_value=['a.root']):
            self.assertIsNone(jobdef_lookup.sample_dataset_target(ds, index=5))


# ---------------------------------------------------------------------------
# 24. _replace_placeholders defer_keys (jobdef.py)
# ---------------------------------------------------------------------------

class TestReplacePlaceholdersDeferKeys(unittest.TestCase):

    def _rp(self, pattern, config, defer_keys=None):
        from utils.jobdef import _replace_placeholders
        return _replace_placeholders(pattern, config, defer_keys=defer_keys)

    def test_desc_replaced_without_defer(self):
        result = self._rp(
            "mcs.mu2e.{desc}.TC.sequencer.art",
            {'desc': 'CeEndpoint', 'dsconf': 'TC'}
        )
        self.assertEqual(result, "mcs.mu2e.CeEndpoint.TC.sequencer.art")

    def test_desc_not_replaced_with_defer(self):
        result = self._rp(
            "mcs.mu2e.{desc}.TC.sequencer.art",
            {'desc': 'CeEndpoint', 'dsconf': 'TC'},
            defer_keys={'desc'}
        )
        self.assertIn('{desc}', result)
        self.assertNotIn('CeEndpoint', result)

    def test_other_keys_still_replaced_when_desc_deferred(self):
        """Deferring desc must not block substitution of other keys."""
        result = self._rp(
            "mcs.mu2e.{desc}.{dsconf}.sequencer.art",
            {'desc': 'CeEndpoint', 'dsconf': 'TestConf'},
            defer_keys={'desc'}
        )
        self.assertIn('{desc}', result)
        self.assertIn('TestConf', result)
        self.assertNotIn('{dsconf}', result)

    def test_none_defer_keys_behaves_like_empty_set(self):
        result = self._rp(
            "mcs.mu2e.{desc}.{dsconf}.sequencer.art",
            {'desc': 'CeEndpoint', 'dsconf': 'TC'},
            defer_keys=None
        )
        self.assertNotIn('{desc}', result)
        self.assertNotIn('{dsconf}', result)

    def test_empty_defer_keys_replaces_all(self):
        result = self._rp(
            "mcs.mu2e.{desc}.{dsconf}.sequencer.art",
            {'desc': 'CeEndpoint', 'dsconf': 'TC'},
            defer_keys=set()
        )
        self.assertNotIn('{desc}', result)
        self.assertNotIn('{dsconf}', result)


# ---------------------------------------------------------------------------
# 25. process_direct_input (runmu2e.py)
# ---------------------------------------------------------------------------

class TestProcessDirectInput(unittest.TestCase):
    """End-to-end tests for process_direct_input() with a real in-memory tarball."""

    def setUp(self):
        import tempfile
        self._orig_dir = os.getcwd()
        self._tmpdir = _mkdtemp()
        os.chdir(self._tmpdir)
        self._tar = _make_tarball(
            _generic_reco_jobpars(),
            "#include \"Production/JobConfig/recoMC/OnSpill.fcl\"\n"
        )

    def tearDown(self):
        os.chdir(self._orig_dir)
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _run(self, fname):
        from utils.runmu2e import process_direct_input
        jobdesc = {
            'tarball': self._tar,
            'inloc': 'tape',
            'outputs': [{'dataset': '*.art', 'location': 'disk'}],
        }
        with patch('utils.jobquery.Mu2eJobPars') as mock_pars:
            mock_pars.return_value.setup.return_value = \
                "/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/MDC2025af/setup.sh"
            return process_direct_input(jobdesc, fname, MagicMock())

    def test_returns_four_tuple(self):
        fname = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.art"
        result = self._run(fname)
        self.assertEqual(len(result), 4)

    def test_fcl_named_after_input_stem(self):
        fname = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.art"
        fcl, _, _, _ = self._run(fname)
        self.assertEqual(
            fcl,
            "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.fcl"
        )

    def test_fcl_file_written_to_disk(self):
        fname = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.art"
        fcl, _, _, _ = self._run(fname)
        self.assertTrue(os.path.isfile(fcl))

    def test_fcl_contains_source_file_names_with_input(self):
        fname = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.art"
        fcl, _, _, _ = self._run(fname)
        content = Path(fcl).read_text()
        self.assertIn('source.fileNames', content)
        self.assertIn(fname, content)

    def test_fcl_contains_output_fileName_override(self):
        fname = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.art"
        fcl, _, _, _ = self._run(fname)
        content = Path(fcl).read_text()
        self.assertIn('outputs.LoopHelixOutput.fileName', content)

    def test_output_filename_uses_desc_from_fname(self):
        fname = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.art"
        fcl, _, _, _ = self._run(fname)
        content = Path(fcl).read_text()
        self.assertIn('CeEndpointOnSpillTriggered', content)

    def test_output_filename_uses_seq_from_fname(self):
        fname = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.art"
        fcl, _, _, _ = self._run(fname)
        content = Path(fcl).read_text()
        self.assertIn('001430_00000042', content)

    def test_different_input_desc_gives_different_output_filename(self):
        fname_a = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000001.art"
        fname_b = "dig.mu2e.CosmicSignalMix1BBTriggered.MDC2025af_best_v1_3.001430_00000001.art"
        fcl_a, _, _, _ = self._run(fname_a)
        fcl_b, _, _, _ = self._run(fname_b)
        content_a = Path(fcl_a).read_text()
        content_b = Path(fcl_b).read_text()
        # Each FCL must mention only its own desc in the output filename
        self.assertIn('CeEndpointOnSpillTriggered', content_a)
        self.assertIn('CosmicSignalMix1BBTriggered', content_b)

    def test_infiles_is_fname(self):
        fname = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.art"
        _, _, infiles, _ = self._run(fname)
        self.assertEqual(infiles, fname)

    def test_outputs_from_jobdesc(self):
        fname = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.art"
        _, _, _, outputs = self._run(fname)
        self.assertEqual(outputs, [{'dataset': '*.art', 'location': 'disk'}])

    def test_setup_script_returned(self):
        fname = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.art"
        _, simjob_setup, _, _ = self._run(fname)
        self.assertIn('/cvmfs/', simjob_setup)

    def test_bad_fname_format_exits(self):
        from utils.runmu2e import process_direct_input
        jobdesc = {'tarball': self._tar, 'inloc': 'tape', 'outputs': []}
        with self.assertRaises(SystemExit):
            process_direct_input(jobdesc, "only.four.parts.art", MagicMock())

    def test_base_fcl_content_included(self):
        """The FCL from the tarball's mu2e.fcl must appear before the overrides."""
        fname = "dig.mu2e.CeEndpointOnSpillTriggered.MDC2025af_best_v1_3.001430_00000042.art"
        fcl, _, _, _ = self._run(fname)
        content = Path(fcl).read_text()
        # The tarball mu2e.fcl starts with an #include
        self.assertIn('#include', content)
        # Direct-input overrides must come after base content
        override_pos = content.find('source.fileNames')
        include_pos = content.find('#include')
        self.assertGreater(override_pos, include_pos)


class TestProcessDirectInputCodeRoot(unittest.TestCase):
    """process_direct_input's own $INPUT_TAR_DIR_LOCAL / code_root
    wiring (utils/runmu2e.py:315-316, `code_root=_code_root_from(args)`
    inside process_direct_input). Same rationale as
    TestProcessJobdefCodeRoot above: a real relative-setup jobpars and
    a real SimpleNamespace `args`, not MagicMock()."""

    def setUp(self):
        self._orig_dir = os.getcwd()
        self._tmpdir = _mkdtemp()
        os.chdir(self._tmpdir)
        jp = _generic_reco_jobpars()
        jp['setup'] = 'Code/setup.sh'
        self._tar = _make_tarball(
            jp, "#include \"Production/JobConfig/recoMC/OnSpill.fcl\"\n")
        self.addCleanup(os.unlink, self._tar)
        self.fname = ("dig.mu2e.CeEndpointOnSpillTriggered."
                      "MDC2025af_best_v1_3.001430_00000042.art")
        self.jobdesc = {
            'tarball': self._tar,
            'inloc': 'tape',
            'outputs': [{'dataset': '*.art', 'location': 'disk'}],
        }

    def tearDown(self):
        os.chdir(self._orig_dir)

    def test_input_tar_dir_local_env_resolves_setup(self):
        from utils.runmu2e import process_direct_input
        code_root = _mkdtemp()
        args = SimpleNamespace()
        with patch.dict(os.environ, {'INPUT_TAR_DIR_LOCAL': code_root}):
            fcl, simjob_setup, fname, outputs = process_direct_input(
                self.jobdesc, self.fname, args)
        self.assertEqual(simjob_setup,
                         os.path.join(code_root, 'Code/setup.sh'))

    def test_missing_code_root_raises(self):
        from utils.runmu2e import process_direct_input
        args = SimpleNamespace()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('INPUT_TAR_DIR_LOCAL', None)
            with self.assertRaises(ValueError):
                process_direct_input(self.jobdesc, self.fname, args)


# ---------------------------------------------------------------------------
# N. calculate_merge_factor — split_lines branch
# ---------------------------------------------------------------------------

class TestCalculateMergeFactorSplitLines(unittest.TestCase):
    """Guard the `split_lines → merge_factor = 1` branch added with the
    text-file splitting input_data shape. Also smoke-test the existing
    shapes to catch accidental regressions."""

    def test_split_lines_returns_one(self):
        from utils.prod_utils import calculate_merge_factor
        config = {"input_data": {"/cvmfs/PBI_Normal.txt": {"split_lines": 1000}}}
        self.assertEqual(calculate_merge_factor(config), 1)

    def test_split_lines_value_is_ignored(self):
        # Any N-line split yields one chunk per job; chunk size doesn't
        # change the merge factor.
        from utils.prod_utils import calculate_merge_factor
        for n in (1, 500, 10000):
            config = {"input_data": {"/cvmfs/x.txt": {"split_lines": n}}}
            self.assertEqual(calculate_merge_factor(config), 1)

    def test_plain_int_still_returned(self):
        from utils.prod_utils import calculate_merge_factor
        config = {"input_data": {"dts.mu2e.X.Y.art": 5}}
        self.assertEqual(calculate_merge_factor(config), 5)

    def test_count_form_still_works(self):
        from utils.prod_utils import calculate_merge_factor
        config = {"input_data": {"dts.mu2e.X.Y.art": {"count": 7, "random": True}}}
        self.assertEqual(calculate_merge_factor(config), 7)

    def test_unknown_dict_spec_raises(self):
        from utils.prod_utils import calculate_merge_factor
        config = {"input_data": {"dts.mu2e.X.Y.art": {"foo": "bar"}}}
        with self.assertRaises(ValueError):
            calculate_merge_factor(config)


# ---------------------------------------------------------------------------
# N+1. Mu2eJobFCL.sequencer — source.runNumber short-circuit
# ---------------------------------------------------------------------------

def _pbi_sequence_jobpars(run=1430, files=None, owner='mu2e', dsconf='TestConf'):
    """Return a jobpars.json dict suitable for a PBISequence job.

    event_id uses `source.runNumber` (the key PBISequence accepts) rather
    than `source.firstRun` (EmptyEvent/RootInput convention). subrunkey
    is empty — PBISequence doesn't accept per-job subrun overrides.
    """
    return {
        "code": "",
        "setup": "/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/TestConf/setup.sh",
        "tbs": {
            "seed": "services.SeedService.baseSeed",
            "subrunkey": "",
            "event_id": {"source.runNumber": run},
            "outfiles": {
                "outputs.PrimaryOutput.fileName":
                    f"dts.{owner}.TestDesc.{dsconf}.sequencer.art"
            },
            "inputs": {"source.fileNames": [1, files or ["PBI_Normal.txt"]]},
        },
        "jobname": f"cnf.{owner}.TestDesc.{dsconf}.0.tar",
        "owner": owner,
        "dsconf": dsconf,
    }


class TestSequencerRunNumber(unittest.TestCase):
    """sequencer() short-circuits on explicit run keys before trying to
    parse input filenames as Mu2e names. Recognized keys:
        - source.firstRun (EmptyEvent / RootInput)
        - source.run      (SamplingInput)
        - source.runNumber (PBISequence — added 2026-04-21)
    """

    def test_runNumber_produces_mu2e_standard_sequencer(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = _pbi_sequence_jobpars(run=1430)
        tar = _make_tarball(jp, "module_type : PBISequence\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            self.assertEqual(job.sequencer(0), "001430_00000000")
            self.assertEqual(job.sequencer(5), "001430_00000005")
            self.assertEqual(job.sequencer(42), "001430_00000042")
        finally:
            os.unlink(tar)

    def test_runNumber_bypasses_filename_parsing(self):
        # Input filename that would fail Mu2eName parsing — verifies
        # the short-circuit fires before the fallback path.
        from utils.jobfcl import Mu2eJobFCL
        jp = _pbi_sequence_jobpars(run=1430, files=["not-a-mu2e-name.txt"])
        tar = _make_tarball(jp, "module_type : PBISequence\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            # If the short-circuit broke, this would raise when parsing
            # the non-conforming basename.
            self.assertEqual(job.sequencer(0), "001430_00000000")
        finally:
            os.unlink(tar)

    def test_firstRun_and_runNumber_agree(self):
        # Different event_id keys should produce the same sequencer for
        # the same run+index.
        from utils.jobfcl import Mu2eJobFCL
        jp_first = _empty_event_jobpars(run=1430)
        jp_num = _pbi_sequence_jobpars(run=1430)
        tar_first = _make_tarball(jp_first, "module_type : EmptyEvent\n")
        tar_num = _make_tarball(jp_num, "module_type : PBISequence\n")
        try:
            job_first = Mu2eJobFCL(tar_first, inloc='dir:/tmp')
            job_num = Mu2eJobFCL(tar_num, inloc='dir:/tmp')
            for index in (0, 3, 100):
                self.assertEqual(job_first.sequencer(index), job_num.sequencer(index))
        finally:
            os.unlink(tar_first)
            os.unlink(tar_num)


# ---------------------------------------------------------------------------
# N+2. Mu2eJobFCL.job_event_settings — event_id_per_index linear overrides
# ---------------------------------------------------------------------------

class TestEventIdPerIndex(unittest.TestCase):
    """Per-index linear overrides on event_id fields. Schema:
        tbs.event_id_per_index = { fcl_key: { offset, step } }
    Evaluated per job as: result[fcl_key] = offset + index * step.
    Applied after base event_id and subrunkey so per-index overrides win.
    """

    def _jobpars_with_per_index(self, per_index, event_id=None, subrunkey=''):
        jp = _pbi_sequence_jobpars(run=1430)
        jp["tbs"]["subrunkey"] = subrunkey
        if event_id is not None:
            jp["tbs"]["event_id"] = event_id
        jp["tbs"]["event_id_per_index"] = per_index
        return jp

    def test_linear_override_applied_per_index(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = self._jobpars_with_per_index({
            "source.firstEventNumber": {"offset": 0, "step": 1000},
        })
        tar = _make_tarball(jp, "module_type : PBISequence\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            self.assertEqual(job.job_event_settings(0)["source.firstEventNumber"], 0)
            self.assertEqual(job.job_event_settings(5)["source.firstEventNumber"], 5000)
            self.assertEqual(job.job_event_settings(25)["source.firstEventNumber"], 25000)
        finally:
            os.unlink(tar)

    def test_nonzero_offset(self):
        from utils.jobfcl import Mu2eJobFCL
        jp = self._jobpars_with_per_index({
            "source.firstEventNumber": {"offset": 42, "step": 10},
        })
        tar = _make_tarball(jp, "module_type : PBISequence\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            self.assertEqual(job.job_event_settings(3)["source.firstEventNumber"], 42 + 30)
        finally:
            os.unlink(tar)

    def test_missing_step_defaults_to_zero(self):
        # A spec with only offset should treat step as 0 (i.e. constant).
        from utils.jobfcl import Mu2eJobFCL
        jp = self._jobpars_with_per_index({
            "source.firstEventNumber": {"offset": 100},
        })
        tar = _make_tarball(jp, "module_type : PBISequence\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            self.assertEqual(job.job_event_settings(0)["source.firstEventNumber"], 100)
            self.assertEqual(job.job_event_settings(9)["source.firstEventNumber"], 100)
        finally:
            os.unlink(tar)

    def test_overrides_base_event_id_on_same_key(self):
        # If event_id fixes a value and event_id_per_index names the same
        # key, the per-index computation wins.
        from utils.jobfcl import Mu2eJobFCL
        jp = self._jobpars_with_per_index(
            per_index={"source.firstEventNumber": {"offset": 0, "step": 500}},
            event_id={"source.runNumber": 1430, "source.firstEventNumber": 999},
        )
        tar = _make_tarball(jp, "module_type : PBISequence\n")
        try:
            job = Mu2eJobFCL(tar, inloc='dir:/tmp')
            self.assertEqual(job.job_event_settings(2)["source.firstEventNumber"], 1000)
        finally:
            os.unlink(tar)


# ---------------------------------------------------------------------------
# N+3. json2jobdef._configure_chunk_mode — chunk-on-grid submit-side logic
# ---------------------------------------------------------------------------

class TestConfigureChunkMode(unittest.TestCase):
    """Submit-side logic for `input_data = {<path>: {"chunk_lines": N}}`.

    Counts lines, computes njobs=ceil(lines/N), records chunk_mode
    metadata in config, and auto-injects the `source.fileNames`
    fcl_override so every job's FCL references the (per-worker-local)
    chunk file.
    """

    def _make_source(self, nlines):
        import tempfile
        f = tempfile.NamedTemporaryFile('w', delete=False, suffix='.txt')
        for i in range(nlines):
            f.write(f"{i}\n")
        f.close()
        self.addCleanup(os.unlink, f.name)
        return f.name

    def _base_config(self, src, chunk_lines):
        return {
            "desc": "TestDesc",
            "dsconf": "TestConf",
            "owner": "mu2e",
            "input_data": {src: {"chunk_lines": chunk_lines}},
        }

    def test_computes_njobs_exactly_divisible(self):
        from utils.json2jobdef import _configure_chunk_mode
        src = self._make_source(nlines=5000)
        cfg = self._base_config(src, chunk_lines=1000)
        _configure_chunk_mode(cfg)
        self.assertEqual(cfg['njobs'], 5)

    def test_computes_njobs_with_remainder(self):
        # 25438 / 1000 = 25 full chunks + 1 short → 26 jobs
        from utils.json2jobdef import _configure_chunk_mode
        src = self._make_source(nlines=25438)
        cfg = self._base_config(src, chunk_lines=1000)
        _configure_chunk_mode(cfg)
        self.assertEqual(cfg['njobs'], 26)

    def test_records_chunk_mode_metadata(self):
        from utils.json2jobdef import _configure_chunk_mode
        src = self._make_source(nlines=100)
        cfg = self._base_config(src, chunk_lines=40)
        _configure_chunk_mode(cfg)
        cm = cfg['chunk_mode']
        self.assertEqual(cm['source'], src)
        self.assertEqual(cm['lines'], 40)
        self.assertEqual(cm['local_filename'], 'chunk.txt')

    def test_auto_injects_source_filenames_override(self):
        from utils.json2jobdef import _configure_chunk_mode
        src = self._make_source(nlines=100)
        cfg = self._base_config(src, chunk_lines=50)
        _configure_chunk_mode(cfg)
        self.assertEqual(cfg['fcl_overrides']['source.fileNames'], ['chunk.txt'])

    def test_does_not_clobber_existing_source_filenames_override(self):
        # setdefault: if user set source.fileNames already, respect it.
        from utils.json2jobdef import _configure_chunk_mode
        src = self._make_source(nlines=100)
        cfg = self._base_config(src, chunk_lines=50)
        cfg['fcl_overrides'] = {'source.fileNames': ['user_chunk.txt']}
        _configure_chunk_mode(cfg)
        self.assertEqual(cfg['fcl_overrides']['source.fileNames'], ['user_chunk.txt'])

    def test_rejects_zero_chunk_lines(self):
        from utils.json2jobdef import _configure_chunk_mode
        src = self._make_source(nlines=100)
        cfg = self._base_config(src, chunk_lines=0)
        with self.assertRaises(ValueError):
            _configure_chunk_mode(cfg)

    def test_rejects_negative_chunk_lines(self):
        from utils.json2jobdef import _configure_chunk_mode
        src = self._make_source(nlines=100)
        cfg = self._base_config(src, chunk_lines=-5)
        with self.assertRaises(ValueError):
            _configure_chunk_mode(cfg)

    def test_rejects_missing_source_file(self):
        from utils.json2jobdef import _configure_chunk_mode
        cfg = self._base_config("/nonexistent/path/foo.txt", chunk_lines=100)
        with self.assertRaises(ValueError):
            _configure_chunk_mode(cfg)

    def test_rejects_multiple_sources(self):
        from utils.json2jobdef import _configure_chunk_mode
        src1 = self._make_source(nlines=10)
        src2 = self._make_source(nlines=20)
        cfg = {
            "desc": "TestDesc", "dsconf": "TestConf", "owner": "mu2e",
            "input_data": {src1: {"chunk_lines": 5}, src2: {"chunk_lines": 5}},
        }
        with self.assertRaises(ValueError):
            _configure_chunk_mode(cfg)


# ---------------------------------------------------------------------------
# 30. jobdef_lookup: dataset → cnf resolution (reused by fcldump + latestDatasets)
# ---------------------------------------------------------------------------

def _cnf_with_output(output_filename, run=1430, njobs=None):
    """In-memory cnf whose single declared output (after sequencer substitution)
    is `output_filename` with the `.sequencer.` token replaced by a real
    sequencer. Used to exercise the output-name match in find_matching_jobdef.
    Pass `njobs` to pin an explicit job count in jobpars."""
    jp = {
        "code": "",
        "setup": "/cvmfs/test/setup.sh",
        "tbs": {
            "seed": "services.SeedService.baseSeed",
            "subrunkey": "source.firstSubRun",
            "event_id": {"source.firstRun": run, "source.maxEvents": 100},
            "outfiles": {"outputs.Output.fileName": output_filename},
        },
        "jobname": "cnf.mu2e.X.TC.0.tar",
        "owner": "mu2e",
        "dsconf": "TC",
    }
    if njobs is not None:
        jp["tbs"]["njobs"] = njobs
    return _make_tarball(jp, "module_type : EmptyEvent\n")


class TestJobdefLookup(unittest.TestCase):

    def test_input_type_required(self):
        from utils.jobdef_lookup import find_matching_jobdef
        with self.assertRaises(ValueError):
            find_matching_jobdef([], "X", input_type=None)

    def test_fast_path_1to1_desc(self):
        """cnf desc == output desc: matched on the fast (name-filter) pass."""
        from utils import jobdef_lookup
        tar = _cnf_with_output("dig.mu2e.CeEndpointOnSpill.MDC2025ap_best_v1_1.sequencer.art")
        try:
            with patch.object(jobdef_lookup, 'locate_tarball', return_value=tar):
                res = jobdef_lookup.find_matching_jobdef(
                    ["cnf.mu2e.CeEndpointOnSpill.MDC2025ap_best_v1_1.0.tar"],
                    "CeEndpointOnSpill", input_type="dig")
            self.assertEqual(res, tar)
        finally:
            os.unlink(tar)

    def test_fallback_suffixed_output(self):
        """cnf desc 'CeEndpoint' produces 'CeEndpointOnSpill' output: matched on
        the fallback pass that scans declared outputs (the suffix case)."""
        from utils import jobdef_lookup
        tar = _cnf_with_output("dig.mu2e.CeEndpointOnSpill.MDC2025ap_best_v1_1.sequencer.art")
        try:
            with patch.object(jobdef_lookup, 'locate_tarball', return_value=tar):
                res = jobdef_lookup.find_matching_jobdef(
                    ["cnf.mu2e.CeEndpoint.MDC2025ap_best_v1_1.0.tar"],
                    "CeEndpointOnSpill", input_type="dig")
            self.assertEqual(res, tar)
        finally:
            os.unlink(tar)

    def test_no_match_returns_none(self):
        from utils import jobdef_lookup
        tar = _cnf_with_output("dig.mu2e.Other.MDC2025ap_best_v1_1.sequencer.art")
        try:
            with patch.object(jobdef_lookup, 'locate_tarball', return_value=tar):
                res = jobdef_lookup.find_matching_jobdef(
                    ["cnf.mu2e.Other.MDC2025ap_best_v1_1.0.tar"],
                    "CeEndpointOnSpill", input_type="dig")
            self.assertIsNone(res)
        finally:
            os.unlink(tar)

    def test_wrong_tier_not_matched(self):
        """Output desc matches but tier differs from input_type → no match."""
        from utils import jobdef_lookup
        tar = _cnf_with_output("mcs.mu2e.CeEndpointOnSpill.MDC2025ap_best_v1_1.sequencer.art")
        try:
            with patch.object(jobdef_lookup, 'locate_tarball', return_value=tar):
                res = jobdef_lookup.find_matching_jobdef(
                    ["cnf.mu2e.CeEndpointOnSpill.MDC2025ap_best_v1_1.0.tar"],
                    "CeEndpointOnSpill", input_type="dig")
            self.assertIsNone(res)
        finally:
            os.unlink(tar)

    def test_output_njobs_map(self):
        """Batch map: each cnf scanned once → {(output desc, tier): njobs}."""
        from utils import jobdef_lookup
        tar = _cnf_with_output(
            "dig.mu2e.CeEndpointOnSpill.MDC2025ap_best_v1_1.sequencer.art", njobs=20)
        try:
            with patch.object(jobdef_lookup, 'list_jobdefs',
                              return_value=["cnf.mu2e.CeEndpoint.MDC2025ap_best_v1_1.0.tar"]), \
                 patch.object(jobdef_lookup, 'locate_tarball', return_value=tar):
                m = jobdef_lookup.output_njobs_map("MDC2025ap_best_v1_1")
            self.assertEqual(m.get(("CeEndpointOnSpill", "dig")), 20)
        finally:
            os.unlink(tar)


# ---------------------------------------------------------------------------
# 31b. chain_emit: per-description merge factor in one entry
# ---------------------------------------------------------------------------

class TestChainEmitDescMapping(unittest.TestCase):
    """`desc` as a {name: settings} mapping — the preferred shape. Keeps
    the merge factor in exactly one place and drops the repeated "desc"
    key that the list-of-dicts form required."""

    TEMPLATE = [{
        "desc": {
            "CeMLeadingLog": 4,
            "NoPrimary": {"merge": 5,
                          "fcl_overrides": {"#include": "mixing/NoPrimary.fcl"}},
        },
        "input_data": ["dts.mu2e.{desc}.{campaign}.art"],
        "dsconf": ["{out_campaign}_best_v1_3"],
        "pbeam": ["Mix1BB"],
        "inloc": ["resilient"],
        "simjob_setup": ["/cvmfs/x/{out_campaign}/setup.sh"],
        "fcl_overrides": [{"services.DbService.version": "v1_3"}],
    }]

    def test_explicit_descs_reads_mapping(self):
        from utils import chain_emit
        self.assertEqual(chain_emit.explicit_descriptions(self.TEMPLATE),
                         ["CeMLeadingLog", "NoPrimary"])

    def test_scalar_value_is_the_merge_factor(self):
        from utils import chain_emit
        self.assertEqual(chain_emit._input_merge(self.TEMPLATE[0], "CeMLeadingLog"), 4)

    def test_dict_value_carries_merge_and_overrides(self):
        from utils import chain_emit
        self.assertEqual(chain_emit._input_merge(self.TEMPLATE[0], "NoPrimary"), 5)

    def test_input_data_is_bare_pattern(self):
        from utils import chain_emit
        self.assertEqual(chain_emit._input_pattern(self.TEMPLATE[0]),
                         "dts.mu2e.{desc}.{campaign}.art")

    def test_synthesize_pins_merge_from_mapping(self):
        from utils import chain_emit
        out = chain_emit.synthesize_entry(
            self.TEMPLATE, "dts.mu2e.CeMLeadingLog.MDC2025ap.art",
            out_campaign="MDC2025au", defer_desc=True)
        self.assertEqual(out['input_data'],
                         [{"dts.mu2e.CeMLeadingLog.MDC2025ap.art": 4}])

    def test_synthesize_applies_mapping_fcl_overrides(self):
        from utils import chain_emit
        out = chain_emit.synthesize_entry(
            self.TEMPLATE, "dts.mu2e.NoPrimary.MDC2025af.art",
            out_campaign="MDC2025au", defer_desc=True)
        self.assertEqual(out['fcl_overrides'][0]["#include"], "mixing/NoPrimary.fcl")
        self.assertEqual(out['fcl_overrides'][0]["services.DbService.version"], "v1_3")

    def test_mapping_desc_dropped_when_deferred(self):
        """The whole mapping must not leak into the emitted config."""
        from utils import chain_emit
        out = chain_emit.synthesize_entry(
            self.TEMPLATE, "dts.mu2e.CeMLeadingLog.MDC2025ap.art",
            out_campaign="MDC2025au", defer_desc=True)
        self.assertNotIn('desc', out)

    def test_missing_merge_fails_loud(self):
        """A desc added without a merge must error, not silently become 1 —
        that would emit an undersized round with several times the jobs."""
        from utils import chain_emit
        tmpl = copy.deepcopy(self.TEMPLATE)
        tmpl[0]['desc']['Forgotten'] = {}
        with self.assertRaises(ValueError) as cm:
            chain_emit._input_merge(tmpl[0], "Forgotten")
        self.assertIn("merge factor", str(cm.exception))

    def test_bad_mapping_value_rejected(self):
        from utils import chain_emit
        tmpl = copy.deepcopy(self.TEMPLATE)
        tmpl[0]['desc']['Bogus'] = "4"
        with self.assertRaises(ValueError):
            chain_emit._desc_map(tmpl[0])


class TestChainEmitPerDescMerge(unittest.TestCase):
    """One template entry, per-desc merge factors. Without this, giving a
    single desc a different merge means duplicating the entry's whole
    pileup/dsconf/override block — two copies that drift."""

    TEMPLATE = [{
        "desc": ["NoPrimary", {"desc": "MuCap1809keVCalo", "merge": 5}],
        "input_data": [{"dts.mu2e.{desc}.{campaign}.art": 1}],
        "dsconf": ["{out_campaign}_best_v1_3"],
        "pbeam": ["Mix1BB"],
        "inloc": ["resilient"],
        "simjob_setup": ["/cvmfs/x/{out_campaign}/setup.sh"],
    }]

    def test_explicit_descs_reads_both_forms(self):
        from utils import chain_emit
        self.assertEqual(chain_emit.explicit_descriptions(self.TEMPLATE),
                         ["NoPrimary", "MuCap1809keVCalo"])

    def test_match_entry_finds_dict_form_desc(self):
        from utils import chain_emit
        e = chain_emit.match_entry(self.TEMPLATE, "MuCap1809keVCalo")
        self.assertIs(e, self.TEMPLATE[0])

    def test_per_desc_merge_overrides_base(self):
        from utils import chain_emit
        entry = self.TEMPLATE[0]
        self.assertEqual(chain_emit._input_merge(entry, "MuCap1809keVCalo"), 5)

    def test_plain_string_desc_keeps_base_merge(self):
        from utils import chain_emit
        entry = self.TEMPLATE[0]
        self.assertEqual(chain_emit._input_merge(entry, "NoPrimary"), 1)

    def test_synthesize_applies_per_desc_merge(self):
        from utils import chain_emit
        out = chain_emit.synthesize_entry(
            self.TEMPLATE, "dts.mu2e.MuCap1809keVCalo.MDC2025ar.art",
            out_campaign="MDC2025au", defer_desc=True)
        self.assertEqual(out['input_data'],
                         [{"dts.mu2e.MuCap1809keVCalo.MDC2025ar.art": 5}])

    def test_synthesize_other_desc_unaffected(self):
        from utils import chain_emit
        out = chain_emit.synthesize_entry(
            self.TEMPLATE, "dts.mu2e.NoPrimary.MDC2025af.art",
            out_campaign="MDC2025au", defer_desc=True)
        self.assertEqual(out['input_data'],
                         [{"dts.mu2e.NoPrimary.MDC2025af.art": 1}])

    def test_dict_desc_without_name_fails_loud(self):
        """A malformed desc item must raise, not silently vanish from the
        roster — a silently-dropped desc is a whole dataset not produced."""
        from utils import chain_emit
        bad = [{**self.TEMPLATE[0], "desc": [{"merge": 5}]}]
        with self.assertRaises(ValueError):
            chain_emit.explicit_descriptions(bad)


class TestChainEmitPerDescOverrides(unittest.TestCase):
    """Per-desc `fcl_overrides` PATCH the entry's base overrides. Without
    this, one desc needing a single extra override (e.g. the NoPrimary.fcl
    trigger include) forces a duplicate of the entry's whole
    pileup/dsconf/fcl block."""

    TEMPLATE = [{
        "desc": [
            "CeMLeadingLog",
            {"desc": "NoPrimary",
             "fcl_overrides": {"#include": "Production/JobConfig/mixing/NoPrimary.fcl"}},
            {"desc": "MuCap1809keVCalo", "merge": 5,
             "fcl_overrides": {"#include": "Production/JobConfig/mixing/NoPrimary.fcl"}},
        ],
        "input_data": [{"dts.mu2e.{desc}.{campaign}.art": 1}],
        "dsconf": ["{out_campaign}_best_v1_3"],
        "pbeam": ["Mix1BB"],
        "inloc": ["resilient"],
        "simjob_setup": ["/cvmfs/x/{out_campaign}/setup.sh"],
        "fcl_overrides": [{
            "services.DbService.purpose": "Sim_best",
            "physics.producers.PBISim.SDF": 0.8,
        }],
    }]

    def _ov(self, dataset):
        from utils import chain_emit
        out = chain_emit.synthesize_entry(self.TEMPLATE, dataset,
                                          out_campaign="MDC2025au", defer_desc=True)
        ov = out['fcl_overrides']
        return ov[0] if isinstance(ov, list) else ov

    def test_per_desc_override_is_added_to_base(self):
        ov = self._ov("dts.mu2e.NoPrimary.MDC2025af.art")
        self.assertEqual(ov["#include"], "Production/JobConfig/mixing/NoPrimary.fcl")
        # base keys survive the patch
        self.assertEqual(ov["services.DbService.purpose"], "Sim_best")
        self.assertEqual(ov["physics.producers.PBISim.SDF"], 0.8)

    def test_desc_without_overrides_gets_base_only(self):
        ov = self._ov("dts.mu2e.CeMLeadingLog.MDC2025ap.art")
        self.assertNotIn("#include", ov)
        self.assertEqual(ov["services.DbService.purpose"], "Sim_best")

    def test_merge_and_overrides_combine(self):
        from utils import chain_emit
        out = chain_emit.synthesize_entry(
            self.TEMPLATE, "dts.mu2e.MuCap1809keVCalo.MDC2025ar.art",
            out_campaign="MDC2025au", defer_desc=True)
        self.assertEqual(out['input_data'],
                         [{"dts.mu2e.MuCap1809keVCalo.MDC2025ar.art": 5}])
        ov = out['fcl_overrides'][0]
        self.assertEqual(ov["#include"], "Production/JobConfig/mixing/NoPrimary.fcl")

    def test_per_desc_override_wins_over_base_key(self):
        from utils import chain_emit
        tmpl = copy.deepcopy(self.TEMPLATE)
        tmpl[0]['desc'][1]['fcl_overrides']["physics.producers.PBISim.SDF"] = 0.5
        out = chain_emit.synthesize_entry(tmpl, "dts.mu2e.NoPrimary.MDC2025af.art",
                                          out_campaign="MDC2025au", defer_desc=True)
        self.assertEqual(out['fcl_overrides'][0]["physics.producers.PBISim.SDF"], 0.5)

    def test_base_template_not_mutated_across_calls(self):
        """Patching must not leak into the shared template dict — the next
        desc would silently inherit the previous one's overrides."""
        self._ov("dts.mu2e.NoPrimary.MDC2025af.art")
        ov = self._ov("dts.mu2e.CeMLeadingLog.MDC2025ap.art")
        self.assertNotIn("#include", ov)


# ---------------------------------------------------------------------------
# 31. chain_emit: template synthesis for latestDatasets --emit
# ---------------------------------------------------------------------------

class TestChainEmit(unittest.TestCase):

    TEMPLATE = {
        "dsconf": "{campaign}_best_v1_1",
        "fcl": "Production/JobConfig/digitize/OnSpill.fcl",
        "input_data": {"dts.mu2e.{desc}.{campaign}.art": 10},
        "fcl_overrides": {
            "outputs.Output.fileName": "dig.owner.{desc}OnSpill.version.sequencer.art",
            "services.DbService.version": "v1_1",
        },
        "inloc": "tape",
        "simjob_setup": "/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/{campaign}/setup.sh",
    }

    def test_input_tier_for_output(self):
        from utils import chain_emit
        self.assertEqual(chain_emit.input_tier_for_output("mcs"), "dig")
        self.assertEqual(chain_emit.input_tier_for_output("dig"), "dts")
        self.assertEqual(chain_emit.input_tier_for_output("nts"), "mcs")
        self.assertEqual(chain_emit.input_tier_for_output("ntd"), "mcs")

    def test_input_tier_for_output_unknown_raises(self):
        from utils import chain_emit
        with self.assertRaises(ValueError):
            chain_emit.input_tier_for_output("dts")

    def test_family_of(self):
        from utils import chain_emit
        self.assertEqual(chain_emit.family_of("MDC2025ap"), "MDC2025")
        self.assertEqual(chain_emit.family_of("MDC2025"), "MDC2025")
        self.assertEqual(chain_emit.family_of("Run1Ban"), "Run1B")
        self.assertEqual(chain_emit.family_of("Run1B"), "Run1B")

    def test_derive_input_defname_family(self):
        from utils import chain_emit
        self.assertEqual(
            chain_emit.derive_input_defname(self.TEMPLATE, "MDC2025"),
            "dts.mu2e.%.MDC2025%.art")

    def test_derive_input_defname_release(self):
        from utils import chain_emit
        self.assertEqual(
            chain_emit.derive_input_defname(self.TEMPLATE, "MDC2025ap"),
            "dts.mu2e.%.MDC2025ap%.art")

    def test_synthesize_substitutes_campaign(self):
        from utils import chain_emit
        entry = chain_emit.synthesize_entry(self.TEMPLATE, "dts.mu2e.CeEndpoint.MDC2025ap.art")
        self.assertEqual(entry['dsconf'], "MDC2025ap_best_v1_1")
        self.assertIn("SimJob/MDC2025ap/", entry['simjob_setup'])
        self.assertNotIn("{campaign}", json.dumps(entry))

    def test_parent_dsconf_substitution(self):
        """{parent_dsconf} = the full dsconf of the input dataset (incl build
        suffix), so an ntuple output can reuse its reco parent's dsconf."""
        from utils import chain_emit
        tmpl = {
            "desc": "{desc}",
            "dsconf": "{parent_dsconf}",
            "fcl": "EventNtuple/fcl/from_mcs-mockdata.fcl",
            "input_data": {"mcs.mu2e.{desc}.{campaign}_best_v1_1.art": 1},
            "fcl_overrides": {"services.TFileService.fileName":
                              "nts.mu2e.{desc}.version.sequencer.root"},
            "inloc": "disk", "simjob_setup": "x",
        }
        e = chain_emit.synthesize_entry(
            tmpl, "mcs.mu2e.CeEndpointOnSpill.MDC2025ap_best_v1_1.art")
        self.assertEqual(e['dsconf'], "MDC2025ap_best_v1_1")
        # Run1B-style suffix with recovery pass is carried through verbatim
        e2 = chain_emit.synthesize_entry(
            tmpl, "mcs.mu2e.CeEndpoint-KL.Run1Ban_best_v1_4-001.art")
        self.assertEqual(e2['dsconf'], "Run1Ban_best_v1_4-001")

    def test_output_datasets(self):
        from utils import chain_emit
        entry = chain_emit.synthesize_entry(self.TEMPLATE, "dts.mu2e.CeEndpoint.MDC2025ap.art")
        self.assertEqual(chain_emit.output_datasets(entry),
                         ["dig.mu2e.CeEndpointOnSpill.MDC2025ap_best_v1_1.art"])

    def test_explicit_desc_list(self):
        """A `desc` list with no {desc} wildcard restricts to those descs."""
        from utils import chain_emit
        tmpl = dict(self.TEMPLATE, desc=["CeEndpoint", "FlatGamma"])
        self.assertFalse(chain_emit.has_wildcard(tmpl))
        self.assertEqual(set(chain_emit.explicit_descriptions(tmpl)),
                         {"CeEndpoint", "FlatGamma"})
        # discovery defname still derived from input_data pattern
        self.assertEqual(chain_emit.derive_input_defname(tmpl, "MDC2025"),
                         "dts.mu2e.%.MDC2025%.art")
        # synthesize pins the concrete desc
        e = chain_emit.synthesize_entry(tmpl, "dts.mu2e.CeEndpoint.MDC2025ap.art")
        self.assertEqual(e['desc'], "CeEndpoint")
        self.assertEqual(e['fcl_overrides']['outputs.Output.fileName'],
                         "dig.owner.CeEndpointOnSpill.version.sequencer.art")

    def test_wildcard_in_desc_field(self):
        from utils import chain_emit
        tmpl = dict(self.TEMPLATE, desc="{desc}")
        self.assertTrue(chain_emit.has_wildcard(tmpl))
        self.assertEqual(chain_emit.explicit_descriptions(tmpl), [])

    def test_no_desc_field_means_discover_all(self):
        """TEMPLATE has no `desc` field: not a wildcard, no explicit descs →
        discovery is unrestricted (the historical default)."""
        from utils import chain_emit
        self.assertFalse(chain_emit.has_wildcard(self.TEMPLATE))
        self.assertEqual(chain_emit.explicit_descriptions(self.TEMPLATE), [])

    def test_list_template_special_match(self):
        """List template: an explicit-desc entry wins for its primary; the
        {desc} wildcard handles the rest; discovery uses the wildcard."""
        from utils import chain_emit
        tmpl = [
            dict(self.TEMPLATE, desc="{desc}"),
            {"desc": "CosmicCRYExtracted",
             "dsconf": "{campaign}_best_v1_1",
             "fcl": "Production/JobConfig/digitize/Extracted.fcl",
             "input_data": {"dts.mu2e.{desc}.{campaign}.art": 10},
             "fcl_overrides": {
                 "outputs.Output.fileName": "dig.owner.{desc}.version.sequencer.art"},
             "inloc": "tape", "simjob_setup": "x"},
        ]
        e = chain_emit.synthesize_entry(tmpl, "dts.mu2e.CosmicCRYExtracted.MDC2025ap.art")
        self.assertEqual(e['fcl'], "Production/JobConfig/digitize/Extracted.fcl")
        self.assertEqual(chain_emit.output_datasets(e),
                         ["dig.mu2e.CosmicCRYExtracted.MDC2025ap_best_v1_1.art"])
        e2 = chain_emit.synthesize_entry(tmpl, "dts.mu2e.FlatGamma.MDC2025ap.art")
        self.assertEqual(e2['fcl'], "Production/JobConfig/digitize/OnSpill.fcl")
        self.assertEqual(e2['fcl_overrides']['outputs.Output.fileName'],
                         "dig.owner.FlatGammaOnSpill.version.sequencer.art")
        self.assertEqual(chain_emit.derive_input_defname(tmpl, "MDC2025"),
                         "dts.mu2e.%.MDC2025%.art")

    def test_synthesize_entry_pins_input(self):
        from utils import chain_emit
        entry = chain_emit.synthesize_entry(self.TEMPLATE, "dts.mu2e.CeEndpoint.MDC2025ap.art")
        self.assertEqual(entry['input_data'], {"dts.mu2e.CeEndpoint.MDC2025ap.art": 10})

    def test_synthesize_entry_substitutes_desc(self):
        from utils import chain_emit
        entry = chain_emit.synthesize_entry(self.TEMPLATE, "dts.mu2e.CeEndpoint.MDC2025ap.art")
        self.assertEqual(entry['fcl_overrides']['outputs.Output.fileName'],
                         "dig.owner.CeEndpointOnSpill.version.sequencer.art")

    def test_synthesize_entry_copies_physics(self):
        from utils import chain_emit
        entry = chain_emit.synthesize_entry(self.TEMPLATE, "dts.mu2e.CeEndpoint.MDC2025ap.art")
        self.assertEqual(entry['dsconf'], "MDC2025ap_best_v1_1")
        self.assertEqual(entry['fcl_overrides']['services.DbService.version'], "v1_1")

    def test_synthesize_entry_no_template_mutation(self):
        from utils import chain_emit
        chain_emit.synthesize_entry(self.TEMPLATE, "dts.mu2e.CeEndpoint.MDC2025ap.art")
        self.assertIn('{desc}', self.TEMPLATE['fcl_overrides']['outputs.Output.fileName'])

    def test_reco_desc_is_full_dig_desc(self):
        """At a reco hop, {desc} carries the dig dataset's full description."""
        from utils import chain_emit
        reco_tmpl = {
            "dsconf": "MDC2025ap_best_v1_1",
            "fcl": "Production/JobConfig/recoMC/OnSpill.fcl",
            "input_data": {"dig.mu2e.{desc}.MDC2025ap_best_v1_1.art": 1},
            "fcl_overrides": {"outputs.LoopHelixOutput.fileName":
                              "mcs.owner.{desc}.version.sequencer.art"},
            "inloc": "tape",
            "simjob_setup": "x",
        }
        entry = chain_emit.synthesize_entry(
            reco_tmpl, "dig.mu2e.CeEndpointOnSpill.MDC2025ap_best_v1_1.art")
        self.assertEqual(entry['fcl_overrides']['outputs.LoopHelixOutput.fileName'],
                         "mcs.owner.CeEndpointOnSpill.version.sequencer.art")

    def test_emit_config_maps_all(self):
        from utils import chain_emit
        cfg = chain_emit.emit_config(
            self.TEMPLATE, ["dts.mu2e.A.MDC2025ap.art", "dts.mu2e.B.MDC2025ap.art"])
        self.assertEqual(len(cfg), 2)
        self.assertEqual(cfg[0]['input_data'], {"dts.mu2e.A.MDC2025ap.art": 10})

    def test_load_template_missing_fails_loud(self):
        from utils import chain_emit
        with self.assertRaises(FileNotFoundError):
            chain_emit.load_template("NoSuchCampaign", "digi", "/tmp/nonexistent_templates_xyz")

    def test_load_template_by_family(self):
        """A release tag (MDC2025ap) resolves to the family dir (MDC2025)."""
        import tempfile
        from utils import chain_emit
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, "MDC2025"))
            with open(os.path.join(d, "MDC2025", "digi.json"), 'w') as f:
                json.dump(self.TEMPLATE, f)
            t = chain_emit.load_template("MDC2025ap", "digi", d)
        self.assertEqual(t['dsconf'], "{campaign}_best_v1_1")

    def test_input_pattern_rejects_multi(self):
        from utils import chain_emit
        with self.assertRaises(ValueError):
            chain_emit.derive_input_defname({"input_data": {"a.art": 1, "b.art": 1}}, "C")

    # --- mixing: out_campaign / defer_desc / dsconf override ---

    MIX_TEMPLATE = {
        "desc": ["CeMLeadingLog", "FlatGamma"],
        "dsconf": ["{out_campaign}_best_v1_1"],
        "input_data": [{"dts.mu2e.{desc}.{campaign}.art": 1}],
        "pbeam": ["Mix1BB"],
        "fcl": ["Production/JobConfig/mixing/Mix.fcl"],
        "fcl_overrides": [{
            "outputs.Output.fileName": "dig.mu2e.{desc}.{dsconf}.sequence.art"}],
        "inloc": ["tape"],
        "simjob_setup": ["/cvmfs/.../SimJob/{out_campaign}/setup.sh"],
    }

    def test_out_campaign_decouples_build_from_input(self):
        """Mixing reads an ap primary but writes the ar build: dsconf and
        simjob_setup use {out_campaign}, not the input campaign."""
        from utils import chain_emit
        e = chain_emit.synthesize_entry(
            self.MIX_TEMPLATE, "dts.mu2e.CeMLeadingLog.MDC2025ap.art",
            out_campaign="MDC2025ar", defer_desc=True)
        self.assertEqual(e['dsconf'], ["MDC2025ar_best_v1_1"])
        self.assertIn("SimJob/MDC2025ar/", e['simjob_setup'][0])

    def test_defer_desc_leaves_desc_literal(self):
        """defer_desc drops the `desc` field and leaves {desc} unsubstituted so
        json2jobdef can append pbeam (desc = input_desc + pbeam) at gen time."""
        from utils import chain_emit
        e = chain_emit.synthesize_entry(
            self.MIX_TEMPLATE, "dts.mu2e.CeMLeadingLog.MDC2025ap.art",
            out_campaign="MDC2025ar", defer_desc=True)
        self.assertNotIn('desc', e)
        self.assertIn('{desc}', e['fcl_overrides'][0]['outputs.Output.fileName'])

    def test_output_datasets_resolves_deferred_desc_via_pbeam(self):
        """output_datasets must expand the literal {desc} to input_desc+pbeam so
        the produced-output (skip-produced) check matches real SAM names."""
        from utils import chain_emit
        e = chain_emit.synthesize_entry(
            self.MIX_TEMPLATE, "dts.mu2e.CeMLeadingLog.MDC2025ap.art",
            out_campaign="MDC2025ar", defer_desc=True)
        self.assertEqual(
            chain_emit.output_datasets(e),
            ["dig.mu2e.CeMLeadingLogMix1BB.MDC2025ar_best_v1_1.art"])

    def test_dsconf_override_pins_build_listform(self):
        """--dsconf overrides the template dsconf outright, preserving the
        list-form container, and flows into the resolved output name."""
        from utils import chain_emit
        e = chain_emit.synthesize_entry(
            self.MIX_TEMPLATE, "dts.mu2e.CeMLeadingLog.MDC2025ap.art",
            out_campaign="MDC2025ar", defer_desc=True,
            dsconf="MDC2025ar_best_v1_3")
        self.assertEqual(e['dsconf'], ["MDC2025ar_best_v1_3"])
        self.assertEqual(
            chain_emit.output_datasets(e),
            ["dig.mu2e.CeMLeadingLogMix1BB.MDC2025ar_best_v1_3.art"])

    def test_dsconf_override_scalar_shape(self):
        """For scalar-dsconf templates (digi/reco) the override stays scalar."""
        from utils import chain_emit
        e = chain_emit.synthesize_entry(
            self.TEMPLATE, "dts.mu2e.CeEndpoint.MDC2025ap.art",
            dsconf="MDC2025ap_best_v1_9")
        self.assertEqual(e['dsconf'], "MDC2025ap_best_v1_9")
        self.assertEqual(chain_emit.output_datasets(e),
                         ["dig.mu2e.CeEndpointOnSpill.MDC2025ap_best_v1_9.art"])


# ---------------------------------------------------------------------------
# 32. latest_per_description (latestDatasets.py)
# ---------------------------------------------------------------------------

class TestLatestPerDescription(unittest.TestCase):

    def test_picks_greatest_dsconf(self):
        from utils.latestDatasets import latest_per_description
        names = [
            "dts.mu2e.A.MDC2025ao.art",
            "dts.mu2e.A.MDC2025ap.art",
            "dts.mu2e.B.MDC2025ap.art",
        ]
        rows, skipped = latest_per_description(names)
        latest = {desc: name for desc, _, name, _ in rows}
        self.assertEqual(latest["A"], "dts.mu2e.A.MDC2025ap.art")
        self.assertEqual(latest["B"], "dts.mu2e.B.MDC2025ap.art")
        self.assertEqual(skipped, [])

    def test_skips_unparseable(self):
        from utils.latestDatasets import latest_per_description
        rows, skipped = latest_per_description(["not-a-name", "dts.mu2e.A.MDC2025ap.art"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(skipped), 1)

    def test_narrow_to_latest_release(self):
        """Family wildcard spans releases; narrow to the single latest one."""
        from utils.latestDatasets import _narrow_to_latest_release
        out = _narrow_to_latest_release([
            "dts.mu2e.CeEndpoint.MDC2025ac.art",     # older release → dropped
            "dts.mu2e.CeMLeadingLog.MDC2025ap.art",
            "dts.mu2e.FlatGamma.MDC2025ap.art",
        ])
        self.assertEqual(set(out), {
            "dts.mu2e.CeMLeadingLog.MDC2025ap.art",
            "dts.mu2e.FlatGamma.MDC2025ap.art",
        })

    def test_superseded_is_complement_of_latest(self):
        """superseded_per_description returns every non-latest version, and is
        the exact set complement of the latest names."""
        from utils.latestDatasets import (latest_per_description,
                                          superseded_per_description)
        names = [
            "dts.mu2e.A.MDC2025ac.art",   # A: superseded (ac < ao < ap)
            "dts.mu2e.A.MDC2025ao.art",   # A: superseded
            "dts.mu2e.A.MDC2025ap.art",   # A: latest
            "dts.mu2e.B.MDC2025ap.art",   # B: single version → never superseded
        ]
        srows, skipped = superseded_per_description(names)
        sup = {name for _, _, name, _ in srows}
        self.assertEqual(sup, {
            "dts.mu2e.A.MDC2025ac.art",
            "dts.mu2e.A.MDC2025ao.art",
        })
        self.assertEqual(skipped, [])
        # count column reports the group's total version count
        self.assertEqual({name: count for _, _, name, count in srows},
                         {"dts.mu2e.A.MDC2025ac.art": 3,
                          "dts.mu2e.A.MDC2025ao.art": 3})
        # exact complement of the latest set (over parseable names)
        latest = {name for _, _, name, _ in latest_per_description(names)[0]}
        allnames = set(names)
        self.assertEqual(sup, allnames - latest)

    def test_superseded_skips_unparseable(self):
        from utils.latestDatasets import superseded_per_description
        srows, skipped = superseded_per_description(
            ["not-a-name", "dts.mu2e.A.MDC2025ap.art"])
        self.assertEqual(srows, [])          # single parseable version → nothing
        self.assertEqual(len(skipped), 1)

    def test_cross_tier_datasets_never_compete(self):
        """A dig remake with a newer dsconf must not mark the latest mcs of
        the same description superseded: supersession is within one series
        (tier.owner.description.extension), never across tiers. Real case:
        dig.CeMLeadingLogMix1BB.MDC2025au_best_v1_3 vs
        mcs.CeMLeadingLogMix1BB.MDC2025au_best_v1_1 (2026-09-11)."""
        from utils.latestDatasets import (latest_per_description,
                                          superseded_per_description)
        names = [
            "mcs.mu2e.A.MDC2025au_best_v1_1.art",   # latest mcs of A
            "dig.mu2e.A.MDC2025au_best_v1_1.art",   # superseded dig of A
            "dig.mu2e.A.MDC2025au_best_v1_3.art",   # latest dig of A
        ]
        latest = {name for _, _, name, _ in latest_per_description(names)[0]}
        self.assertEqual(latest, {
            "mcs.mu2e.A.MDC2025au_best_v1_1.art",
            "dig.mu2e.A.MDC2025au_best_v1_3.art",
        })
        sup = {name for _, _, name, _ in superseded_per_description(names)[0]}
        self.assertEqual(sup, {"dig.mu2e.A.MDC2025au_best_v1_1.art"})

    def test_cross_tier_same_dsconf_no_arbitrary_tiebreak(self):
        """dig and mcs at the SAME dsconf are two single-version series:
        neither supersedes the other, regardless of input order (the old
        description-only grouping broke this tie by SAM return order)."""
        from utils.latestDatasets import superseded_per_description
        names = ["mcs.mu2e.B.MDC2025au_best_v1_5.art",
                 "dig.mu2e.B.MDC2025au_best_v1_5.art"]
        for order in (names, list(reversed(names))):
            srows, skipped = superseded_per_description(order)
            self.assertEqual(srows, [])
            self.assertEqual(skipped, [])

    def test_injected_order_key_overrides_dsconf(self):
        """Real MDC2020 case: the ntuple series sorts BELOW the release series
        lexicographically ('-' < 'a') but was created six months later. An
        injected date key must beat dsconf order."""
        import datetime as _dt
        from utils.latestDatasets import latest_per_description
        stale = "nts.mu2e.CeEndpointMix1BBTriggered.MDC2020aw_best_v1_3_v06_06_00.root"
        newest = "nts.mu2e.CeEndpointMix1BBTriggered.MDC2020-001.root"
        dates = {stale: _dt.datetime(2025, 9, 6), newest: _dt.datetime(2026, 3, 10)}
        # dsconf order picks the stale one — this is the bug being fixed
        rows, _ = latest_per_description([stale, newest])
        self.assertEqual(rows[0][2], stale)
        # the injected key picks the actually-newest
        rows, _ = latest_per_description([stale, newest], order_key=dates.__getitem__)
        self.assertEqual(rows[0][2], newest)

    def test_superseded_honors_same_order_key(self):
        """--superseded means 'every version that is not the latest', so it must
        order by the SAME key — otherwise a dataset lands in both listings or
        in neither."""
        import datetime as _dt
        from utils.latestDatasets import (latest_per_description,
                                          superseded_per_description)
        a_stale = "nts.mu2e.A.MDC2020aw_best_v1_3_v06_06_00.root"
        a_new = "nts.mu2e.A.MDC2020-001.root"
        b_only = "nts.mu2e.B.MDC2020-001.root"
        names = [a_stale, a_new, b_only]
        dates = {a_stale: _dt.datetime(2025, 9, 6),
                 a_new: _dt.datetime(2026, 3, 10),
                 b_only: _dt.datetime(2026, 3, 10)}
        key = dates.__getitem__
        latest = {n for _, _, n, _ in latest_per_description(names, key)[0]}
        sup = {n for _, _, n, _ in superseded_per_description(names, key)[0]}
        self.assertEqual(latest, {a_new, b_only})
        self.assertEqual(sup, {a_stale})
        self.assertEqual(sup, set(names) - latest)      # exact complement

    def test_creation_date_key_queries_only_contended(self):
        """Single-version descriptions have nothing to compare against, so they
        must never cost a SAM call."""
        import datetime as _dt
        from utils import latestDatasets
        a_one = "nts.mu2e.A.MDC2020aw_best_v1_3_v06_06_00.root"   # A: contended
        a_two = "nts.mu2e.A.MDC2020-001.root"                     # A: contended
        b_only = "nts.mu2e.B.MDC2020-001.root"                    # B: singleton
        asked = []

        def fake(name):
            asked.append(name)
            return _dt.datetime(2026, 3, 10)

        with patch.object(latestDatasets, 'definition_creation_date',
                          side_effect=fake):
            key = latestDatasets._creation_date_key([a_one, a_two, b_only])
        self.assertEqual(set(asked), {a_one, a_two})
        self.assertNotIn(b_only, asked)
        # the unqueried singleton still gets a usable rank, not a KeyError
        self.assertEqual(key(b_only), _dt.datetime.min)

    def test_creation_date_key_fails_loud_on_missing_date(self):
        """No date for a contended dataset must abort, naming it — never
        silently revert to lexicographic order."""
        import datetime as _dt
        from utils import latestDatasets
        dated = "nts.mu2e.A.MDC2020aw_best_v1_3_v06_06_00.root"
        undated = "nts.mu2e.A.MDC2020-001.root"
        with patch.object(latestDatasets, 'definition_creation_date',
                          side_effect=lambda n: None if n == undated
                          else _dt.datetime(2025, 9, 6)):
            with self.assertRaises(SystemExit) as cm:
                latestDatasets._creation_date_key([dated, undated])
        self.assertIn(undated, str(cm.exception))

    def test_dsconf_mode_makes_no_sam_calls(self):
        """The default path must stay free of SAM round trips — the --emit
        chain relies on it."""
        from utils import latestDatasets

        def boom(name):
            raise AssertionError(f"SAM queried in dsconf mode: {name}")

        with patch.object(latestDatasets, 'definition_creation_date',
                          side_effect=boom):
            key = latestDatasets._order_key_for(
                "dsconf", ["nts.mu2e.A.MDC2020-000.root",
                           "nts.mu2e.A.MDC2020-001.root"])
        self.assertIsNone(key)

    def test_duplicate_name_not_split_across_latest_and_superseded(self):
        """A repeated input name (e.g. `cat a.txt b.txt | --stdin`) must not
        land in both the latest and superseded listings — that would
        nominate a live dataset for retirement."""
        from utils.latestDatasets import (latest_per_description,
                                          superseded_per_description)
        names = ["dts.mu2e.A.MDC2025ap.art", "dts.mu2e.A.MDC2025ap.art"]
        rows, _ = latest_per_description(names)
        self.assertEqual([r[2] for r in rows], ["dts.mu2e.A.MDC2025ap.art"])
        srows, _ = superseded_per_description(names)
        self.assertEqual(srows, [])

    def test_order_key_for_time_ranks_by_mocked_date(self):
        """_order_key_for('time', ...) must return a callable that ranks by
        SAM creation date, and that key must actually change the winner
        latest_per_description picks (proves the wiring isn't a no-op)."""
        import datetime as _dt
        from utils import latestDatasets
        stale = "nts.mu2e.CeEndpointMix1BBTriggered.MDC2020aw_best_v1_3_v06_06_00.root"
        newest = "nts.mu2e.CeEndpointMix1BBTriggered.MDC2020-001.root"
        dates = {stale: _dt.datetime(2025, 9, 6), newest: _dt.datetime(2026, 3, 10)}
        with patch.object(latestDatasets, 'definition_creation_date',
                          side_effect=lambda n: dates[n]):
            key = latestDatasets._order_key_for("time", [stale, newest])
        self.assertIsNotNone(key)
        rows, _ = latestDatasets.latest_per_description([stale, newest], key)
        self.assertEqual(rows[0][2], newest)

    def test_main_stdin_latest_by_time_picks_newest_by_date(self):
        """End-to-end: main() with --stdin --latest-by time must print the
        newest-by-date name, not the lexicographic winner (which would be
        `stale` here since '-' < 'a'). Mutating _order_key_for into a no-op
        (lambda latest_by, names: None) makes this fail."""
        import contextlib
        import datetime as _dt
        from utils import latestDatasets
        stale = "nts.mu2e.CeEndpointMix1BBTriggered.MDC2020aw_best_v1_3_v06_06_00.root"
        newest = "nts.mu2e.CeEndpointMix1BBTriggered.MDC2020-001.root"
        dates = {stale: _dt.datetime(2025, 9, 6), newest: _dt.datetime(2026, 3, 10)}
        stdin = io.StringIO(f"{stale}\n{newest}\n")
        buf = io.StringIO()
        with patch.object(sys, 'argv', ['latestDatasets', '--stdin', '--latest-by', 'time']), \
             patch.object(sys, 'stdin', stdin), \
             patch.object(latestDatasets, 'definition_creation_date',
                          side_effect=lambda n: dates[n]), \
             contextlib.redirect_stdout(buf):
            latestDatasets.main()
        self.assertEqual(buf.getvalue().strip(), newest)

    def test_main_superseded_latest_by_time_lists_the_older_one(self):
        """Complement of the above: --superseded --latest-by time must list
        the OLDER-by-date name (`stale`), not the newer one."""
        import contextlib
        import datetime as _dt
        from utils import latestDatasets
        stale = "nts.mu2e.CeEndpointMix1BBTriggered.MDC2020aw_best_v1_3_v06_06_00.root"
        newest = "nts.mu2e.CeEndpointMix1BBTriggered.MDC2020-001.root"
        dates = {stale: _dt.datetime(2025, 9, 6), newest: _dt.datetime(2026, 3, 10)}
        stdin = io.StringIO(f"{stale}\n{newest}\n")
        buf = io.StringIO()
        with patch.object(sys, 'argv',
                          ['latestDatasets', '--stdin', '--superseded', '--latest-by', 'time']), \
             patch.object(sys, 'stdin', stdin), \
             patch.object(latestDatasets, 'definition_creation_date',
                          side_effect=lambda n: dates[n]), \
             contextlib.redirect_stdout(buf):
            latestDatasets.main()
        self.assertEqual(buf.getvalue().strip(), stale)


# ---------------------------------------------------------------------------
# 33. latestDatasets --emit arg validation
# ---------------------------------------------------------------------------

class TestListerArgValidation(unittest.TestCase):
    """Lister mode needs a source. Bare --complete-only (no defname/campaign/
    stdin) must error rather than silently do nothing."""

    def test_no_source_errors(self):
        from utils import latestDatasets
        with patch.object(sys, 'argv', ['latestDatasets', '--complete-only']):
            with self.assertRaises(SystemExit):
                latestDatasets.main()


# ---------------------------------------------------------------------------
# 34. --skip-produced (latestDatasets._filter_unproduced)
# ---------------------------------------------------------------------------

class TestSkipProduced(unittest.TestCase):

    def test_filter_unproduced_drops_existing(self):
        """Inputs whose this-stage output already exists in SAM are dropped."""
        from utils import latestDatasets
        tmpl = TestChainEmit.TEMPLATE
        # digi output of CeEndpoint "exists"; FlatGamma's does not.
        with patch.object(latestDatasets, '_dataset_exists',
                          side_effect=lambda name: "CeEndpoint" in name):
            kept = latestDatasets._filter_unproduced(
                ["dts.mu2e.CeEndpoint.MDC2025ap.art",
                 "dts.mu2e.FlatGamma.MDC2025ap.art"], tmpl)
        self.assertEqual(kept, ["dts.mu2e.FlatGamma.MDC2025ap.art"])


# ---------------------------------------------------------------------------
# 32. Mu2eJobBase job arithmetic (hoisted single implementation)
# ---------------------------------------------------------------------------

class TestJobArithmeticConsolidation(unittest.TestCase):
    """sequencer/job_outputs/job_event_settings/job_seed/njobs live once in
    Mu2eJobBase; the worker names its real output files through them, so
    Mu2eJobPars (submit, submissions, jobdef_lookup) must return
    byte-identical answers. Regression tests for the divergences that existed
    while jobiodetail.py and jobquery.py carried stale copies."""

    def _tar(self, tbs, owner=None, dsconf=None):
        jp = {"code": "", "setup": "/cvmfs/test/setup.sh", "tbs": tbs,
              "jobname": "cnf.mu2e.X.TC.0.tar"}
        if owner is not None:
            jp["owner"] = owner
        if dsconf is not None:
            jp["dsconf"] = dsconf
        return _make_tarball(jp, "module_type : EmptyEvent\n")

    def test_sequencer_from_index_via_jobpars(self):
        """Position-based sequencer (worker semantics), not the parent file's
        name — old Mu2eJobIO returned the parent sequencer, so recovery
        mispredicted filenames whenever the parent dataset had holes."""
        from utils.jobquery import Mu2eJobPars
        tar = self._tar({
            "inputs": {"source.fileNames":
                       [1, ["dts.mu2e.P.C.001470_00000005.art",
                            "dts.mu2e.P.C.001470_00000009.art"]]},
            "sequencer_from_index": True,
        })
        try:
            jp = Mu2eJobPars(tar)
            self.assertEqual(jp.sequencer(0), "001470_00000000")
            self.assertEqual(jp.sequencer(1), "001470_00000001")
        finally:
            os.unlink(tar)

    def test_pbisequence_runnumber_sequencer(self):
        """source.runNumber (PBISequence) — old Mu2eJobIO raised on it."""
        from utils.jobquery import Mu2eJobPars
        tar = self._tar({"event_id": {"source.runNumber": 1202}})
        try:
            self.assertEqual(Mu2eJobPars(tar).sequencer(7), "001202_00000007")
        finally:
            os.unlink(tar)

    def test_event_id_precedence_over_inputs(self):
        """Mix-shaped jobdef (inputs AND event_id): the run number wins —
        old Mu2eJobIO consulted the inputs first."""
        from utils.jobquery import Mu2eJobPars
        tar = self._tar({
            "event_id": {"source.firstRun": 1470},
            "inputs": {"source.fileNames":
                       [1, ["dts.mu2e.P.C.000999_00000042.art"]]},
        })
        try:
            self.assertEqual(Mu2eJobPars(tar).sequencer(0), "001470_00000000")
        finally:
            os.unlink(tar)

    def test_job_outputs_owner_version_substitution(self):
        """`.owner.`/`.version.` placeholders substitute through Mu2eJobPars
        exactly as through Mu2eJobFCL — old Mu2eJobIO left them literal."""
        from utils.jobquery import Mu2eJobPars
        from utils.jobfcl import Mu2eJobFCL
        tar = self._tar({
            "event_id": {"source.firstRun": 1430},
            "outfiles": {"outputs.Output.fileName":
                         "dig.owner.Foo.version.sequencer.art"},
        }, owner="alice", dsconf="Conf1")
        try:
            expected = {"outputs.Output.fileName":
                        "dig.alice.Foo.Conf1.001430_00000003.art"}
            self.assertEqual(Mu2eJobPars(tar).job_outputs(3), expected)
            self.assertEqual(Mu2eJobFCL(tar).job_outputs(3), expected)
        finally:
            os.unlink(tar)

    def test_njobs_embedded_wins(self):
        """tbs.njobs (declared cap) beats the derived count."""
        from utils.jobquery import Mu2eJobPars
        tar = self._tar({
            "njobs": 3,
            "inputs": {"source.fileNames":
                       [1, [f"dts.mu2e.P.C.001470_{i:08d}.art" for i in range(5)]]},
        })
        try:
            self.assertEqual(Mu2eJobPars(tar).njobs(), 3)
        finally:
            os.unlink(tar)

    def test_njobs_derived_from_inputs(self):
        from utils.jobquery import Mu2eJobPars
        tar = self._tar({
            "inputs": {"source.fileNames":
                       [3, [f"dts.mu2e.P.C.001470_{i:08d}.art" for i in range(10)]]},
        })
        try:
            self.assertEqual(Mu2eJobPars(tar).njobs(), 4)  # ceil(10/3)
        finally:
            os.unlink(tar)

    def test_njobs_from_samplinginput(self):
        """Resampler jobdefs derive njobs from samplinginput — the old
        Mu2eJobFCL copy answered 0 for these."""
        from utils.jobfcl import Mu2eJobFCL
        tar = self._tar({
            "samplinginput": {"source.fileNames":
                              [4, [f"sim.mu2e.S.C.001470_{i:08d}.art" for i in range(10)]]},
        })
        try:
            self.assertEqual(Mu2eJobFCL(tar).njobs(), 3)  # ceil(10/4)
        finally:
            os.unlink(tar)

    def test_njobs_open_ended_is_zero(self):
        """Generator with no embedded count: 0 = 'not a property of this
        jobdef' (count lives in the submission map), never a guess."""
        from utils.jobquery import Mu2eJobPars
        tar = self._tar({"event_id": {"source.firstRun": 1430}})
        try:
            self.assertEqual(Mu2eJobPars(tar).njobs(), 0)
        finally:
            os.unlink(tar)

    def test_njobs_invalid_merge_fails_loud(self):
        """No silent merge_factor=1 fallback (old jobquery behavior)."""
        from utils.jobquery import Mu2eJobPars
        tar = self._tar({
            "inputs": {"source.fileNames": [0, ["dts.mu2e.P.C.001470_00000000.art"]]},
        })
        try:
            with self.assertRaises(ValueError):
                Mu2eJobPars(tar).njobs()
        finally:
            os.unlink(tar)


# ---------------------------------------------------------------------------
# 32b. Mu2eJobPars.recipe (reconstruct the build config from a cnf)
# ---------------------------------------------------------------------------

class TestJobParsRecipe(unittest.TestCase):
    """A cnf in SAM is sometimes the ONLY surviving record of how it was
    built — the MDC2025ar generic reco/evnt entries were never committed.
    recipe() surfaces the embedded template fcl (the `fcl` + `fcl_overrides`
    of the json2jobdef entry), which neither jobquery's other queries nor
    fcldump exposed."""

    FCL = ('#include "Production/JobConfig/recoMC/OnSpill.fcl"\n'
           'outputs.LoopHelixOutput.fileName: "mcs.owner.{desc}.version.sequencer.art"\n'
           'services.DbService.purpose: "Sim_best"\n'
           'services.DbService.version: "v1_1"\n')

    def _tar(self):
        jp = {
            "code": "",
            "setup": "/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/MDC2025au/setup.sh",
            "jobname": "cnf.mu2e.reco.MDC2025au_best_v1_1.0.tar",
            "tbs": {"outfiles": {
                "outputs.LoopHelixOutput.fileName":
                    "mcs.mu2e.{desc}.MDC2025au_best_v1_1.sequencer.art"}},
        }
        return _make_tarball(jp, self.FCL)

    def test_recipe_reports_jobpars_fields(self):
        from utils.jobquery import Mu2eJobPars
        tar = self._tar()
        try:
            out = Mu2eJobPars(tar).recipe()
        finally:
            os.unlink(tar)
        self.assertIn("cnf.mu2e.reco.MDC2025au_best_v1_1.0.tar", out)
        self.assertIn("SimJob/MDC2025au/setup.sh", out)
        self.assertIn("njobs: 0", out)  # no tbs capacity -> generic
        self.assertIn("mcs.mu2e.{desc}.MDC2025au_best_v1_1.sequencer.art", out)

    def test_recipe_includes_override_block_verbatim(self):
        """The override lines are the part you cannot get anywhere else."""
        from utils.jobquery import Mu2eJobPars
        tar = self._tar()
        try:
            out = Mu2eJobPars(tar).recipe()
        finally:
            os.unlink(tar)
        for line in self.FCL.strip().splitlines():
            self.assertIn(line, out)

    def test_recipe_without_embedded_fcl_reports_absence(self):
        """A code-tarball cnf has no mu2e.fcl; say so rather than raising —
        the jobpars half of the recipe is still worth printing."""
        from utils.jobquery import Mu2eJobPars
        tar = _make_tarball({"code": "", "setup": "/cvmfs/test/setup.sh",
                             "jobname": "cnf.mu2e.X.TC.0.tar",
                             "tbs": {}}, fcl_content=None)
        try:
            out = Mu2eJobPars(tar).recipe()
        finally:
            os.unlink(tar)
        self.assertIn("cnf.mu2e.X.TC.0.tar", out)
        self.assertIn("no embedded mu2e.fcl", out)


# ---------------------------------------------------------------------------
# 32c. jobquery code-mode honesty (Task 7: sidecar delivery, no embedding)
# ---------------------------------------------------------------------------

class TestJobqueryCodeMode(unittest.TestCase):
    """Delivery is by jobsub sidecar (--tar_file_name); nothing is embedded
    in the cnf. codesize() returning 0 is the honest answer, and the old
    extract_code() — which pulled out any tar member ending in .tar — is
    actively wrong under sidecar delivery and must be gone."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def _cnf(self, jobpars):
        path = os.path.join(self.dir, 'cnf.mu2e.Demo.Run1Baq.0.tar')
        blob = json.dumps(jobpars).encode()
        with tarfile.open(path, 'w') as tar:
            info = tarfile.TarInfo('jobpars.json')
            info.size = len(blob)
            tar.addfile(info, io.BytesIO(blob))
        return path

    def test_recipe_reports_code_mode(self):
        from utils.jobquery import Mu2eJobPars
        cnf = self._cnf({'code': '', 'setup': 'Code/setup.sh',
                         'code_ref': {'sha256': 'a' * 64, 'size': 12,
                                      'source_path': '/exp/Code.tar.bz2'},
                         'tbs': {'outfiles': {}}, 'jobname': 'demo'})
        text = Mu2eJobPars(cnf).recipe()
        self.assertIn('/exp/Code.tar.bz2', text)
        self.assertIn('a' * 64, text)

    def test_recipe_omits_code_line_for_musing_cnf(self):
        from utils.jobquery import Mu2eJobPars
        cnf = self._cnf({'code': '', 'setup': '/cvmfs/x/setup.sh',
                         'tbs': {'outfiles': {}}, 'jobname': 'demo'})
        self.assertNotIn('code:', Mu2eJobPars(cnf).recipe())

    def test_extract_code_is_gone(self):
        # It extracted any member ending in .tar, which under sidecar
        # delivery would pull out something that is not code at all.
        from utils.jobquery import Mu2eJobPars
        self.assertFalse(hasattr(Mu2eJobPars, 'extract_code'))


# ---------------------------------------------------------------------------
# 33. jobdef._resolve_njobs (build-time tbs.njobs embedding)
# ---------------------------------------------------------------------------

class TestResolveNjobs(unittest.TestCase):

    def _files(self, n):
        return [f"dts.mu2e.P.C.001470_{i:08d}.art" for i in range(n)]

    def test_declared_within_capacity(self):
        from utils.jobdef import _resolve_njobs
        tbs = {"inputs": {"source.fileNames": [2, self._files(10)]}}
        self.assertEqual(_resolve_njobs({"njobs": 4}, tbs), 4)

    def test_declared_exceeds_capacity_fails_loud(self):
        from utils.jobdef import _resolve_njobs
        tbs = {"inputs": {"source.fileNames": [2, self._files(10)]}}
        with self.assertRaises(ValueError):
            _resolve_njobs({"njobs": 6}, tbs)

    def test_query_mode_derives(self):
        from utils.jobdef import _resolve_njobs
        tbs = {"inputs": {"source.fileNames": [3, self._files(10)]}}
        self.assertEqual(_resolve_njobs({"njobs": -1}, tbs), 4)

    def test_generator_declared(self):
        from utils.jobdef import _resolve_njobs
        self.assertEqual(_resolve_njobs({"njobs": 500}, {"event_id": {"source.firstRun": 1430}}), 500)

    def test_generator_undeclared_omitted(self):
        from utils.jobdef import _resolve_njobs
        self.assertIsNone(_resolve_njobs({}, {"event_id": {"source.firstRun": 1430}}))

    def test_generic_tarball_omitted(self):
        """Absence is load-bearing for generic tarballs (direct-input mode)."""
        from utils.jobdef import _resolve_njobs
        tbs = {"outfiles": {"outputs.Output.fileName": "nts.owner.{desc}.version.sequencer.root"}}
        self.assertIsNone(_resolve_njobs({"njobs": 500, "generic_tarball": True}, tbs))

    def test_samplinginput_capacity(self):
        from utils.jobdef import _resolve_njobs
        tbs = {"samplinginput": {"source.fileNames": [4, self._files(10)]}}
        self.assertEqual(_resolve_njobs({"njobs": -1}, tbs), 3)


# ---------------------------------------------------------------------------
# 33. normalize_input_data — single home of the input_data shape grammar
# ---------------------------------------------------------------------------

class TestNormalizeInputData(unittest.TestCase):

    def _one(self, input_data):
        from utils.config_utils import normalize_input_data
        specs = normalize_input_data(input_data)
        self.assertEqual(len(specs), 1)
        return specs[0]

    def test_plain_merge_factor(self):
        s = self._one({"dts.mu2e.NoPrimary.Run1Ban.art": 10})
        self.assertEqual((s.source, s.per_job, s.random, s.max_nfiles),
                         ("dts.mu2e.NoPrimary.Run1Ban.art", 10, False, None))

    def test_count_random(self):
        s = self._one({"dts.mu2e.NeutralsFlash.MDC2025ac.art":
                       {"count": 5000, "random": True}})
        self.assertEqual((s.per_job, s.random), (5000, True))

    def test_merge_factor_with_max_nfiles(self):
        s = self._one({"dts.mu2e.CosmicCRYAll.MDC2025ap.art":
                       {"merge_factor": 10, "max_nfiles": 10000}})
        self.assertEqual((s.per_job, s.max_nfiles), (10, 10000))

    def test_count_wins_over_merge_factor(self):
        s = self._one({"a.b.c.d.art": {"count": 3, "merge_factor": 7}})
        self.assertEqual(s.per_job, 3)

    def test_split_and_chunk_have_no_per_job(self):
        s = self._one({"/cvmfs/x/PBI.txt": {"chunk_lines": 1000}})
        self.assertEqual((s.per_job, s.chunk_lines), (None, 1000))
        s = self._one({"/tmp/PBI.txt": {"split_lines": 500}})
        self.assertEqual((s.per_job, s.split_lines), (None, 500))

    def test_non_dict_raises(self):
        from utils.config_utils import normalize_input_data
        with self.assertRaises(ValueError):
            normalize_input_data("dts.mu2e.X.C.art")
        with self.assertRaises(ValueError):
            normalize_input_data(None)

    def test_unknown_spec_key_raises(self):
        """Typos like 'marge_factor' must fail loud, not be silently ignored."""
        from utils.config_utils import normalize_input_data
        with self.assertRaises(ValueError):
            normalize_input_data({"a.b.c.d.art": {"marge_factor": 2}})

    def test_bad_max_nfiles_raises(self):
        from utils.config_utils import normalize_input_data
        for bad in (0, -5, "10"):
            with self.assertRaises(ValueError):
                normalize_input_data({"a.b.c.d.art": {"count": 1, "max_nfiles": bad}})

    def test_order_preserved_multi_dataset(self):
        from utils.config_utils import normalize_input_data
        specs = normalize_input_data({"a.b.first.d.art": 1, "a.b.second.d.art": 2})
        self.assertEqual([s.source for s in specs],
                         ["a.b.first.d.art", "a.b.second.d.art"])

    def test_consumers_share_the_grammar(self):
        """calculate_merge_factor reads the normalized spec (split_lines→1,
        missing merge info fails with the historical message)."""
        from utils.prod_utils import calculate_merge_factor
        self.assertEqual(calculate_merge_factor(
            {"input_data": {"a.b.c.d.art": {"count": 4}}}), 4)
        self.assertEqual(calculate_merge_factor(
            {"input_data": {"/tmp/f.txt": {"split_lines": 100}}}), 1)
        with self.assertRaises(ValueError):
            calculate_merge_factor({"input_data": {"a.b.c.d.art": {"random": True}}})


# ---------------------------------------------------------------------------
# 34. firstjob index windows — statistics expansion without seed collisions
# ---------------------------------------------------------------------------

class TestFirstjobOf(unittest.TestCase):
    """firstjob_of: fail-loud accessor for the cnf-index window start."""

    def test_default_zero(self):
        from utils.jobdesc import firstjob_of
        self.assertEqual(firstjob_of({'tarball': 'x'}), 0)

    def test_explicit_value(self):
        from utils.jobdesc import firstjob_of
        self.assertEqual(firstjob_of({'firstjob': 5000}), 5000)

    def test_malformed_raises(self):
        """A silently-ignored firstjob would rerun indices [0, njobs) and
        duplicate physics (baseSeed = 1 + index) — must fail loud."""
        from utils.jobdesc import firstjob_of
        for bad in (-1, '5000', 5000.0, True):
            with self.assertRaises(ValueError):
                firstjob_of({'firstjob': bad})


class TestResolveMapIndex(unittest.TestCase):
    """Global job index → (entry, cnf-local index) dispatch for the single
    entry ops['jobdesc'] ships. `local = global + firstjob`, gated on
    `global < njobs`; a generic entry (no njobs) occupies no index space."""

    def test_resolve_entry_index_single_entry(self):
        from utils.prod_utils import resolve_entry_index
        entry = {'tarball': 'cnf.mu2e.D.C.0.tar', 'njobs': 10,
                 'inloc': 'tape', 'outputs': []}
        got_entry, local = resolve_entry_index(entry, 3)
        self.assertIs(got_entry, entry)
        self.assertEqual(local, 3)

    def test_resolve_entry_index_applies_firstjob(self):
        from utils.prod_utils import resolve_entry_index
        entry = {'tarball': 'cnf.mu2e.D.C.0.tar', 'njobs': 10,
                 'firstjob': 100, 'inloc': 'tape', 'outputs': []}
        _, local = resolve_entry_index(entry, 3)
        self.assertEqual(local, 103)

    def test_resolve_entry_index_out_of_range(self):
        from utils.prod_utils import resolve_entry_index
        entry = {'tarball': 'cnf.mu2e.D.C.0.tar', 'njobs': 10,
                 'inloc': 'tape', 'outputs': []}
        self.assertEqual(resolve_entry_index(entry, 10), (None, None))

    def test_resolve_entry_index_generic_entry_has_no_slots(self):
        from utils.prod_utils import resolve_entry_index
        entry = {'tarball': 'cnf.mu2e.D.C.0.tar', 'inloc': 'tape',
                 'outputs': []}
        self.assertEqual(resolve_entry_index(entry, 0), (None, None))


class TestComputeJobsetWindow(unittest.TestCase):
    """Direct backend: jobset stays entry-relative (PROCESS space); a
    windowed entry sizes it from the entry's njobs and validates capacity."""

    def _opts(self, first=None, num=None, indices=None):
        from types import SimpleNamespace
        return SimpleNamespace(first=first, num=num, indices=indices)

    def test_plain_uses_cnf_njobs(self):
        from utils.submit import _compute_jobset
        self.assertEqual(_compute_jobset(self._opts(), 4), [0, 1, 2, 3])

    def test_windowed_open_ended_cnf(self):
        """Open-ended cnf (capacity 0): entry njobs sizes the jobset;
        indices stay 0-based — the offset is applied worker-side."""
        from utils.submit import _compute_jobset
        self.assertEqual(
            _compute_jobset(self._opts(), 0, firstjob=5000, entry_njobs=3),
            [0, 1, 2])

    def test_windowed_within_closed_capacity(self):
        from utils.submit import _compute_jobset
        self.assertEqual(
            _compute_jobset(self._opts(), 7000, firstjob=5000, entry_njobs=2000),
            list(range(2000)))

    def test_window_exceeding_capacity_raises(self):
        from utils.submit import _compute_jobset
        with self.assertRaises(ValueError):
            _compute_jobset(self._opts(), 5000, firstjob=5000, entry_njobs=1)

    def test_windowed_without_njobs_raises(self):
        from utils.submit import _compute_jobset
        with self.assertRaises(ValueError):
            _compute_jobset(self._opts(), 0, firstjob=5000)

    def test_first_num_carve_within_window(self):
        from utils.submit import _compute_jobset
        self.assertEqual(
            _compute_jobset(self._opts(first=1, num=2), 0,
                            firstjob=5000, entry_njobs=4),
            [1, 2])

    def test_indices_returns_exact_scattered_set(self):
        """--indices is the whole point: a set --first/--num cannot express."""
        from utils.submit import _compute_jobset
        self.assertEqual(
            _compute_jobset(self._opts(indices=[14719, 15944, 24301]), 0),
            [14719, 15944, 24301])

    def test_indices_on_open_ended_cnf(self):
        """capacity 0 = open-ended: no upper bound to check against."""
        from utils.submit import _compute_jobset
        self.assertEqual(_compute_jobset(self._opts(indices=[99999]), 0), [99999])

    def test_indices_within_closed_capacity(self):
        from utils.submit import _compute_jobset
        self.assertEqual(_compute_jobset(self._opts(indices=[0, 9]), 10), [0, 9])

    def test_indices_beyond_closed_capacity_raises(self):
        from utils.submit import _compute_jobset
        with self.assertRaises(ValueError):
            _compute_jobset(self._opts(indices=[0, 10]), 10)

    def test_indices_rejects_windowed_entry(self):
        """Indices are absolute; a firstjob offset would double-count."""
        from utils.submit import _compute_jobset
        with self.assertRaises(ValueError):
            _compute_jobset(self._opts(indices=[5001]), 0,
                            firstjob=5000, entry_njobs=10)

    def test_indices_rejects_first_num(self):
        from utils.submit import _compute_jobset
        with self.assertRaises(ValueError):
            _compute_jobset(self._opts(first=1, indices=[5]), 0)

    def test_indices_negative_raises(self):
        from utils.submit import _compute_jobset
        with self.assertRaises(ValueError):
            _compute_jobset(self._opts(indices=[-1, 5]), 0)


class TestParseIndices(unittest.TestCase):
    """--indices/--indices-file parsing: sorted, deduped, comment-tolerant."""

    def test_none_when_neither_given(self):
        from utils.submit import parse_indices
        self.assertIsNone(parse_indices(None, None))

    def test_comma_and_whitespace_separated(self):
        from utils.submit import parse_indices
        self.assertEqual(parse_indices('3,1 2', None), [1, 2, 3])

    def test_sorts_and_dedupes(self):
        from utils.submit import parse_indices
        self.assertEqual(parse_indices('5,1,5,1', None), [1, 5])

    def test_mutually_exclusive(self):
        from utils.submit import parse_indices
        with self.assertRaises(ValueError):
            parse_indices('1', '/tmp/whatever')

    def test_non_integer_raises(self):
        from utils.submit import parse_indices
        with self.assertRaises(ValueError):
            parse_indices('1,abc', None)

    def test_empty_spec_raises(self):
        from utils.submit import parse_indices
        with self.assertRaises(ValueError):
            parse_indices(' , ', None)

    def test_file_ignores_comments_and_blanks(self):
        """Accepts `#`-prefixed comment headers (the historical mkrecovery
        --print-indices format), whose `# <tarball>` headers must not parse
        as indices."""
        import tempfile
        from utils.submit import parse_indices
        with tempfile.NamedTemporaryFile('w', suffix='.txt', delete=False) as f:
            f.write("# cnf.mu2e.MuStopPileup.Run1Ban-001.0.tar\n"
                    "14719\n"
                    "\n"
                    "15944  # trailing comment\n")
            path = f.name
        try:
            self.assertEqual(parse_indices(None, path), [14719, 15944])
        finally:
            os.unlink(path)


class TestIndicesOpsEntryContract(unittest.TestCase):
    """The worker-side half of --indices: submit_entry ships
    `{**entry, firstjob: 0, njobs: max+1}`, which must make resolve_entry_index
    an identity (local == the absolute cnf index) for every submitted index."""

    def test_resolve_entry_index_is_identity(self):
        from utils.prod_utils import resolve_entry_index
        indices = [14719, 15944, 24301]
        ops_entry = {'tarball': 'cnf.mu2e.X.0.tar', 'firstjob': 0,
                     'njobs': indices[-1] + 1}          # mirrors submit.py
        for k in indices:
            entry, local = resolve_entry_index(ops_entry, k)
            self.assertIsNotNone(entry, f"index {k} unreachable")
            self.assertEqual(local, k)

    def test_njobs_without_the_plus_one_drops_the_max_index(self):
        """Pins the +1: resolve_entry_index gates on `global < njobs`, so
        njobs == max would put the largest index out of range."""
        from utils.prod_utils import resolve_entry_index
        ops_entry = {'tarball': 'cnf.mu2e.X.0.tar', 'firstjob': 0, 'njobs': 24301}
        self.assertEqual(resolve_entry_index(ops_entry, 24301), (None, None))


class TestLogStorageLocation(unittest.TestCase):
    """Logs go to persistent disk regardless of where data lands — the
    Mu2e convention (and what the retired POMS path did). Only `scratch` runs keep
    logs beside their data (no persistent scope for non-mu2epro accounts).

    Regression: the first direct campaign put 500 log files on tape
    because logs inherited the data output's location.
    """

    def test_tape_data_keeps_logs_on_disk(self):
        from utils.job_common import log_storage_location
        outputs = [{'dataset': 'dig.mu2e.*.art', 'location': 'tape'}]
        self.assertEqual(log_storage_location(outputs), 'disk')

    def test_scratch_data_keeps_logs_on_scratch(self):
        from utils.job_common import log_storage_location
        outputs = [{'dataset': 'dig.mu2e.*.art', 'location': 'scratch'}]
        self.assertEqual(log_storage_location(outputs), 'scratch')

    def test_disk_data_keeps_logs_on_disk(self):
        from utils.job_common import log_storage_location
        outputs = [{'dataset': 'dig.mu2e.*.art', 'location': 'disk'}]
        self.assertEqual(log_storage_location(outputs), 'disk')

    def test_accepts_entry_dict(self):
        from utils.job_common import log_storage_location
        entry = {'tarball': 'cnf.mu2e.X.0.tar',
                 'outputs': [{'dataset': 'dig.mu2e.*.art', 'location': 'tape'}]}
        self.assertEqual(log_storage_location(entry), 'disk')

    def test_missing_outputs_defaults_to_disk(self):
        from utils.job_common import log_storage_location
        self.assertEqual(log_storage_location([]), 'disk')
        self.assertEqual(log_storage_location({}), 'disk')

    def test_only_first_output_consulted_for_scratch(self):
        """A tape-first entry stays on disk even with a scratch sibling."""
        from utils.job_common import log_storage_location
        outputs = [{'dataset': 'dig.mu2e.*.art', 'location': 'tape'},
                   {'dataset': 'nts.mu2e.*.root', 'location': 'scratch'}]
        self.assertEqual(log_storage_location(outputs), 'disk')

    # — owner-aware routing (2026-08-31): user tokens have NO
    #   /mu2e/persistent/datasets scope, so a user-owned log can never
    #   go to 'disk'. Found live: self-owned tape campaign failed
    #   submit-time token acquisition on the persistent log scope. ——

    def test_user_owner_tape_data_logs_to_scratch(self):
        from utils.job_common import log_storage_location
        outputs = [{'dataset': 'sim.oksuzian.*.art', 'location': 'tape'}]
        self.assertEqual(log_storage_location(outputs, owner='oksuzian'),
                         'scratch')

    def test_user_owner_disk_data_logs_to_scratch(self):
        from utils.job_common import log_storage_location
        outputs = [{'dataset': 'sim.oksuzian.*.art', 'location': 'disk'}]
        self.assertEqual(log_storage_location(outputs, owner='oksuzian'),
                         'scratch')

    def test_mu2e_owner_tape_data_logs_to_disk(self):
        from utils.job_common import log_storage_location
        outputs = [{'dataset': 'dig.mu2e.*.art', 'location': 'tape'}]
        self.assertEqual(log_storage_location(outputs, owner='mu2e'), 'disk')

    def test_user_owner_outstage_stays_outstage(self):
        """outstage means undeclared — owner routing must not override."""
        from utils.job_common import log_storage_location
        outputs = [{'dataset': '*.art', 'location': 'outstage'}]
        self.assertEqual(log_storage_location(outputs, owner='oksuzian'),
                         'outstage')

    def test_user_owner_missing_outputs_logs_to_scratch(self):
        from utils.job_common import log_storage_location
        self.assertEqual(log_storage_location([], owner='oksuzian'),
                         'scratch')


class TestPushLogsParents(unittest.TestCase):
    """The log push must never name a parents file that isn't on disk.

    pushOutput reports `ERROR - parents file parents_list.txt not found`
    and then exits 0, so a missing parents file makes the log push a
    silent no-op — the log never reaches SAM. That bites hardest on the
    failure path, where push_data is skipped and the log is the only
    evidence left. Observed 2026-07-21 on index 519.

    Whether parents are wanted at all is now the caller's `track_parents`
    (the same flag push_data gets), not a guess from which filename
    parameter was passed.
    """

    def _capture(self, tmpdir, *, log_file, track_parents, with_parents, cnf=''):
        """Run push_logs in tmpdir and return the parents column it chose."""
        from utils import runmu2e
        captured = {}

        def fake_push_output(output_specs, output_file="output.txt",
                             simjob_setup=None):
            captured['specs'] = output_specs
            return 0

        (Path(tmpdir) / log_file).write_text('log contents\n')
        if with_parents:
            (Path(tmpdir) / 'parents_list.txt').write_text('in1.art\n')

        cwd = os.getcwd()
        try:
            os.chdir(tmpdir)
            with patch.object(runmu2e, 'push_output', fake_push_output):
                runmu2e.push_logs(log_file, track_parents=track_parents, cnf=cnf)
        finally:
            os.chdir(cwd)

        self.assertIn('specs', captured, "push_output was never called")
        self.assertEqual(len(captured['specs']), 1)
        return captured['specs'][0][2]

    def test_art_success_uses_parents_list(self):
        """Data push ran and wrote parents_list.txt — use it."""
        with tempfile.TemporaryDirectory() as d:
            parents = self._capture(d, log_file='log.mu2e.X.MDC2025ar.519.log',
                                    track_parents=True, with_parents=True)
            self.assertEqual(parents, 'parents_list.txt')

    def test_art_failure_falls_back_to_none(self):
        """mu2e failed, push_data was skipped, so parents_list.txt does not
        exist — the log must still be declarable."""
        with tempfile.TemporaryDirectory() as d:
            parents = self._capture(d, log_file='log.mu2e.X.MDC2025ar.519.log',
                                    track_parents=True, with_parents=False)
            self.assertEqual(parents, 'none')

    def test_untracked_parents_is_none_even_with_a_file(self):
        """track_parents=False (inloc dir:, g4bl) — a stray parents_list.txt
        must not be named: the job's inputs are not SAM parents."""
        with tempfile.TemporaryDirectory() as d:
            parents = self._capture(d, log_file='log.mu2e.Y.MDC2025ar.7.log',
                                    track_parents=False, with_parents=True)
            self.assertEqual(parents, 'none')

    def test_cnf_makes_the_list_wanted_without_tracked_inputs(self):
        """track_parents=False but a cnf was declared: push_data wrote a
        cnf-only parents_list.txt and the log names it too (ADR 0003)."""
        with tempfile.TemporaryDirectory() as d:
            parents = self._capture(d, log_file='log.mu2e.Y.MDC2025ar.7.log',
                                    track_parents=False, with_parents=True,
                                    cnf='cnf.mu2e.Y.MDC2025ar.0.tar')
            self.assertEqual(parents, 'parents_list.txt')

    def test_g4bl_still_none(self):
        """g4bl has no SAM parents (track_parents=False)."""
        with tempfile.TemporaryDirectory() as d:
            parents = self._capture(d, log_file='log.mu2e.G.MDC2025ar.3.log',
                                    track_parents=False, with_parents=False)
            self.assertEqual(parents, 'none')

    def test_missing_log_skips_push(self):
        """No log on disk: warn and return 0, never call pushOutput."""
        from utils import runmu2e
        with tempfile.TemporaryDirectory() as d:
            cwd = os.getcwd()
            try:
                os.chdir(d)
                with patch.object(runmu2e, 'push_output') as po:
                    rc = runmu2e.push_logs('log.mu2e.X.MDC2025ar.1.log')
            finally:
                os.chdir(cwd)
            self.assertEqual(rc, 0)
            po.assert_not_called()


class TestMaterializeLog(unittest.TestCase):
    """The SAM-named log must exist BEFORE the manifest is appended.

    Production logs never carried a manifest (2026-09-01: 0 `mu2egrid
    manifest` lines in log.mu2e.CeEndpoint.Run1Ban-001.617) because
    push_logs created the file from $JSB_TMP/JOBSUB_LOG_FILE only
    after _emit_manifest had already found it missing. _materialize_log
    is that copy, moved ahead of the manifest step.
    """

    LOG = 'log.mu2e.X.MDC2025ar.519.log'

    def _run(self, tmpdir, *, jsb_content, preexisting=None):
        from utils import runmu2e
        jsb = Path(tmpdir) / 'jsb_tmp'
        jsb.mkdir()
        if jsb_content is not None:
            (jsb / 'JOBSUB_LOG_FILE').write_text(jsb_content)
        log = Path(tmpdir) / self.LOG
        if preexisting is not None:
            log.write_text(preexisting)
        with patch.dict(os.environ, {'JSB_TMP': str(jsb)}):
            runmu2e._materialize_log(str(log))
        return log

    def test_copies_jobsub_log_into_sam_name(self):
        with tempfile.TemporaryDirectory() as d:
            log = self._run(d, jsb_content='worker stdout\n')
            self.assertEqual(log.read_text(), 'worker stdout\n')

    def test_jobsub_log_overwrites_runner_written_log(self):
        """The worker log is the superset (every runner streams to stdout)."""
        with tempfile.TemporaryDirectory() as d:
            log = self._run(d, jsb_content='full worker log\n',
                            preexisting='partial\n')
            self.assertEqual(log.read_text(), 'full worker log\n')

    def test_missing_jobsub_log_keeps_existing_file(self):
        with tempfile.TemporaryDirectory() as d:
            log = self._run(d, jsb_content=None, preexisting='runner log\n')
            self.assertEqual(log.read_text(), 'runner log\n')

    def test_no_jsb_tmp_and_no_log_warns_only(self):
        """Nothing to copy and nothing on disk: warn, create nothing."""
        from utils import runmu2e
        with tempfile.TemporaryDirectory() as d:
            env = {k: v for k, v in os.environ.items() if k != 'JSB_TMP'}
            with patch.dict(os.environ, env, clear=True):
                runmu2e._materialize_log(str(Path(d) / self.LOG))
            self.assertFalse((Path(d) / self.LOG).exists())


class TestFinishJob(unittest.TestCase):
    """The shared direct-mode push tail. Runners hand it a JobRun; it
    materializes the log, appends the manifest, and pushes data (success
    only) then the log (always), honoring --dry-run."""

    OUTPUTS = [{'dataset': 'mcs.*.art', 'location': 'scratch'}]
    LOG = 'log.testuser.X.TestConf.00000000.log'

    def _job(self, **over):
        from utils.runmu2e import JobRun
        d = dict(outputs=self.OUTPUTS, log_file=self.LOG, job_failed=False,
                 owner='testuser', infiles='in1.art in2.art',
                 simjob_setup='/cvmfs/setup.sh', track_parents=True)
        d.update(over)
        return JobRun(**d)

    def _in_tmp(self):
        d = tempfile.mkdtemp(prefix='finish_job_')
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        cwd = os.getcwd()
        os.chdir(d)
        self.addCleanup(os.chdir, cwd)
        env = {k: v for k, v in os.environ.items() if k != 'JSB_TMP'}
        patcher = patch.dict(os.environ, env, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        return Path(d)

    def test_jobrun_defaults_describe_a_parentless_job(self):
        from utils.runmu2e import JobRun
        job = JobRun(outputs=self.OUTPUTS, log_file=self.LOG,
                     job_failed=False, owner='testuser')
        self.assertEqual(job.infiles, '')
        self.assertIsNone(job.simjob_setup)
        self.assertFalse(job.track_parents)

    def test_dry_run_skips_pushes(self):
        from utils import runmu2e
        self._in_tmp()
        with patch.object(runmu2e, '_push_all') as pa:
            failed = runmu2e._finish_job(types.SimpleNamespace(dry_run=True),
                                         self._job())
        self.assertFalse(failed)
        pa.assert_not_called()

    def test_failed_job_pushes_log_only(self):
        from utils import runmu2e
        d = self._in_tmp()
        (d / self.LOG).write_text('log\n')
        with patch.object(runmu2e, 'push_data') as pd, \
             patch.object(runmu2e, 'push_logs') as pl:
            failed = runmu2e._finish_job(types.SimpleNamespace(dry_run=False),
                                         self._job(job_failed=True))
        self.assertTrue(failed)
        pd.assert_not_called()
        pl.assert_called_once_with(self.LOG, simjob_setup='/cvmfs/setup.sh',
                                   location='scratch', track_parents=True, cnf='')

    def test_success_forwards_jobrun_fields_to_both_pushes(self):
        from utils import runmu2e
        d = self._in_tmp()
        (d / self.LOG).write_text('log\n')
        with patch.object(runmu2e, 'push_data') as pd, \
             patch.object(runmu2e, 'push_logs') as pl:
            failed = runmu2e._finish_job(types.SimpleNamespace(dry_run=False),
                                         self._job())
        self.assertFalse(failed)
        pd.assert_called_once_with(self.OUTPUTS, 'in1.art in2.art',
                                   simjob_setup='/cvmfs/setup.sh',
                                   track_parents=True, cnf='')
        pl.assert_called_once_with(self.LOG, simjob_setup='/cvmfs/setup.sh',
                                   location='scratch', track_parents=True, cnf='')

    def test_log_location_follows_owner_and_outputs(self):
        """A user's data on scratch means the log goes to scratch too —
        the worker token has no persistent-disk log scope for users."""
        from utils import runmu2e
        d = self._in_tmp()
        (d / self.LOG).write_text('log\n')
        with patch.object(runmu2e, 'push_data'), \
             patch.object(runmu2e, 'push_logs') as pl:
            runmu2e._finish_job(types.SimpleNamespace(dry_run=False),
                                self._job())
        self.assertEqual(pl.call_args.kwargs['location'], 'scratch')

    def test_manifest_appended_after_jobsub_log_copy(self):
        """Ordering pin for the production fix: the copied worker log
        comes first, the manifest naming the outputs is appended after."""
        from utils import runmu2e
        d = self._in_tmp()
        jsb = d / 'jsb_tmp'
        jsb.mkdir()
        (jsb / 'JOBSUB_LOG_FILE').write_text('worker stdout\n')
        out_name = 'mcs.testuser.X.TestConf.00000000.art'
        (d / out_name).write_bytes(b'x')
        captured = io.StringIO()
        with patch.dict(os.environ, {'JSB_TMP': str(jsb)}), \
             patch.object(runmu2e, '_push_all'), \
             patch('sys.stdout', captured):
            runmu2e._finish_job(types.SimpleNamespace(dry_run=False),
                                self._job())
        text = (d / self.LOG).read_text()
        self.assertTrue(text.startswith('worker stdout\n'), text[:80])
        self.assertIn('mu2egrid manifest', text)
        self.assertIn(out_name, text)

        # C1: the manifest must also reach stdout — on a worker that IS
        # $JSB_TMP/JOBSUB_LOG_FILE, which is what pushOutput's writeLog
        # rewrites a pushOutput-bound SAM log from (see the module's
        # verified writeLog fact), so the file append alone never ships.
        stdout_text = captured.getvalue()
        self.assertIn('mu2egrid manifest', stdout_text)
        self.assertIn(out_name, stdout_text)

    def test_failed_job_manifest_names_no_outputs(self):
        from utils import runmu2e
        d = self._in_tmp()
        (d / self.LOG).write_text('log\n')
        out_name = 'mcs.testuser.X.TestConf.00000000.art'
        (d / out_name).write_bytes(b'x')
        with patch.object(runmu2e, '_push_all'):
            runmu2e._finish_job(types.SimpleNamespace(dry_run=False),
                                self._job(job_failed=True))
        text = (d / self.LOG).read_text()
        self.assertIn('mu2egrid manifest', text)
        # _emit_manifest's `ls -al` block (unconditional, out of this
        # test's scope) legitimately lists every file in cwd, stray
        # outputs included — that's a diagnostic dump, not the pushed
        # manifest. What must be empty for a failed job is the SHA256
        # section, whose lines are "<hex>  <file>" (two spaces); the ls
        # line for the same file has one space before the name, so this
        # distinguishes them.
        self.assertNotIn(f"  {out_name}", text)

    def test_manifest_step_failure_does_not_block_pushes(self):
        """M1: an OSError out of the materialize/manifest step (e.g. an
        ENOSPC writing the log) must not skip _push_all — the pushes are
        the last chance to get anything registered in SAM for this job."""
        from utils import runmu2e
        d = self._in_tmp()
        (d / self.LOG).write_text('log\n')  # so _emit_manifest is reached
        with patch.object(runmu2e, '_emit_manifest',
                          side_effect=OSError('disk full')), \
             patch.object(runmu2e, '_push_all') as pa:
            failed = runmu2e._finish_job(types.SimpleNamespace(dry_run=False),
                                         self._job())
        self.assertFalse(failed)
        pa.assert_called_once()


class TestEmitManifest(unittest.TestCase):
    """C1: the manifest block _emit_manifest appends to the log file must
    also be printed to stdout, byte-for-byte identical. On a worker,
    stdout IS $JSB_TMP/JOBSUB_LOG_FILE, which OfflineOps pushOutput's
    writeLog rewrites every pushOutput-bound log from (discarding the
    file append) — the print is what actually lands the manifest in a
    disk/scratch/tape SAM log."""

    def test_printed_block_equals_appended_block(self):
        from utils import runmu2e
        d = tempfile.mkdtemp(prefix='emit_manifest_')
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        cwd = os.getcwd()
        os.chdir(d)
        self.addCleanup(os.chdir, cwd)

        log = Path(d) / 'log.testuser.X.TestConf.00000000.log'
        log.write_text('')  # starts empty: file content == appended block
        out_name = 'mcs.testuser.X.TestConf.00000000.art'
        (Path(d) / out_name).write_bytes(b'payload')

        captured = io.StringIO()
        with patch('sys.stdout', captured):
            runmu2e._emit_manifest(str(log), [out_name])

        self.assertEqual(captured.getvalue(), log.read_text())
        self.assertIn('mu2egrid manifest', captured.getvalue())
        self.assertIn('mu2egrid manifest selfcheck', captured.getvalue())
        self.assertIn(out_name, captured.getvalue())


class TestPushDataExcludesInputs(unittest.TestCase):
    """A job's inputs must never appear in the push manifest.

    In direct-input mode the fetched input art file sits in cwd, so a
    broad outputs glob ('*.art') matches it — pushOutput then treats the
    original at its dataset path as a stale orphan and tries to DELETE
    production data. Observed on smoke cluster 29444911 (2026-08-02);
    only the mcs-only token scope blocked the delete.
    """

    IN = 'dig.mu2e.CePLeadingLogOnSpill.MDC2025au_best_v1_5.001430_00000000.art'
    OUT = 'mcs.mu2e.CePLeadingLogOnSpill.MDC2025au_best_v1_1.001430_00000000.art'

    def _pushed(self, tmpdir, outputs, infiles):
        from utils import runmu2e
        captured = {}

        def fake_push_output(output_specs, output_file="output.txt",
                             simjob_setup=None):
            captured['specs'] = output_specs
            return 0

        (Path(tmpdir) / self.IN).write_text('input art\n')
        (Path(tmpdir) / self.OUT).write_text('output art\n')
        cwd = os.getcwd()
        try:
            os.chdir(tmpdir)
            with patch.object(runmu2e, 'push_output', fake_push_output):
                runmu2e.push_data(outputs, infiles)
        finally:
            os.chdir(cwd)
        return [spec[1] for spec in captured['specs']]

    def test_broad_glob_skips_the_input_copy(self):
        with tempfile.TemporaryDirectory() as d:
            pushed = self._pushed(
                d, [{'dataset': '*.art', 'location': 'tape'}],
                infiles=self.IN)
            self.assertEqual(pushed, [self.OUT])

    def test_parents_list_still_carries_the_input(self):
        with tempfile.TemporaryDirectory() as d:
            self._pushed(d, [{'dataset': '*.art', 'location': 'tape'}],
                         infiles=self.IN)
            self.assertEqual(
                (Path(d) / 'parents_list.txt').read_text(),
                self.IN + '\n')

    # -- ADR 0003: the cnf is a SAM parent of every output ----------------

    CNF = 'cnf.mu2e.X.MDC2025au_best_v1_5.0.tar'

    def _pushed_with(self, tmpdir, outputs, infiles, **kw):
        from utils import runmu2e
        captured = {}

        def fake_push_output(output_specs, output_file="output.txt",
                             simjob_setup=None):
            captured['specs'] = output_specs
            return 0

        (Path(tmpdir) / self.IN).write_text('input art\n')
        (Path(tmpdir) / self.OUT).write_text('output art\n')
        cwd = os.getcwd()
        try:
            os.chdir(tmpdir)
            with patch.object(runmu2e, 'push_output', fake_push_output):
                runmu2e.push_data(outputs, infiles, **kw)
        finally:
            os.chdir(cwd)
        return captured['specs']

    def test_cnf_joins_the_inputs_in_parents_list(self):
        with tempfile.TemporaryDirectory() as d:
            specs = self._pushed_with(d, [{'dataset': '*.art', 'location': 'tape'}],
                                      infiles=self.IN, cnf=self.CNF)
            self.assertEqual((Path(d) / 'parents_list.txt').read_text(),
                             f'{self.IN}\n{self.CNF}\n')
            self.assertEqual({s[2] for s in specs}, {'parents_list.txt'})

    def test_cnf_is_a_parent_even_when_inputs_are_not(self):
        """inloc dir: (track_parents=False) used to mean 'none'. The cnf
        is SAM-registered regardless, so it is declared on its own."""
        with tempfile.TemporaryDirectory() as d:
            specs = self._pushed_with(d, [{'dataset': '*.art', 'location': 'tape'}],
                                      infiles=self.IN, track_parents=False, cnf=self.CNF)
            self.assertEqual((Path(d) / 'parents_list.txt').read_text(), self.CNF + '\n')
            self.assertEqual({s[2] for s in specs}, {'parents_list.txt'})

    def test_no_inputs_and_no_cnf_is_still_none(self):
        with tempfile.TemporaryDirectory() as d:
            specs = self._pushed_with(d, [{'dataset': '*.art', 'location': 'tape'}],
                                      infiles=self.IN, track_parents=False)
            self.assertFalse((Path(d) / 'parents_list.txt').exists())
            self.assertEqual({s[2] for s in specs}, {'none'})

    def test_resampler_style_job_declares_only_the_cnf(self):
        """infiles='' (no SAM inputs) used to write an empty parents file;
        now the cnf is the one parent."""
        with tempfile.TemporaryDirectory() as d:
            self._pushed_with(d, [{'dataset': '*.art', 'location': 'tape'}],
                              infiles='', cnf=self.CNF)
            self.assertEqual((Path(d) / 'parents_list.txt').read_text(), self.CNF + '\n')

    def test_no_infiles_pushes_everything(self):
        """Resampler-style jobs (infiles='') keep the old behavior."""
        with tempfile.TemporaryDirectory() as d:
            pushed = self._pushed(
                d, [{'dataset': '*.art', 'location': 'tape'}], infiles='')
            self.assertEqual(sorted(pushed), sorted([self.IN, self.OUT]))


class TestPushAllKeepsTheLog(unittest.TestCase):
    """A data-push failure must never skip the log push.

    Root cause 2026-07-27, CeMLeadingLog indices 2 and 418: attempt 1 left
    a partially-written file on tape without completing its SAM
    declaration. Every retry then ran mu2e to completion (hours of CPU),
    and pushOutput's `recover` path tried to gfal-rm the orphan to replace
    it — HTTP 403, because tape is write-once. pushOutput exited 2, the
    CalledProcessError propagated out of _push_with_retry, and the log
    push that sits AFTER the data push never ran. Three attempts, zero
    forensic evidence in SAM. The log is the only witness to a data-push
    failure, so it has to survive one.
    """

    @staticmethod
    def _cpe(rc=2):
        return subprocess.CalledProcessError(rc, 'pushOutput')

    def test_both_succeed_calls_both(self):
        from utils import runmu2e
        calls = []
        runmu2e._push_all(lambda: calls.append('data'),
                          lambda: calls.append('log'))
        self.assertEqual(calls, ['data', 'log'])

    def test_data_failure_still_pushes_log(self):
        """The regression under test: log push runs despite the data raise."""
        from utils import runmu2e
        calls = []

        def data():
            calls.append('data')
            raise self._cpe()

        with self.assertRaises(subprocess.CalledProcessError):
            runmu2e._push_all(data, lambda: calls.append('log'))
        self.assertIn('log', calls, "log push was skipped by the data failure")

    def test_data_failure_propagates_so_condor_sees_it(self):
        """Pushing the log must not swallow the failure — the job still fails."""
        from utils import runmu2e
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            runmu2e._push_all(self._raise_data, lambda: None)
        self.assertEqual(cm.exception.returncode, 2)

    @staticmethod
    def _raise_data():
        raise subprocess.CalledProcessError(2, 'pushOutput')

    def test_log_failure_does_not_mask_data_failure(self):
        """When BOTH fail, the data-push error is the real story and wins."""
        from utils import runmu2e

        def log():
            raise subprocess.CalledProcessError(7, 'pushOutput')

        with self.assertRaises(subprocess.CalledProcessError) as cm:
            runmu2e._push_all(self._raise_data, log)
        self.assertEqual(cm.exception.returncode, 2,
                         "log-push rc masked the data-push rc")

    def test_log_failure_alone_still_fails_the_job(self):
        """Data fine but the log never landed: CB2 says don't pass silently."""
        from utils import runmu2e

        def log():
            raise subprocess.CalledProcessError(5, 'pushOutput')

        with self.assertRaises(subprocess.CalledProcessError) as cm:
            runmu2e._push_all(lambda: None, log)
        self.assertEqual(cm.exception.returncode, 5)

    def test_skipped_data_push_still_logs(self):
        """mu2e-failed path passes a no-op data push; the log must still go."""
        from utils import runmu2e
        calls = []
        runmu2e._push_all(lambda: None, lambda: calls.append('log'))
        self.assertEqual(calls, ['log'])


class TestTerminalPushError(unittest.TestCase):
    """A 403-on-delete is terminal: no retry can ever clear it.

    pushOutput's `recover` path deletes an existing target to replace it.
    On /pnfs/mu2e/tape that delete always 403s (write-once, no delete
    right), so retrying re-runs mu2e for hours and dies identically.
    Burning the attempt cap this way cost three full mixing jobs on
    CeMLeadingLog 2/418 before a human saw it.
    """

    ORPHAN_403 = (
        "WARNING - output file exists for /pnfs/mu2e/tape/phy-sim/dig/mu2e/X\n"
        "INFO - running recover\n"
        "ERROR - rm failed for try 0 for https://fndcadoor.fnal.gov:2880/...\n"
        "gfal-rm error: 1 (Operation not permitted) - DavPosix::unlink  "
        "HTTP 403 : Permission refused\n"
        "pushOutput status at exit: 2\n"
    )

    def test_orphan_403_is_terminal(self):
        from utils import runmu2e
        self.assertTrue(runmu2e._is_terminal_push_error(self.ORPHAN_403))

    def test_transient_failure_is_not_terminal(self):
        from utils import runmu2e
        self.assertFalse(runmu2e._is_terminal_push_error(
            "ERROR - copy failed for try 0\ncurl: (56) Recv failure\n"))

    def test_no_output_is_not_terminal(self):
        """Unknown output must stay retryable — fail open on classification,
        closed on action."""
        from utils import runmu2e
        self.assertFalse(runmu2e._is_terminal_push_error(None))
        self.assertFalse(runmu2e._is_terminal_push_error(''))

    def test_403_without_rm_is_not_terminal(self):
        """A 403 on the WRITE is a scope problem, not the orphan poison
        pill — don't claim the same diagnosis for it."""
        from utils import runmu2e
        self.assertFalse(runmu2e._is_terminal_push_error(
            "ERROR - copy failed\nHTTP 403 : Permission refused\n"))

    def test_retry_stops_immediately_on_terminal(self):
        """The whole point: one attempt, not four."""
        from utils import runmu2e
        calls = []

        def push():
            calls.append(1)
            raise subprocess.CalledProcessError(2, 'pushOutput',
                                                output=self.ORPHAN_403)

        with self.assertRaises(subprocess.CalledProcessError):
            runmu2e._push_with_retry(push, retries=3, base_delay=0)
        self.assertEqual(len(calls), 1,
                         f"retried a terminal failure {len(calls)} times")

    def test_retry_still_retries_transient(self):
        """Don't over-fit: ordinary failures keep their retries."""
        from utils import runmu2e
        calls = []

        def push():
            calls.append(1)
            raise subprocess.CalledProcessError(1, 'pushOutput',
                                                output="curl: (56) Recv failure")

        with self.assertRaises(subprocess.CalledProcessError):
            runmu2e._push_with_retry(push, retries=2, base_delay=0)
        self.assertEqual(len(calls), 3)




class TestStorageScopeCoversPhysicalPath(unittest.TestCase):
    """A token scope must actually cover the path it is meant to protect.

    Upstream mu2ejobsub derives a scope path by ONE rule
    (token_request_dirname): strip the /pnfs prefix, change nothing else.
    So `storage_scope(f, loc)` must be a prefix of `dataset_dir(ds, loc)`
    with /pnfs removed — for every location.

    tape breaks that if you insert `datasets/` unconditionally: the
    physical tape layout has no such component (see dataset_dir), so the
    scope named a path nothing lives at and granted nothing. Writes kept
    working via the separate broad `storage.create:/mu2e`, which under
    the WLCG profile permits upload but NOT delete — so pushOutput's
    recover path could never remove a stale file and 403'd forever
    (CeMLeadingLog 2/418, 2026-07-27).
    """

    DIG = 'dig.mu2e.CeMLeadingLogMix1BB.MDC2025au_best_v1_3.001430_00000003.art'
    LOG = 'log.mu2e.CeMLeadingLogMix1BB.MDC2025au_best_v1_3.001430_00000003.log'

    def _assert_scope_covers(self, filename, location):
        from utils.file_resolver import dataset_dir, storage_scope
        from utils.job_common import Mu2eName
        scope = storage_scope(filename, location)
        physical = dataset_dir(str(Mu2eName.parse(filename).dataset), location)
        self.assertTrue(
            physical.startswith('/pnfs' + scope + '/'),
            f"scope {scope!r} does not cover physical path {physical!r}")

    def test_tape_scope_covers_tape_path(self):
        self._assert_scope_covers(self.DIG, 'tape')

    def test_disk_scope_covers_disk_path(self):
        self._assert_scope_covers(self.LOG, 'disk')

    def test_scratch_scope_covers_scratch_path(self):
        self._assert_scope_covers(self.DIG, 'scratch')

    def test_tape_scope_has_no_datasets_component(self):
        """The specific regression: tape's physical layout omits it."""
        from utils.file_resolver import storage_scope
        self.assertEqual(storage_scope(self.DIG, 'tape'),
                         '/mu2e/tape/phy-sim/dig/mu2e')

    def test_disk_scope_keeps_datasets_component(self):
        """Don't over-correct — disk's physical layout really does have it."""
        from utils.file_resolver import storage_scope
        self.assertEqual(storage_scope(self.LOG, 'disk'),
                         '/mu2e/persistent/datasets/phy-etc/log/mu2e')


class TestRunSubmitClusterVerification(unittest.TestCase):
    """_run_submit must not report 'submitted' without a parsed cluster ID —
    jobsub_lite can exit 0 while its internal condor_submit failed (the
    2026-07-10 condor_vault_storer incident)."""

    def _run(self, returncode, stdout):
        from utils import submit
        fake = MagicMock(returncode=returncode, stdout=stdout, stderr='')
        with patch.object(submit.subprocess, 'run', return_value=fake):
            return submit._run_submit(['jobsub_submit', '-N', '5'], 'cnf.tar', 5)

    def test_exit0_with_cluster_is_submitted(self):
        r = self._run(0, "5000 job(s) submitted to cluster 28708717.\n")
        self.assertEqual((r['status'], r['cluster_id']), ('submitted', '28708717'))

    def test_exit0_without_cluster_is_failed(self):
        r = self._run(0, "Submitting job(s)\nError: condor_submit exited "
                         "with failed status code 1\n")
        self.assertEqual((r['status'], r['cluster_id']), ('failed', None))

    def test_nonzero_exit_is_failed(self):
        r = self._run(1, "")
        self.assertEqual(r['status'], 'failed')


class TestValidateWindow(unittest.TestCase):
    """validate_window is the single owner of the window rule, shared by
    build_jobdesc and the submit path (_compute_jobset)."""

    def test_open_ended_any_window(self):
        from utils.jobdesc import validate_window
        validate_window(5000, 100, 0)      # capacity 0 = open-ended
        validate_window(5000, 100, None)

    def test_closed_capacity_enforced(self):
        from utils.jobdesc import validate_window
        validate_window(5000, 2000, 7000)  # exactly fits
        with self.assertRaises(ValueError):
            validate_window(5000, 2001, 7000)

    def test_njobs_required(self):
        from utils.jobdesc import validate_window
        with self.assertRaises(ValueError):
            validate_window(5000, None, 0)


class TestValidateJobdescFirstjob(unittest.TestCase):
    """Dispatch boundary: firstjob on an njobs-less entry must fail loud
    (maps are hand-edited; a silently-dropped window duplicates physics)."""

    def test_firstjob_without_njobs_rejected(self):
        from utils.runmu2e import validate_jobdesc
        bad = {'tarball': 'cnf.mu2e.X.C.0.tar', 'inloc': 'tape',
               'outputs': [], 'firstjob': 5000}
        with self.assertRaises(SystemExit):
            validate_jobdesc(bad)

    def test_firstjob_with_njobs_accepted(self):
        from utils.runmu2e import validate_jobdesc
        ok = {'tarball': 'cnf.mu2e.X.C.0.tar', 'inloc': 'tape',
              'outputs': [], 'firstjob': 5000, 'njobs': 10}
        self.assertEqual(validate_jobdesc(ok), False)  # normal mode


# ---------------------------------------------------------------------------
# Submission ledger (utils/submission_ledger.py) — direct-backend recovery
# ---------------------------------------------------------------------------
class TestSubmissionLedger(unittest.TestCase):
    def setUp(self):
        import tempfile
        from utils import submission_ledger as sl
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'submissions.db')
        self.entry = {'tarball': 'cnf.mu2e.TestDesc.TestConf.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                      'njobs': 5, 'inloc': 'tape',
                      'outputs': [{'location': 'tape'}]}

    def _record(self, indices=(0, 1, 2), parent=None):
        return self.sl.record_submission(
            self.db, tarball=self.entry['tarball'], entry=self.entry,
            indices=list(indices), jobsub_id='12345678.0@jobsub03.fnal.gov',
            cluster_id='12345678', origin='/tmp/map.json', parent_id=parent)

    def test_record_and_read_roundtrip(self):
        rid = self._record()
        rows = self.sl.open_rows(self.db)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row['id'], rid)
        self.assertEqual(row['state'], 'active')
        self.assertEqual(row['attempt'], 1)
        self.assertIsNone(row['parent_id'])
        self.assertEqual(row['indices'], [0, 1, 2])
        self.assertEqual(row['entry'], self.entry)
        self.assertEqual(row['jobsub_id'], '12345678.0@jobsub03.fnal.gov')
        self.assertEqual(row['cluster_id'], '12345678')

    def test_indices_stored_sorted(self):
        self._record(indices=(7, 2, 5))
        self.assertEqual(self.sl.open_rows(self.db)[0]['indices'], [2, 5, 7])

    def test_child_attempt_increments(self):
        rid = self._record()
        child = self._record(indices=(2,), parent=rid)
        rows = {r['id']: r for r in self.sl.open_rows(self.db)}
        self.assertEqual(rows[child]['attempt'], 2)
        self.assertEqual(rows[child]['parent_id'], rid)

    def test_unknown_parent_rejected(self):
        with self.assertRaises(ValueError):
            self._record(parent=999)

    def test_close_row_removes_from_open(self):
        rid = self._record()
        self.sl.close_row(self.db, rid, 'complete', note='all verified')
        self.assertEqual(self.sl.open_rows(self.db), [])
        allr = self.sl.all_rows(self.db)
        self.assertEqual(allr[0]['state'], 'complete')
        self.assertEqual(allr[0]['note'], 'all verified')
        self.assertIsNotNone(allr[0]['closed_utc'])

    def test_close_invalid_state_rejected(self):
        rid = self._record()
        with self.assertRaises(ValueError):
            self.sl.close_row(self.db, rid, 'bogus')
        with self.assertRaises(ValueError):
            self.sl.close_row(self.db, rid, 'active')

    def test_close_row_rejects_reservation_states(self):
        # 'submitting' and 'failed' are reservation states owned by
        # reserve_submission/fail_reservation — close_row must not be
        # able to move an active row into either, even though both are
        # now members of STATES.
        rid = self._record()
        with self.assertRaises(ValueError):
            self.sl.close_row(self.db, rid, 'failed')
        with self.assertRaises(ValueError):
            self.sl.close_row(self.db, rid, 'submitting')

    def test_close_nonactive_row_rejected(self):
        rid = self._record()
        self.sl.close_row(self.db, rid, 'complete')
        with self.assertRaises(ValueError):
            self.sl.close_row(self.db, rid, 'exhausted')

    def test_missing_db_dir_fails_loudly(self):
        import sqlite3
        with self.assertRaises(sqlite3.OperationalError):
            self.sl.record_submission(
                '/nonexistent-dir-recovery-test/sub.db', tarball='t',
                entry={}, indices=[0], jobsub_id=None, cluster_id='1')


class TestTwoPhaseLedgerWrite(unittest.TestCase):
    def setUp(self):
        from utils import submission_ledger as sl
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'submissions.db')
        self.entry = {'tarball': 'cnf.mu2e.TestDesc.TestConf.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                      'njobs': 5, 'inloc': 'tape',
                      'outputs': [{'location': 'tape'}]}

    def _reserve(self, indices=(0, 1, 2)):
        return self.sl.reserve_submission(
            self.db, tarball=self.entry['tarball'], entry=self.entry,
            indices=list(indices), origin='/tmp/map.json')

    def test_reserved_row_records_indices_before_any_cluster_exists(self):
        rid = self._reserve()
        rows = self.sl.all_rows(self.db)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['id'], rid)
        self.assertEqual(rows[0]['state'], 'submitting')
        self.assertEqual(rows[0]['indices'], [0, 1, 2])
        self.assertIsNone(rows[0]['cluster_id'])
        self.assertIsNone(rows[0]['jobsub_id'])

    def test_reserved_row_is_not_an_open_row(self):
        # The recovery loop must not treat a not-yet-submitted window as
        # a live submission to verify.
        self._reserve()
        self.assertEqual(self.sl.open_rows(self.db), [])

    def test_reserved_row_is_visible_to_reserved_rows(self):
        rid = self._reserve()
        self.assertEqual([r['id'] for r in self.sl.reserved_rows(self.db)],
                         [rid])

    def test_attach_cluster_promotes_to_active(self):
        rid = self._reserve()
        self.sl.attach_cluster(self.db, rid, jobsub_id='99.0@jobsub03.fnal.gov',
                               cluster_id='99')
        rows = self.sl.open_rows(self.db)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['state'], 'active')
        self.assertEqual(rows[0]['cluster_id'], '99')
        self.assertEqual(rows[0]['jobsub_id'], '99.0@jobsub03.fnal.gov')
        self.assertEqual(self.sl.reserved_rows(self.db), [])

    def test_attach_cluster_twice_raises(self):
        rid = self._reserve()
        self.sl.attach_cluster(self.db, rid, jobsub_id='99.0@s', cluster_id='99')
        with self.assertRaises(ValueError):
            self.sl.attach_cluster(self.db, rid, jobsub_id='99.0@s',
                                   cluster_id='99')

    def test_fail_reservation_closes_the_row(self):
        rid = self._reserve()
        self.sl.fail_reservation(self.db, rid, 'jobsub_submit returned 1')
        row = self.sl.all_rows(self.db)[0]
        self.assertEqual(row['state'], 'failed')
        self.assertIsNotNone(row['closed_utc'])
        self.assertIn('jobsub_submit', row['note'])
        self.assertEqual(self.sl.open_rows(self.db), [])

    def test_failed_window_still_blocks_reuse(self):
        # jobsub_submit can exit non-zero having already created a
        # cluster, so a failed reservation's window is NOT proven free.
        # It must keep blocking until a human reconciles it.
        from utils.submissions import _slice_overlaps_ledger
        rid = self._reserve(indices=(0, 1, 2))
        self.sl.fail_reservation(self.db, rid, 'submit failed')
        self.assertTrue(_slice_overlaps_ledger(
            self.db, self.entry['tarball'], 0, 0, 3))

    def test_reserved_window_blocks_a_duplicate_slice(self):
        # The crash window itself: reserved, process dies, next tick.
        from utils.submissions import _slice_overlaps_ledger
        self._reserve(indices=(0, 1, 2))
        self.assertTrue(_slice_overlaps_ledger(
            self.db, self.entry['tarball'], 0, 0, 3))

    # — reconcile: the ONLY exit from a blocking failed/stuck window ----
    # Without it a failed submit deadlocked its campaign permanently:
    # the 'failed' row keeps overlapping, top_up re-pauses on every
    # tick, and `resume` cannot help because it is the ROW, not the
    # cursor, that blocks. Hand-editing sqlite was the only escape.

    def test_reconciled_failed_window_stops_blocking(self):
        from utils.submissions import _slice_overlaps_ledger
        rid = self._reserve(indices=(0, 1, 2))
        self.sl.fail_reservation(self.db, rid, 'submit failed')
        self.assertTrue(_slice_overlaps_ledger(
            self.db, self.entry['tarball'], 0, 0, 3))
        self.sl.reconcile_row(self.db, rid, 'jobsub_q checked, no jobs')
        self.assertFalse(_slice_overlaps_ledger(
            self.db, self.entry['tarball'], 0, 0, 3))

    def test_reconciled_reservation_stops_blocking(self):
        from utils.submissions import _slice_overlaps_ledger
        rid = self._reserve(indices=(0, 1, 2))
        self.sl.reconcile_row(self.db, rid, 'jobsub_q checked, no jobs')
        self.assertFalse(_slice_overlaps_ledger(
            self.db, self.entry['tarball'], 0, 0, 3))

    def test_reconcile_keeps_the_row_and_records_the_assertion(self):
        # The row is closed, never deleted: the audit trail of the
        # failed attempt (and of who said the window was free) survives.
        rid = self._reserve()
        self.sl.fail_reservation(self.db, rid, 'submit failed')
        was = self.sl.reconcile_row(self.db, rid, 'jobsub_q checked')
        row = self.sl.all_rows(self.db)[0]
        self.assertEqual(was, 'failed')
        self.assertEqual(row['id'], rid)
        self.assertEqual(row['state'], 'reconciled')
        self.assertIsNotNone(row['closed_utc'])
        self.assertIn('jobsub_q checked', row['note'])

    def test_reconcile_refuses_a_row_that_is_not_failed_or_reserved(self):
        # An active row has a live cluster; clearing its window would
        # re-feed indices that are genuinely running.
        rid = self._reserve()
        self.sl.attach_cluster(self.db, rid, jobsub_id='9.0@s',
                               cluster_id='9')
        with self.assertRaises(ValueError) as ctx:
            self.sl.reconcile_row(self.db, rid, 'x')
        self.assertIn('active', str(ctx.exception))

    def test_reconcile_unknown_row_raises(self):
        with self.assertRaises(ValueError):
            self.sl.reconcile_row(self.db, 999, 'x')

    def test_nothing_reconciles_a_row_automatically(self):
        # The safety property that keeps 'failed' rows blocking: only an
        # explicit human invocation may clear one, so no tick function
        # may call reconcile_row.
        import inspect
        from utils import submissions
        src = inspect.getsource(submissions)
        for fn in ('top_up', 'drain_tick', 'process_row', '_run_pass'):
            body = inspect.getsource(getattr(submissions, fn))
            self.assertNotIn('reconcile_row', body, fn)
        # It is reachable from exactly one place: the CLI verb.
        self.assertEqual(src.count('reconcile_row('), 1)


class TestCampaignLedger(unittest.TestCase):
    """campaigns table in utils/submission_ledger.py (sliced submission)."""

    def setUp(self):
        import tempfile
        from utils import submission_ledger as sl
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'submissions.db')
        self.entry = {'tarball': 'cnf.mu2e.TestDesc.TestConf.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                      'njobs': 10, 'inloc': 'tape',
                      'outputs': [{'location': 'tape'}]}

    def _create(self, tarball=None, slice_size=4):
        return self.sl.create_campaign(
            self.db, tarball=tarball or self.entry['tarball'],
            entry=self.entry, slice_size=slice_size,
            origin='/tmp/map.json')

    def test_create_and_read_roundtrip(self):
        cid = self._create()
        camps = self.sl.active_campaigns(self.db)
        self.assertEqual(len(camps), 1)
        c = camps[0]
        self.assertEqual(c['id'], cid)
        self.assertEqual(c['state'], 'active')
        self.assertEqual(c['cursor'], 0)
        self.assertEqual(c['slice_size'], 4)
        self.assertEqual(c['entry'], self.entry)
        self.assertEqual(c['origin'], '/tmp/map.json')
        self.assertIsNone(c['closed_utc'])

    def test_duplicate_active_tarball_refused(self):
        self._create()
        with self.assertRaises(ValueError):
            self._create()

    def test_duplicate_paused_tarball_refused(self):
        """A paused campaign still owns its index space: enqueue-after-
        pause then resume would feed two campaigns into the same
        indices (double submit) — refuse it just like an active one."""
        cid = self._create()
        self.sl.set_campaign_state(self.db, cid, 'paused')
        with self.assertRaises(ValueError) as cm:
            self._create()
        self.assertIn('paused', str(cm.exception))

    def test_reenqueue_allowed_after_close(self):
        cid = self._create()
        self.sl.set_campaign_state(self.db, cid, 'cancelled')
        self._create()  # must not raise
        self.assertEqual(len(self.sl.all_campaigns(self.db)), 2)

    def test_set_slice_returns_old_and_stores_new(self):
        cid = self._create(slice_size=500)
        old = self.sl.set_campaign_slice(self.db, cid, 2000)
        self.assertEqual(old, 500)
        self.assertEqual(self.sl.active_campaigns(self.db)[0]['slice_size'],
                         2000)

    def test_set_slice_allowed_while_paused(self):
        """Retuning a paused campaign is legitimate: the new size binds
        when it is resumed."""
        cid = self._create(slice_size=500)
        self.sl.set_campaign_state(self.db, cid, 'paused')
        self.sl.set_campaign_slice(self.db, cid, 1500)
        c = [x for x in self.sl.all_campaigns(self.db) if x['id'] == cid][0]
        self.assertEqual(c['slice_size'], 1500)

    def test_set_slice_refused_on_closed_campaign(self):
        """Nothing reads slice_size after close — accepting it would
        report success for a no-op."""
        cid = self._create()
        self.sl.set_campaign_state(self.db, cid, 'complete')
        with self.assertRaises(ValueError) as cm:
            self.sl.set_campaign_slice(self.db, cid, 2000)
        self.assertIn('complete', str(cm.exception))

    def test_set_slice_rejects_zero_and_negative(self):
        cid = self._create(slice_size=500)
        for bad in (0, -1):
            with self.assertRaises(ValueError):
                self.sl.set_campaign_slice(self.db, cid, bad)
        self.assertEqual(self.sl.active_campaigns(self.db)[0]['slice_size'],
                         500)

    def test_set_slice_unknown_campaign_raises(self):
        with self.assertRaises(ValueError) as cm:
            self.sl.set_campaign_slice(self.db, 999, 2000)
        self.assertIn('999', str(cm.exception))

    def test_slice_size_validated(self):
        with self.assertRaises(ValueError):
            self._create(slice_size=0)

    def test_set_memory_returns_old_and_stores_new(self):
        cid = self._create()
        self.assertIsNone(self.sl.set_campaign_memory(self.db, cid, '3000MB'))
        self.assertEqual(
            self.sl.active_campaigns(self.db)[0]['entry']['memory'], '3000MB')
        # Second call reports what the first one set.
        self.assertEqual(
            self.sl.set_campaign_memory(self.db, cid, '4000MB'), '3000MB')

    def test_set_memory_preserves_other_entry_keys(self):
        cid = self._create()
        self.sl.set_campaign_memory(self.db, cid, '3000MB')
        entry = self.sl.active_campaigns(self.db)[0]['entry']
        for k, v in self.entry.items():
            self.assertEqual(entry[k], v)

    def test_set_memory_allowed_while_paused(self):
        cid = self._create()
        self.sl.set_campaign_state(self.db, cid, 'paused')
        self.sl.set_campaign_memory(self.db, cid, '3000MB')
        self.assertEqual(
            self.sl.all_campaigns(self.db)[0]['entry']['memory'], '3000MB')

    def test_set_memory_refused_on_closed_campaign(self):
        cid = self._create()
        self.sl.set_campaign_state(self.db, cid, 'complete')
        with self.assertRaises(ValueError) as cm:
            self.sl.set_campaign_memory(self.db, cid, '3000MB')
        self.assertIn('complete', str(cm.exception))

    def test_set_memory_rejects_malformed_values(self):
        cid = self._create()
        for bad in ('3000', 'lots', '3000 MB', '', '-1MB', '3000mb'):
            with self.assertRaises(ValueError, msg=bad):
                self.sl.set_campaign_memory(self.db, cid, bad)
        # ...and never wrote a partial value on the way out.
        self.assertNotIn(
            'memory', self.sl.active_campaigns(self.db)[0]['entry'])

    def test_set_memory_unknown_campaign_raises(self):
        with self.assertRaises(ValueError) as cm:
            self.sl.set_campaign_memory(self.db, 999, '3000MB')
        self.assertIn('999', str(cm.exception))

    def test_set_entry_key_sets_inloc(self):
        cid = self._create()
        previous, rows = self.sl.set_campaign_entry_key(
            self.db, cid, 'inloc', 'resilient')
        self.assertEqual(previous, 'tape')
        self.assertEqual(rows, [])
        self.assertEqual(
            self.sl.active_campaigns(self.db)[0]['entry']['inloc'],
            'resilient')

    def test_set_entry_key_preserves_other_keys(self):
        cid = self._create()
        self.sl.set_campaign_entry_key(self.db, cid, 'inloc', 'resilient')
        entry = self.sl.active_campaigns(self.db)[0]['entry']
        self.assertEqual(entry['tarball'], self.entry['tarball'])
        self.assertEqual(entry['njobs'], self.entry['njobs'])
        self.assertEqual(entry['outputs'], self.entry['outputs'])

    def test_set_entry_key_refuses_non_whitelisted_key(self):
        """tarball/njobs/firstjob/input_pattern define the campaign's
        identity and index space — editing them in place corrupts a live
        campaign rather than fixing it."""
        cid = self._create()
        for bad in ('tarball', 'njobs', 'firstjob', 'input_pattern',
                    'outputs', 'nonsense'):
            with self.assertRaises(ValueError, msg=bad) as cm:
                self.sl.set_campaign_entry_key(self.db, cid, bad, 'x')
            self.assertIn('not editable', str(cm.exception))
        self.assertEqual(
            self.sl.active_campaigns(self.db)[0]['entry'], self.entry)

    def test_set_entry_key_validates_inloc(self):
        cid = self._create()
        for good in ('tape', 'disk', 'resilient', 'stash', 'none',
                     'dir:/pnfs/mu2e/persistent/x'):
            self.sl.set_campaign_entry_key(self.db, cid, 'inloc', good)
        for bad in ('Resilient', 'dir:relative/path', 'dir:', 'nfs', ''):
            with self.assertRaises(ValueError, msg=bad):
                self.sl.set_campaign_entry_key(self.db, cid, 'inloc', bad)
        # last good value survived every rejected write
        self.assertEqual(
            self.sl.active_campaigns(self.db)[0]['entry']['inloc'],
            'dir:/pnfs/mu2e/persistent/x')

    def test_set_entry_key_validates_lifetime(self):
        cid = self._create()
        for good in ('48h', '3600s', '30m', '2d'):
            self.sl.set_campaign_entry_key(
                self.db, cid, 'expected_lifetime', good)
        for bad in ('48', '48 h', '48hr', 'forever', ''):
            with self.assertRaises(ValueError, msg=bad):
                self.sl.set_campaign_entry_key(
                    self.db, cid, 'expected_lifetime', bad)

    def test_set_entry_key_validates_disk_like_memory(self):
        cid = self._create()
        self.sl.set_campaign_entry_key(self.db, cid, 'disk', '50GB')
        with self.assertRaises(ValueError):
            self.sl.set_campaign_entry_key(self.db, cid, 'disk', '50 GB')

    def test_set_entry_key_refused_on_closed_campaign(self):
        cid = self._create()
        self.sl.set_campaign_state(self.db, cid, 'complete')
        with self.assertRaises(ValueError) as cm:
            self.sl.set_campaign_entry_key(
                self.db, cid, 'inloc', 'resilient')
        self.assertIn('complete', str(cm.exception))

    def test_set_entry_key_allowed_while_paused(self):
        cid = self._create()
        self.sl.set_campaign_state(self.db, cid, 'paused')
        self.sl.set_campaign_entry_key(self.db, cid, 'inloc', 'resilient')
        self.assertEqual(
            self.sl.all_campaigns(self.db)[0]['entry']['inloc'], 'resilient')

    def test_set_entry_key_unknown_campaign_raises(self):
        with self.assertRaises(ValueError) as cm:
            self.sl.set_campaign_entry_key(self.db, 999, 'inloc', 'tape')
        self.assertIn('999', str(cm.exception))

    def _row(self, state='active', entry=None):
        """One submissions row on this campaign's tarball."""
        rid = self.sl.record_submission(
            self.db, tarball=self.entry['tarball'],
            entry=entry or dict(self.entry), indices=[0, 1],
            jobsub_id='1.0@sched', cluster_id='1')
        if state != 'active':
            self.sl.close_row(self.db, rid, state)
        return rid

    def test_cascade_off_by_default_protects_recovery_floor(self):
        """An UNSET memory is what earns a recovery the 4000MB floor, so
        the default must not push a value into dispatched rows."""
        cid = self._create()
        rid = self._row()
        self.sl.set_campaign_entry_key(self.db, cid, 'memory', '3000MB')
        row = [r for r in self.sl.all_rows(self.db) if r['id'] == rid][0]
        self.assertNotIn('memory', row['entry'])

    def test_cascade_updates_open_rows_and_reports_ids(self):
        cid = self._create()
        rid = self._row()
        previous, changed = self.sl.set_campaign_entry_key(
            self.db, cid, 'inloc', 'resilient', include_open_rows=True)
        self.assertEqual(previous, 'tape')
        self.assertEqual(changed, [rid])
        row = [r for r in self.sl.all_rows(self.db) if r['id'] == rid][0]
        self.assertEqual(row['entry']['inloc'], 'resilient')

    def test_cascade_skips_closed_rows(self):
        cid = self._create()
        closed = self._row(state='complete')
        open_id = self._row()
        _, changed = self.sl.set_campaign_entry_key(
            self.db, cid, 'inloc', 'resilient', include_open_rows=True)
        self.assertEqual(changed, [open_id])
        by_id = {r['id']: r for r in self.sl.all_rows(self.db)}
        self.assertEqual(by_id[closed]['entry']['inloc'], 'tape')
        self.assertEqual(by_id[open_id]['entry']['inloc'], 'resilient')

    def test_cascade_preserves_row_specific_keys(self):
        """A recovery child's snapshot may differ from the campaign's
        (e.g. firstjob dropped); the cascade must touch only its key."""
        cid = self._create()
        child = dict(self.entry)
        child['memory'] = '4000MB'
        rid = self._row(entry=child)
        self.sl.set_campaign_entry_key(
            self.db, cid, 'inloc', 'resilient', include_open_rows=True)
        row = [r for r in self.sl.all_rows(self.db) if r['id'] == rid][0]
        self.assertEqual(row['entry']['memory'], '4000MB')
        self.assertEqual(row['entry']['inloc'], 'resilient')

    def test_cascade_leaves_other_tarballs_alone(self):
        cid = self._create()
        other = self.sl.record_submission(
            self.db, tarball='cnf.mu2e.Other.TestConf.0.tar',
            entry={'tarball': 'cnf.mu2e.Other.TestConf.0.tar',
                   'njobs': 3, 'inloc': 'tape', 'outputs': []},
            indices=[0], jobsub_id='2.0@sched', cluster_id='2')
        _, changed = self.sl.set_campaign_entry_key(
            self.db, cid, 'inloc', 'resilient', include_open_rows=True)
        self.assertNotIn(other, changed)
        row = [r for r in self.sl.all_rows(self.db) if r['id'] == other][0]
        self.assertEqual(row['entry']['inloc'], 'tape')

    def test_cascade_reaches_open_rows_of_a_complete_campaign(self):
        """'complete' means every SLICE was dispatched, not that every job
        landed — the open rows still recover, and recovery reads the ROW's
        entry. Refusing the cascade here made inloc uncorrectable without
        hand-editing entry_json (campaign 54, 2026-08-12)."""
        cid = self._create()
        rid = self._row()
        self.sl.set_campaign_state(self.db, cid, 'complete')
        previous, changed = self.sl.set_campaign_entry_key(
            self.db, cid, 'inloc', 'resilient', include_open_rows=True)
        self.assertEqual(previous, 'tape')
        self.assertEqual(changed, [rid])
        row = [r for r in self.sl.all_rows(self.db) if r['id'] == rid][0]
        self.assertEqual(row['entry']['inloc'], 'resilient')

    def test_settled_campaign_snapshot_is_left_unchanged(self):
        """The campaign snapshot is deliberately NOT edited once the
        cursor is settled: no future slice reads it, so writing there
        would record an intent nothing executes."""
        cid = self._create()
        self._row()
        self.sl.set_campaign_state(self.db, cid, 'complete')
        self.sl.set_campaign_entry_key(
            self.db, cid, 'inloc', 'resilient', include_open_rows=True)
        camp = [c for c in self.sl.all_campaigns(self.db)
                if c['id'] == cid][0]
        self.assertEqual(camp['entry']['inloc'], 'tape')

    def test_cascade_reaches_open_rows_of_a_cancelled_campaign(self):
        """A cancelled campaign still recovers its dispatched rows, so it
        needs the same correction path as a complete one."""
        cid = self._create()
        rid = self._row()
        self.sl.set_campaign_state(self.db, cid, 'cancelled')
        _, changed = self.sl.set_campaign_entry_key(
            self.db, cid, 'inloc', 'resilient', include_open_rows=True)
        self.assertEqual(changed, [rid])
        row = [r for r in self.sl.all_rows(self.db) if r['id'] == rid][0]
        self.assertEqual(row['entry']['inloc'], 'resilient')

    def test_settled_refusal_points_at_the_flag_not_just_the_state(self):
        """Without the flag a settled campaign is still refused — but the
        message must name the way forward, or the operator concludes the
        value is uncorrectable and reaches for sqlite."""
        cid = self._create()
        self.sl.set_campaign_state(self.db, cid, 'complete')
        with self.assertRaises(ValueError) as cm:
            self.sl.set_campaign_entry_key(
                self.db, cid, 'inloc', 'resilient')
        self.assertIn('--include-open-rows', str(cm.exception))

    def test_advance_cursor(self):
        cid = self._create()
        self.sl.advance_campaign(self.db, cid, 4)
        self.assertEqual(self.sl.active_campaigns(self.db)[0]['cursor'], 4)

    def test_advance_backward_refused(self):
        cid = self._create()
        self.sl.advance_campaign(self.db, cid, 4)
        with self.assertRaises(ValueError):
            self.sl.advance_campaign(self.db, cid, 2)

    def test_advance_nonactive_refused(self):
        cid = self._create()
        self.sl.set_campaign_state(self.db, cid, 'paused')
        with self.assertRaises(ValueError):
            self.sl.advance_campaign(self.db, cid, 4)

    def test_state_transitions(self):
        cid = self._create()
        self.sl.set_campaign_state(self.db, cid, 'paused', note='op pause')
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['state'], 'paused')
        self.sl.set_campaign_state(self.db, cid, 'active')   # resume
        c = self.sl.active_campaigns(self.db)[0]
        self.assertEqual(c['state'], 'active')
        self.assertIsNone(c['closed_utc'])                   # reopened
        self.sl.set_campaign_state(self.db, cid, 'complete')
        self.assertIsNotNone(self.sl.all_campaigns(self.db)[0]['closed_utc'])

    def test_invalid_transitions_raise(self):
        cid = self._create()
        with self.assertRaises(ValueError):
            self.sl.set_campaign_state(self.db, cid, 'nonsense')
        self.sl.set_campaign_state(self.db, cid, 'complete')
        with self.assertRaises(ValueError):
            self.sl.set_campaign_state(self.db, cid, 'active')  # complete is terminal
        with self.assertRaises(ValueError):
            self.sl.set_campaign_state(self.db, 999, 'paused')  # no such id


# ---------------------------------------------------------------------------
# Entry resource keys (utils/jobdesc.py, utils/submit.py)
# ---------------------------------------------------------------------------
class TestEntryResources(unittest.TestCase):
    """memory/disk/expected_lifetime: entry keys, precedence, snapshot."""

    def _opts(self, memory=None, disk=None, expected_lifetime=None):
        import argparse
        return argparse.Namespace(memory=memory, disk=disk,
                                  expected_lifetime=expected_lifetime)

    def test_resources_of_subset(self):
        from utils.jobdesc import resources_of
        self.assertEqual(resources_of({'tarball': 't'}), {})
        self.assertEqual(
            resources_of({'memory': '4000MB', 'njobs': 5}),
            {'memory': '4000MB'})
        self.assertEqual(
            resources_of({'memory': '4000MB', 'disk': '50GB',
                          'expected_lifetime': '48h'}),
            {'memory': '4000MB', 'disk': '50GB', 'expected_lifetime': '48h'})

    def test_resources_of_nonstring_raises(self):
        from utils.jobdesc import resources_of
        with self.assertRaises(ValueError):
            resources_of({'memory': 4000})

    def test_effective_cli_beats_entry(self):
        from utils.submit import _effective_resources
        eff = _effective_resources({'memory': '4000MB'},
                                   self._opts(memory='8000MB'))
        self.assertEqual(eff['memory'], '8000MB')

    def test_effective_entry_beats_default(self):
        from utils.submit import _effective_resources
        eff = _effective_resources({'memory': '4000MB'}, self._opts())
        self.assertEqual(eff['memory'], '4000MB')
        self.assertIsNone(eff['disk'])            # None -> jobsub_argv builtin
        self.assertIsNone(eff['expected_lifetime'])

    def test_snapshot_merges_without_mutating(self):
        from utils.submit import _snapshot_entry
        entry = {'tarball': 't', 'njobs': 5}
        snap = _snapshot_entry(entry, {'memory': '8000MB', 'disk': None,
                                       'expected_lifetime': None})
        self.assertEqual(snap['memory'], '8000MB')
        self.assertNotIn('disk', snap)
        self.assertNotIn('memory', entry)         # original untouched

    def test_build_jobdesc_projects_core_keys(self):
        from utils.json2jobdef import build_jobdesc
        config = {'desc': 'D', 'dsconf': 'C', 'owner': 'mu2e',
                  'inloc': 'tape', 'njobs': 7,
                  'outloc': {'*.art': 'tape'},
                  'simjob_setup': '/cvmfs/x/setup.sh'}
        with patch('utils.json2jobdef.get_parfile_name',
                   return_value='cnf.mu2e.D.C.0.tar'):
            entry = build_jobdesc(config)
        self.assertEqual(entry['tarball'], 'cnf.mu2e.D.C.0.tar')
        self.assertEqual(entry['inloc'], 'tape')
        self.assertEqual(entry['njobs'], 7)
        self.assertEqual(entry['outputs'],
                         [{'dataset': '*.art', 'location': 'tape'}])

    def test_build_jobdesc_omits_njobs_for_generic(self):
        """Absence of njobs is what makes runmu2e pick direct-input
        mode, so a generic tarball must not carry one."""
        from utils.json2jobdef import build_jobdesc
        config = {'desc': 'D', 'dsconf': 'C', 'owner': 'mu2e',
                  'inloc': 'tape', 'njobs': 7, 'generic_tarball': True,
                  'outloc': {'*.art': 'tape'},
                  'simjob_setup': '/cvmfs/x/setup.sh'}
        with patch('utils.json2jobdef.get_parfile_name',
                   return_value='cnf.mu2e.D.C.0.tar'):
            entry = build_jobdesc(config)
        self.assertNotIn('njobs', entry)

    def test_build_jobdesc_rejects_non_dict_outloc(self):
        from utils.json2jobdef import build_jobdesc
        config = {'desc': 'D', 'dsconf': 'C', 'owner': 'mu2e',
                  'inloc': 'tape', 'njobs': 7,
                  'outloc': [{'*.art': 'tape'}],
                  'simjob_setup': '/cvmfs/x/setup.sh'}
        with patch('utils.json2jobdef.get_parfile_name',
                   return_value='cnf.mu2e.D.C.0.tar'):
            with self.assertRaises(ValueError):
                build_jobdesc(config)

    def test_build_jobdesc_passes_resource_keys(self):
        entry = self._entry({
            'desc': 'TestDesc', 'dsconf': 'TestConf', 'owner': 'mu2e',
            'inloc': 'tape', 'njobs': 5, 'memory': '4000MB',
            'outloc': {'sim.mu2e.TestDesc.TestConf.art': 'tape'}})
        self.assertEqual(entry['memory'], '4000MB')
        self.assertNotIn('disk', entry)           # absent key stays absent

    def _drain_config(self, **over):
        cfg = {'desc': 'evnt', 'dsconf': 'TestConf', 'owner': 'mu2e',
               'inloc': 'tape', 'generic_tarball': True,
               'input_pattern': 'mcs.mu2e.%OnSpill.TestConf.art',
               'prestage': True,
               'outloc': {'nts.*.root': 'tape'}}
        cfg.update(over)
        return cfg

    def _entry(self, config):
        from utils.json2jobdef import build_jobdesc
        return build_jobdesc(config)

    def test_build_jobdesc_passes_draining_keys(self):
        """A draining campaign must be enqueueable straight from its
        config. input_pattern and prestage are read off the ENTRY
        (is_draining, _validate_draining_entry, drain_tick's residency
        gate), so leaving them in the JSON config alone would silently
        produce a non-draining campaign."""
        entry = self._entry(self._drain_config())
        self.assertEqual(entry['input_pattern'],
                         'mcs.mu2e.%OnSpill.TestConf.art')
        self.assertIs(entry['prestage'], True)
        self.assertNotIn('njobs', entry)          # generic => no index space

    def test_draining_outputs_glob_comes_from_outloc(self):
        """The tier-specific glob is config, not a hand edit: '*.art' outputs
        on a draining entry let the worker declare its own fetched INPUT for
        push, which pushOutput's orphan recovery then tried to DELETE from
        tape (2026-08-02 smoke). outloc is where that is fixed once."""
        entry = self._entry(self._drain_config())
        self.assertEqual(entry['outputs'],
                         [{'dataset': 'nts.*.root', 'location': 'tape'}])

    def test_input_pattern_without_generic_tarball_is_refused(self):
        """Emitting both input_pattern and njobs would leave the entry
        self-contradictory: is_draining() says draining while njobs claims a
        fixed window. Refuse rather than build it."""
        cfg = self._drain_config(njobs=5)
        del cfg['generic_tarball']
        with self.assertRaises(SystemExit):
            self._entry(cfg)

    def test_non_draining_entry_gains_no_draining_keys(self):
        entry = self._entry({
            'desc': 'TestDesc', 'dsconf': 'TestConf', 'owner': 'mu2e',
            'inloc': 'tape', 'njobs': 5,
            'outloc': {'sim.mu2e.TestDesc.TestConf.art': 'tape'}})
        self.assertNotIn('input_pattern', entry)
        self.assertNotIn('prestage', entry)


class TestSubmitEntryResourceWiring(unittest.TestCase):
    """submit_entry must actually pass the EFFECTIVE resources
    (entry key, no CLI flag) into build_jobsub_argv — the precedence
    logic itself is covered above (_effective_resources), this closes
    the gap that nothing proved submit_entry wires it through."""

    def test_entry_memory_reaches_build_jobsub_argv(self):
        from utils.submit import submit_entry, SubmitOptions

        entry = {'tarball': 'cnf.mu2e.NoSuchTarballXYZ.TestConf.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                 'njobs': 5, 'inloc': 'tape',
                 'outputs': [{'location': 'tape'}], 'memory': '4000MB'}
        options = SubmitOptions(
            ledger_db='/tmp/unused-resource-wiring.db',
            dry_run=True, origin='/tmp/m.json')

        captured = {}

        def fake_build_jobsub_argv(**kwargs):
            captured.update(kwargs)
            return ['--fake-argv']

        with patch('utils.submit._jobsub_argv.build_jobsub_argv',
                   side_effect=fake_build_jobsub_argv):
            result = submit_entry(entry, 0, options)

        self.assertEqual(result['status'], 'dry_run')
        # entry key, no CLI flag -> _effective_resources picks the entry
        self.assertEqual(captured['memory'], '4000MB')
        self.assertIsNone(captured['disk'])
        self.assertIsNone(captured['expected_lifetime'])


class TestJobsubArgvCodeTarball(unittest.TestCase):
    """The code tarball rides jobsub's --tar_file_name (RCDS/cvmfs,
    published once, no per-job copy) — NOT -f dropbox://, which
    transfers per job. mu2eprodsys:474-475 does the same."""

    def _argv(self, **extra):
        from utils.jobsub_argv import build_jobsub_argv
        return build_jobsub_argv(
            entry={'tarball': 'cnf.mu2e.Demo.Run1Baq_best_v1_5.0.tar',
                   'outputs': []},
            jobset=[0, 1, 2],
            jobdef_path='/tmp/cnf.mu2e.Demo.Run1Baq_best_v1_5.0.tar',
            ops_json_path='/tmp/ops.json',
            prodtools_dir='/cvmfs/fake/prodtools/v0.0.0',
            submitter='me',
            **extra)

    def test_no_code_tarball_no_flag(self):
        argv = self._argv()
        self.assertNotIn('--tar_file_name', argv)

    def test_code_tarball_adds_flag(self):
        argv = self._argv(code_tarball='/exp/build/Code.tar.bz2')
        idx = argv.index('--tar_file_name')
        self.assertEqual(argv[idx + 1], 'dropbox:///exp/build/Code.tar.bz2')

    def test_code_tarball_does_not_displace_the_two_input_files(self):
        # Regression guard: --tar_file_name is a DIFFERENT mechanism from
        # -f dropbox://. The cnf and the ops JSON must still ship — and
        # nothing else does for a release entry: prodtools comes from
        # cvmfs (the dev-checkout opt-in is TestDevProdtoolsTarball).
        argv = self._argv(code_tarball='/exp/build/Code.tar.bz2')
        shipped = [argv[i + 1] for i, a in enumerate(argv) if a == '-f']
        self.assertEqual(len(shipped), 2)
        self.assertIn('dropbox:///tmp/ops.json', shipped)
        self.assertIn('dropbox:///tmp/cnf.mu2e.Demo.Run1Baq_best_v1_5.0.tar',
                      shipped)
        self.assertFalse([x for x in shipped if 'prodtools' in x])

    def test_worker_runs_the_release_named_by_env_and_executable(self):
        """The release reaches the worker two ways, both required:
        MU2EGRID_PRODTOOLS_DIR (runjob.sh cannot find itself — jobsub
        copies the executable into the sandbox) and the executable path
        (the release's own runjob.sh). No dropbox tarball, no
        MU2EGRID_PRODTOOLS_TAR."""
        argv = self._argv()
        i = argv.index('-e', 0)
        envs = [argv[j + 1] for j, a in enumerate(argv) if a == '-e']
        self.assertIn('MU2EGRID_PRODTOOLS_DIR=/cvmfs/fake/prodtools/v0.0.0', envs)
        self.assertFalse([e for e in envs if e.startswith('MU2EGRID_PRODTOOLS_TAR')])
        self.assertEqual(argv[-1], 'file:///cvmfs/fake/prodtools/v0.0.0/bin/runjob.sh')

    def test_executable_stays_last(self):
        argv = self._argv(code_tarball='/exp/build/Code.tar.bz2')
        self.assertTrue(argv[-1].startswith('file://'))


class TestSubmitPassesCodeTarball(unittest.TestCase):
    """submit_entry must actually wire the entry's code tarball into
    build_jobsub_argv. Same shape as TestSubmitEntryResourceWiring
    (test_unit.py:5323), which closes the identical gap for memory."""

    def test_entry_code_reaches_build_jobsub_argv(self):
        from utils.submit import submit_entry, SubmitOptions

        entry = {'tarball': 'cnf.mu2e.NoSuchTarballXYZ.TestConf.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                 'njobs': 5, 'inloc': 'tape',
                 'outputs': [{'location': 'tape'}],
                 'code': '/exp/build/Code.tar.bz2'}
        options = SubmitOptions(
            ledger_db='/tmp/unused-code-wiring.db',
            dry_run=True, origin='/tmp/m.json')

        captured = {}

        def fake_build_jobsub_argv(**kwargs):
            captured.update(kwargs)
            return ['--fake-argv']

        with patch('utils.submit._jobsub_argv.build_jobsub_argv',
                   side_effect=fake_build_jobsub_argv):
            result = submit_entry(entry, 0, options)

        self.assertEqual(result['status'], 'dry_run')
        self.assertEqual(captured['code_tarball'], '/exp/build/Code.tar.bz2')

    def test_musing_entry_passes_none(self):
        from utils.submit import submit_entry, SubmitOptions

        entry = {'tarball': 'cnf.mu2e.NoSuchTarballXYZ.TestConf.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                 'njobs': 5, 'inloc': 'tape',
                 'outputs': [{'location': 'tape'}]}
        options = SubmitOptions(
            ledger_db='/tmp/unused-code-wiring.db',
            dry_run=True, origin='/tmp/m.json')

        captured = {}

        def fake_build_jobsub_argv(**kwargs):
            captured.update(kwargs)
            return ['--fake-argv']

        with patch('utils.submit._jobsub_argv.build_jobsub_argv',
                   side_effect=fake_build_jobsub_argv):
            submit_entry(entry, 0, options)

        self.assertIsNone(captured['code_tarball'])


class TestSubmitEntryCodeGate(unittest.TestCase):
    """The per-submit half of Finding 1's fix: submit_entry must refuse
    when the code-tarball digest gate fails, and must never reach
    _run_submit. Deleting the check_code_tarball call in
    _preflight_inputs (or moving it back below the draining
    early-return) must fail this test."""

    ENTRY = {'tarball': 'cnf.mu2e.NoSuchTarballXYZ.TestConf.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
             'njobs': 5, 'inloc': 'tape',
             'outputs': [{'location': 'tape'}],
             'code': '/exp/build/Code.tar.bz2'}

    class FakePars:
        def __init__(self, path):
            pass

        def njobs(self):
            return 5

        def input_datasets(self):
            return []

        def job_outputs(self, index):
            return {}

    def _opts(self, **over):
        from utils.submit import SubmitOptions
        base = dict(ledger_db='/tmp/unused-code-gate.db', dry_run=False,
                    origin='/tmp/m.json')
        base.update(over)
        return SubmitOptions(**base)

    def test_bad_code_tarball_refuses_and_does_not_submit(self):
        from utils import submit
        from utils.check_inputs import Problem
        bad = [Problem(dataset='code', filename=self.ENTRY['code'],
                       kind='code_mismatch', detail='digest does not match')]
        with patch.object(submit, '_ensure_local_tarball',
                          return_value=Path('/tmp/t.tar')), \
             patch('utils.jobquery.Mu2eJobPars', self.FakePars), \
             patch.object(submit, 'check_code_tarball',
                          return_value=(False, bad)), \
             patch.object(submit, '_run_submit') as rs:
            with self.assertRaises(SystemExit):
                submit.submit_entry(dict(self.ENTRY), 0, self._opts())
        rs.assert_not_called()


# ---------------------------------------------------------------------------
# submit_map --enqueue (utils/submit.py) — sliced-campaign submission
# ---------------------------------------------------------------------------
class TestEnqueue(unittest.TestCase):
    """enqueue_entry: campaign registration, no submission. The only
    caller left is json2jobdef --enqueue; these tests exercise the
    function directly rather than through a CLI argv."""

    def setUp(self):
        import tempfile
        from utils import submission_ledger as sl
        from utils import submit
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'submissions.db')
        self.entry = {'tarball': 'cnf.mu2e.TestDesc.TestConf.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                      'njobs': 10, 'inloc': 'tape',
                      'outputs': [{'location': 'tape'}]}
        # Task 6 enqueue gate reads the tarball; stub tarball resolution
        # and the pre-flight check so these campaign-registration tests
        # stay file-free (as they were before the gate existed).
        tb_patcher = patch.object(submit, '_ensure_local_tarball',
                                  return_value=Path(self.entry['tarball']))
        ci_patcher = patch.object(submit, 'check_inputs',
                                  return_value=(True, []))
        cc_patcher = patch.object(submit, 'check_code_tarball',
                                  return_value=(True, []))
        tb_patcher.start()
        ci_patcher.start()
        cc_patcher.start()
        self.addCleanup(tb_patcher.stop)
        self.addCleanup(ci_patcher.stop)
        self.addCleanup(cc_patcher.stop)

    def test_enqueue_writes_campaign(self):
        from utils.submit import enqueue_entry
        camp_id = enqueue_entry(self.entry, ledger_db=self.db,
                                slice_size=100, provenance='/tmp/m.json')
        camps = self.sl.active_campaigns(self.db)
        self.assertEqual([c['id'] for c in camps], [camp_id])
        c = camps[0]
        self.assertEqual(c['tarball'], self.entry['tarball'])
        self.assertEqual(c['slice_size'], 100)
        self.assertEqual(c['cursor'], 0)
        self.assertEqual(c['origin'], '/tmp/m.json')
        self.assertEqual(c['entry'], self.entry)
        # nothing submitted: the submissions table stays empty
        self.assertEqual(self.sl.open_rows(self.db), [])

    def test_enqueue_merges_cli_resources_into_snapshot(self):
        from utils.submit import enqueue_entry
        enqueue_entry(self.entry, ledger_db=self.db, slice_size=100,
                      resources={'memory': '4000MB'})
        c = self.sl.active_campaigns(self.db)[0]
        self.assertEqual(c['entry']['memory'], '4000MB')
        self.assertNotIn('memory', self.entry)     # original untouched

    def test_enqueue_dry_run_writes_nothing(self):
        from utils.submit import enqueue_entry
        result = enqueue_entry(self.entry, ledger_db=self.db,
                               slice_size=100, dry_run=True)
        self.assertIsNone(result)
        self.assertEqual(self.sl.all_campaigns(self.db), [])

    def test_enqueue_duplicate_is_hard_error(self):
        from utils.submit import enqueue_entry
        enqueue_entry(self.entry, ledger_db=self.db, slice_size=100)
        with self.assertRaises(SystemExit):
            enqueue_entry(self.entry, ledger_db=self.db, slice_size=100)

    def test_enqueue_generic_entry_refused(self):
        from utils.submit import enqueue_entry
        generic = {'tarball': 'cnf.mu2e.G.C.0.tar', 'inloc': 'tape',
                   'outputs': []}   # no njobs
        with self.assertRaises(SystemExit):
            enqueue_entry(generic, ledger_db=self.db, slice_size=100)

    def test_enqueue_zero_njobs_refused(self):
        """njobs_of(entry) is None misses njobs: 0 — a zero-job campaign
        is nonsensical and must be refused just like the missing case."""
        from utils.submit import enqueue_entry
        zero = {'tarball': 'cnf.mu2e.Z.C.0.tar', 'njobs': 0,
                'inloc': 'tape', 'outputs': []}
        with self.assertRaises(SystemExit):
            enqueue_entry(zero, ledger_db=self.db, slice_size=100)

    def test_enqueue_db_failure_is_hard_error(self):
        from utils.submit import enqueue_entry
        with self.assertRaises(SystemExit):
            enqueue_entry(self.entry,
                         ledger_db='/nonexistent-dir-enqueue-test/s.db',
                         slice_size=10)

    def test_enqueue_entry_returns_campaign_id(self):
        from utils.submit import enqueue_entry
        camp_id = enqueue_entry(self.entry, ledger_db=self.db,
                                slice_size=2)
        camps = self.sl.active_campaigns(self.db)
        self.assertEqual(len(camps), 1)
        self.assertEqual(camps[0]['id'], camp_id)
        self.assertEqual(camps[0]['slice_size'], 2)
        self.assertEqual(camps[0]['entry'], self.entry)

    def test_enqueue_entry_dry_run_returns_none(self):
        from utils.submit import enqueue_entry
        self.assertIsNone(enqueue_entry(
            self.entry, ledger_db=self.db, slice_size=2, dry_run=True))
        self.assertEqual(self.sl.all_campaigns(self.db), [])

    def test_enqueue_entry_records_provenance(self):
        from utils.submit import enqueue_entry
        enqueue_entry(self.entry, ledger_db=self.db, slice_size=2,
                      provenance='data/x.json#Desc@Conf')
        self.assertEqual(
            self.sl.active_campaigns(self.db)[0]['origin'],
            'data/x.json#Desc@Conf')


class TestEnqueueErrorStyle(unittest.TestCase):
    """Operator-reachable enqueue_entry failures are one-line
    json2jobdef: messages, not tracebacks."""

    def setUp(self):
        import tempfile
        from utils import submit
        self.tmp = _mkdtemp()
        self.db = os.path.join(self.tmp, 'sub.db')
        # Task 6 enqueue gate reads the tarball; stub tarball resolution
        # and the pre-flight check so these tests stay file-free.
        tb_patcher = patch.object(submit, '_ensure_local_tarball',
                                  return_value=Path('cnf.mu2e.E.C.0.tar'))
        ci_patcher = patch.object(submit, 'check_inputs',
                                  return_value=(True, []))
        cc_patcher = patch.object(submit, 'check_code_tarball',
                                  return_value=(True, []))
        tb_patcher.start()
        ci_patcher.start()
        cc_patcher.start()
        self.addCleanup(tb_patcher.stop)
        self.addCleanup(ci_patcher.stop)
        self.addCleanup(cc_patcher.stop)

    def _entry(self, tarball='cnf.mu2e.E.C.0.tar'):
        return {'tarball': tarball, 'njobs': 50,
                'prodtools_dir': FAKE_PRODTOOLS_DIR}

    def test_duplicate_enqueue_one_line_no_traceback(self):
        from utils.submit import enqueue_entry
        enqueue_entry(self._entry(), ledger_db=self.db, slice_size=10)
        with self.assertRaises(SystemExit) as cm:
            enqueue_entry(self._entry(), ledger_db=self.db, slice_size=10)
        msg = str(cm.exception.code)
        self.assertTrue(msg.startswith('json2jobdef: '), msg)
        self.assertNotIn('\n', msg)
        self.assertNotIn('Traceback', msg)

    def test_db_error_one_line(self):
        from utils.submit import enqueue_entry
        bad_db = os.path.join(self.tmp, 'no', 'such', 'dir', 'sub.db')
        with self.assertRaises(SystemExit) as cm:
            enqueue_entry(self._entry(), ledger_db=bad_db, slice_size=10)
        self.assertTrue(str(cm.exception.code).startswith('json2jobdef: '))


# ---------------------------------------------------------------------------
# Submission log (utils/submit.py) — dated per-attempt record
# ---------------------------------------------------------------------------
class TestSubmissionLog(unittest.TestCase):
    """Dated per-submission log beside the ledger DB (all origins:
    manual runs, cron slices, recovery resubmits)."""

    def setUp(self):
        import tempfile
        self.dbdir = _mkdtemp()
        self.db = os.path.join(self.dbdir, 'submissions.db')

    def _opts(self):
        from utils.submit import SubmitOptions
        return SubmitOptions(ledger_db=self.db, origin='/tmp/m.json')

    def _result(self, status='submitted'):
        return {'tarball': 'cnf.mu2e.T.C.0.tar', 'cluster_id': '123',
                'jobsub_id': '123.0@js.fnal.gov', 'njobs': 3,
                'status': status,
                'raw_output': 'Use job id 123.0@js.fnal.gov ...\n'}

    def _read_log(self):
        from utils.submit import _submission_log_path
        with open(_submission_log_path(self.db)) as f:
            return f.read()

    def test_success_block_appended(self):
        from utils.submit import _log_submission
        _log_submission(100, [0, 1, 2], self._result(), self._opts())
        text = self._read_log()
        self.assertIn('status=submitted', text)
        self.assertIn('cnf.mu2e.T.C.0.tar', text)
        self.assertIn('[100..102]', text)          # absolute indices
        self.assertIn('Use job id 123.0@js.fnal.gov', text)

    def test_failure_block_appended(self):
        from utils.submit import _log_submission
        _log_submission(0, [0], self._result(status='failed'), self._opts())
        self.assertIn('status=failed', self._read_log())

    def test_appends_not_truncates(self):
        from utils.submit import _log_submission
        _log_submission(0, [0], self._result(), self._opts())
        _log_submission(0, [1], self._result(), self._opts())
        self.assertEqual(self._read_log().count('=== end'), 2)

    def test_write_failure_never_raises(self):
        from utils.submit import _log_submission, SubmitOptions
        opts = SubmitOptions(
            ledger_db='/nonexistent-dir-submitlog-test/s.db',
            origin='/tmp/m.json')
        _log_submission(0, [0], self._result(), opts)  # must not raise

    def test_run_submit_carries_raw_output(self):
        from utils import submit
        fake = MagicMock(
            returncode=0, stderr='warn\n',
            stdout='1 job(s) submitted to cluster 12345678.\n'
                   'Use job id 12345678.0@jobsub03.fnal.gov to retrieve output\n')
        with patch('utils.submit.subprocess.run', return_value=fake):
            r = submit._run_submit(['jobsub_submit'], 'cnf.tar', 3)
        self.assertIn('Use job id', r['raw_output'])
        self.assertIn('warn', r['raw_output'])

    def test_run_submit_failure_carries_raw_output(self):
        from utils import submit
        fake = MagicMock(returncode=1, stderr='boom\n', stdout='')
        with patch('utils.submit.subprocess.run', return_value=fake):
            r = submit._run_submit(['jobsub_submit'], 'cnf.tar', 3)
        self.assertEqual(r['status'], 'failed')
        self.assertIn('boom', r['raw_output'])


class TestRecoverCap(unittest.TestCase):
    """Cap resolution + queue counting for the top-up phase."""

    def test_resolve_cap_flag_wins(self):
        from utils import submissions as recover
        with patch.dict(os.environ, {'MU2E_MAX_QUEUED': '5'}):
            self.assertEqual(recover.resolve_cap(42), 42)

    def test_resolve_cap_env_beats_default(self):
        from utils import submissions as recover
        with patch.dict(os.environ, {'MU2E_MAX_QUEUED': '5'}):
            self.assertEqual(recover.resolve_cap(None), 5)

    def test_resolve_cap_default(self):
        from utils import submissions as recover
        env = {k: v for k, v in os.environ.items() if k != 'MU2E_MAX_QUEUED'}
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(recover.resolve_cap(None),
                             recover.DEFAULT_MAX_QUEUED)

    def test_resolve_cap_bad_env_exits(self):
        from utils import submissions as recover
        with patch.dict(os.environ, {'MU2E_MAX_QUEUED': 'lots'}):
            with self.assertRaises(SystemExit):
                recover.resolve_cap(None)

    def _runner(self, stdout, rc=0):
        def run(cmd, capture_output=True, text=True):
            self.cmd = cmd
            return MagicMock(returncode=rc, stdout=stdout, stderr='')
        return run

    # Realistic jobsub_q DEFAULT-table shapes (captured 2026-07-21).
    # The -af JobStatus passthrough is unreliable on jobsub_lite (blank
    # values / dropped --user filter), so the probes parse this table.
    _HDR = ('JOBSUBJOBID                             OWNER       \t'
            'SUBMITTED     RUNTIME   ST PRIO   SIZE  COMMAND')
    _SUM = ('0 total; 0 completed, 0 removed, 0 idle, 0 running, '
            '0 held, 0 suspended')

    @staticmethod
    def _row(jobid, owner, st):
        return (f'{jobid}            {owner}   \t01/09 06:00   '
                f'0+00:00:00 {st}    0    0.0 job.sh ')

    def test_total_queued_counts_idle_and_running_only(self):
        from utils.submissions import total_queued
        table = '\n'.join([
            self._HDR, self._SUM,
            self._row('1.0@jobsub01.fnal.gov', 'mu2epro', 'I'),
            self._row('2.0@jobsub01.fnal.gov', 'mu2epro', 'R'),
            self._row('3.0@jobsub02.fnal.gov', 'mu2epro', 'R'),
            self._row('4.0@jobsub02.fnal.gov', '|-WORKER_12', 'H'),
            self._row('5.0@jobsub02.fnal.gov', 'mu2epro', 'X'),
        ]) + '\n'
        # USER pinned: the query follows the SUBMITTING identity now
        # (see queue_owner), and the ksu block exports USER=mu2epro, so
        # this is what production still asks for.
        with patch.dict(os.environ, {'USER': 'mu2epro'}):
            n = total_queued(runner=self._runner(table))
        self.assertEqual(n, 3)              # held / removed excluded
        self.assertEqual(self.cmd, ['jobsub_q', '--user', 'mu2epro'])

    def test_total_queued_empty_table_is_zero(self):
        from utils.submissions import total_queued
        table = self._HDR + '\n' + self._SUM + '\n'
        self.assertEqual(total_queued(runner=self._runner(table)), 0)

    def test_total_queued_headerless_is_none(self):
        # No JOBSUBJOBID header = we did not get the table (error page,
        # empty stdout, or the -af blank-line failure mode) — fail closed.
        from utils.submissions import total_queued
        self.assertIsNone(total_queued(runner=self._runner('')))
        self.assertIsNone(total_queued(runner=self._runner('\n\n\n')))

    def test_total_queued_failure_is_none(self):
        from utils.submissions import total_queued
        self.assertIsNone(total_queued(runner=self._runner('', rc=1)))

    def test_total_queued_garbage_row_is_none(self):
        from utils.submissions import total_queued
        table = '\n'.join([self._HDR,
                           self._row('1.0@jobsub01.fnal.gov', 'mu2epro',
                                     'I'),
                           'some unexpected diagnostic line']) + '\n'
        self.assertIsNone(total_queued(runner=self._runner(table)))

    def test_total_queued_unknown_state_is_none(self):
        from utils.submissions import total_queued
        table = '\n'.join([self._HDR,
                           self._row('1.0@jobsub01.fnal.gov', 'mu2epro',
                                     'Z')]) + '\n'
        self.assertIsNone(total_queued(runner=self._runner(table)))

    def test_total_queued_skips_token_noise(self):
        from utils.submissions import total_queued
        table = '\n'.join([
            'Attempting to get token from https://htvaultprod ... ok',
            'Storing bearer token in /tmp/bt_token_mu2e_x',
            self._HDR, self._SUM,
            self._row('1.0@jobsub01.fnal.gov', 'mu2epro', 'I'),
        ]) + '\n'
        self.assertEqual(total_queued(runner=self._runner(table)), 1)


class TestTopUp(unittest.TestCase):
    """Slice feeding: cap gate, whole slices, round-robin, pause."""

    def setUp(self):
        import tempfile
        from utils import submission_ledger as sl
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'submissions.db')
        self.calls = []

    def _campaign(self, tarball='cnf.mu2e.A.C.0.tar', njobs=10, slice=4):
        entry = {'tarball': tarball, 'njobs': njobs, 'inloc': 'tape',
                 'outputs': []}
        return self.sl.create_campaign(self.db, tarball=tarball,
                                       entry=entry, slice_size=slice)

    def _submit(self, ok=True):
        def fn(camp, n, db_path):
            self.calls.append((camp['id'], camp['cursor'], n))
            return ok
        return fn

    def test_feeds_until_complete(self):
        from utils.submissions import top_up
        cid = self._campaign(njobs=10, slice=4)
        s = top_up(self.db, cap=100, count_fn=lambda: 0,
                   submit_fn=self._submit())
        self.assertEqual(self.calls, [(cid, 0, 4), (cid, 4, 4), (cid, 8, 2)])
        self.assertEqual(s['slice'], 3)
        self.assertEqual(s['campaign-complete'], 1)
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['state'],
                         'complete')

    def test_cap_stops_whole_slice(self):
        from utils.submissions import top_up
        self._campaign(njobs=10, slice=4)
        s = top_up(self.db, cap=100, count_fn=lambda: 97,
                   submit_fn=self._submit())
        self.assertEqual(self.calls, [])            # 97+4 > 100: wait
        self.assertEqual(s['cap-wait'], 1)
        self.assertEqual(self.sl.active_campaigns(self.db)[0]['cursor'], 0)

    def test_cap_exact_fit_submits(self):
        from utils.submissions import top_up
        self._campaign(njobs=4, slice=4)
        top_up(self.db, cap=100, count_fn=lambda: 96,
               submit_fn=self._submit())
        self.assertEqual(len(self.calls), 1)        # 96+4 == 100 fits

    def test_submitted_slices_consume_headroom(self):
        from utils.submissions import top_up
        self._campaign(njobs=10, slice=4)
        s = top_up(self.db, cap=8, count_fn=lambda: 0,
                   submit_fn=self._submit())
        self.assertEqual(len(self.calls), 2)        # 0+4, 4+4; 8+2 > 8 waits
        self.assertEqual(s['cap-wait'], 1)

    def test_failure_pauses_without_advancing(self):
        from utils.submissions import top_up
        cid = self._campaign()
        s = top_up(self.db, cap=100, count_fn=lambda: 0,
                   submit_fn=self._submit(ok=False))
        c = self.sl.all_campaigns(self.db)[0]
        self.assertEqual(c['state'], 'paused')
        self.assertEqual(c['cursor'], 0)
        self.assertEqual(s['campaign-paused'], 1)

    def test_round_robin_two_campaigns(self):
        from utils.submissions import top_up
        a = self._campaign(tarball='cnf.mu2e.A.C.0.tar', njobs=4, slice=2)
        b = self._campaign(tarball='cnf.mu2e.B.C.0.tar', njobs=2, slice=2)
        top_up(self.db, cap=100, count_fn=lambda: 0,
               submit_fn=self._submit())
        self.assertEqual(self.calls,
                         [(a, 0, 2), (b, 0, 2), (a, 2, 2)])

    def test_campaign_filter_ticks_only_the_named_one(self):
        from utils.submissions import top_up
        a = self._campaign(tarball='cnf.mu2e.A.C.0.tar', njobs=4, slice=2)
        b = self._campaign(tarball='cnf.mu2e.B.C.0.tar', njobs=4, slice=2)
        top_up(self.db, cap=100, count_fn=lambda: 0,
               submit_fn=self._submit(), only_campaign=b)
        self.assertEqual(self.calls, [(b, 0, 2), (b, 2, 2)])
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['cursor'], 0)

    def test_campaign_filter_absent_ticks_every_active_campaign(self):
        # Omitting --campaign/only_campaign must keep ticking every
        # active campaign — the production cron calls `submissions run`
        # with no filter and must not be scoped down by this change.
        from utils.submissions import top_up
        a = self._campaign(tarball='cnf.mu2e.A.C.0.tar', njobs=2, slice=2)
        b = self._campaign(tarball='cnf.mu2e.B.C.0.tar', njobs=2, slice=2)
        top_up(self.db, cap=100, count_fn=lambda: 0, submit_fn=self._submit())
        self.assertEqual({c for c, _, _ in self.calls}, {a, b})

    def test_no_campaigns_skips_count(self):
        from utils.submissions import top_up
        def boom():
            raise AssertionError("count_fn must not be called")
        self.assertEqual(top_up(self.db, cap=100, count_fn=boom), {})

    def test_count_failure_skips_topup(self):
        from utils.submissions import top_up
        self._campaign()
        s = top_up(self.db, cap=100, count_fn=lambda: None,
                   submit_fn=self._submit())
        self.assertEqual(self.calls, [])
        self.assertEqual(s['count-error'], 1)

    def test_dry_run_reports_and_writes_nothing(self):
        from utils.submissions import top_up
        def boom(camp, n, db_path):
            raise AssertionError("submit_fn must not be called in dry-run")
        self._campaign(njobs=10, slice=4)
        s = top_up(self.db, cap=100, dry_run=True, count_fn=lambda: 0,
                   submit_fn=boom)
        self.assertEqual(s['would-slice'], 3)
        self.assertEqual(s['would-campaign-complete'], 1)
        c = self.sl.active_campaigns(self.db)[0]
        self.assertEqual(c['cursor'], 0)            # DB untouched
        self.assertEqual(c['state'], 'active')

    # — crash-window / ledger-overlap guard (Fix 2) -----------------------

    def test_overlap_pauses_without_submitting(self):
        """A ledger row already covering part of the next slice window
        (crash-window: parent submit_map died after jobsub_submit
        succeeded but before its own ledger write) must pause the
        campaign rather than resubmit — never a blind double-submit."""
        from utils.submissions import top_up
        tarball = 'cnf.mu2e.A.C.0.tar'
        cid = self._campaign(tarball=tarball, njobs=10, slice=4)
        self.sl.record_submission(
            self.db, tarball=tarball, entry={}, indices=[2],
            jobsub_id='9.0@js', cluster_id='9')  # inside [0,4)
        s = top_up(self.db, cap=100, count_fn=lambda: 0,
                   submit_fn=self._submit())
        self.assertEqual(self.calls, [])
        c = self.sl.all_campaigns(self.db)[0]
        self.assertEqual(c['state'], 'paused')
        self.assertEqual(c['cursor'], 0)
        self.assertIn('crash-window', c['note'])
        self.assertEqual(s['campaign-paused'], 1)

    def test_overlap_pause_note_names_the_blocking_row_and_the_fix(self):
        # The note used to say "reconcile cursor manually", which is
        # wrong twice: it is the ROW that blocks, not the cursor, and
        # there was no verb that could clear it. It must name the row id
        # and the reconcile verb, or the operator loops on `resume`.
        from utils.submissions import top_up
        tarball = 'cnf.mu2e.A.C.0.tar'
        rid = self.sl.reserve_submission(
            self.db, tarball=tarball, entry={}, indices=[1],
            origin='m.json')
        self.sl.fail_reservation(self.db, rid, 'submit failed')
        self._campaign(tarball=tarball, njobs=10, slice=4)
        top_up(self.db, cap=100, count_fn=lambda: 0,
               submit_fn=self._submit())
        note = self.sl.all_campaigns(self.db)[0]['note']
        self.assertIn(f'row {rid}', note)
        self.assertIn(f'submissions reconcile {rid}', note)

    def test_overlap_below_cursor_does_not_block(self):
        """Ledger rows for the same tarball covering only windows BELOW
        the cursor (e.g. the recovery loop's own resubmits of already-
        submitted-but-missing indices) must not block a future slice —
        those indices can never intersect [cursor, cursor+n)."""
        from utils.submissions import top_up
        tarball = 'cnf.mu2e.A.C.0.tar'
        cid = self._campaign(tarball=tarball, njobs=10, slice=4)
        self.sl.advance_campaign(self.db, cid, 4)  # simulate prior slice
        self.sl.record_submission(
            self.db, tarball=tarball, entry={}, indices=[0, 1, 2, 3],
            jobsub_id='9.0@js', cluster_id='9')  # all below cursor=4
        s = top_up(self.db, cap=100, count_fn=lambda: 0,
                   submit_fn=self._submit())
        self.assertEqual(self.calls, [(cid, 4, 4), (cid, 8, 2)])
        self.assertNotIn('campaign-paused', s)
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['state'],
                         'complete')

    def test_overlap_dry_run_reports_and_writes_nothing(self):
        from utils.submissions import top_up
        def boom(camp, n, db_path):
            raise AssertionError("submit_fn must not be called in dry-run")
        tarball = 'cnf.mu2e.A.C.0.tar'
        self._campaign(tarball=tarball, njobs=10, slice=4)
        self.sl.record_submission(
            self.db, tarball=tarball, entry={}, indices=[1],
            jobsub_id='9.0@js', cluster_id='9')  # inside [0,4)
        s = top_up(self.db, cap=100, dry_run=True, count_fn=lambda: 0,
                   submit_fn=boom)
        self.assertEqual(s['would-pause-overlap'], 1)
        self.assertNotIn('would-slice', s)
        c = self.sl.active_campaigns(self.db)[0]
        self.assertEqual(c['cursor'], 0)            # DB untouched
        self.assertEqual(c['state'], 'active')

    # — self-heal fully-submitted-but-unclosed campaigns (Fix 3) ----------

    def test_self_heal_closes_stuck_complete_campaign(self):
        """cursor == njobs but state still 'active' (crash between
        advance_campaign and set_campaign_state('complete') on a prior
        tick) must self-heal to 'complete', not stay stuck forever."""
        from utils.submissions import top_up
        cid = self._campaign(njobs=6, slice=4)
        self.sl.advance_campaign(self.db, cid, 6)  # fully submitted already
        s = top_up(self.db, cap=100, count_fn=lambda: 0,
                   submit_fn=self._submit())
        self.assertEqual(self.calls, [])            # nothing left to submit
        c = self.sl.all_campaigns(self.db)[0]
        self.assertEqual(c['state'], 'complete')
        self.assertIn('self-heal', c['note'])
        self.assertEqual(s['campaign-complete'], 1)

    def test_self_heal_dry_run_leaves_active(self):
        from utils.submissions import top_up
        cid = self._campaign(njobs=6, slice=4)
        self.sl.advance_campaign(self.db, cid, 6)
        s = top_up(self.db, cap=100, dry_run=True, count_fn=lambda: 0,
                   submit_fn=lambda *a: self.fail('must not submit'))
        self.assertEqual(s['would-campaign-complete'], 1)
        c = self.sl.active_campaigns(self.db)[0]
        self.assertEqual(c['state'], 'active')       # DB untouched


class TestSubmitSlice(unittest.TestCase):
    """submit_slice calls submit_entry in-process via SubmitOptions."""

    def test_options_and_entry_content(self):
        from utils import submissions as recover
        from utils import submit
        from utils import submission_ledger as sl
        import tempfile
        db = os.path.join(_mkdtemp(), 'led.db')
        entry = {'tarball': 'cnf.mu2e.W.C.0.tar', 'njobs': 50,
                 'firstjob': 100, 'inloc': 'tape', 'outputs': [],
                 'memory': '4000MB'}
        camp = {'id': 7, 'cursor': 10, 'slice_size': 5, 'entry': entry,
                'tarball': entry['tarball']}
        captured = {}
        def submit_fn(e, idx, options):
            captured['entry'] = e
            captured['idx'] = idx
            captured['options'] = options
            # A real submit_entry success leaves a new ACTIVE ledger
            # row behind (reserve -> jobsub_submit -> attach_cluster);
            # submit_slice now requires that evidence, not merely a
            # non-raising call (Fix A).
            sl.record_submission(db, tarball=entry['tarball'], entry=e,
                                 indices=[110, 111, 112, 113, 114],
                                 jobsub_id='1.0@js', cluster_id='1')
        ok = recover.submit_slice(camp, 5, db, submit_fn=submit_fn)
        self.assertTrue(ok)
        options = captured['options']
        self.assertIsInstance(options, submit.SubmitOptions)
        self.assertEqual(options.first, 10)
        self.assertEqual(options.num, 5)
        self.assertEqual(options.ledger_db, db)
        self.assertEqual(options.origin, 'campaign 7')
        self.assertEqual(captured['idx'], 0)
        # entry ships VERBATIM — firstjob preserved, exactly like a
        # manual windowed submission.
        self.assertEqual(captured['entry'], entry)

    def test_raising_submit_fn_is_failure(self):
        from utils import submissions as recover
        camp = {'id': 1, 'cursor': 0, 'slice_size': 2, 'tarball': 't',
                'entry': {'tarball': 't', 'njobs': 2}}
        def boom(entry, idx, options):
            raise RuntimeError('jobsub exploded')
        ok = recover.submit_slice(camp, 2, '/tmp/led.db', submit_fn=boom)
        self.assertFalse(ok)


class TestManageCampaign(unittest.TestCase):
    def setUp(self):
        import tempfile
        from utils import submission_ledger as sl
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'submissions.db')
        self.cid = sl.create_campaign(
            self.db, tarball='cnf.mu2e.M.C.0.tar',
            entry={'tarball': 'cnf.mu2e.M.C.0.tar', 'njobs': 5},
            slice_size=2)

    def test_pause_resume_cancel(self):
        from utils.submissions import manage_campaign
        manage_campaign(self.db, self.cid, 'pause')
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['state'], 'paused')
        manage_campaign(self.db, self.cid, 'resume')
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['state'], 'active')
        manage_campaign(self.db, self.cid, 'cancel')
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['state'],
                         'cancelled')

    def test_resume_active_raises(self):
        from utils.submissions import manage_campaign
        with self.assertRaises(ValueError):
            manage_campaign(self.db, self.cid, 'resume')

    # --- cancel --close-rows -------------------------------------------
    # Cancelling stops the campaign expanding; the rows it already
    # dispatched keep getting recovered. --close-rows is the other
    # intent -- abandon the round -- and it has to be typed out.

    def _row(self, indices, tarball=None):
        return self.sl.record_submission(
            self.db, tarball=tarball or 'cnf.mu2e.M.C.0.tar',
            entry={'tarball': tarball or 'cnf.mu2e.M.C.0.tar', 'njobs': 5},
            indices=indices, jobsub_id='1.0@js', cluster_id='1')

    def _state(self, row_id):
        return self.sl.row_by_id(self.db, row_id)['state']

    def test_bare_cancel_leaves_rows_active(self):
        from utils.submissions import manage_campaign
        rid = self._row([0, 1])
        manage_campaign(self.db, self.cid, 'cancel')
        self.assertEqual(self._state(rid), 'active')

    def test_close_rows_exhausts_open_rows_with_the_note(self):
        from utils.submissions import manage_campaign
        a, b = self._row([0, 1]), self._row([2, 3])
        closed = manage_campaign(self.db, self.cid, 'cancel',
                                 note='killed by hand', close_rows=True)
        self.assertEqual(closed, [a, b])
        self.assertEqual(self._state(a), 'exhausted')
        self.assertEqual(self._state(b), 'exhausted')
        self.assertEqual(self.sl.row_by_id(self.db, a)['note'],
                         'killed by hand')
        camp = self.sl.all_campaigns(self.db)[0]
        self.assertEqual(camp['state'], 'cancelled')
        self.assertEqual(camp['note'], 'killed by hand')

    def test_close_rows_leaves_closed_rows_and_other_tarballs_alone(self):
        from utils.submissions import manage_campaign
        done = self._row([0, 1])
        self.sl.close_row(self.db, done, 'complete')
        other_cid = self.sl.create_campaign(
            self.db, tarball='cnf.mu2e.OTHER.C.0.tar',
            entry={'tarball': 'cnf.mu2e.OTHER.C.0.tar', 'njobs': 5},
            slice_size=2)
        other = self._row([0, 1], tarball='cnf.mu2e.OTHER.C.0.tar')
        mine = self._row([4])
        closed = manage_campaign(self.db, self.cid, 'cancel',
                                 close_rows=True)
        self.assertEqual(closed, [mine])
        self.assertEqual(self._state(done), 'complete')
        self.assertEqual(self._state(other), 'active')
        self.assertEqual(
            [c['state'] for c in self.sl.all_campaigns(self.db)
             if c['id'] == other_cid], ['active'])

    def test_close_rows_refuses_while_a_row_is_submitting(self):
        from utils.submissions import manage_campaign
        rid = self._row([0, 1])
        self.sl.reserve_submission(
            self.db, tarball='cnf.mu2e.M.C.0.tar',
            entry={'tarball': 'cnf.mu2e.M.C.0.tar', 'njobs': 5},
            indices=[2, 3])
        with self.assertRaises(ValueError) as cm:
            manage_campaign(self.db, self.cid, 'cancel', close_rows=True)
        self.assertIn('submitting', str(cm.exception))
        # nothing written: the campaign is untouched too
        self.assertEqual(self._state(rid), 'active')
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['state'], 'active')

    def test_close_rows_works_on_an_already_cancelled_campaign(self):
        from utils.submissions import manage_campaign
        rid = self._row([0, 1])
        manage_campaign(self.db, self.cid, 'cancel')
        closed = manage_campaign(self.db, self.cid, 'cancel',
                                 close_rows=True)
        self.assertEqual(closed, [rid])
        self.assertEqual(self._state(rid), 'exhausted')

    def test_close_rows_works_on_a_complete_campaign(self):
        """A fully submitted campaign is 'complete' while its rows still
        verify, and complete -> cancelled is not a ledger transition. The
        flag must still close the rows, leaving the campaign complete."""
        from utils.submissions import manage_campaign
        rid = self._row([0, 1])
        self.sl.set_campaign_state(self.db, self.cid, 'complete')
        closed = manage_campaign(self.db, self.cid, 'cancel',
                                 note='deck bug; not recovering',
                                 close_rows=True)
        self.assertEqual(closed, [rid])
        self.assertEqual(self._state(rid), 'exhausted')
        self.assertEqual(self.sl.row_by_id(self.db, rid)['note'],
                         'deck bug; not recovering')
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['state'],
                         'complete')

    def test_close_rows_only_applies_to_cancel(self):
        from utils.submissions import manage_campaign
        with self.assertRaises(ValueError):
            manage_campaign(self.db, self.cid, 'pause', close_rows=True)


# ---------------------------------------------------------------------------
# submissions CLI verb structure (utils/submissions.py) — workflow hardening
# ---------------------------------------------------------------------------
class TestSubmissionsVerbs(unittest.TestCase):
    """Safe-by-default CLI: bare invocation is read-only status; the
    mutating tick requires the `run` verb; campaign management verbs
    validate transitions and fail with one-line errors."""

    def setUp(self):
        import tempfile
        from utils import submission_ledger as sl
        self.sl = sl
        self.dbdir = _mkdtemp()
        self.db = os.path.join(self.dbdir, 'sub.db')

    def _campaign(self, tarball='cnf.mu2e.V.C.0.tar', njobs=4):
        return self.sl.create_campaign(
            self.db, tarball=tarball,
            entry={'tarball': tarball, 'njobs': njobs},
            slice_size=2, origin='m.json')

    def test_bare_invocation_is_status(self):
        from utils import submissions
        import io as _io
        self.sl.record_submission(
            self.db, tarball='cnf.mu2e.V.C.0.tar', entry={}, indices=[0],
            jobsub_id='1.0@js', cluster_id='1')
        buf = _io.StringIO()
        with patch('sys.stdout', buf), \
             patch.object(submissions, 'process_row',
                          side_effect=AssertionError('bare must not run')), \
             patch.object(submissions, 'top_up',
                          side_effect=AssertionError('bare must not top up')), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db]):
            submissions.main()
        out = buf.getvalue()
        self.assertIn('queue cap in effect', out)
        self.assertIn('cnf.mu2e.V.C.0.tar', out)
        # read-only: no lock file created
        self.assertFalse(
            os.path.exists(os.path.join(self.dbdir, 'submissions.lock')))

    def test_status_verb_same_as_bare(self):
        from utils import submissions
        import io as _io
        buf = _io.StringIO()
        with patch('sys.stdout', buf), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'status']):
            submissions.main()
        self.assertIn('empty', buf.getvalue().lower())

    def test_run_verb_processes_rows_and_locks(self):
        from utils import submissions
        self.sl.record_submission(
            self.db, tarball='t', entry={}, indices=[0],
            jobsub_id='1.0@js', cluster_id='1')
        with patch.object(submissions, 'process_row',
                          return_value='complete') as pr, \
             patch.object(submissions, 'live_clusters', return_value={}), \
             patch.object(submissions, 'top_up', return_value={}), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run']):
            submissions.main()
        self.assertEqual(pr.call_count, 1)
        self.assertTrue(
            os.path.exists(os.path.join(self.dbdir, 'submissions.lock')))

    def test_run_dry_run_takes_no_lock(self):
        from utils import submissions
        with patch.object(submissions, 'top_up', return_value={}), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run', '--dry-run']):
            submissions.main()
        self.assertFalse(
            os.path.exists(os.path.join(self.dbdir, 'submissions.lock')))

    def test_pause_and_resume_verbs(self):
        from utils import submissions
        cid = self._campaign()
        with patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'pause', str(cid)]):
            submissions.main()
        camp = self.sl.all_campaigns(self.db)[0]
        self.assertEqual(camp['state'], 'paused')
        with patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'resume', str(cid)]):
            submissions.main()
        camp = self.sl.all_campaigns(self.db)[0]
        self.assertEqual(camp['state'], 'active')

    def test_cancel_verb(self):
        from utils import submissions
        cid = self._campaign()
        with patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'cancel', str(cid)]):
            submissions.main()
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['state'],
                         'cancelled')

    def test_reconcile_verb_unblocks_a_deadlocked_campaign(self):
        # The whole deadlock, end to end through the CLI: a failed
        # submit leaves a 'failed' row over [0,2), top_up pauses, and
        # `resume` alone would be re-paused on the next tick because the
        # ROW still overlaps. `reconcile` is the only escape.
        from utils import submissions
        cid = self._campaign()
        rid = self.sl.reserve_submission(
            self.db, tarball='cnf.mu2e.V.C.0.tar',
            entry={'tarball': 'cnf.mu2e.V.C.0.tar', 'njobs': 4},
            indices=[0, 1], origin='m.json')
        self.sl.fail_reservation(self.db, rid, 'jobsub_submit returned 1')
        self.assertTrue(submissions._slice_overlaps_ledger(
            self.db, 'cnf.mu2e.V.C.0.tar', 0, 0, 2))
        with patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'reconcile', str(rid)]):
            submissions.main()
        self.assertEqual(self.sl.all_rows(self.db)[0]['state'], 'reconciled')
        self.assertFalse(submissions._slice_overlaps_ledger(
            self.db, 'cnf.mu2e.V.C.0.tar', 0, 0, 2))
        # It does not touch the campaign: resume is still the operator's
        # own, separate decision.
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['state'],
                         'active')
        self.assertEqual(self.sl.all_campaigns(self.db)[0]['id'], cid)

    def test_reconcile_verb_help_states_what_the_operator_asserts(self):
        # The safety property is carried by the human running it: the
        # help must say they are asserting the window's jobs are gone
        # from the queue, since nothing else can check that.
        from utils import submissions
        import io as _io
        buf = _io.StringIO()
        with patch('sys.stdout', buf), \
             patch.object(sys, 'argv', ['submissions', 'reconcile', '--help']):
            with self.assertRaises(SystemExit):
                submissions.main()
        text = ' '.join(buf.getvalue().split())
        self.assertIn('jobsub_q', text)
        self.assertIn('genuinely absent from the queue', text)

    def test_reconcile_verb_bad_row_exits_one_line(self):
        from utils import submissions
        with patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'reconcile', '404']):
            with self.assertRaises(SystemExit) as cm:
                submissions.main()
        self.assertIn('submissions:', str(cm.exception.code))

    def test_invalid_transition_one_line_exit_1(self):
        from utils import submissions
        cid = self._campaign()
        self.sl.set_campaign_state(self.db, cid, 'cancelled')
        with patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'resume', str(cid)]):
            with self.assertRaises(SystemExit) as cm:
                submissions.main()
        msg = str(cm.exception.code)
        self.assertIn('submissions:', msg)
        self.assertNotIn('\n', msg)

    def test_old_style_flags_rejected(self):
        from utils import submissions
        for bad in (['--status'], ['--dry-run'], ['--pause-campaign', '1']):
            with patch.object(sys, 'argv',
                              ['submissions', '--db', self.db] + bad):
                with self.assertRaises(SystemExit) as cm:
                    submissions.main()
            self.assertNotEqual(cm.exception.code, 0)


# ---------------------------------------------------------------------------
# submit_map ledger hook (utils/submit.py) — direct-backend recovery
# ---------------------------------------------------------------------------
class TestSubmitLedgerHook(unittest.TestCase):
    """Direct-backend ledger hook in utils/submit.py."""

    def test_parse_jobsub_id_full_form(self):
        from utils.submit import _parse_jobsub_id
        out = ("Transferring files...\n"
               "1 job(s) submitted to cluster 12345678.\n"
               "Use job id 12345678.0@jobsub03.fnal.gov to retrieve output\n")
        self.assertEqual(_parse_jobsub_id(out),
                         '12345678.0@jobsub03.fnal.gov')

    def test_parse_jobsub_id_absent(self):
        from utils.submit import _parse_jobsub_id
        self.assertIsNone(_parse_jobsub_id("submitted to cluster 12345678\n"))

    def test_run_submit_carries_jobsub_id(self):
        from utils import submit
        fake = MagicMock(
            returncode=0, stderr='',
            stdout='1 job(s) submitted to cluster 12345678.\n'
                   'Use job id 12345678.0@jobsub03.fnal.gov to retrieve output\n')
        with patch('utils.submit.subprocess.run', return_value=fake):
            r = submit._run_submit(['jobsub_submit'], 'cnf.tar', 3)
        self.assertEqual(r['status'], 'submitted')
        self.assertEqual(r['jobsub_id'], '12345678.0@jobsub03.fnal.gov')

    def _opts(self, db, parent=None):
        from utils.submit import SubmitOptions
        return SubmitOptions(ledger_db=db, ledger_parent=parent,
                             origin='/tmp/m.json')

    def test_reserve_then_attach_absolute_indices(self):
        import tempfile
        from utils import submit, submission_ledger
        db = os.path.join(_mkdtemp(), 'sub.db')
        entry = {'tarball': 'cnf.mu2e.T.C.0.tar', 'njobs': 3, 'firstjob': 100}
        rid = submit._reserve_in_ledger(entry, 100, [0, 1, 2],
                                        self._opts(db))
        result = {'tarball': 'cnf.mu2e.T.C.0.tar', 'cluster_id': '1',
                  'jobsub_id': '1.0@js.fnal.gov', 'njobs': 3,
                  'status': 'submitted'}
        submit._attach_cluster(rid, result, self._opts(db))
        row = submission_ledger.open_rows(db)[0]
        self.assertEqual(row['indices'], [100, 101, 102])
        self.assertEqual(row['entry'], entry)
        self.assertEqual(row['jobsub_id'], '1.0@js.fnal.gov')
        self.assertEqual(row['origin'], '/tmp/m.json')

    def test_reserve_then_attach_parent_chains(self):
        import tempfile
        from utils import submit, submission_ledger
        db = os.path.join(_mkdtemp(), 'sub.db')
        rid = submission_ledger.record_submission(
            db, tarball='t', entry={}, indices=[0, 1],
            jobsub_id='1.0@js', cluster_id='1')
        child = submit._reserve_in_ledger({'tarball': 't'}, 0, [1],
                                          self._opts(db, parent=rid))
        result = {'tarball': 't', 'cluster_id': '2', 'jobsub_id': '2.0@js',
                  'njobs': 1, 'status': 'submitted'}
        submit._attach_cluster(child, result, self._opts(db, parent=rid))
        rows = submission_ledger.open_rows(db)
        self.assertEqual(rows[1]['attempt'], 2)
        self.assertEqual(rows[1]['parent_id'], rid)

    def test_reserve_failure_raises(self):
        # An unrecordable window must not be submitted — the caller
        # (_submit_one) reserves BEFORE jobsub_submit, so this raising
        # is what makes the whole submission fail closed.
        from utils import submit
        with self.assertRaises(Exception):
            submit._reserve_in_ledger(
                {}, 0, [0],
                self._opts('/nonexistent-dir-recovery-test/s.db'))

    def test_attach_failure_does_not_raise(self):
        # By the time attach runs, the submission already happened —
        # a ledger problem here must only warn.
        from utils import submit
        result = {'tarball': 't', 'cluster_id': '1', 'jobsub_id': None,
                  'njobs': 1, 'status': 'submitted'}
        submit._attach_cluster(
            1, result, self._opts('/nonexistent-dir-recovery-test/s.db'))

    def test_fail_reservation_does_not_raise(self):
        from utils import submit
        result = {'status': 'failed', 'cluster_id': None}
        submit._fail_reservation(
            1, result, self._opts('/nonexistent-dir-recovery-test/s.db'))

    def test_none_row_id_is_a_noop(self):
        # A caller can still hand a None row_id (e.g. a reservation
        # step it chose to skip), and both closing calls must tolerate
        # that without touching the DB.
        from utils import submit
        opts = self._opts('/nonexistent-dir-recovery-test/s.db')
        submit._attach_cluster(None, {'cluster_id': '1'}, opts)
        submit._fail_reservation(None, {'status': 'failed'}, opts)


class TestSubmitReservesBeforeSubmitting(unittest.TestCase):
    """Ordering is the whole contract: the row must exist while
    jobsub_submit is in flight."""

    def setUp(self):
        from utils import submit, submission_ledger as sl
        from utils.submit import SubmitOptions
        self.submit = submit
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'submissions.db')
        self.entry = {'tarball': 'cnf.mu2e.TestDesc.TestConf.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                      'njobs': 5, 'inloc': 'tape',
                      'outputs': [{'location': 'tape'}]}
        self.opts = SubmitOptions(ledger_db=self.db, origin='/tmp/map.json',
                                  ledger_parent=None)

    def test_row_exists_and_is_reserved_during_submit(self):
        seen = {}

        def fake_run_submit(*a, **kw):
            rows = self.sl.all_rows(self.db)
            seen['states'] = [r['state'] for r in rows]
            seen['indices'] = rows[0]['indices'] if rows else None
            return {'status': 'submitted', 'cluster_id': '4242',
                    'jobsub_id': '4242.0@jobsub03.fnal.gov',
                    'tarball': self.entry['tarball'], 'njobs': 3}

        rid = self.submit._reserve_in_ledger(
            self.entry, 0, [0, 1, 2], self.opts)
        result = fake_run_submit()
        self.assertEqual(seen['states'], ['submitting'])
        self.assertEqual(seen['indices'], [0, 1, 2])

        self.submit._attach_cluster(rid, result, self.opts)
        self.assertEqual(self.sl.open_rows(self.db)[0]['cluster_id'], '4242')

    def test_failed_submit_marks_the_reservation_failed(self):
        rid = self.submit._reserve_in_ledger(
            self.entry, 0, [0, 1, 2], self.opts)
        self.submit._fail_reservation(
            rid, {'status': 'failed', 'cluster_id': None}, self.opts)
        self.assertEqual(self.sl.all_rows(self.db)[0]['state'], 'failed')

    def test_unwritable_ledger_raises_before_any_submit(self):
        self.opts = self.opts._replace(ledger_db='/proc/nope/submissions.db')
        with self.assertRaises(Exception):
            self.submit._reserve_in_ledger(self.entry, 0, [0, 1, 2], self.opts)


class TestDirectPathPreflight(unittest.TestCase):
    def setUp(self):
        from utils import submit
        self.submit = submit
        self.entry = {'tarball': 'cnf.mu2e.TestDesc.TestConf.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                      'njobs': 5, 'inloc': 'tape',
                      'outputs': [{'location': 'tape'}]}

    def test_direct_submit_refuses_on_bad_inputs(self):
        from utils.check_inputs import Problem
        bad = [Problem(dataset='dts.mu2e.X.Y.art', filename='x.art',
                       kind='missing', detail='0 files')]
        with patch('utils.submit.check_code_tarball',
                   return_value=(True, [])), \
             patch('utils.submit.check_inputs', return_value=(False, bad)):
            ok, problems = self.submit._preflight_inputs(
                self.entry, '/tmp/cnf.mu2e.TestDesc.TestConf.0.tar')
        self.assertFalse(ok)
        self.assertEqual(problems, bad)

    def test_direct_submit_passes_on_good_inputs(self):
        with patch('utils.submit.check_code_tarball',
                   return_value=(True, [])), \
             patch('utils.submit.check_inputs', return_value=(True, [])):
            ok, problems = self.submit._preflight_inputs(
                self.entry, '/tmp/cnf.mu2e.TestDesc.TestConf.0.tar')
        self.assertTrue(ok)

    def test_generic_cnf_skips_the_check(self):
        # A generic (direct-input) cnf bakes no inputs — there is
        # nothing to pre-flight, and calling check_inputs would fail.
        generic = dict(self.entry)
        generic['input_pattern'] = 'dig.mu2e.%OnSpill.X.art'
        with patch('utils.submit.check_code_tarball',
                   return_value=(True, [])), \
             patch('utils.submit.check_inputs') as chk:
            ok, problems = self.submit._preflight_inputs(
                generic, '/tmp/cnf.mu2e.TestDesc.TestConf.0.tar')
        self.assertTrue(ok)
        chk.assert_not_called()

    def test_bad_code_tarball_short_circuits_before_check_inputs(self):
        # check_code_tarball runs FIRST (above the draining early-return,
        # see _preflight_inputs' docstring) — a code mismatch must block
        # the submit even when check_inputs would have passed, and
        # check_inputs must never even be called.
        from utils.check_inputs import Problem
        bad = [Problem(dataset='code', filename='/some/Code.tar.bz2',
                       kind='code_mismatch', detail='sha256 does not match')]
        with patch('utils.submit.check_code_tarball',
                   return_value=(False, bad)), \
             patch('utils.submit.check_inputs') as chk:
            ok, problems = self.submit._preflight_inputs(
                self.entry, '/tmp/cnf.mu2e.TestDesc.TestConf.0.tar')
        self.assertFalse(ok)
        self.assertEqual(problems, bad)
        chk.assert_not_called()

    def test_bad_code_tarball_blocks_a_draining_entry(self):
        # The ordering case the previous test cannot cover: a draining
        # (generic) entry returns True before check_inputs is ever
        # consulted, so the code gate is the ONLY gate it has. Moving
        # check_code_tarball below the draining early-return fails here
        # and nowhere else.
        from utils.check_inputs import Problem
        draining = dict(self.entry)
        draining['input_pattern'] = 'dig.mu2e.%OnSpill.X.art'
        bad = [Problem(dataset='code', filename='/some/Code.tar.bz2',
                       kind='code_mismatch', detail='sha256 does not match')]
        with patch('utils.submit.check_code_tarball',
                   return_value=(False, bad)), \
             patch('utils.submit.check_inputs') as chk:
            ok, problems = self.submit._preflight_inputs(
                draining, '/tmp/cnf.mu2e.TestDesc.TestConf.0.tar')
        self.assertFalse(ok)
        self.assertEqual(problems, bad)
        chk.assert_not_called()

    def test_musing_entry_real_cnf_passes_code_gate_unaffected(self):
        # The REAL check_code_tarball (not mocked) against a real cnf
        # tarball carrying no code_ref, and an entry carrying no 'code'
        # — the common Musing-entry case. Proves the code gate added
        # to _preflight_inputs costs nothing for the production path:
        # one cnf parse, immediate short-circuit, no sha256.
        tmpdir = _mkdtemp()
        cnf_path = os.path.join(tmpdir, 'cnf.mu2e.TestDesc.TestConf.0.tar')
        jobpars = {
            'code': '', 'setup': '/cvmfs/mu2e.opensciencegrid.org/'
                                  'Musings/SimJob/TestConf/setup.sh',
            'tbs': {},
        }
        blob = json.dumps(jobpars).encode()
        with tarfile.open(cnf_path, 'w') as tar:
            info = tarfile.TarInfo('jobpars.json')
            info.size = len(blob)
            tar.addfile(info, io.BytesIO(blob))

        with patch('utils.submit.check_inputs', return_value=(True, [])):
            ok, problems = self.submit._preflight_inputs(
                self.entry, cnf_path)
        self.assertTrue(ok)
        self.assertEqual(problems, [])


class TestSubmitResolveLedgerDb(unittest.TestCase):
    """utils/submit.py's writer-side counterpart to
    submissions.resolve_db: a DEFAULTED --ledger-db gets its directory
    created; an operator-supplied one never does."""

    def setUp(self):
        from utils import submit, submission_ledger as sl
        self.submit = submit
        self.sl = sl

    def test_defaulted_ledger_db_creates_its_directory(self):
        base = _mkdtemp()
        derived = os.path.join(base, 'someuser', 'prodtools',
                               'submissions.db')
        opts = SimpleNamespace(ledger_db=None)
        with patch.object(self.sl, 'ledger_for', return_value=derived):
            got = self.submit._resolve_ledger_db(opts)
        self.assertEqual(got, derived)
        self.assertTrue(os.path.isdir(os.path.dirname(derived)))

    def test_explicit_ledger_db_directory_is_never_created(self):
        base = _mkdtemp()
        explicit = os.path.join(base, 'no', 'such', 'dir',
                                'submissions.db')
        opts = SimpleNamespace(ledger_db=explicit)
        got = self.submit._resolve_ledger_db(opts)
        self.assertEqual(got, explicit)
        self.assertFalse(os.path.isdir(os.path.dirname(explicit)))


class TestSubmissionsRunCreatesFreshLedgerDir(unittest.TestCase):
    """A mutating verb's _acquire_lock does a bare open() for the lock
    file — if resolve_db hadn't already created the directory, `run`
    against a never-used personal ledger would crash there before
    reaching any ledger logic. Exercised via main(), not resolve_db in
    isolation, so the actual gap is covered."""

    def test_run_creates_directory_before_acquiring_the_lock(self):
        from utils import submissions
        base = _mkdtemp()
        derived = os.path.join(base, 'freshuser2', 'prodtools',
                               'submissions.db')
        with patch.object(submissions.submission_ledger, 'ledger_for',
                          return_value=derived), \
             patch.object(sys, 'argv', ['submissions', 'run']):
            submissions.main()
        self.assertTrue(os.path.isdir(os.path.dirname(derived)))


# ---------------------------------------------------------------------------
# 13. Scoped index scan (utils/jobdef_lookup.py)
# ---------------------------------------------------------------------------

class TestBuildFileMapsScoped(unittest.TestCase):
    def test_scoped_scan_matches_windowed_scan(self):
        from utils.jobquery import Mu2eJobPars
        from utils.jobdef_lookup import build_file_maps
        files = [f"sim.mu2e.In.C.00000000_{i:08d}.art" for i in range(6)]
        tar = _make_tarball(_root_input_jobpars(files))
        try:
            jp = Mu2eJobPars(tar)
            ds = 'sim.mu2e.TestDesc.TestConf.art'
            full = build_file_maps(jp, [ds], njobs=6)[ds]
            self.assertEqual(len(full), 6)
            scoped = build_file_maps(jp, [ds], njobs=0, indices=[1, 4])[ds]
            expect = {f: i for f, i in full.items() if i in (1, 4)}
            self.assertEqual(scoped, expect)
            self.assertEqual(sorted(set(scoped.values())), [1, 4])
        finally:
            os.unlink(tar)


class TestRecoverLoop(unittest.TestCase):
    """utils/submissions.py — drain gate, verify, cap semantics."""

    def setUp(self):
        import tempfile
        from utils import submission_ledger as sl
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'sub.db')
        self.entry = {'tarball': 'cnf.mu2e.T.C.0.tar', 'njobs': 3}
        self.rid = sl.record_submission(
            self.db, tarball='cnf.mu2e.T.C.0.tar', entry=self.entry,
            indices=[0, 1, 2], jobsub_id='1.0@js.fnal.gov', cluster_id='1')
        self.row = sl.open_rows(self.db)[0]

    def _process(self, qstate='drained', missing=(), partial=(),
                 resub_ok=True, max_attempts=3, dry_run=False,
                 verify_exc=None, resub_writes_child=True):
        from utils import submissions as recover
        calls = {}

        def fake_verify(row):
            if verify_exc:
                raise verify_exc
            return list(missing), list(partial)

        def fake_resubmit(row, miss, db_path):
            calls['resubmit'] = (row['id'], list(miss), db_path)
            if resub_ok and resub_writes_child:
                self.sl.record_submission(
                    db_path, tarball=row['tarball'], entry=row['entry'],
                    indices=list(miss), jobsub_id='2.0@js.fnal.gov',
                    cluster_id='2', parent_id=row['id'])
            return resub_ok

        action = recover.process_row(
            self.row, self.db, max_attempts, clusters={}, dry_run=dry_run,
            queue_state_fn=lambda cid, clusters: qstate,
            verify_fn=fake_verify, resubmit_fn=fake_resubmit)
        return action, calls

    def test_running_skips(self):
        action, calls = self._process(qstate='running')
        self.assertEqual(action, 'running')
        self.assertNotIn('resubmit', calls)
        self.assertEqual(self.sl.open_rows(self.db)[0]['state'], 'active')

    def test_held_reports_and_skips(self):
        action, calls = self._process(qstate='held')
        self.assertEqual(action, 'held')
        self.assertNotIn('resubmit', calls)
        self.assertEqual(self.sl.open_rows(self.db)[0]['state'], 'active')

    def test_queue_error_skips(self):
        action, _ = self._process(qstate='error')
        self.assertEqual(action, 'queue-error')
        self.assertEqual(self.sl.open_rows(self.db)[0]['state'], 'active')

    def test_complete_closes_row(self):
        action, _ = self._process(missing=())
        self.assertEqual(action, 'complete')
        self.assertEqual(self.sl.all_rows(self.db)[0]['state'], 'complete')

    def test_missing_resubmits_and_marks_recovered(self):
        action, calls = self._process(missing=(1,))
        self.assertEqual(action, 'resubmitted')
        self.assertEqual(calls['resubmit'], (self.rid, [1], self.db))
        rows = self.sl.all_rows(self.db)
        self.assertEqual(rows[0]['state'], 'recovered')
        self.assertEqual(rows[1]['state'], 'active')
        self.assertEqual(rows[1]['attempt'], 2)
        self.assertEqual(rows[1]['indices'], [1])

    def test_cap_exhausts_without_resubmit(self):
        action, calls = self._process(missing=(1,), max_attempts=1)
        self.assertEqual(action, 'exhausted')
        self.assertNotIn('resubmit', calls)
        self.assertEqual(self.sl.all_rows(self.db)[0]['state'], 'exhausted')

    def test_dry_run_never_submits(self):
        action, calls = self._process(missing=(1,), dry_run=True)
        self.assertEqual(action, 'would-resubmit')
        self.assertNotIn('resubmit', calls)
        self.assertEqual(self.sl.open_rows(self.db)[0]['state'], 'active')

    def test_dry_run_complete_keeps_row_active(self):
        action, calls = self._process(missing=(), dry_run=True)
        self.assertEqual(action, 'would-complete')
        self.assertEqual(self.sl.open_rows(self.db)[0]['state'], 'active')

    def test_dry_run_at_cap_keeps_row_active(self):
        action, calls = self._process(missing=(1,), max_attempts=1,
                                      dry_run=True)
        self.assertEqual(action, 'would-exhaust')
        self.assertNotIn('resubmit', calls)
        self.assertEqual(self.sl.open_rows(self.db)[0]['state'], 'active')

    def test_verify_error_keeps_row_active(self):
        action, _ = self._process(verify_exc=RuntimeError('no tarball'))
        self.assertEqual(action, 'verify-error')
        self.assertEqual(self.sl.open_rows(self.db)[0]['state'], 'active')

    def test_resubmit_failure_keeps_row_active(self):
        action, _ = self._process(missing=(1,), resub_ok=False)
        self.assertEqual(action, 'resubmit-error')
        self.assertEqual(self.sl.open_rows(self.db)[0]['state'], 'active')

    def test_crash_window_child_already_active_repairs(self):
        child = self.sl.record_submission(
            self.db, tarball='cnf.mu2e.T.C.0.tar', entry=self.entry,
            indices=[1], jobsub_id='2.0@js', cluster_id='2',
            parent_id=self.rid)
        action, calls = self._process(missing=(1,))
        self.assertEqual(action, 'child-active')
        self.assertNotIn('resubmit', calls)
        rows = {r['id']: r for r in self.sl.all_rows(self.db)}
        self.assertEqual(rows[self.rid]['state'], 'recovered')
        self.assertEqual(rows[child]['state'], 'active')

    def test_crash_window_dry_run_previews_repair(self):
        child = self.sl.record_submission(
            self.db, tarball='cnf.mu2e.T.C.0.tar', entry=self.entry,
            indices=[1], jobsub_id='2.0@js', cluster_id='2',
            parent_id=self.rid)
        action, calls = self._process(missing=(1,), dry_run=True)
        self.assertEqual(action, 'would-recover')
        self.assertNotIn('resubmit', calls)
        self.assertEqual(sorted(r['id'] for r in self.sl.open_rows(self.db)),
                         [self.rid, child])

    def test_queue_is_read_for_the_submitting_identity(self):
        # Measured 2026-08-09: live_clusters/total_queued defaulted to
        # user='mu2epro'. A self run queried PRODUCTION's queue, its own
        # live cluster was absent from that snapshot, and
        # cluster_queue_state reads absent as 'drained' — so a running
        # row was verified, found 2/2 outputs missing (its jobs had not
        # finished) and recovered mid-flight. Every tick would duplicate.
        from unittest import mock
        from utils import submissions as subs
        seen = []

        def fake_run(cmd, **kw):
            seen.append(cmd)
            return types.SimpleNamespace(returncode=0, stdout='')

        with mock.patch.dict(os.environ, {'USER': 'someuser'}):
            subs.live_clusters(runner=fake_run)
            subs.total_queued(runner=fake_run)
        for cmd in seen:
            self.assertEqual(cmd[cmd.index('--user') + 1], 'someuser')
        # Under ksu the block exports USER=mu2epro, so production is
        # unchanged by this generalization.
        with mock.patch.dict(os.environ, {'USER': 'mu2epro'}):
            self.assertEqual(subs.queue_owner(), 'mu2epro')

    def test_explicit_user_still_wins(self):
        from unittest import mock
        from utils import submissions as subs
        seen = []

        def fake_run(cmd, **kw):
            seen.append(cmd)
            return types.SimpleNamespace(returncode=0, stdout='')

        with mock.patch.dict(os.environ, {'USER': 'someuser'}):
            subs.live_clusters(user='mu2epro', runner=fake_run)
        self.assertEqual(seen[0][seen[0].index('--user') + 1], 'mu2epro')

    def test_reserved_child_blocks_the_resubmit(self):
        # Regression, measured 2026-08-09: killing `submissions run`
        # mid-recovery leaves the child in 'submitting'. open_rows()
        # selects state='active' only, so the orphan was invisible to
        # the crash-window repair and a SECOND child was cut for the
        # same indices. If the kill had landed after jobsub_submit
        # created the cluster, that is duplicate physics.
        child = self.sl.reserve_submission(
            self.db, tarball='cnf.mu2e.T.C.0.tar', entry=self.entry,
            indices=[1], parent_id=self.rid)
        action, calls = self._process(missing=(1,))
        self.assertEqual(action, 'child-reserved')
        self.assertNotIn('resubmit', calls)
        rows = {r['id']: r for r in self.sl.all_rows(self.db)}
        # Fail-closed: the parent is NOT closed (its window is unproven)
        # and no third row appeared.
        self.assertEqual(rows[self.rid]['state'], 'active')
        self.assertEqual(rows[child]['state'], 'submitting')
        self.assertEqual(len(rows), 2)

    def test_reserved_child_blocks_even_at_the_attempt_cap(self):
        # 'exhausted' would close the parent and stop watching it while
        # a possibly-live cluster is still unaccounted for.
        self.sl.reserve_submission(
            self.db, tarball='cnf.mu2e.T.C.0.tar', entry=self.entry,
            indices=[1], parent_id=self.rid)
        action, _ = self._process(missing=(1,), max_attempts=1)
        self.assertEqual(action, 'child-reserved')

    def test_reserved_child_is_an_attention_outcome(self):
        # Otherwise the tick exits 0 and a cron would never surface it.
        from utils.submissions import ATTENTION_KEYS
        self.assertIn('child-reserved', ATTENTION_KEYS)

    def test_child_active_wins_over_cap(self):
        self.sl.record_submission(
            self.db, tarball='cnf.mu2e.T.C.0.tar', entry=self.entry,
            indices=[1], jobsub_id='2.0@js', cluster_id='2',
            parent_id=self.rid)
        action, _ = self._process(missing=(1,), max_attempts=1)
        self.assertEqual(action, 'child-active')

    def test_resubmit_without_child_row_flags_unwatched(self):
        action, calls = self._process(missing=(1,), resub_writes_child=False)
        self.assertEqual(action, 'child-missing')
        rows = self.sl.all_rows(self.db)
        self.assertEqual(rows[0]['state'], 'recovered')
        self.assertIn('unwatched', rows[0]['note'])

    def test_missing_cluster_id_reported(self):
        rid2 = self.sl.record_submission(
            self.db, tarball='t2', entry={}, indices=[0],
            jobsub_id=None, cluster_id=None)
        row2 = [r for r in self.sl.open_rows(self.db) if r['id'] == rid2][0]
        from utils import submissions as recover
        action = recover.process_row(
            row2, self.db, 3, clusters={'9': ['R']},
            queue_state_fn=lambda cid, cl: self.fail('must not be called'),
            verify_fn=lambda r: ([], []),
            resubmit_fn=lambda r, m, d: self.fail('must not be called'))
        self.assertEqual(action, 'queue-error')

    def test_cluster_queue_state_logic(self):
        from utils.submissions import cluster_queue_state as cqs
        # None snapshot (query untrusted) → error, never drained (fail-closed)
        self.assertEqual(cqs('9', None), 'error')
        # cluster absent from a good snapshot → drained
        self.assertEqual(cqs('9', {}), 'drained')
        self.assertEqual(cqs('9', {'8': ['R']}), 'drained')
        # any idle/running job → running
        self.assertEqual(cqs('9', {'9': ['R']}), 'running')
        self.assertEqual(cqs('9', {'9': ['I']}), 'running')
        # running wins over a stray held job (don't halt a working cluster)
        self.assertEqual(cqs('9', {'9': ['R', 'H']}), 'running')
        # every non-terminal job held → held (all preempted, human decides)
        self.assertEqual(cqs('9', {'9': ['H', 'H']}), 'held')
        # only terminal rows lingering (completed/removed) → drained
        self.assertEqual(cqs('9', {'9': ['C', 'X']}), 'drained')
        self.assertEqual(cqs('9', {'9': ['H', 'C']}), 'held')
        # cluster_id coerced to str (ledger may hand an int)
        self.assertEqual(cqs(9, {'9': ['R']}), 'running')

    def test_live_clusters_parsing(self):
        from utils import submissions as recover
        # Same module — a `from test.test_unit import ...` package-path import
        # resolves only when the suite is run as `python -m unittest
        # test.test_unit`, and blows up under `unittest discover -s test`.
        T = TestRecoverCap
        def r(stdout, rc=0):
            return MagicMock(returncode=rc, stdout=stdout, stderr='')
        hdr, summ, row = T._HDR, T._SUM, T._row
        # a good table → {cluster: [states]}, procs of one cluster aggregate,
        # and the probe queries the --user table (not per-jobid)
        cmd = {}
        def run(c, capture_output=True, text=True):
            cmd['argv'] = c
            return r('\n'.join([
                hdr, summ,
                row('9.0@jobsub01.fnal.gov', 'mu2epro', 'R'),
                row('9.1@jobsub01.fnal.gov', 'mu2epro', 'I'),
                row('12.0@jobsub02.fnal.gov', '|-WORKER_3', 'H'),
            ]) + '\n')
        # USER pinned: the query follows the SUBMITTING identity now
        # (see queue_owner); the ksu block exports USER=mu2epro, so
        # production still asks for exactly this.
        with patch.dict(os.environ, {'USER': 'mu2epro'}):
            self.assertEqual(recover.live_clusters(runner=run),
                             {'9': ['R', 'I'], '12': ['H']})
        self.assertEqual(cmd['argv'], ['jobsub_q', '--user', 'mu2epro'])
        # empty-but-valid table → {} (a real drained account), NOT None
        self.assertEqual(
            recover.live_clusters(runner=lambda *a, **k: r(hdr + '\n' + summ)),
            {})
        # command failure / headerless / garbage row → None (fail-closed)
        self.assertIsNone(recover.live_clusters(
            runner=lambda *a, **k: r('', rc=1)))
        self.assertIsNone(recover.live_clusters(
            runner=lambda *a, **k: r('No jobs found\n')))
        self.assertIsNone(recover.live_clusters(runner=lambda *a, **k: r(
            '\n'.join([hdr, row('9.0@jobsub01.fnal.gov', 'mu2epro', 'Z')]))))

    def test_verify_row_missing_and_partial(self):
        from utils import submissions as recover
        files = [f"sim.mu2e.In.C.00000000_{i:08d}.art" for i in range(3)]
        jpars = _root_input_jobpars(files)
        jpars['tbs']['outfiles']['outputs.SecondOutput.fileName'] = \
            "dig.mu2e.TestDesc.TestConf.sequencer.art"
        tar = _make_tarball(jpars)
        try:
            row = {'id': 1, 'tarball': 'cnf.mu2e.T.C.0.tar',
                   'indices': [0, 1, 2], 'entry': {}, 'attempt': 1,
                   'jobsub_id': 'x'}
            dig_ds = 'dig.mu2e.TestDesc.TestConf.art'
            from utils.jobquery import Mu2eJobPars
            jp = Mu2eJobPars(tar)

            def fake_lister(ds):
                out = []
                for i in (0, 1, 2):
                    for f in jp.job_outputs(i).values():
                        if str(Mu2eName.parse(f).dataset) != ds:
                            continue
                        if i == 2:
                            continue          # idx 2: nothing landed
                        if i == 1 and ds == dig_ds:
                            continue          # idx 1: dig stream missing
                        out.append(f)
                return out

            with patch.object(recover, 'sam_physical_path_or_none', return_value=tar):
                missing, partial = recover.verify_row(
                    row, sam_lister=fake_lister)
            self.assertEqual(missing, [1, 2])
            self.assertEqual(partial, [1])
        finally:
            os.unlink(tar)

    def test_verify_row_unlocatable_tarball_raises(self):
        from utils import submissions as recover
        row = {'id': 1, 'tarball': 'cnf.mu2e.gone.C.0.tar',
               'indices': [0], 'entry': {}, 'attempt': 1, 'jobsub_id': 'x'}
        with patch.object(recover, 'sam_physical_path_or_none', return_value=None):
            with self.assertRaises(RuntimeError):
                recover.verify_row(row, sam_lister=lambda ds: [])

    def test_verify_row_nonart_outputs_report_missing(self):
        """A non-art (.root) output stream must verify normally, not raise.

        output_datasets() used to coerce every parsed name through
        .with_extension('art'), so a .root-only cnf derived a phantom
        '...art' dataset that never matched the real '.root' filenames;
        build_file_maps found nothing, every index fell into
        expected-less 'unverifiable', and verify_row raised RuntimeError
        — this test used to pin THAT (buggy) behavior. With the real
        extension preserved, an empty SAM listing means every index is
        genuinely missing, not unverifiable."""
        from utils import submissions as recover
        files = [f"sim.mu2e.In.C.00000000_{i:08d}.art" for i in range(2)]
        jpars = _root_input_jobpars(files)
        jpars['tbs']['outfiles']['outputs.PrimaryOutput.fileName'] = \
            "nts.mu2e.TestDesc.TestConf.sequencer.root"
        tar = _make_tarball(jpars)
        try:
            row = {'id': 1, 'tarball': 'cnf.mu2e.T.C.0.tar',
                   'indices': [0, 1], 'entry': {}, 'attempt': 1,
                   'jobsub_id': 'x'}
            with patch.object(recover, 'sam_physical_path_or_none', return_value=tar):
                missing, partial = recover.verify_row(row, sam_lister=lambda ds: [])
            self.assertEqual(missing, [0, 1])
            self.assertEqual(partial, [])
        finally:
            os.unlink(tar)

    def test_resubmit_drops_firstjob_and_ships_options(self):
        from utils import submissions as recover
        row = {'id': 7, 'tarball': 'cnf.mu2e.T.C.0.tar',
               'entry': {'tarball': 'cnf.mu2e.T.C.0.tar', 'njobs': 5,
                         'firstjob': 100, 'inloc': 'tape'},
               'indices': [100, 102], 'attempt': 1, 'jobsub_id': '1.0@js'}
        captured = {}

        def fake_submit(entry, idx, options):
            captured['entry'] = entry
            captured['idx'] = idx
            captured['options'] = options
            return {'status': 'submitted'}

        ok = recover.resubmit(row, [100, 102], '/tmp/led.db',
                              submit_fn=fake_submit)
        self.assertTrue(ok)
        self.assertEqual(captured['idx'], 0)
        options = captured['options']
        self.assertEqual(options.ledger_parent, 7)
        self.assertEqual(options.ledger_db, '/tmp/led.db')
        self.assertEqual(options.indices, [100, 102])
        self.assertFalse(options.dry_run)
        entry = captured['entry']
        self.assertNotIn('firstjob', entry)
        self.assertEqual(entry['njobs'], 5)
        recover.resubmit(row, [100], '/tmp/led.db', dry_run=True,
                         submit_fn=fake_submit)
        self.assertTrue(captured['options'].dry_run)


class TestRecoverCLI(unittest.TestCase):
    def setUp(self):
        import tempfile
        from utils import submission_ledger as sl
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'sub.db')

    def test_print_status_empty(self):
        from utils import submissions as recover
        import io as _io
        buf = _io.StringIO()
        with patch('sys.stdout', buf):
            recover.print_status(self.db)
        self.assertIn('empty', buf.getvalue().lower())

    def test_print_status_lists_rows(self):
        from utils import submissions as recover
        import io as _io
        rid = self.sl.record_submission(
            self.db, tarball='cnf.mu2e.T.C.0.tar', entry={}, indices=[0, 1],
            jobsub_id='1.0@js', cluster_id='1')
        self.sl.close_row(self.db, rid, 'complete')
        self.sl.record_submission(
            self.db, tarball='cnf.mu2e.T2.C.0.tar', entry={}, indices=[3],
            jobsub_id='2.0@js', cluster_id='2')
        buf = _io.StringIO()
        with patch('sys.stdout', buf):
            recover.print_status(self.db)
        out = buf.getvalue()
        self.assertIn('complete', out)
        self.assertIn('active', out)
        self.assertIn('cnf.mu2e.T2.C.0.tar', out)

    def test_status_surfaces_stuck_reservations(self):
        from utils import submissions as recover
        rid = self.sl.reserve_submission(
            self.db, tarball='cnf.mu2e.D.C.0.tar', entry={},
            indices=[0, 1, 2])
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            recover.print_status(self.db)
        self.assertIn('NEEDS RECONCILIATION', out.getvalue())
        self.assertIn(f'row {rid}', out.getvalue())

    def test_main_exit_2_on_attention(self):
        from utils import submissions as recover
        self.sl.record_submission(
            self.db, tarball='t', entry={}, indices=[0],
            jobsub_id='1.0@js', cluster_id='1')
        with patch.object(recover, 'process_row', return_value='held'), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run']):
            with self.assertRaises(SystemExit) as cm:
                recover.main()
        self.assertEqual(cm.exception.code, 2)

    def test_main_exit_2_on_dry_run_would_exhaust(self):
        from utils import submissions as recover
        self.sl.record_submission(
            self.db, tarball='t', entry={}, indices=[0],
            jobsub_id='1.0@js', cluster_id='1')
        with patch.object(recover, 'process_row',
                          return_value='would-exhaust'), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run', '--dry-run']):
            with self.assertRaises(SystemExit) as cm:
                recover.main()
        self.assertEqual(cm.exception.code, 2)

    def test_main_exit_0_when_clean(self):
        from utils import submissions as recover
        self.sl.record_submission(
            self.db, tarball='t', entry={}, indices=[0],
            jobsub_id='1.0@js', cluster_id='1')
        with patch.object(recover, 'process_row', return_value='complete'), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run']):
            recover.main()  # returns without SystemExit

    def test_main_lock_contention_exits(self):
        import fcntl
        from utils import submissions as recover
        self.sl.record_submission(
            self.db, tarball='t', entry={}, indices=[0],
            jobsub_id='1.0@js', cluster_id='1')
        lock_path = os.path.join(os.path.dirname(self.db), 'submissions.lock')
        fh = open(lock_path, 'w')
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            with patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                            'run']):
                with self.assertRaises(SystemExit) as cm:
                    recover.main()
            self.assertIn('submissions.lock', str(cm.exception.code))
        finally:
            fh.close()


class TestSubmissionsExitHonesty(unittest.TestCase):
    """A stalled loop must not impersonate a healthy one: queue-count
    failure and lingering paused campaigns exit 2 every tick."""

    def setUp(self):
        import tempfile
        from utils import submission_ledger as sl
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'sub.db')

    def test_count_error_exits_2(self):
        from utils import submissions
        with patch.object(submissions, 'top_up',
                          return_value={'count-error': 1}), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run']):
            with self.assertRaises(SystemExit) as cm:
                submissions.main()
        self.assertEqual(cm.exception.code, 2)

    def test_lingering_paused_campaign_exits_2(self):
        from utils import submissions
        cid = self.sl.create_campaign(
            self.db, tarball='cnf.mu2e.P.C.0.tar',
            entry={'tarball': 'cnf.mu2e.P.C.0.tar', 'njobs': 4},
            slice_size=2)
        self.sl.set_campaign_state(self.db, cid, 'paused',
                                   note='paused on a PREVIOUS tick')
        with patch.object(submissions, 'top_up', return_value={}), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run']):
            with self.assertRaises(SystemExit) as cm:
                submissions.main()
        self.assertEqual(cm.exception.code, 2)

    def test_lingering_paused_exits_2_under_dry_run(self):
        from utils import submissions
        cid = self.sl.create_campaign(
            self.db, tarball='cnf.mu2e.P2.C.0.tar',
            entry={'tarball': 'cnf.mu2e.P2.C.0.tar', 'njobs': 4},
            slice_size=2)
        self.sl.set_campaign_state(self.db, cid, 'paused')
        with patch.object(submissions, 'top_up', return_value={}), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run', '--dry-run']):
            with self.assertRaises(SystemExit) as cm:
                submissions.main()
        self.assertEqual(cm.exception.code, 2)

    def test_clean_run_still_exits_0(self):
        from utils import submissions
        with patch.object(submissions, 'top_up', return_value={}), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run']):
            submissions.main()  # no SystemExit

    def test_drain_error_exits_2(self):
        # A permanently stuck draining campaign (state_fn/gate_fn keeps
        # raising) must not go silent — drain-error is a summary key
        # exactly like count-error and must trigger the same exit-2.
        from utils import submissions
        with patch.object(submissions, 'top_up', return_value={}), \
             patch.object(submissions, 'drain_tick',
                          return_value={'drain-error': 1}), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run']):
            with self.assertRaises(SystemExit) as cm:
                submissions.main()
        self.assertEqual(cm.exception.code, 2)

    def test_campaign_flag_threads_to_top_up_only(self):
        # `--campaign` scopes the top-up feed to one campaign; the
        # recovery pass and drain_tick are unaffected (drain_tick takes
        # no such filter at all).
        from utils import submissions
        with patch.object(submissions, 'top_up',
                          return_value={}) as top_up_mock, \
             patch.object(submissions, 'drain_tick',
                          return_value={}) as drain_mock, \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run', '--campaign', '7']):
            submissions.main()
        self.assertEqual(top_up_mock.call_args.kwargs.get('only_campaign'), 7)
        self.assertNotIn('only_campaign', drain_mock.call_args.kwargs)

    def test_campaign_flag_absent_passes_none_to_top_up(self):
        # No --campaign given: the cron's own invocation must keep
        # ticking every active campaign (only_campaign=None).
        from utils import submissions
        with patch.object(submissions, 'top_up',
                          return_value={}) as top_up_mock, \
             patch.object(submissions, 'drain_tick', return_value={}), \
             patch.object(sys, 'argv', ['submissions', '--db', self.db,
                                        'run']):
            submissions.main()
        self.assertIsNone(top_up_mock.call_args.kwargs.get('only_campaign'))


class TestPauseNotePreservation(unittest.TestCase):
    def setUp(self):
        import tempfile
        from utils import submission_ledger as sl
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'sub.db')
        self.cid = sl.create_campaign(
            self.db, tarball='cnf.mu2e.N.C.0.tar',
            entry={'tarball': 'cnf.mu2e.N.C.0.tar', 'njobs': 4},
            slice_size=2)

    def _note(self):
        return self.sl.all_campaigns(self.db)[0]['note']

    def test_resume_preserves_pause_note(self):
        self.sl.set_campaign_state(self.db, self.cid, 'paused',
                                   note='crash-window suspected')
        self.sl.set_campaign_state(self.db, self.cid, 'active')
        self.assertEqual(self._note(), 'crash-window suspected')

    def test_resume_clears_closed_utc(self):
        self.sl.set_campaign_state(self.db, self.cid, 'paused', note='x')
        self.sl.set_campaign_state(self.db, self.cid, 'active')
        self.assertIsNone(self.sl.all_campaigns(self.db)[0]['closed_utc'])

    def test_pause_verb_custom_note(self):
        from utils import submissions
        with patch.object(sys, 'argv',
                          ['submissions', '--db', self.db, 'pause',
                           str(self.cid), '--note', 'draining for O2']):
            submissions.main()
        self.assertEqual(self._note(), 'draining for O2')

    def test_pause_verb_default_note(self):
        from utils import submissions
        with patch.object(sys, 'argv',
                          ['submissions', '--db', self.db, 'pause',
                           str(self.cid)]):
            submissions.main()
        self.assertEqual(self._note(), 'operator pause')


class TestSplitInputs(unittest.TestCase):
    """split_inputs reads the frozen input file lists from a cnf tarball,
    splitting primary (tbs.inputs) from pileup (tbs.auxin), grouped by
    dataset and deduplicated — no per-index reconstruction."""

    def _tar(self, inputs=None, auxin=None, samplinginput=None):
        jp = {
            "code": "", "setup": "/cvmfs/x/setup.sh",
            "tbs": {"seed": "services.SeedService.baseSeed"},
            "jobname": "cnf.mu2e.TestDesc.TestConf.0.tar",
            "owner": "mu2e", "dsconf": "TestConf",
        }
        if inputs is not None:
            jp["tbs"]["inputs"] = inputs
        if auxin is not None:
            jp["tbs"]["auxin"] = auxin
        if samplinginput is not None:
            jp["tbs"]["samplinginput"] = samplinginput
        return _make_tarball(jp)

    def test_splits_primary_and_pileup_by_dataset(self):
        from utils.check_inputs import split_inputs
        tar = self._tar(
            inputs={"source.fileNames": [1, [
                "dts.mu2e.Prim.CampA.001430_00000000.art",
                "dts.mu2e.Prim.CampA.001430_00000001.art"]]},
            auxin={"physics.filters.M.fileNames": [1, [
                "dts.mu2e.Pile.CampB.001430_00000005.art"]]},
        )
        primary, auxin = split_inputs(tar)
        os.unlink(tar)
        self.assertEqual(set(primary), {"dts.mu2e.Prim.CampA.art"})
        self.assertEqual(len(primary["dts.mu2e.Prim.CampA.art"]), 2)
        self.assertEqual(set(auxin), {"dts.mu2e.Pile.CampB.art"})

    def test_dedups_repeated_files(self):
        from utils.check_inputs import split_inputs
        f = "dts.mu2e.Pile.CampB.001430_00000005.art"
        tar = self._tar(auxin={
            "physics.filters.A.fileNames": [1, [f]],
            "physics.filters.B.fileNames": [1, [f]],
        })
        _, auxin = split_inputs(tar)
        os.unlink(tar)
        self.assertEqual(auxin["dts.mu2e.Pile.CampB.art"], [f])

    def test_missing_sections_yield_empty(self):
        from utils.check_inputs import split_inputs
        tar = self._tar()
        primary, auxin = split_inputs(tar)
        os.unlink(tar)
        self.assertEqual(primary, {})
        self.assertEqual(auxin, {})

    def test_problem_is_frozen(self):
        from utils.check_inputs import Problem
        p = Problem("ds", "f.art", "truncated", "detail")
        self.assertEqual(p.kind, "truncated")
        with self.assertRaises(Exception):
            p.kind = "missing"

    def test_samplinginput_folded_into_primary(self):
        from utils.check_inputs import split_inputs
        tar = self._tar(
            samplinginput={"physics.filters.resampler.fileNames": [1, [
                "dts.mu2e.NeutralsCat.MDC2025ab.001430_00000007.art"]]})
        primary, auxin = split_inputs(tar)
        os.unlink(tar)
        self.assertEqual(set(primary), {"dts.mu2e.NeutralsCat.MDC2025ab.art"})
        self.assertEqual(auxin, {})


class TestCheckResilient(unittest.TestCase):
    """Resilient pileup: present AND size matches SAM. Catches the
    2026-07-21 truncation (1 MiB stub) and a purge (missing entirely).
    mdh cannot see resilient, so this is a direct os.path.getsize vs the
    SAM-recorded size."""

    DS = "dts.mu2e.Pile.CampB.art"
    F1 = "dts.mu2e.Pile.CampB.001430_00000000.art"
    F2 = "dts.mu2e.Pile.CampB.001430_00000001.art"

    def test_all_present_and_sized_ok(self):
        from utils.check_inputs import check_resilient
        probs = check_resilient(
            self.DS, [self.F1, self.F2],
            sam_sizes=lambda ds: {self.F1: 100, self.F2: 200},
            disk_size=lambda p: 100 if self.F1 in p else 200)
        self.assertEqual(probs, [])

    def test_truncated_file_flagged(self):
        from utils.check_inputs import check_resilient
        probs = check_resilient(
            self.DS, [self.F1],
            sam_sizes=lambda ds: {self.F1: 113643009},
            disk_size=lambda p: 1048576)
        self.assertEqual(len(probs), 1)
        self.assertEqual(probs[0].kind, "truncated")
        self.assertEqual(probs[0].filename, self.F1)

    def test_missing_file_flagged(self):
        from utils.check_inputs import check_resilient
        probs = check_resilient(
            self.DS, [self.F1],
            sam_sizes=lambda ds: {self.F1: 100},
            disk_size=lambda p: None)
        self.assertEqual(len(probs), 1)
        self.assertEqual(probs[0].kind, "missing")

    def test_no_sam_size_is_query_error(self):
        from utils.check_inputs import check_resilient
        probs = check_resilient(
            self.DS, [self.F1],
            sam_sizes=lambda ds: {},
            disk_size=lambda p: 100)
        self.assertEqual(len(probs), 1)
        self.assertEqual(probs[0].kind, "query_error")

    def test_sam_lookup_raises_is_query_error(self):
        from utils.check_inputs import check_resilient
        def boom(ds):
            raise RuntimeError("SAM down")
        probs = check_resilient(
            self.DS, [self.F1, self.F2],
            sam_sizes=boom,
            disk_size=lambda p: 100)
        self.assertEqual([p.kind for p in probs], ["query_error", "query_error"])
        self.assertEqual(len(probs), 2)

    def test_default_disk_size_absent_is_none(self):
        from utils.check_inputs import _default_disk_size
        self.assertIsNone(_default_disk_size("/pnfs/mu2e/resilient/nope/x.art"))


class TestFileSizesInDataset(unittest.TestCase):
    """file_sizes_in_dataset returns {filename: size} from one
    list-files --fileinfo call."""

    def test_maps_name_to_size(self):
        import collections
        from utils import samweb_wrapper
        FI = collections.namedtuple("fileinfo",
                                    "file_name file_id file_size event_count")
        fake_client = MagicMock()
        fake_client.listFiles.return_value = [
            FI("dts.mu2e.Pile.CampB.001430_00000000.art", 1, 111, 9),
            FI("dts.mu2e.Pile.CampB.001430_00000001.art", 2, 222, 9),
        ]
        with patch.object(samweb_wrapper, "_client",
                          return_value=fake_client):
            out = samweb_wrapper.file_sizes_in_dataset("dts.mu2e.Pile.CampB.art")
        self.assertEqual(out, {
            "dts.mu2e.Pile.CampB.001430_00000000.art": 111,
            "dts.mu2e.Pile.CampB.001430_00000001.art": 222})
        # one query, fileinfo requested
        _, kwargs = fake_client.listFiles.call_args
        self.assertTrue(kwargs.get("fileinfo"))


class TestCheckTape(unittest.TestCase):
    """Primary / tape inputs: NEARLINE (evicted) must block with a
    /prestage hint; ONLINE passes; unknown storage or query failure fails
    closed."""

    DS = "dts.mu2e.Prim.CampA.art"
    F1 = "dts.mu2e.Prim.CampA.001430_00000000.art"
    F2 = "dts.mu2e.Prim.CampA.001430_00000001.art"

    def test_online_passes(self):
        from utils.check_inputs import check_tape
        probs = check_tape(
            self.DS, [self.F1, self.F2],
            locality=lambda loc, fs: {self.F1: "ONLINE",
                                      self.F2: "ONLINE_AND_NEARLINE"},
            dataset_location=lambda ds: "enstore")
        self.assertEqual(probs, [])

    def test_nearline_blocks_with_prestage_hint(self):
        from utils.check_inputs import check_tape
        probs = check_tape(
            self.DS, [self.F1],
            locality=lambda loc, fs: {self.F1: "NEARLINE"},
            dataset_location=lambda ds: "enstore")
        self.assertEqual(len(probs), 1)
        self.assertEqual(probs[0].kind, "nearline")
        self.assertIn("/prestage", probs[0].detail)

    def test_disk_dataset_queries_disk_location(self):
        from utils.check_inputs import check_tape
        seen = {}
        def loc(mdh_loc, fs):
            seen["loc"] = mdh_loc
            return {self.F1: "ONLINE"}
        probs = check_tape(self.DS, [self.F1], locality=loc,
                           dataset_location=lambda ds: "dcache")
        self.assertEqual(probs, [])
        self.assertEqual(seen["loc"], "disk")

    def test_enstore_dataset_queries_tape_location(self):
        from utils.check_inputs import check_tape
        seen = {}
        def loc(mdh_loc, fs):
            seen["loc"] = mdh_loc
            return {self.F1: "ONLINE"}
        check_tape(self.DS, [self.F1], locality=loc,
                   dataset_location=lambda ds: "enstore")
        self.assertEqual(seen["loc"], "tape")

    def test_missing_reported(self):
        from utils.check_inputs import check_tape
        probs = check_tape(
            self.DS, [self.F1],
            locality=lambda loc, fs: {self.F1: "MISSING"},
            dataset_location=lambda ds: "enstore")
        self.assertEqual(probs[0].kind, "missing")

    def test_unknown_storage_location_fails_closed(self):
        from utils.check_inputs import check_tape
        probs = check_tape(
            self.DS, [self.F1],
            locality=lambda loc, fs: {self.F1: "ONLINE"},
            dataset_location=lambda ds: "N/A")
        self.assertEqual(len(probs), 1)
        self.assertEqual(probs[0].kind, "query_error")

    def test_locality_error_fails_closed(self):
        from utils.check_inputs import check_tape
        probs = check_tape(
            self.DS, [self.F1],
            locality=lambda loc, fs: {self.F1: "ERROR"},
            dataset_location=lambda ds: "enstore")
        self.assertEqual(probs[0].kind, "query_error")


class TestDefaultLocalityParsing(unittest.TestCase):
    """_default_locality queries mdh per file via the Python API.

    The `mdh query-dcache` CLI aborts at the FIRST file absent from the
    queried area, so one persistent-resident file in a tape dataset
    truncated stdout and forced a fail-closed ERROR for all 5000 files of
    dts.mu2e.RPCInternalPhysical.MDC2025ap — 4997 of which were in fact
    ONLINE_AND_NEARLINE. Per-file lookups cannot have that failure mode.
    """

    F1 = "dts.mu2e.Prim.CampA.001430_00000000.art"
    F2 = "dts.mu2e.Prim.CampA.001430_00000001.art"

    @staticmethod
    def _client(by_loc, fail_times=0):
        """Fake MdhClient. `by_loc` maps location -> {filename: locality};
        a 404 raises RuntimeError as the real client does."""
        state = {'left': fail_times}

        class FakeClient:
            def query_dcache(self, filename, location="tape"):
                if state['left'] > 0:
                    state['left'] -= 1
                    raise ConnectionError("transient")
                loc = by_loc.get(location, {})
                if filename not in loc:
                    raise RuntimeError(f"File not found in dCache: /pnfs/{location}/{filename}")
                return {'fileLocality': loc[filename]}
        return FakeClient

    def _run(self, by_loc, files=None, fail_times=0):
        from utils import check_inputs
        fake = types.ModuleType("mdh")
        fake.MdhClient = self._client(by_loc, fail_times)
        with patch.dict(sys.modules, {"mdh": fake}):
            return check_inputs._default_locality("tape", files or [self.F1, self.F2])

    def test_all_found_on_tape(self):
        out = self._run({"tape": {self.F1: "ONLINE_AND_NEARLINE", self.F2: "NEARLINE"}})
        self.assertEqual(out, {self.F1: "ONLINE_AND_NEARLINE", self.F2: "NEARLINE"})

    def test_disk_resident_file_is_online_not_missing(self):
        """A file on persistent/disk has no tape copy — nothing to stage."""
        out = self._run({"tape": {self.F1: "ONLINE_AND_NEARLINE"},
                         "disk": {self.F2: "ONLINE"}})
        self.assertEqual(out, {self.F1: "ONLINE_AND_NEARLINE", self.F2: "ONLINE"})

    def test_absent_everywhere_is_missing(self):
        out = self._run({"tape": {self.F1: "ONLINE"}})
        self.assertEqual(out, {self.F1: "ONLINE", self.F2: "MISSING"})

    def test_one_offlocation_file_does_not_poison_the_rest(self):
        """The regression: a split dataset must not fail every file."""
        files = [f"dts.mu2e.Prim.CampA.001430_{i:08d}.art" for i in range(50)]
        tape = {f: "ONLINE_AND_NEARLINE" for f in files if f != files[7]}
        out = self._run({"tape": tape, "disk": {files[7]: "ONLINE"}}, files=files)
        self.assertEqual(out[files[7]], "ONLINE")
        self.assertTrue(all(out[f] == "ONLINE_AND_NEARLINE"
                            for f in files if f != files[7]))

    def test_transient_transport_error_is_retried(self):
        """A concurrency blip must not block a campaign."""
        out = self._run({"tape": {self.F1: "ONLINE_AND_NEARLINE"}},
                        files=[self.F1], fail_times=2)
        self.assertEqual(out, {self.F1: "ONLINE_AND_NEARLINE"})

    def test_persistent_transport_error_fails_closed(self):
        out = self._run({"tape": {self.F1: "ONLINE_AND_NEARLINE"}},
                        files=[self.F1], fail_times=99)
        self.assertEqual(out, {self.F1: "ERROR"})

    def test_unimportable_mdh_fails_closed(self):
        from utils import check_inputs
        with patch.dict(sys.modules, {"mdh": None}):
            out = check_inputs._default_locality("tape", [self.F1])
        self.assertEqual(out, {self.F1: "ERROR"})


class TestCheckInputs(unittest.TestCase):
    """check_inputs assembles split_inputs + the two checks with the
    inloc routing, returning (ok, problems)."""

    def _tar(self, inputs=None, auxin=None, samplinginput=None):
        jp = {"code": "", "setup": "/cvmfs/x/setup.sh",
              "tbs": {"seed": "s"}, "jobname": "cnf.mu2e.T.C.0.tar",
              "owner": "mu2e", "dsconf": "C"}
        if inputs is not None:
            jp["tbs"]["inputs"] = inputs
        if auxin is not None:
            jp["tbs"]["auxin"] = auxin
        if samplinginput is not None:
            jp["tbs"]["samplinginput"] = samplinginput
        return _make_tarball(jp)

    PRIM = "dts.mu2e.Prim.CampA.001430_00000000.art"
    PILE = "dts.mu2e.Pile.CampB.001430_00000005.art"

    def _tar_both(self):
        return self._tar(
            inputs={"source.fileNames": [1, [self.PRIM]]},
            auxin={"physics.filters.M.fileNames": [1, [self.PILE]]})

    def test_all_clean(self):
        from utils.check_inputs import check_inputs
        tar = self._tar_both()
        ok, probs = check_inputs(
            tar, "resilient",
            sam_sizes=lambda ds: {self.PILE: 100},
            disk_size=lambda p: 100,
            locality=lambda loc, fs: {f: "ONLINE" for f in fs},
            dataset_location=lambda ds: "dcache")
        os.unlink(tar)
        self.assertTrue(ok)
        self.assertEqual(probs, [])

    def test_resilient_pileup_checked_by_size_not_mdh(self):
        from utils.check_inputs import check_inputs
        tar = self._tar_both()
        called = {"mdh": []}
        def loc(mdh_loc, fs):
            called["mdh"].extend(fs)
            return {f: "ONLINE" for f in fs}
        ok, probs = check_inputs(
            tar, "resilient",
            sam_sizes=lambda ds: {self.PILE: 100},
            disk_size=lambda p: 1048576,      # truncated pileup
            locality=loc, dataset_location=lambda ds: "dcache")
        os.unlink(tar)
        self.assertFalse(ok)
        self.assertEqual([p.kind for p in probs], ["truncated"])
        # pileup went through the resilient size path, never mdh
        self.assertNotIn(self.PILE, called["mdh"])

    def test_nearline_primary_blocks(self):
        from utils.check_inputs import check_inputs
        tar = self._tar_both()
        ok, probs = check_inputs(
            tar, "resilient",
            sam_sizes=lambda ds: {self.PILE: 100},
            disk_size=lambda p: 100,
            locality=lambda loc, fs: {f: "NEARLINE" for f in fs},
            dataset_location=lambda ds: "enstore")
        os.unlink(tar)
        self.assertFalse(ok)
        self.assertEqual([p.kind for p in probs], ["nearline"])

    def test_missing_resilient_not_reclassified_as_tape(self):
        # The flagged subtlety: a pileup file absent from resilient must
        # be reported 'missing', NOT quietly checked as a tape input.
        from utils.check_inputs import check_inputs
        tar = self._tar(auxin={"physics.filters.M.fileNames":
                               [1, [self.PILE]]})
        def loc(mdh_loc, fs):
            raise AssertionError("pileup must not reach the tape path")
        ok, probs = check_inputs(
            tar, "resilient",
            sam_sizes=lambda ds: {self.PILE: 100},
            disk_size=lambda p: None,          # purged from resilient
            locality=loc, dataset_location=lambda ds: "enstore")
        os.unlink(tar)
        self.assertFalse(ok)
        self.assertEqual([p.kind for p in probs], ["missing"])

    def test_non_resilient_inloc_routes_pileup_to_tape(self):
        from utils.check_inputs import check_inputs
        tar = self._tar(auxin={"physics.filters.M.fileNames":
                               [1, [self.PILE]]})
        ok, probs = check_inputs(
            tar, "tape",
            sam_sizes=lambda ds: {},
            disk_size=lambda p: None,
            locality=lambda loc, fs: {f: "ONLINE" for f in fs},
            dataset_location=lambda ds: "enstore")
        os.unlink(tar)
        self.assertTrue(ok)

    def test_samplinginput_nearline_blocks(self):
        from utils.check_inputs import check_inputs
        SAMP = "dts.mu2e.NeutralsCat.MDC2025ab.001430_00000007.art"
        tar = self._tar(samplinginput={"physics.filters.r.fileNames": [1, [SAMP]]})
        ok, probs = check_inputs(
            tar, "resilient",
            sam_sizes=lambda ds: {},
            disk_size=lambda p: None,
            locality=lambda loc, fs: {f: "NEARLINE" for f in fs},
            dataset_location=lambda ds: "enstore")
        os.unlink(tar)
        self.assertFalse(ok)
        self.assertEqual([p.kind for p in probs], ["nearline"])


class TestCheckDirInloc(unittest.TestCase):
    """`dir:<path>` inloc: residency is a filesystem existence check on
    that path — never a SAM query. Filenames are bare basenames written
    verbatim by json2jobdef for this shape and need not parse as Mu2e
    names, so the branch must bypass _group_by_dataset entirely."""

    PRIM = "sim.oksuzian.TargetStops.GridSmoke.001430_00000000.art"
    BARE = "PBI_Normal_33344.txt"          # not a Mu2eName — must not crash
    PILE = "dts.mu2e.Pile.CampB.001430_00000005.art"

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def _write(self, name, content=b"x"):
        with open(os.path.join(self.dir, name), "wb") as f:
            f.write(content)

    def _tar(self, files, auxin=None):
        jp = {"code": "", "setup": "/cvmfs/x/setup.sh",
              "tbs": {"seed": "s",
                      "inputs": {"source.fileNames": [1, list(files)]}},
              "jobname": "cnf.mu2e.T.C.0.tar", "owner": "mu2e",
              "dsconf": "C"}
        if auxin is not None:
            jp["tbs"]["auxin"] = auxin
        return _make_tarball(jp)

    def _boom(self, *a, **k):
        raise AssertionError("SAM must not be queried for a dir: inloc")

    def _check(self, tar):
        from utils.check_inputs import check_inputs
        ok, probs = check_inputs(tar, f"dir:{self.dir}",
                                 sam_sizes=self._boom,
                                 locality=self._boom,
                                 dataset_location=self._boom)
        os.unlink(tar)
        return ok, probs

    def test_all_present_no_sam_query(self):
        # The pinning assertion: every SAM-facing injectable raises.
        self._write(self.PRIM)
        self._write(self.BARE)
        ok, probs = self._check(self._tar([self.PRIM, self.BARE]))
        self.assertTrue(ok)
        self.assertEqual(probs, [])

    def test_missing_file_reported(self):
        self._write(self.PRIM)
        ok, probs = self._check(self._tar([self.PRIM, self.BARE]))
        self.assertFalse(ok)
        self.assertEqual([(p.filename, p.kind) for p in probs],
                         [(self.BARE, "missing")])

    def test_zero_size_is_truncated(self):
        self._write(self.PRIM, content=b"")
        ok, probs = self._check(self._tar([self.PRIM]))
        self.assertFalse(ok)
        self.assertEqual([p.kind for p in probs], ["truncated"])

    def test_auxin_checked_against_dir_too(self):
        self._write(self.PRIM)
        tar = self._tar([self.PRIM],
                        auxin={"physics.filters.M.fileNames":
                               [1, [self.PILE]]})
        ok, probs = self._check(tar)
        self.assertFalse(ok)
        self.assertEqual([(p.filename, p.kind) for p in probs],
                         [(self.PILE, "missing")])

    def test_stat_error_fails_closed(self):
        from utils.check_inputs import check_dir
        def boom_size(path):
            raise RuntimeError("filesystem exploded")
        probs = check_dir([self.PRIM], self.dir, file_size=boom_size)
        self.assertEqual([p.kind for p in probs], ["query_error"])

    def test_non_dir_inloc_still_routes_to_sam_checks(self):
        # Regression guard: the new branch must not widen. A tape inloc
        # still resolves through locality/dataset_location.
        from utils.check_inputs import check_inputs
        seen = {"called": False}
        def loc(mdh_loc, fs):
            seen["called"] = True
            return {f: "ONLINE" for f in fs}
        tar = self._tar([self.PILE])
        ok, probs = check_inputs(tar, "tape",
                                 sam_sizes=lambda ds: {},
                                 locality=loc,
                                 dataset_location=lambda ds: "enstore")
        os.unlink(tar)
        self.assertTrue(ok)
        self.assertTrue(seen["called"])


class TestCheckCodeTarball(unittest.TestCase):
    """The entry names a tarball; the cnf remembers the digest of the
    tarball it was built against. They must still agree at submit time."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.code = _make_code_tarball(os.path.join(self.dir, 'Code.tar.bz2'))

    def _cnf(self, jobpars):
        """A cnf tarball carrying just jobpars.json — enough for
        Mu2eJobPars to read code_ref."""
        path = os.path.join(self.dir, 'cnf.mu2e.Demo.Run1Baq.0.tar')
        blob = json.dumps(jobpars).encode()
        with tarfile.open(path, 'w') as tar:
            info = tarfile.TarInfo('jobpars.json')
            info.size = len(blob)
            tar.addfile(info, io.BytesIO(blob))
        return path

    def test_matching_digest_passes(self):
        from utils.check_inputs import check_code_tarball
        from utils.jobdef import build_code_ref
        cnf = self._cnf({'code': '', 'setup': 'Code/setup.sh',
                         'code_ref': build_code_ref(self.code), 'tbs': {}})
        ok, problems = check_code_tarball({'code': self.code}, cnf)
        self.assertTrue(ok)
        self.assertEqual(problems, [])

    def test_musing_cnf_and_musing_entry_pass(self):
        from utils.check_inputs import check_code_tarball
        cnf = self._cnf({'code': '', 'setup': '/cvmfs/x/setup.sh', 'tbs': {}})
        ok, problems = check_code_tarball({}, cnf)
        self.assertTrue(ok)

    def test_changed_bytes_refused(self):
        from utils.check_inputs import check_code_tarball
        from utils.jobdef import build_code_ref
        cnf = self._cnf({'code': '', 'setup': 'Code/setup.sh',
                         'code_ref': build_code_ref(self.code), 'tbs': {}})
        # Rebuild in place with different content, as `muse tarball` would.
        _make_code_tarball(self.code)
        with open(self.code, 'ab') as fh:
            fh.write(b'\x00')
        ok, problems = check_code_tarball({'code': self.code}, cnf)
        self.assertFalse(ok)
        self.assertEqual(problems[0].kind, 'code_mismatch')

    def test_deleted_tarball_refused(self):
        from utils.check_inputs import check_code_tarball
        from utils.jobdef import build_code_ref
        cnf = self._cnf({'code': '', 'setup': 'Code/setup.sh',
                         'code_ref': build_code_ref(self.code), 'tbs': {}})
        os.unlink(self.code)
        ok, problems = check_code_tarball({'code': self.code}, cnf)
        self.assertFalse(ok)
        self.assertEqual(problems[0].kind, 'missing')

    def test_entry_has_code_but_cnf_does_not(self):
        from utils.check_inputs import check_code_tarball
        cnf = self._cnf({'code': '', 'setup': '/cvmfs/x/setup.sh', 'tbs': {}})
        ok, problems = check_code_tarball({'code': self.code}, cnf)
        self.assertFalse(ok)
        self.assertEqual(problems[0].kind, 'code_mismatch')

    def test_cnf_has_code_but_entry_does_not(self):
        from utils.check_inputs import check_code_tarball
        from utils.jobdef import build_code_ref
        cnf = self._cnf({'code': '', 'setup': 'Code/setup.sh',
                         'code_ref': build_code_ref(self.code), 'tbs': {}})
        ok, problems = check_code_tarball({}, cnf)
        self.assertFalse(ok)
        self.assertEqual(problems[0].kind, 'code_mismatch')

    def test_corrupt_cnf_reports_query_error_instead_of_raising(self):
        """Mu2eJobBase.__init__ opens the cnf with tarfile.open unguarded;
        a garbage/truncated cnf raises tarfile.ReadError there. That must
        surface as a Problem, not an uncaught traceback — the draining
        branch never used to parse the cnf at all, so this is the only
        thing standing between a half-staged cnf and a crash mid-enqueue."""
        from utils.check_inputs import check_code_tarball
        corrupt = os.path.join(self.dir, 'cnf.mu2e.Bad.Conf.0.tar')
        with open(corrupt, 'wb') as fh:
            fh.write(b'not a tarball, just garbage bytes')
        ok, problems = check_code_tarball({'code': self.code}, corrupt)
        self.assertFalse(ok)
        self.assertEqual(problems[0].kind, 'query_error')


class TestCheckInputsCLI(unittest.TestCase):
    """format_report + main: grouped report, exit 0 clean / 2 on problems."""

    def test_script_mode_help_runs_standalone(self):
        """bin/check_inputs execs `python3 utils/check_inputs.py`; running
        as a script (not `-m`) must resolve `import utils.*`, and the module
        must load without the Mu2e environment so `--help` works. A fresh
        subprocess has neither the repo root on sys.path nor the test's
        samweb_client stub, so it reproduces the real invocation. Regression:
        the module shipped without the sys.path insert AND with a top-level
        samweb_wrapper import, so `bin/check_inputs` died on ModuleNotFound."""
        import subprocess
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        script = os.path.join(repo_root, 'utils', 'check_inputs.py')
        env = {k: v for k, v in os.environ.items() if k != 'PYTHONPATH'}
        r = subprocess.run([sys.executable, script, '--help'],
                           capture_output=True, text=True, cwd=repo_root,
                           env=env, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn('usage', r.stdout)
        self.assertIn('--inloc', r.stdout)

    def test_format_report_ok(self):
        from utils.check_inputs import format_report
        text = format_report("cnf.mu2e.T.C.0.tar", [])
        self.assertIn("cnf.mu2e.T.C.0.tar", text)
        self.assertIn("OK", text)

    def test_format_report_groups_problems(self):
        from utils.check_inputs import format_report, Problem
        probs = [
            Problem("dts.mu2e.Pile.CampB.art", "f1.art", "truncated", "1 != 2"),
            Problem("dts.mu2e.Prim.CampA.art", "f2.art", "nearline",
                    "run /prestage dts.mu2e.Prim.CampA.art"),
        ]
        text = format_report("cnf.mu2e.T.C.0.tar", probs)
        self.assertIn("truncated", text)
        self.assertIn("/prestage", text)
        self.assertIn("dts.mu2e.Pile.CampB.art", text)

    def test_main_returns_2_on_problem(self):
        from utils import check_inputs as ci
        with patch.object(ci, "check_inputs",
                          return_value=(False, [ci.Problem(
                              "ds", "f.art", "truncated", "d")])):
            rc = ci.main(["--inloc", "resilient", "cnf.mu2e.T.C.0.tar"])
        self.assertEqual(rc, 2)

    def test_main_returns_0_when_clean(self):
        from utils import check_inputs as ci
        with patch.object(ci, "check_inputs", return_value=(True, [])):
            rc = ci.main(["cnf.mu2e.T.C.0.tar"])
        self.assertEqual(rc, 0)

    def test_main_default_inloc_is_resilient(self):
        from utils import check_inputs as ci
        seen = {}
        def fake(tar, inloc, **kw):
            seen["inloc"] = inloc
            return (True, [])
        with patch.object(ci, "check_inputs", side_effect=fake):
            ci.main(["cnf.mu2e.T.C.0.tar"])
        self.assertEqual(seen["inloc"], "resilient")


class TestEnqueueInputGate(unittest.TestCase):
    """enqueue_entry refuses to create a campaign when an entry's inputs
    fail the pre-flight check (exit 2, no ledger row)."""

    def test_failing_check_blocks_and_creates_no_campaign(self):
        from utils import submit
        entry = {"tarball": "cnf.mu2e.T.C.0.tar", "inloc": "resilient", "prodtools_dir": FAKE_PRODTOOLS_DIR,
                 "njobs": 100, "outputs": [{"dataset": "dig.mu2e.*.art",
                                            "location": "tape"}]}
        created = []
        with patch.object(submit, "_ensure_local_tarball",
                          return_value=Path("cnf.mu2e.T.C.0.tar")), \
             patch.object(submit, "check_inputs",
                          return_value=(False, [submit.Problem(
                              "dts.mu2e.Pile.CampB.art", "f.art",
                              "truncated", "1 != 2")])), \
             patch.object(submit.submission_ledger, "create_campaign",
                          side_effect=lambda *a, **k: created.append(1)):
            with self.assertRaises(SystemExit) as cm:
                submit.enqueue_entry(entry, ledger_db="/tmp/never.db",
                                     slice_size=500)
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(created, [])   # no campaign row

    def test_passing_check_creates_campaign(self):
        from utils import submit
        entry = {"tarball": "cnf.mu2e.T.C.0.tar", "inloc": "resilient", "prodtools_dir": FAKE_PRODTOOLS_DIR,
                 "njobs": 100, "outputs": [{"dataset": "dig.mu2e.*.art",
                                            "location": "tape"}]}
        with patch.object(submit, "_ensure_local_tarball",
                          return_value=Path("cnf.mu2e.T.C.0.tar")), \
             patch.object(submit, "check_inputs", return_value=(True, [])), \
             patch.object(submit, "check_code_tarball",
                          return_value=(True, [])), \
             patch.object(submit.submission_ledger, "create_campaign",
                          return_value=7):
            camp_id = submit.enqueue_entry(entry, ledger_db="/tmp/never.db",
                                           slice_size=500)
        self.assertEqual(camp_id, 7)

    def test_failing_code_check_blocks_and_creates_no_campaign(self):
        """A code_mismatch from check_code_tarball must gate the NORMAL
        branch exactly like a check_inputs failure does — exit 2, no
        ledger row — even though check_inputs itself passed clean."""
        from utils import submit
        entry = {"tarball": "cnf.mu2e.T.C.0.tar", "inloc": "resilient", "prodtools_dir": FAKE_PRODTOOLS_DIR,
                 "njobs": 100, "outputs": [{"dataset": "dig.mu2e.*.art",
                                            "location": "tape"}]}
        created = []
        with patch.object(submit, "_ensure_local_tarball",
                          return_value=Path("cnf.mu2e.T.C.0.tar")), \
             patch.object(submit, "check_inputs", return_value=(True, [])), \
             patch.object(submit, "check_code_tarball",
                          return_value=(False, [submit.Problem(
                              "code", "cnf.mu2e.T.C.0.tar",
                              "code_mismatch", "digest mismatch")])), \
             patch.object(submit.submission_ledger, "create_campaign",
                          side_effect=lambda *a, **k: created.append(1)):
            with self.assertRaises(SystemExit) as cm:
                submit.enqueue_entry(entry, ledger_db="/tmp/never.db",
                                     slice_size=500)
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(created, [])   # no campaign row


# ---------------------------------------------------------------------------
# MCP adapters
# ---------------------------------------------------------------------------

class TestMcpAdapters(unittest.TestCase):
    def test_error_shape(self):
        from prodtools_mcp.adapters import error
        e = error('not_found', 'no such dataset', 'check the name')
        self.assertEqual(e, {'error': {'kind': 'not_found',
                                       'message': 'no such dataset',
                                       'remedy': 'check the name'}})

    def test_error_rejects_unknown_kind(self):
        from prodtools_mcp.adapters import error
        with self.assertRaises(ValueError):
            error('banana', 'nope')

    def test_safe_tool_passes_success_through(self):
        from prodtools_mcp.adapters import safe_tool

        @safe_tool
        def ok():
            return {'value': 1}
        self.assertEqual(ok(), {'value': 1})

    def test_safe_tool_converts_toolerror(self):
        from prodtools_mcp.adapters import safe_tool, ToolError

        @safe_tool
        def boom():
            raise ToolError('catalog_unavailable', 'SAM down', 'retry later')
        self.assertEqual(boom()['error']['kind'], 'catalog_unavailable')
        self.assertEqual(boom()['error']['remedy'], 'retry later')

    def test_safe_tool_traps_systemexit(self):
        """SystemExit derives from BaseException; an uncaught one would
        terminate the server rather than fail one call."""
        from prodtools_mcp.adapters import safe_tool

        @safe_tool
        def exits():
            sys.exit('MU2E_MAX_QUEUED is not an integer')
        result = exits()
        self.assertEqual(result['error']['kind'], 'internal')
        self.assertIn('MU2E_MAX_QUEUED', result['error']['message'])

    def test_safe_tool_converts_unexpected_exception(self):
        from prodtools_mcp.adapters import safe_tool

        @safe_tool
        def raises():
            raise RuntimeError('kaboom')
        result = raises()
        self.assertEqual(result['error']['kind'], 'internal')
        self.assertIn('kaboom', result['error']['message'])

    def test_safe_tool_keeps_stdout_clean(self):
        """stdout IS the JSON-RPC channel. A print() inside a util must
        not reach it (utils/famtree.py:71 does exactly this)."""
        from prodtools_mcp.adapters import safe_tool

        @safe_tool
        def chatty():
            print("No files found for dataset: dts.mu2e.X.Y.art")
            return {'ok': True}

        out, err = io.StringIO(), io.StringIO()
        with patch.object(sys, 'stdout', out), patch.object(sys, 'stderr', err):
            result = chatty()
        self.assertEqual(result, {'ok': True})
        self.assertEqual(out.getvalue(), '')
        self.assertIn('No files found', err.getvalue())

    def test_classify_import_error_is_env_missing(self):
        """env_missing was declared in ERROR_KINDS and produced nowhere."""
        from prodtools_mcp.adapters import classify_catalog_error
        err = classify_catalog_error(
            ImportError("No module named 'samweb_client'"), 'boom')
        self.assertEqual(err.kind, 'env_missing')
        self.assertIn('muse setup ops', err.remedy)

    def test_classify_auth_markers_are_auth_expired(self):
        """An expired token used to arrive as catalog_unavailable with
        'Check SAM availability' — advice about a service that is fine.
        These are word-only markers on exception types with no `.code`
        (see test_classify_code_takes_priority_over_text for the
        SAMWebHTTPError path). Bare digits ('401'/'403') are deliberately
        NOT markers here: see
        test_classify_5xx_with_digits_in_url_is_not_auth_expired."""
        from prodtools_mcp.adapters import classify_catalog_error
        for msg in ('HTTPError: 403 Forbidden', 'Unauthorized',
                    'bearer token has expired',
                    'Authentication failed: credential expired'):
            err = classify_catalog_error(RuntimeError(msg), 'boom')
            self.assertEqual(err.kind, 'auth_expired', msg)
            self.assertIn('own shell', err.remedy)

    def test_classify_falls_back_to_catalog_unavailable(self):
        from prodtools_mcp.adapters import classify_catalog_error
        err = classify_catalog_error(
            RuntimeError('Connection refused'), 'boom')
        self.assertEqual(err.kind, 'catalog_unavailable')

    def test_classify_code_401_and_403_are_auth_expired(self):
        """SAMWebHTTPError.code is a plain int (verified against the
        installed ops env 2026-07-26); classification must key on it."""
        from prodtools_mcp.adapters import classify_catalog_error

        class FakeHTTPError(Exception):
            def __init__(self, code, msg):
                super().__init__(msg)
                self.code = code

        for code in (401, 403):
            err = classify_catalog_error(
                FakeHTTPError(code, 'Forbidden'), 'boom')
            self.assertEqual(err.kind, 'auth_expired', code)

    def test_classify_5xx_with_digits_in_url_is_not_auth_expired(self):
        """Regression pin. Mu2e filenames routinely contain digit runs
        that collide with '401'/'403' — e.g. a sequencer number in
        dig.mu2e.FlatGamma.MDC2025au_best_v1_3.001430_00004031.art — and
        SAMWebHTTPError.__str__ embeds the URL (and therefore the
        filename) for every 5xx. A plain SAM outage on such a file must
        stay catalog_unavailable, not send the operator to renew a fine
        token."""
        from prodtools_mcp.adapters import classify_catalog_error

        class FakeHTTPError(Exception):
            def __init__(self, code, msg):
                super().__init__(msg)
                self.code = code

        msg = ('HTTP error: 503 Service Unavailable\n'
               'URL: https://samweb.fnal.gov/sam/mu2e/api/files/name/'
               'dig.mu2e.FlatGamma.MDC2025au_best_v1_3.'
               '001430_00004031.art/metadata')
        err = classify_catalog_error(FakeHTTPError(503, msg), 'boom')
        self.assertEqual(err.kind, 'catalog_unavailable')

    def test_classify_no_code_text_fallback_still_catches_auth(self):
        """A 4xx-style failure that arrives as some other exception type
        (no `.code`) with an unambiguous word marker must still classify
        as auth_expired."""
        from prodtools_mcp.adapters import classify_catalog_error
        err = classify_catalog_error(
            RuntimeError('Authentication failed: credential expired'),
            'boom')
        self.assertEqual(err.kind, 'auth_expired')

    def test_safe_tool_preserves_name(self):
        from prodtools_mcp.adapters import safe_tool

        @safe_tool
        def my_tool():
            """Docstring survives."""
            return {}
        self.assertEqual(my_tool.__name__, 'my_tool')
        self.assertEqual(my_tool.__doc__, 'Docstring survives.')


# ---------------------------------------------------------------------------
# MCP read-only ledger
# ---------------------------------------------------------------------------

class TestMcpLedgerRo(unittest.TestCase):
    def _make_db(self, tmpdir):
        """Build a real ledger via the writer, so the read path is tested
        against the actual schema rather than a hand-rolled copy."""
        from utils import submission_ledger
        db = os.path.join(tmpdir, 'ledger.db')
        entry = {'njobs': 4000, 'outputs': [
            {'dataset': 'dig.mu2e.FlatGamma.MDC2025au_best_v1_3.art',
             'location': 'tape'}]}
        cid = submission_ledger.create_campaign(
            db, tarball='cnf.mu2e.FlatGamma.MDC2025au_best_v1_3.0.tar',
            entry=entry, slice_size=500, origin='/tmp/map_au.json')
        submission_ledger.record_submission(
            db, tarball='cnf.mu2e.FlatGamma.MDC2025au_best_v1_3.0.tar',
            entry=entry, indices=[0, 1, 2], jobsub_id='29308498.0@sched',
            cluster_id='29308498', origin='/tmp/map_au.json')
        return db, cid

    def test_campaigns_returns_parsed_entry(self):
        from prodtools_mcp import ledger_ro
        with tempfile.TemporaryDirectory() as td:
            db, cid = self._make_db(td)
            camps = ledger_ro.campaigns(db)
        self.assertEqual(len(camps), 1)
        self.assertEqual(camps[0]['id'], cid)
        self.assertEqual(camps[0]['slice_size'], 500)
        self.assertIsInstance(camps[0]['entry'], dict)
        self.assertEqual(camps[0]['entry']['njobs'], 4000)

    def test_campaigns_filters_by_state(self):
        from prodtools_mcp import ledger_ro
        with tempfile.TemporaryDirectory() as td:
            db, _ = self._make_db(td)
            self.assertEqual(len(ledger_ro.campaigns(db, state='active')), 1)
            self.assertEqual(len(ledger_ro.campaigns(db, state='complete')), 0)

    def test_rows_returns_parsed_indices_and_cluster(self):
        from prodtools_mcp import ledger_ro
        with tempfile.TemporaryDirectory() as td:
            db, _ = self._make_db(td)
            rows = ledger_ro.rows(db)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['indices'], [0, 1, 2])
        self.assertEqual(rows[0]['cluster_id'], '29308498')

    def test_issues_no_ddl(self):
        """The writer's _connect runs CREATE statements on every connect;
        the read path must not, or a read-only DB raises OperationalError."""
        from prodtools_mcp import ledger_ro
        seen = []
        real_connect = sqlite3.connect

        class ConnectionSpy:
            def __init__(self, con):
                object.__setattr__(self, '_con', con)
            def execute(self, sql, *rest):
                seen.append(sql)
                return self._con.execute(sql, *rest)
            def __getattr__(self, name):
                return getattr(self._con, name)
            def __setattr__(self, name, value):
                if name == '_con':
                    object.__setattr__(self, name, value)
                else:
                    setattr(self._con, name, value)

        def spy_connect(*a, **kw):
            con = real_connect(*a, **kw)
            return ConnectionSpy(con)

        with tempfile.TemporaryDirectory() as td:
            db, _ = self._make_db(td)
            with patch.object(sqlite3, 'connect', spy_connect):
                ledger_ro.campaigns(db)
        self.assertTrue(seen, "expected at least one statement")
        for sql in seen:
            self.assertNotIn('CREATE', sql.upper())

    def test_snapshot_returns_both_tables(self):
        from prodtools_mcp import ledger_ro
        with tempfile.TemporaryDirectory() as td:
            db, cid = self._make_db(td)
            camps, subs = ledger_ro.snapshot(db)
        self.assertEqual([c['id'] for c in camps], [cid])
        self.assertEqual(camps[0]['entry']['njobs'], 4000)
        self.assertEqual(subs[0]['indices'], [0, 1, 2])

    def test_snapshot_uses_one_connection_and_one_transaction(self):
        """campaigns() + rows() are two snapshots on two connections. The
        cron commits record_submission and advance_campaign separately, so
        a read between them shows a cursor that disagrees with the rows."""
        from prodtools_mcp import ledger_ro
        opened, sql_seen = [], []
        real_connect = sqlite3.connect

        class ConnectionSpy:
            def __init__(self, con):
                object.__setattr__(self, '_con', con)
            def execute(self, sql, *rest):
                sql_seen.append(sql)
                return self._con.execute(sql, *rest)
            def __getattr__(self, name):
                return getattr(self._con, name)
            def __setattr__(self, name, value):
                if name == '_con':
                    object.__setattr__(self, name, value)
                else:
                    setattr(self._con, name, value)

        def spy_connect(*a, **kw):
            opened.append(a[0])
            return ConnectionSpy(real_connect(*a, **kw))

        with tempfile.TemporaryDirectory() as td:
            db, _ = self._make_db(td)
            with patch.object(sqlite3, 'connect', spy_connect):
                ledger_ro.snapshot(db)
        self.assertEqual(len(opened), 1, f'expected one connection: {opened}')
        self.assertIn('BEGIN', sql_seen)
        self.assertLess(sql_seen.index('BEGIN'),
                        min(i for i, s in enumerate(sql_seen)
                            if s.startswith('SELECT')))
        for sql in sql_seen:
            self.assertNotIn('CREATE', sql.upper())

    def test_missing_db_is_catalog_unavailable(self):
        from prodtools_mcp import ledger_ro
        from prodtools_mcp.adapters import ToolError
        with self.assertRaises(ToolError) as ctx:
            ledger_ro.campaigns('/nonexistent/path/ledger.db')
        self.assertEqual(ctx.exception.kind, 'catalog_unavailable')

    def test_snapshot_on_broken_db_is_catalog_unavailable(self):
        from prodtools_mcp import ledger_ro
        from prodtools_mcp.adapters import ToolError
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'empty.db')
            sqlite3.connect(db).close()      # exists, but has no tables
            with self.assertRaises(ToolError) as ctx:
                ledger_ro.snapshot(db)
        self.assertEqual(ctx.exception.kind, 'catalog_unavailable')

    def test_operational_error_becomes_catalog_unavailable(self):
        """A DB missing an expected object must surface as a typed error,
        not an OperationalError traceback."""
        from prodtools_mcp import ledger_ro
        from prodtools_mcp.adapters import ToolError
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'empty.db')
            sqlite3.connect(db).close()      # exists, but has no tables
            with self.assertRaises(ToolError) as ctx:
                ledger_ro.campaigns(db)
        self.assertEqual(ctx.exception.kind, 'catalog_unavailable')

    def test_corrupt_db_becomes_catalog_unavailable(self):
        """A truncated/corrupt ledger raises sqlite3.DatabaseError, which
        is NOT an OperationalError — it must still surface as a typed
        error rather than a raw traceback."""
        from prodtools_mcp import ledger_ro
        from prodtools_mcp.adapters import ToolError
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'corrupt.db')
            with open(db, 'wb') as fh:
                fh.write(b'this is not a sqlite database at all')
            with self.assertRaises(ToolError) as ctx:
                ledger_ro.campaigns(db)
        self.assertEqual(ctx.exception.kind, 'catalog_unavailable')


# ---------------------------------------------------------------------------
# MCP status tools
# ---------------------------------------------------------------------------

def _job(status, code=None, reason=None):
    """One fake HTCondor ClassAd projection, in the {ClusterId,
    JobStatus, HoldReasonCode, HoldReason} shape condor.query_owner_jobs
    returns."""
    return {'JobStatus': status, 'HoldReasonCode': code, 'HoldReason': reason}


class TestMcpQueueBlock(unittest.TestCase):
    def test_unknown_omits_counts_entirely(self):
        """A failed/untrustworthy HTCondor query must NOT serialize as
        running:0 — that reads as 'drained' and could trigger recovery
        against live jobs. This is the single most important behavior
        in the server — keep it passing across any change to the
        underlying queue source."""
        from prodtools_mcp.tools.status import queue_block
        block = queue_block(['29308498'], None)
        self.assertEqual(block['state'], 'unknown')
        self.assertIn('reason', block)
        for key in ('running', 'idle', 'held'):
            self.assertNotIn(key, block)

    def test_counts_by_job_status(self):
        from prodtools_mcp.tools.status import queue_block
        block = queue_block(['29308498'], {'29308498': [
            _job(2), _job(2), _job(1), _job(5, code=34, reason='held one'),
        ]})
        self.assertEqual(block['state'], 'known')
        self.assertEqual(block['running'], 2)
        self.assertEqual(block['idle'], 1)
        self.assertEqual(block['held'], 1)

    def test_absent_cluster_is_zero_not_unknown(self):
        """A genuinely drained cluster is a real zero, distinct from
        an unknown snapshot."""
        from prodtools_mcp.tools.status import queue_block
        block = queue_block(['29308498'], {'99999999': [_job(2)]})
        self.assertEqual(block['state'], 'known')
        self.assertEqual(block['running'], 0)
        self.assertEqual(block['idle'], 0)
        self.assertEqual(block['held'], 0)
        self.assertEqual(block['clusters'], [])
        self.assertNotIn('hold_reasons', block)

    def test_drained_cluster_is_known_with_real_zeros(self):
        """An empty snapshot (no cluster has any job left) is a genuine
        drain, not 'unknown' — 'unknown' and 'drained' must stay
        distinguishable."""
        from prodtools_mcp.tools.status import queue_block
        block = queue_block(['29308498'], {})
        self.assertEqual(block['state'], 'known')
        self.assertEqual(block['running'], 0)
        self.assertEqual(block['idle'], 0)
        self.assertEqual(block['held'], 0)
        self.assertEqual(block['clusters'], [])
        self.assertNotIn('hold_reasons', block)

    def test_held_zero_omits_hold_reasons_key(self):
        """No empty list sitting around to be misread as data."""
        from prodtools_mcp.tools.status import queue_block
        block = queue_block(['1'], {'1': [_job(2), _job(1)]})
        self.assertEqual(block['held'], 0)
        self.assertNotIn('hold_reasons', block)

    def test_hold_reasons_present_and_sorted_by_count(self):
        from prodtools_mcp.tools.status import queue_block
        block = queue_block(['1'], {'1': [
            _job(5, code=34, reason='Error from slot1_1@fnpc1.fnal.gov: '
                                     'Docker job has gone over memory '
                                     'limit of 2000 Mb'),
            _job(5, code=34, reason='Error from slot1_2@fnpc2.fnal.gov: '
                                     'Docker job has gone over memory '
                                     'limit of 2000 Mb'),
            _job(5, code=12, reason='via condor_rm (by user oksuzian)'),
            _job(5, code=34, reason='Error from slot1_3@fnpc3.fnal.gov: '
                                     'Docker job has gone over memory '
                                     'limit of 2000 Mb'),
        ]})
        self.assertEqual(block['held'], 4)
        reasons = block['hold_reasons']
        self.assertEqual([r['code'] for r in reasons], [34, 12])
        self.assertEqual(reasons[0]['count'], 3)
        self.assertEqual(reasons[1]['count'], 1)
        self.assertIn('memory limit', reasons[0]['example'])

    def test_hold_reason_aggregation_trap_by_code_not_text(self):
        """THE lesson: several held jobs sharing one HoldReasonCode but
        each with a DIFFERENT reason string (real data embeds the slot
        and host, so every string is unique) must collapse to ONE
        entry with the right count — not N entries of 1. Aggregating by
        the HoldReason text instead of the code returns garbage."""
        from prodtools_mcp.tools.status import queue_block
        jobs = [
            _job(5, code=34,
                reason=f'Error from slot1_{i}@fnpc191{i}.fnal.gov: Docker '
                       f'job has gone over memory limit of 2000 Mb')
            for i in range(6)
        ]
        block = queue_block(['1'], {'1': jobs})
        self.assertEqual(block['held'], 6)
        self.assertEqual(len(block['hold_reasons']), 1)
        self.assertEqual(block['hold_reasons'][0]['code'], 34)
        self.assertEqual(block['hold_reasons'][0]['count'], 6)

    def test_default_clusters_fn_delegates_to_condor_module(self):
        """The wiring point queue_block callers use — must call the new
        independent condor.py path, not utils.submissions."""
        from prodtools_mcp.tools import status
        from prodtools_mcp import condor
        with patch.object(condor, 'query_owner_jobs',
                          return_value=({'1': [_job(2)]}, None)) as mock:
            result = status._default_clusters_fn()
        mock.assert_called_once_with(condor.OWNER)
        self.assertEqual(result, ({'1': [_job(2)]}, None))


class TestMcpCondor(unittest.TestCase):
    """condor.py: the MCP server's own in-process HTCondor ClassAd
    query path, independent of utils.submissions.live_clusters (which
    backs the live production cron and is not touched here)."""

    def test_only_jobsub_schedds_are_kept(self):
        """The pool advertises 8 daemons; only the ~6 whose Name starts
        with 'jobsub' are the schedds that carry mu2epro's jobs."""
        from prodtools_mcp import condor
        ads = [{'Name': 'jobsub01.fnal.gov'}, {'Name': 'jobsub04.fnal.gov'},
              {'Name': 'collector01.fnal.gov'},
              {'Name': 'negotiator.fnal.gov'}]
        kept = [a['Name'] for a in ads if condor._is_jobsub_schedd(a)]
        self.assertEqual(kept, ['jobsub01.fnal.gov', 'jobsub04.fnal.gov'])

    def test_query_schedd_filters_server_side_with_projection(self):
        """Owner and JobStatus belong in the CONSTRAINT (server-side),
        not fetched wholesale and filtered in Python; and only the
        four needed attributes are projected — never a whole ClassAd."""
        from prodtools_mcp import condor
        calls = []

        class FakeSchedd:
            def __init__(self, ad):
                calls.append(('Schedd', ad))

            def query(self, constraint, projection=None):
                calls.append(('query', constraint, projection))
                return []

        fake_htcondor = types.SimpleNamespace(Schedd=FakeSchedd)
        with patch.dict(sys.modules, {'htcondor2': fake_htcondor}):
            result = condor._query_schedd('sched-a.fnal.gov', 'mu2epro')

        self.assertEqual(result, [])
        self.assertEqual(calls[0], ('Schedd', 'sched-a.fnal.gov'))
        _, constraint, projection = calls[1]
        self.assertIn('Owner=="mu2epro"', constraint)
        self.assertIn(f'JobStatus=={condor.IDLE}', constraint)
        self.assertIn(f'JobStatus=={condor.RUNNING}', constraint)
        self.assertIn(f'JobStatus=={condor.HELD}', constraint)
        self.assertEqual(set(projection),
                         {'ClusterId', 'JobStatus', 'HoldReasonCode',
                          'HoldReason'})

    def test_locate_jobsub_schedds_uses_v2_collector_and_filters(self):
        """_locate_jobsub_schedds must call htcondor2.Collector().locateAll
        with the v2 enum spelling DaemonType (not the v1 DaemonTypes --
        one character, and the exact bug that caused the incident: it
        would import fine and only fail against the real 25.0.12 pool).
        The fake below defines ONLY DaemonType, so `htcondor.DaemonTypes`
        raises AttributeError and a regression to the v1 spelling fails
        this test instead of only failing in production."""
        from prodtools_mcp import condor

        schedd_sentinel = object()
        locate_calls = []

        class FakeCollector:
            def locateAll(self, daemon_type):
                locate_calls.append(daemon_type)
                return [{'Name': 'jobsub01.fnal.gov'},
                       {'Name': 'jobsub04.fnal.gov'},
                       {'Name': 'collector01.fnal.gov'},
                       {'Name': 'negotiator.fnal.gov'}]

        fake_daemon_type = types.SimpleNamespace(Schedd=schedd_sentinel)
        fake_htcondor = types.SimpleNamespace(
            Collector=FakeCollector, DaemonType=fake_daemon_type)
        with patch.dict(sys.modules, {'htcondor2': fake_htcondor}):
            result = condor._locate_jobsub_schedds()

        self.assertEqual([ad['Name'] for ad in result],
                         ['jobsub01.fnal.gov', 'jobsub04.fnal.gov'])
        self.assertEqual(locate_calls, [schedd_sentinel])

    def test_hold_reasons_groups_by_code_not_text(self):
        from prodtools_mcp import condor
        jobs = [
            {'HoldReasonCode': 34,
             'HoldReason': 'Error from slot1_1@a.fnal.gov: Docker job has '
                           'gone over memory limit of 2000 Mb'},
            {'HoldReasonCode': 34,
             'HoldReason': 'Error from slot1_2@b.fnal.gov: Docker job has '
                           'gone over memory limit of 2000 Mb'},
            {'HoldReasonCode': 12, 'HoldReason': 'via condor_rm'},
        ]
        result = condor.hold_reasons(jobs)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], {'code': 34, 'count': 2,
                                     'example': jobs[0]['HoldReason']})
        self.assertEqual(result[1]['code'], 12)
        self.assertEqual(result[1]['count'], 1)

    def test_query_owner_jobs_aggregates_across_schedds(self):
        from prodtools_mcp import condor

        def schedds():
            return ['sched-a', 'sched-b']

        def query(sd, owner):
            self.assertEqual(owner, 'mu2epro')
            if sd == 'sched-a':
                return [{'ClusterId': 1, 'JobStatus': 2,
                        'HoldReasonCode': None, 'HoldReason': None}]
            return [{'ClusterId': 1, 'JobStatus': 5,
                    'HoldReasonCode': 34, 'HoldReason': 'oom'}]

        clusters, reason = condor.query_owner_jobs(schedds_fn=schedds,
                                                   query_fn=query)
        self.assertEqual(len(clusters['1']), 2)

    def test_schedd_discovery_failure_is_unknown(self):
        from prodtools_mcp import condor

        def boom():
            raise RuntimeError('collector unreachable')

        clusters, reason = condor.query_owner_jobs(schedds_fn=boom,
                                                   query_fn=None)
        self.assertIsNone(clusters)

    def test_one_unreachable_schedd_makes_the_whole_result_unknown(self):
        """A per-schedd failure must not silently drop that schedd's
        jobs — an undercount reads as 'drained' and could trigger
        recovery against jobs that are still live there. Skip that
        schedd's data, but the OVERALL result must come back untrusted,
        not a partial count from the schedds that did answer."""
        from prodtools_mcp import condor

        def schedds():
            return ['sched-a', 'sched-b']

        def query(sd, owner):
            if sd == 'sched-a':
                raise RuntimeError('timed out talking to sched-a')
            return [{'ClusterId': 1, 'JobStatus': 2,
                    'HoldReasonCode': None, 'HoldReason': None}]

        clusters, reason = condor.query_owner_jobs(schedds_fn=schedds,
                                                   query_fn=query)
        self.assertIsNone(clusters)

    def test_query_owner_jobs_bounds_wall_clock(self):
        """A sync tool blocks its caller until it returns — a hung
        schedd must not wedge the call. Bounded by `timeout`;
        a timeout is 'unknown' (None), never zero, and the call must
        actually return close to the bound, not the full hang."""
        import time
        from prodtools_mcp import condor

        def schedds():
            return ['sched-a']

        def hang(sd, owner):
            time.sleep(5)
            return []

        start = time.monotonic()
        clusters, reason = condor.query_owner_jobs(
            timeout=0.2, schedds_fn=schedds, query_fn=hang)
        elapsed = time.monotonic() - start
        self.assertIsNone(clusters)
        self.assertLess(elapsed, 2.0)

    def test_parse_version_reads_the_condor_banner(self):
        from prodtools_mcp import condor
        banner = ('$CondorVersion: 25.0.12 2026-07-07 BuildID: 930047 '
                  'PackageID: 25.0.12-1 $')
        self.assertEqual(condor.parse_version(banner), '25.0.12')
        self.assertEqual(condor.series(condor.parse_version(banner)), '25.0')

    def test_parse_version_returns_none_rather_than_guessing(self):
        """No fallbacks: an unparseable banner is unknown, not a
        plausible default. A wrong-but-plausible version is exactly how
        the stale pin went unnoticed."""
        from prodtools_mcp import condor
        self.assertIsNone(condor.parse_version('not a banner'))
        self.assertIsNone(condor.parse_version(''))
        self.assertIsNone(condor.parse_version(None))
        self.assertIsNone(condor.series(None))

    def test_node_version_banner_never_inherits_stdin(self):
        """_node_version_banner shells to condor_version(1) INSIDE the MCP
        server, whose own stdin IS the JSON-RPC channel — a child that
        inherits it can consume protocol bytes and hang the server. This
        is the same trap the write server's runner.run_cli pins near
        test/test_unit.py:10189 (`push_cnf`'s `cat $BEARER_TOKEN_FILE`
        hung on inherited stdin for 30 minutes). Also pins the absolute
        path: `muse setup ops` rewrites PATH, so a bare 'condor_version'
        would resolve to nothing or the wrong binary."""
        from prodtools_mcp import condor
        with patch('subprocess.run') as run:
            run.return_value = SimpleNamespace(
                returncode=0,
                stdout=b'$CondorVersion: 25.0.12 2026-07-07 $')
            condor._node_version_banner()
        self.assertEqual(run.call_args.args[0], [condor.CONDOR_VERSION_BIN])
        self.assertEqual(run.call_args.args[0], ['/usr/bin/condor_version'])
        self.assertEqual(run.call_args.kwargs.get('stdin'),
                         subprocess.DEVNULL,
                         'child stdin must be DEVNULL, never inherited')

    def test_version_report_matching_series_has_no_reason(self):
        from prodtools_mcp import condor
        report = condor.version_report(
            client_fn=lambda: '$CondorVersion: 25.0.9 2026-01-01 $',
            node_fn=lambda: '$CondorVersion: 25.0.12 2026-07-07 $')
        self.assertEqual(report['client'], '25.0.9')
        self.assertEqual(report['node'], '25.0.12')
        self.assertIs(report['series_match'], True)
        self.assertIsNone(report['reason'])

    def test_version_report_names_both_versions_on_mismatch(self):
        """The reason must carry BOTH numbers: this exact mismatch
        (client 23.0.28, node 25.0.12) surfaced as an authentication
        failure at the collector and cost a full diagnosis session."""
        from prodtools_mcp import condor
        report = condor.version_report(
            client_fn=lambda: '$CondorVersion: 23.0.28 2025-08-21 $',
            node_fn=lambda: '$CondorVersion: 25.0.12 2026-07-07 $')
        self.assertIs(report['series_match'], False)
        self.assertIn('23.0.28', report['reason'])
        self.assertIn('25.0.12', report['reason'])

    def test_version_report_unknown_side_is_none_not_a_match(self):
        """series_match must be None, never True, when a side is
        unreadable — claiming agreement we cannot verify is the failure
        this whole change exists to prevent."""
        from prodtools_mcp import condor

        def boom():
            raise RuntimeError('condor_version not found')

        report = condor.version_report(
            client_fn=lambda: '$CondorVersion: 25.0.12 2026-07-07 $',
            node_fn=boom)
        self.assertEqual(report['client'], '25.0.12')
        self.assertIsNone(report['node'])
        self.assertIsNone(report['series_match'])
        self.assertIn('condor_version not found', report['reason'])

    def test_version_report_client_import_failure_is_reported(self):
        from prodtools_mcp import condor

        def boom():
            raise ModuleNotFoundError("No module named 'htcondor2'")

        report = condor.version_report(
            client_fn=boom,
            node_fn=lambda: '$CondorVersion: 25.0.12 2026-07-07 $')
        self.assertIsNone(report['client'])
        self.assertIsNone(report['series_match'])
        self.assertIn('htcondor2', report['reason'])

    def test_query_owner_jobs_returns_a_reason_for_discovery_failure(self):
        """The bare `return None` threw away the only evidence of what
        went wrong. A collector authentication failure must not be
        reported as a schedd problem."""
        from prodtools_mcp import condor

        def boom():
            raise RuntimeError('Failed communication with collector')

        clusters, reason = condor.query_owner_jobs(schedds_fn=boom,
                                                   query_fn=None)
        self.assertIsNone(clusters)
        self.assertIn('Failed communication with collector', reason)

    def test_query_owner_jobs_reason_for_a_failing_schedd(self):
        from prodtools_mcp import condor

        def schedds():
            return ['sched-a', 'sched-b']

        def query(sd, owner):
            if sd == 'sched-a':
                raise RuntimeError('timed out talking to sched-a')
            return [{'ClusterId': 1, 'JobStatus': 2,
                     'HoldReasonCode': None, 'HoldReason': None}]

        clusters, reason = condor.query_owner_jobs(schedds_fn=schedds,
                                                   query_fn=query)
        self.assertIsNone(clusters)
        self.assertIn('timed out talking to sched-a', reason)

    def test_query_owner_jobs_reason_names_the_schedd_not_the_whole_ad(self):
        """schedds_fn() returns real htcondor2 location ClassAds in
        production, whose str() dumps the ENTIRE ad (Name, MyAddress,
        CondorVersion, ...) — hundreds of bytes that must not land in
        `reason`. A ClassAd-like double (dict-shaped, has .get) must
        contribute only its Name."""
        from prodtools_mcp import condor

        class FakeAd(dict):
            def __str__(self):
                return ('[ Name = "jobsub01.fnal.gov"; MyAddress = '
                       '"<127.0.0.1:9618>"; CondorVersion = "..."; '
                       'DaemonStartTime = 1234567890 ]')

        sched_a = FakeAd(Name='jobsub01.fnal.gov')

        def schedds():
            return [sched_a]

        def query(sd, owner):
            raise RuntimeError('timed out')

        clusters, reason = condor.query_owner_jobs(schedds_fn=schedds,
                                                   query_fn=query)
        self.assertIsNone(clusters)
        self.assertIn('jobsub01.fnal.gov', reason)
        self.assertNotIn('MyAddress', reason)
        self.assertNotIn('CondorVersion', reason)

    def test_query_owner_jobs_reason_for_a_timeout(self):
        import time
        from prodtools_mcp import condor

        def schedds():
            return ['sched-a']

        def hang(sd, owner):
            time.sleep(5)
            return []

        clusters, reason = condor.query_owner_jobs(
            timeout=0.2, schedds_fn=schedds, query_fn=hang)
        self.assertIsNone(clusters)
        self.assertIn('timed out', reason.lower())

    def test_query_owner_jobs_success_has_no_reason(self):
        from prodtools_mcp import condor

        def schedds():
            return ['sched-a']

        def query(sd, owner):
            return [{'ClusterId': 1, 'JobStatus': 2,
                     'HoldReasonCode': None, 'HoldReason': None}]

        clusters, reason = condor.query_owner_jobs(schedds_fn=schedds,
                                                   query_fn=query)
        self.assertEqual(len(clusters['1']), 1)
        self.assertIsNone(reason)

    def test_no_schedds_found_is_untrusted_with_a_reason(self):
        from prodtools_mcp import condor
        clusters, reason = condor.query_owner_jobs(
            schedds_fn=lambda: [], query_fn=None)
        self.assertIsNone(clusters)
        self.assertIn('no jobsub schedds', reason)

    def test_get_server_info_reports_the_condor_versions(self):
        """A reader must be able to see the client/node agreement in one
        cheap call, without having to provoke a failure first."""
        from prodtools_mcp import condor, server
        fake = {'client': '25.0.12', 'node': '25.0.12',
                'series_match': True, 'reason': None}
        with patch.object(condor, 'version_report', return_value=fake):
            info = server.get_server_info()
        self.assertEqual(info['condor'], fake)

    def test_get_server_info_survives_an_unreadable_version(self):
        """get_server_info must not raise just because condor_version is
        missing — it is the tool a reader calls when things are broken."""
        from prodtools_mcp import condor, server
        with patch.object(condor, 'version_report',
                          side_effect=RuntimeError('no such file')):
            info = server.get_server_info()
        self.assertIsNone(info['condor']['series_match'])
        self.assertIn('no such file', info['condor']['reason'])

    def test_module_imports_with_no_htcondor_wheel_anywhere(self):
        """condor.py must import cleanly even when neither htcondor nor
        htcondor2 is importable, i.e. no `import htcondor[2]` may sit at
        module scope.

        This node cannot exercise that by just running the suite: it has
        the python3-condor-25.0.12 RPM installed, which puts htcondor2 on
        system python3.9's path, so `import prodtools_mcp.condor` would
        succeed here even if someone hoisted the import to module level.
        The guard has to be explicit, forcing both import names to fail
        and confirming the module still loads."""
        import importlib
        from prodtools_mcp import condor
        try:
            with patch.dict(sys.modules, {'htcondor': None,
                                          'htcondor2': None}):
                importlib.reload(condor)
        finally:
            # Leave a clean, non-mocked module behind for the rest of
            # the suite regardless of whether the reload above raised.
            importlib.reload(condor)


class TestMcpCampaignStatus(unittest.TestCase):
    DRAIN_TARBALL = 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar'
    # A draining entry: input_pattern, NO njobs, and an outputs glob that
    # is a worker cwd filename pattern rather than a dataset name.
    DRAIN_ENTRY = {'tarball': DRAIN_TARBALL, 'inloc': 'tape',
                   'input_pattern': 'dig.mu2e.%.MDC2025au_best_v1_5.art',
                   'outputs': [{'dataset': 'mcs.*.art', 'location': 'tape'}]}

    def _make_draining_db(self, tmpdir):
        """Draining campaign with 3 input files dispatched: 2 of desc
        AAA, 1 of desc BBB — so per-dataset dispatched counts differ."""
        from utils import submission_ledger
        db = os.path.join(tmpdir, 'ledger.db')
        submission_ledger.create_campaign(
            db, tarball=self.DRAIN_TARBALL, entry=self.DRAIN_ENTRY,
            slice_size=500, origin='/tmp/map_drain.json')
        submission_ledger.record_submission(
            db, tarball=self.DRAIN_TARBALL, entry=self.DRAIN_ENTRY,
            indices=[_mk_file('AAA', 1), _mk_file('AAA', 2),
                     _mk_file('BBB', 1)],
            jobsub_id='29448530.0@sched', cluster_id='29448530',
            origin='/tmp/map_drain.json')
        return db

    def _make_db(self, tmpdir):
        from utils import submission_ledger
        db = os.path.join(tmpdir, 'ledger.db')
        entry = {'njobs': 4000, 'outputs': [
            {'dataset': 'dig.mu2e.FlatGamma.MDC2025au_best_v1_3.art',
             'location': 'tape'}]}
        submission_ledger.create_campaign(
            db, tarball='cnf.mu2e.FlatGamma.MDC2025au_best_v1_3.0.tar',
            entry=entry, slice_size=500, origin='/tmp/map_au.json')
        submission_ledger.record_submission(
            db, tarball='cnf.mu2e.FlatGamma.MDC2025au_best_v1_3.0.tar',
            entry=entry, indices=[0, 1], jobsub_id='29308498.0@sched',
            cluster_id='29308498', origin='/tmp/map_au.json')
        return db

    def test_ledger_only_when_no_campaign_named(self):
        """The bare call must not touch the network — otherwise a 23-row
        ledger fans out to one SAM count per output dataset."""
        from prodtools_mcp.tools import status

        def boom(*a, **kw):
            raise AssertionError("network call in ledger-only mode")

        with tempfile.TemporaryDirectory() as td:
            db = self._make_db(td)
            result = status.campaign_status(
                db_path=db, clusters_fn=boom, count_fn=boom)
        self.assertEqual(len(result['campaigns']), 1)
        camp = result['campaigns'][0]
        self.assertNotIn('queue', camp)
        self.assertNotIn('outputs', camp)
        self.assertEqual(camp['njobs'], 4000)
        self.assertEqual(camp['slice_size'], 500)

    def test_no_integer_entry_field(self):
        """The ledger stores the whole entry dict as entry_json; an index
        into the map is not recoverable, so we must not invent one."""
        from prodtools_mcp.tools import status
        with tempfile.TemporaryDirectory() as td:
            db = self._make_db(td)
            camp = status.campaign_status(db_path=db)['campaigns'][0]
        self.assertNotIn('entry', camp)

    def test_named_campaign_includes_queue_and_outputs(self):
        from prodtools_mcp.tools import status
        running_job = {'JobStatus': 2, 'HoldReasonCode': None,
                      'HoldReason': None}
        with tempfile.TemporaryDirectory() as td:
            db = self._make_db(td)
            result = status.campaign_status(
                campaign='MDC2025au', db_path=db,
                clusters_fn=lambda owner: (
                    {'29308498': [running_job, running_job]}, None),
                count_fn=lambda ds: 412,
                job_pars_fn=lambda tb: _IndexedPars(tb))
        camp = result['campaigns'][0]
        self.assertEqual(camp['queue']['running'], 2)
        out = camp['outputs']['datasets'][0]
        self.assertEqual(out['produced'], 412)
        self.assertEqual(out['expected_at_completion'], 4000)

    def test_outputs_report_submitted_alongside_njobs(self):
        """Every direct campaign is sliced. njobs alone under-reports a
        live one: cursor 500 of 4000 with all 500 landed is 100% of what
        is in flight, not 12.5%."""
        from utils import submission_ledger
        from prodtools_mcp.tools import status
        with tempfile.TemporaryDirectory() as td:
            db = self._make_db(td)
            camps = submission_ledger.all_campaigns(db)
            submission_ledger.advance_campaign(db, camps[0]['id'], 500)
            result = status.campaign_status(
                campaign='MDC2025au', db_path=db, include_queue=False,
                count_fn=lambda ds: 500,
                job_pars_fn=lambda tb: _IndexedPars(tb))
        out = result['campaigns'][0]['outputs']['datasets'][0]
        self.assertEqual(out['submitted'], 500)
        self.assertEqual(out['expected_at_completion'], 4000)
        self.assertEqual(out['produced'], 500)

    def test_indexed_outputs_name_real_datasets_not_the_worker_glob(self):
        """REGRESSION: an INDEXED entry's outputs[].dataset is a worker
        filename glob ('*.art') exactly as a draining one's is — every
        production map writes it (map_rmc_phasespace_au,
        map_extracted_reco_au, map_pitargetstops_run1bap, and the three
        Run1Bap campaigns of 2026-08-09 all do). Feeding it to a SAM
        dimension raised 'Parse error ... dh.dataset *.art', so this
        block had NEVER returned state='known' for any indexed campaign.
        The draining branch learned this first; the indexed branch was
        left behind."""
        from prodtools_mcp.tools import status
        asked = []

        def count_fn(ds):
            asked.append(ds)
            return 9

        with tempfile.TemporaryDirectory() as td:
            db = self._make_db(td)
            result = status.campaign_status(
                campaign='MDC2025au', db_path=db, include_queue=False,
                count_fn=count_fn,
                job_pars_fn=lambda tb: _IndexedPars(tb))
        block = result['campaigns'][0]['outputs']
        self.assertEqual(block['state'], 'known')
        self.assertNotIn('*.art', asked)
        self.assertEqual([d['dataset'] for d in block['datasets']],
                         ['dts.mu2e.CosmicCRY.MDC2025au.art'])
        self.assertEqual(block['datasets'][0]['produced'], 9)

    def test_indexed_outputs_unknown_when_the_cnf_is_unreadable(self):
        """The names now come from the cnf, so an unreadable cnf must
        report unknown — never an empty dataset list, which reads as
        'this campaign produces nothing'."""
        from prodtools_mcp.tools import status

        def boom(tarball):
            raise RuntimeError('tarball not locatable in SAM')

        with tempfile.TemporaryDirectory() as td:
            db = self._make_db(td)
            result = status.campaign_status(
                campaign='MDC2025au', db_path=db, include_queue=False,
                count_fn=lambda ds: 0, job_pars_fn=boom)
        block = result['campaigns'][0]['outputs']
        self.assertEqual(block['state'], 'unknown')
        self.assertNotIn('datasets', block)
        self.assertIn('not locatable', block['reason'])

    def test_draining_outputs_name_real_datasets_not_the_worker_glob(self):
        """REGRESSION: a draining entry's outputs[].dataset is the
        worker's cwd filename glob ('mcs.*.art'), not a dataset. Feeding
        it to a SAM dimension raised 'Parse error ... dh.dataset
        mcs.*.art', so EVERY draining campaign reported outputs
        state='unknown' and was invisible to campaign_status (observed
        live on production campaign 48, 2026-08-02)."""
        from prodtools_mcp.tools import status
        asked = []

        def count_fn(ds):
            asked.append(ds)
            return 7

        with tempfile.TemporaryDirectory() as td:
            db = self._make_draining_db(td)
            result = status.campaign_status(
                campaign_id=1, db_path=db, include_queue=False,
                count_fn=count_fn, job_pars_fn=lambda tb: _DrainPars(tb))
        block = result['campaigns'][0]['outputs']
        self.assertEqual(block['state'], 'known')
        self.assertNotIn('mcs.*.art', asked)
        self.assertEqual(
            sorted(d['dataset'] for d in block['datasets']),
            ['mcs.mu2e.AAA.MDC2025au_best_v1_5.art',
             'mcs.mu2e.BBB.MDC2025au_best_v1_5.art'])

    def test_draining_outputs_count_dispatched_per_dataset(self):
        """Two inputs of desc AAA and one of BBB were dispatched; the
        per-dataset denominator must follow the input desc, not the
        campaign total."""
        from prodtools_mcp.tools import status
        with tempfile.TemporaryDirectory() as td:
            db = self._make_draining_db(td)
            result = status.campaign_status(
                campaign_id=1, db_path=db, include_queue=False,
                count_fn=lambda ds: 0,
                job_pars_fn=lambda tb: _DrainPars(tb))
        got = {d['dataset']: d['dispatched']
               for d in result['campaigns'][0]['outputs']['datasets']}
        self.assertEqual(got['mcs.mu2e.AAA.MDC2025au_best_v1_5.art'], 2)
        self.assertEqual(got['mcs.mu2e.BBB.MDC2025au_best_v1_5.art'], 1)

    def test_draining_outputs_omit_expected_at_completion(self):
        """The input dataset is still growing, so no completion
        denominator exists. Emitting njobs (None) as a denominator would
        invite a division and a bogus percentage."""
        from prodtools_mcp.tools import status
        with tempfile.TemporaryDirectory() as td:
            db = self._make_draining_db(td)
            result = status.campaign_status(
                campaign_id=1, db_path=db, include_queue=False,
                count_fn=lambda ds: 3,
                job_pars_fn=lambda tb: _DrainPars(tb))
        for d in result['campaigns'][0]['outputs']['datasets']:
            self.assertNotIn('expected_at_completion', d)

    def test_draining_outputs_unreadable_cnf_is_unknown_not_zero(self):
        """Fail-closed, like the queue block: an unlocatable tarball must
        not render as produced=0, which reads as 'nothing landed' and
        could trigger a recovery pass against good data."""
        from prodtools_mcp.tools import status

        def boom(tarball):
            raise RuntimeError('tarball not locatable in SAM')

        with tempfile.TemporaryDirectory() as td:
            db = self._make_draining_db(td)
            result = status.campaign_status(
                campaign_id=1, db_path=db, include_queue=False,
                count_fn=lambda ds: 0, job_pars_fn=boom)
        block = result['campaigns'][0]['outputs']
        self.assertEqual(block['state'], 'unknown')
        self.assertNotIn('datasets', block)
        self.assertIn('not locatable', block['reason'])

    def test_draining_outputs_before_first_dispatch(self):
        """Day 1: the campaign exists but nothing has been handed to the
        grid, so no output dataset is nameable yet. That is 'known and
        empty', not 'unknown' — nothing failed."""
        from utils import submission_ledger
        from prodtools_mcp.tools import status
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'ledger.db')
            submission_ledger.create_campaign(
                db, tarball=self.DRAIN_TARBALL, entry=self.DRAIN_ENTRY,
                slice_size=500, origin='/tmp/map_drain.json')
            result = status.campaign_status(
                campaign_id=1, db_path=db, include_queue=False,
                count_fn=lambda ds: 0,
                job_pars_fn=lambda tb: _DrainPars(tb))
        block = result['campaigns'][0]['outputs']
        self.assertEqual(block['state'], 'known')
        self.assertEqual(block['datasets'], [])

    def test_row_counts_are_per_state_not_open_closed(self):
        """`exhausted` is where a human must take over. Bucketed as
        'closed' beside complete/recovered it was invisible."""
        from utils import submission_ledger
        from prodtools_mcp.tools import status
        tarball = 'cnf.mu2e.FlatGamma.MDC2025au_best_v1_3.0.tar'
        entry = {'njobs': 4000, 'outputs': []}
        with tempfile.TemporaryDirectory() as td:
            db = self._make_db(td)
            for state in ('complete', 'exhausted', 'exhausted'):
                rid = submission_ledger.record_submission(
                    db, tarball=tarball, entry=entry, indices=[9],
                    jobsub_id='1.0@s', cluster_id='1', origin='/tmp/m')
                submission_ledger.close_row(db, rid, state)
            result = status.campaign_status(
                campaign='MDC2025au', db_path=db, include_outputs=False,
                clusters_fn=lambda owner: (None, None))
        rows = result['campaigns'][0]['rows']
        self.assertEqual(rows['exhausted'], 2)
        self.assertEqual(rows['complete'], 1)
        self.assertEqual(rows['active'], 1)      # the one from _make_db
        self.assertEqual(rows['recovered'], 0)
        self.assertNotIn('closed', rows)

    def test_empty_ledger_is_empty_list_not_not_found(self):
        """A bare call and list_campaigns() must agree about 'nothing
        here': list_campaigns returns [], so this must not raise."""
        from utils import submission_ledger
        from prodtools_mcp.tools import status
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'empty.db')
            submission_ledger.all_campaigns(db)      # creates the schema
            result = status.campaign_status(db_path=db)
            listed = status.list_campaigns(db_path=db)
        self.assertEqual(result['campaigns'], [])
        self.assertEqual(listed['count'], 0)

    def test_named_campaign_on_empty_ledger_still_not_found(self):
        from utils import submission_ledger
        from prodtools_mcp.tools import status
        from prodtools_mcp.adapters import ToolError
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'empty.db')
            submission_ledger.all_campaigns(db)
            with self.assertRaises(ToolError) as ctx:
                status.campaign_status(campaign='MDC2025au', db_path=db)
        self.assertEqual(ctx.exception.kind, 'not_found')

    def test_output_count_failure_is_unknown_not_zero(self):
        from prodtools_mcp.tools import status

        def boom(ds):
            raise RuntimeError('SAM down')

        with tempfile.TemporaryDirectory() as td:
            db = self._make_db(td)
            result = status.campaign_status(
                campaign='MDC2025au', db_path=db,
                clusters_fn=lambda owner: (None, None), count_fn=boom)
        camp = result['campaigns'][0]
        self.assertEqual(camp['outputs']['state'], 'unknown')
        self.assertNotIn('datasets', camp['outputs'])

    def test_unknown_campaign_is_not_found(self):
        from prodtools_mcp.tools import status
        from prodtools_mcp.adapters import ToolError
        with tempfile.TemporaryDirectory() as td:
            db = self._make_db(td)
            with self.assertRaises(ToolError) as ctx:
                status.campaign_status(campaign='MDC9999zz', db_path=db)
        self.assertEqual(ctx.exception.kind, 'not_found')

    def test_shared_tarball_adds_conflation_note(self):
        """Rows correlate to a campaign by tarball only — no FK — so a
        reused tarball must be flagged, not silently merged."""
        from utils import submission_ledger
        from prodtools_mcp.tools import status
        with tempfile.TemporaryDirectory() as td:
            db = self._make_db(td)
            camps = submission_ledger.all_campaigns(db)
            submission_ledger.set_campaign_state(db, camps[0]['id'],
                                                 'complete')
            submission_ledger.create_campaign(
                db, tarball='cnf.mu2e.FlatGamma.MDC2025au_best_v1_3.0.tar',
                entry={'njobs': 4000, 'outputs': []}, slice_size=500)
            result = status.campaign_status(db_path=db)
        self.assertTrue(any('note' in c for c in result['campaigns']))

    def test_queue_block_carries_the_supplied_reason(self):
        from prodtools_mcp.tools.status import queue_block
        block = queue_block(['1'], None, 'mu2epro',
                            reason='collector rejected our token')
        self.assertEqual(block['state'], 'unknown')
        self.assertEqual(block['reason'], 'collector rejected our token')

    def test_queue_block_unknown_still_omits_every_count(self):
        """An unknown block must have no zero to misread as drained."""
        from prodtools_mcp.tools.status import queue_block
        block = queue_block(['1'], None, 'mu2epro', reason='anything')
        for key in ('running', 'idle', 'held', 'clusters'):
            self.assertNotIn(key, block)

    def test_queue_block_ignores_a_reason_on_success(self):
        """A reason belongs only to an untrusted result; a known block
        that carried one would invite branching on it."""
        from prodtools_mcp.tools.status import queue_block
        block = queue_block(['1'], {'1': [{'JobStatus': 2}]}, 'mu2epro',
                            reason='stale text')
        self.assertEqual(block['state'], 'known')
        self.assertNotIn('reason', block)

    def test_campaign_status_wires_the_clusters_fn_reason_into_queue_block(self):
        """The queue_reason bound from clusters_fn() at the campaign_status
        seam must reach queue_block()'s `reason` unchanged. Every other
        clusters_fn double in this suite returns (something, None), so a
        dropped `reason=queue_reason` at the call site is invisible to
        them — an `unknown` block would silently fall back to
        queue_block's fixed 'could not reach every schedd' text, which
        blames the wrong layer (see queue_block's docstring)."""
        from prodtools_mcp.tools import status
        distinctive = 'collector rejected SCITOKENS auth for client 23.0.28'
        with tempfile.TemporaryDirectory() as td:
            db = self._make_db(td)
            result = status.campaign_status(
                campaign='MDC2025au', db_path=db, include_outputs=False,
                clusters_fn=lambda owner: (None, distinctive))
        camp = result['campaigns'][0]
        self.assertEqual(camp['queue']['state'], 'unknown')
        self.assertEqual(camp['queue']['reason'], distinctive)


class TestMcpReadIdentity(unittest.TestCase):
    """`mine` selects whose ledger AND whose queue — from one resolution.

    The bug this closes: a run_as="self" campaign could be written but
    not watched, and the failure was silent. An empty answer from the
    production ledger is indistinguishable from "no campaigns", and a
    queue counted against the wrong account reads as "nothing running".
    That is the read-side twin of 171517f, where live_clusters()
    defaulted to mu2epro, a self tick did not find its own cluster in
    production's queue, and absent-from-snapshot read as 'drained'.
    """

    def setUp(self):
        from prodtools_mcp import condor
        from prodtools_mcp.tools import status
        self.status = status
        self.condor = condor

    def test_default_returns_no_ledger_path_so_the_env_override_lives(self):
        # NOT the resolved production path. ledger_ro.DEFAULT_DB is
        # os.environ.get('MU2E_SUBMISSION_DB', PRODUCTION_DB); returning
        # a concrete path here would reach the same file in the common
        # case while silently destroying the override.
        db, owner = self.status._resolve_identity(False)
        self.assertIsNone(db)
        self.assertEqual(owner, self.condor.OWNER)

    def test_mine_resolves_the_ledger_to_the_calling_account(self):
        with patch('getpass.getuser', return_value='alice'):
            db, owner = self.status._resolve_identity(True)
        self.assertEqual(db,
                         '/exp/mu2e/data/users/alice/prodtools/submissions.db')
        self.assertEqual(owner, 'alice')

    def test_ledger_and_queue_cannot_name_different_accounts(self):
        # The whole point of one resolution. If a later edit reads
        # os.environ['USER'] on one side and getpass on the other, these
        # two diverge and this test says so.
        with patch('getpass.getuser', return_value='bob'):
            db, owner = self.status._resolve_identity(True)
        self.assertIn('/users/%s/' % owner, db)

    def test_resolution_creates_nothing_on_disk(self):
        # A read-only server has no first run. resolve_db() in the CLI
        # mkdirs a derived path; this must not.
        with patch('getpass.getuser', return_value='nobody_qqq'):
            db, _ = self.status._resolve_identity(True)
        self.assertFalse(os.path.exists(os.path.dirname(db)))
        self.assertFalse(os.path.exists(db))

    def test_queue_block_names_the_account_it_counted(self):
        block = self.status.queue_block(['1'], {}, 'alice')
        self.assertEqual(block['owner'], 'alice')

    def test_queue_block_names_the_account_even_when_unknown(self):
        # A fail-closed 'unknown' from the WRONG account is the most
        # misleading answer this server can give; it must still say whose.
        block = self.status.queue_block(['1'], None, 'alice')
        self.assertEqual(block['state'], 'unknown')
        self.assertEqual(block['owner'], 'alice')

    def test_default_clusters_fn_passes_the_owner_to_condor(self):
        with patch.object(self.condor, 'query_owner_jobs',
                          return_value=(None, None)) as q:
            self.status._default_clusters_fn('alice')
        self.assertEqual(q.call_args.args[0], 'alice')

    def test_default_clusters_fn_without_an_owner_asks_for_production(self):
        with patch.object(self.condor, 'query_owner_jobs',
                          return_value=(None, None)) as q:
            self.status._default_clusters_fn()
        self.assertEqual(q.call_args.args[0], self.condor.OWNER)

    def test_default_clusters_fn_appends_version_note_on_mismatch(self):
        """A failed query plus a known client/pool version mismatch must
        surface BOTH the original failure text and the version report's
        own reason — the mismatch is the cause that looks least like
        itself (an auth failure at the collector), so it must not
        silently replace or drop the original text."""
        with patch.object(self.condor, 'query_owner_jobs',
                          return_value=(None, 'could not reach schedd')), \
             patch.object(self.condor, 'version_report',
                          return_value={'client': '23.0.28', 'node': '25.0.12',
                                       'series_match': False,
                                       'reason': 'client 23.0.28 vs node '
                                                 '25.0.12'}):
            clusters, reason = self.status._default_clusters_fn()
        self.assertIsNone(clusters)
        self.assertIn('could not reach schedd', reason)
        self.assertIn('client 23.0.28 vs node 25.0.12', reason)

    def test_default_clusters_fn_leaves_reason_alone_on_matching_series(self):
        """series_match True must not append a spurious note — the
        version report was consulted and cleared the client/pool
        mismatch as the cause, so the original reason stands alone."""
        with patch.object(self.condor, 'query_owner_jobs',
                          return_value=(None, 'timed out')), \
             patch.object(self.condor, 'version_report',
                          return_value={'client': '25.0.12', 'node': '25.0.12',
                                       'series_match': True, 'reason': None}):
            clusters, reason = self.status._default_clusters_fn()
        self.assertIsNone(clusters)
        self.assertEqual(reason, 'timed out')

    def test_default_clusters_fn_leaves_reason_alone_when_match_unknown(self):
        """series_match None (a version was unreadable) must NOT be
        treated as a mismatch any more than as a match — appending a
        note here would claim a comparison that was never actually
        made."""
        with patch.object(self.condor, 'query_owner_jobs',
                          return_value=(None, 'timed out')), \
             patch.object(self.condor, 'version_report',
                          return_value={'client': None, 'node': '25.0.12',
                                       'series_match': None,
                                       'reason': 'cannot compare HTCondor '
                                                 'versions'}):
            clusters, reason = self.status._default_clusters_fn()
        self.assertIsNone(clusters)
        self.assertEqual(reason, 'timed out')

    def test_campaign_status_threads_the_owner_into_the_queue_seam(self):
        # Asserted through the seam, not by reading the constant: a test
        # double that ignores identity would prove nothing about threading.
        seen = {}

        def fake_clusters(owner):
            seen['owner'] = owner
            return {}, None

        with tempfile.TemporaryDirectory() as td:
            db = TestMcpCampaignStatus()._make_db(td)
            result = self.status.campaign_status(
                campaign='MDC2025au', db_path=db, include_outputs=False,
                clusters_fn=fake_clusters)
        self.assertEqual(seen['owner'], self.condor.OWNER)
        self.assertEqual(result['campaigns'][0]['queue']['owner'],
                         self.condor.OWNER)

    def test_mine_true_threads_the_caller_into_both_axes(self):
        # Patches the ACCOUNT (getpass + ledger_for), not the resolver:
        # a mock that just hands back (db, 'alice') would still pass if
        # `mine` were silently dropped before reaching _resolve_identity,
        # or if the queue call reverted to condor.OWNER. Both axes must
        # actually be threaded from `mine=True` for this to go green.
        from utils import submission_ledger
        seen = {}

        def fake_clusters(owner):
            seen['owner'] = owner
            return {}, None

        with tempfile.TemporaryDirectory() as td:
            db = TestMcpCampaignStatus()._make_db(td)
            with patch('getpass.getuser', return_value='alice'), \
                 patch.object(submission_ledger, 'ledger_for',
                              return_value=db):
                result = self.status.campaign_status(
                    mine=True, campaign='MDC2025au', include_outputs=False,
                    clusters_fn=fake_clusters)
        self.assertEqual(seen['owner'], 'alice')          # kills Finding 2
        self.assertEqual(result['db_path'], db)           # kills Finding 1
        self.assertEqual(result['campaigns'][0]['queue']['owner'], 'alice')

    def test_an_explicit_db_path_still_wins(self):
        # db_path is the injection seam the existing tests use; `mine`
        # must not take it away from them. mine=True so resolved_db is
        # non-None and there is something for db_path to actually win
        # over — with mine defaulted, resolved_db is None and this test
        # is vacuous.
        with tempfile.TemporaryDirectory() as td:
            db = TestMcpCampaignStatus()._make_db(td)
            result = self.status.campaign_status(mine=True, db_path=db)
        self.assertEqual(result['db_path'], db)

    def test_list_campaigns_names_the_ledger_it_read(self):
        # Its silence was harmless only while there was one possible
        # answer. With `mine` there are two.
        with tempfile.TemporaryDirectory() as td:
            db = TestMcpCampaignStatus()._make_db(td)
            result = self.status.list_campaigns(db_path=db)
        self.assertEqual(result['db_path'], db)
        self.assertEqual(result['count'], 1)

    def test_list_campaigns_mine_reads_the_callers_ledger(self):
        # Same patch pair as the campaign_status version: patches the
        # ACCOUNT, not the resolver, so a mutation that hardcodes
        # `_resolve_identity(False)` inside list_campaigns cannot pass.
        from utils import submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = TestMcpCampaignStatus()._make_db(td)
            with patch('getpass.getuser', return_value='alice'), \
                 patch.object(submission_ledger, 'ledger_for',
                              return_value=db):
                result = self.status.list_campaigns(mine=True)
        self.assertEqual(result['db_path'], db)
        self.assertEqual(result['count'], 1)

    def test_list_campaigns_default_reports_the_production_ledger(self):
        from prodtools_mcp import ledger_ro
        with patch.object(self.status.ledger_ro, 'campaigns',
                          return_value=[]) as camps:
            result = self.status.list_campaigns()
        self.assertIsNone(camps.call_args.args[0])
        self.assertEqual(result['db_path'], ledger_ro.DEFAULT_DB)

    def test_list_campaigns_still_rejects_an_unknown_state(self):
        from prodtools_mcp.adapters import ToolError
        with self.assertRaises(ToolError) as ctx:
            self.status.list_campaigns(state='banana')
        self.assertEqual(ctx.exception.kind, 'invalid_argument')

    def test_server_info_advertises_the_identity_parameter(self):
        # A client must be able to discover `mine` without reading the
        # source, and must be told where OTHER accounts are read from --
        # `submissions --db <path> status`, not this server.
        from prodtools_mcp import server
        info = server.get_server_info()
        self.assertIn('mine', info['identity']['parameter'])
        self.assertIn('production', info['identity']['default'])
        self.assertIn('--db', info['identity']['other_accounts'])

    def test_registered_wrappers_pass_mine_through(self):
        # The wrapper is hand-written argument-by-argument, so a new
        # parameter on the tool function is NOT automatically exposed.
        # Checked per-wrapper, not against the whole source blob: a
        # shared assertIn would still pass if only ONE of the two
        # wrappers kept `mine` and the other lost it, since each marker
        # string would still appear once, sourced from the survivor.
        import inspect
        from prodtools_mcp import server
        src = inspect.getsource(server.create_mcp_server)
        campaign_status_src = src[src.index('def campaign_status'):
                                  src.index('def list_campaigns')]
        list_campaigns_src = src[src.index('def list_campaigns'):
                                 src.index('def find_datasets')]
        for wrapper_src in (campaign_status_src, list_campaigns_src):
            self.assertIn('mine: bool = False', wrapper_src)
            self.assertIn('mine=mine', wrapper_src)


class TestMcpListCampaigns(unittest.TestCase):
    def test_filters_by_state(self):
        from utils import submission_ledger
        from prodtools_mcp.tools import status
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'ledger.db')
            submission_ledger.create_campaign(
                db, tarball='cnf.a.0.tar', entry={'njobs': 10},
                slice_size=5)
            active = status.list_campaigns(state='active', db_path=db)
            done = status.list_campaigns(state='complete', db_path=db)
        self.assertEqual(active['count'], 1)
        self.assertEqual(done['count'], 0)

    def test_rejects_bad_state(self):
        from prodtools_mcp.tools import status
        from prodtools_mcp.adapters import ToolError
        with self.assertRaises(ToolError) as ctx:
            status.list_campaigns(state='banana', db_path='/x')
        self.assertEqual(ctx.exception.kind, 'invalid_argument')


# ---------------------------------------------------------------------------
# MCP discovery tools
# ---------------------------------------------------------------------------

class TestMcpFindDatasets(unittest.TestCase):
    NAMES = [
        'dig.mu2e.FlatGamma.MDC2025au_best_v1_3.art',
        'dig.mu2e.FlatGamma.MDC2025ar_best_v1_1.art',
        'dts.mu2e.CeMLeadingLog.MDC2025au.art',
    ]

    def _spy(self, names=None):
        """Record the defname string handed to samweb. The filter is a SQL
        LIKE; asserting only the RETURN value cannot tell `*` from `%`,
        and `*` silently matches nothing against the live catalog."""
        seen = []

        def fetch(pattern, user):
            seen.append(pattern)
            return self.NAMES if names is None else names
        return seen, fetch

    def test_query_uses_sql_like_wildcard_not_glob(self):
        """SAM's defname filter is a SQL LIKE: the wildcard is `%`. A `*`
        returns zero rows against the live catalog, which would render as
        'no datasets' — the empty result the spec forbids."""
        from prodtools_mcp.tools import discovery
        seen, fetch = self._spy()
        discovery.find_datasets(campaign='MDC2025au', tier='cnf',
                                fetch_fn=fetch)
        self.assertEqual(seen, ['cnf.%.%.MDC2025au%.%'])
        self.assertNotIn('*', seen[0])

    def test_caller_glob_is_translated_to_like(self):
        """Callers will type `*`; translate rather than return nothing."""
        from prodtools_mcp.tools import discovery
        seen, fetch = self._spy()
        discovery.find_datasets(pattern='cnf.mu2e.*.MDC2025au_best_v1_3.tar',
                                fetch_fn=fetch)
        self.assertEqual(seen, ['cnf.mu2e.%.MDC2025au_best_v1_3.tar'])

    def test_query_pushes_desc_into_defname(self):
        from prodtools_mcp.tools import discovery
        seen, fetch = self._spy()
        discovery.find_datasets(desc='FlatGamma', fetch_fn=fetch)
        self.assertEqual(seen, ['%.%.FlatGamma.%.%'])

    def test_query_is_all_wildcards_with_no_filters(self):
        from prodtools_mcp.tools import discovery
        seen, fetch = self._spy()
        discovery.find_datasets(fetch_fn=fetch)
        self.assertEqual(seen, ['%.%.%.%.%'])

    def test_parses_name_fields(self):
        from prodtools_mcp.tools import discovery
        res = discovery.find_datasets(pattern='*', fetch_fn=lambda p, u: self.NAMES)
        first = [d for d in res['datasets']
                 if d['name'].startswith('dig.mu2e.FlatGamma.MDC2025au')][0]
        self.assertEqual(first['tier'], 'dig')
        self.assertEqual(first['owner'], 'mu2e')
        self.assertEqual(first['desc'], 'FlatGamma')
        self.assertEqual(first['dsconf'], 'MDC2025au_best_v1_3')
        self.assertEqual(first['file_format'], 'art')

    def test_filters_by_campaign_and_tier(self):
        from prodtools_mcp.tools import discovery
        res = discovery.find_datasets(campaign='MDC2025au', tier='dig',
                                      fetch_fn=lambda p, u: self.NAMES)
        self.assertEqual(res['count'], 1)
        self.assertEqual(res['datasets'][0]['dsconf'], 'MDC2025au_best_v1_3')

    def test_always_reports_basis(self):
        """A definition listing must never be mistaken for existence."""
        from prodtools_mcp.tools import discovery
        res = discovery.find_datasets(pattern='*', fetch_fn=lambda p, u: self.NAMES)
        self.assertIn('basis', res)
        self.assertIn('list-definitions', res['basis'])

    def test_require_files_drops_empty_definitions(self):
        from prodtools_mcp.tools import discovery
        counts = {n: (0 if 'ar_best' in n else 5) for n in self.NAMES}
        res = discovery.find_datasets(pattern='*', require_files=True,
                                      fetch_fn=lambda p, u: self.NAMES,
                                      count_fn=lambda ds: counts[ds])
        self.assertTrue(all('ar_best' not in d['name'] for d in res['datasets']))
        self.assertEqual(res['count'], 2)

    def test_catalog_failure_is_not_empty_list(self):
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError

        def boom(pattern, user):
            raise RuntimeError('SAM unreachable')

        with self.assertRaises(ToolError) as ctx:
            discovery.find_datasets(pattern='*', fetch_fn=boom)
        self.assertEqual(ctx.exception.kind, 'catalog_unavailable')

    def test_latest_only_keeps_newest_dsconf_per_description(self):
        """latest_per_description returns (rows, skipped) with rows as
        4-tuples, not a flat name list — regression guard."""
        from prodtools_mcp.tools import discovery
        res = discovery.find_datasets(pattern='*', latest_only=True,
                                      fetch_fn=lambda p, u: self.NAMES)
        names = [d['name'] for d in res['datasets']]
        self.assertIn('dig.mu2e.FlatGamma.MDC2025au_best_v1_3.art', names)
        self.assertNotIn('dig.mu2e.FlatGamma.MDC2025ar_best_v1_1.art', names)

    def test_require_files_count_failure_is_catalog_unavailable(self):
        """A raising count_fn under require_files must fail loudly, not
        silently drop the dataset."""
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError

        def boom(ds):
            raise RuntimeError('SAM down')

        with self.assertRaises(ToolError) as ctx:
            discovery.find_datasets(pattern='*', require_files=True,
                                    fetch_fn=lambda p, u: self.NAMES,
                                    count_fn=boom)
        self.assertEqual(ctx.exception.kind, 'catalog_unavailable')

    def test_expired_token_is_auth_expired_not_catalog_unavailable(self):
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError

        def boom(pattern, user):
            raise RuntimeError('HTTP 401: token has expired')

        with self.assertRaises(ToolError) as ctx:
            discovery.find_datasets(fetch_fn=boom)
        self.assertEqual(ctx.exception.kind, 'auth_expired')

    def test_missing_ops_env_is_env_missing(self):
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError

        def boom(pattern, user):
            raise ImportError("No module named 'samweb_client'")

        with self.assertRaises(ToolError) as ctx:
            discovery.find_datasets(fetch_fn=boom)
        self.assertEqual(ctx.exception.kind, 'env_missing')

    def test_result_is_capped_and_truncated_is_honest(self):
        """~20,000 definitions exist; truncated was hardcoded False."""
        from prodtools_mcp.tools import discovery
        many = [f'dig.mu2e.D{i:04d}.MDC2025au_best_v1_3.art'
                for i in range(1200)]
        res = discovery.find_datasets(limit=10, fetch_fn=lambda p, u: many)
        self.assertEqual(res['count'], 10)
        self.assertEqual(res['limit'], 10)
        self.assertTrue(res['truncated'])
        untruncated = discovery.find_datasets(
            limit=10, fetch_fn=lambda p, u: many[:3])
        self.assertFalse(untruncated['truncated'])

    def test_default_limit_is_applied(self):
        from prodtools_mcp.tools import discovery
        many = [f'dig.mu2e.D{i:05d}.MDC2025au_best_v1_3.art'
                for i in range(discovery.DEFAULT_LIMIT + 7)]
        res = discovery.find_datasets(fetch_fn=lambda p, u: many)
        self.assertEqual(res['count'], discovery.DEFAULT_LIMIT)
        self.assertTrue(res['truncated'])

    def test_require_files_over_limit_is_refused_not_fanned_out(self):
        """require_files costs one serial HTTP round-trip per record and
        a sync tool blocks its caller until it returns."""
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError
        many = [f'dig.mu2e.D{i:04d}.MDC2025au_best_v1_3.art'
                for i in range(300)]
        calls = []

        with self.assertRaises(ToolError) as ctx:
            discovery.find_datasets(require_files=True, limit=10,
                                    fetch_fn=lambda p, u: many,
                                    count_fn=lambda ds: calls.append(ds) or 1)
        self.assertEqual(ctx.exception.kind, 'invalid_argument')
        self.assertEqual(calls, [], 'must refuse before querying SAM')

    def test_rejects_bad_limit(self):
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError
        for bad in (0, -1, 'many', 2.5, discovery.MAX_LIMIT + 1):
            with self.assertRaises(ToolError) as ctx:
                discovery.find_datasets(limit=bad,
                                        fetch_fn=lambda p, u: self.NAMES)
            self.assertEqual(ctx.exception.kind, 'invalid_argument')

    def test_limit_has_a_hard_ceiling(self):
        """The require_files refusal's own remedy says 'raise limit
        deliberately' — without a ceiling that invites
        require_files=True, limit=100000 and exactly the serial-query
        fan-out the refusal exists to prevent."""
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError
        with self.assertRaises(ToolError) as ctx:
            discovery.find_datasets(limit=discovery.MAX_LIMIT + 1,
                                    fetch_fn=lambda p, u: self.NAMES)
        self.assertEqual(ctx.exception.kind, 'invalid_argument')
        # And the ceiling itself is usable, not off-by-one.
        ok = discovery.find_datasets(limit=discovery.MAX_LIMIT,
                                     fetch_fn=lambda p, u: self.NAMES)
        self.assertEqual(ok['limit'], discovery.MAX_LIMIT)


class TestMcpDatasetDetails(unittest.TestCase):
    SUMMARY = {'file_count': 800, 'total_event_count': 4000000,
               'total_file_size': 4294967296}

    def test_composes_summary_and_creation_date(self):
        from prodtools_mcp.tools import discovery
        import datetime as _dt
        res = discovery.dataset_details(
            'dig.mu2e.FlatGamma.MDC2025au_best_v1_3.art',
            summary_fn=lambda ds: self.SUMMARY,
            created_fn=lambda ds: _dt.datetime(2026, 7, 25, 2, 11,
                                               tzinfo=_dt.timezone.utc))
        self.assertTrue(res['exists'])
        self.assertEqual(res['file_count'], 800)
        self.assertEqual(res['event_count'], 4000000)
        self.assertEqual(res['total_size_bytes'], 4294967296)
        self.assertEqual(res['created_utc'], '2026-07-25T02:11:00+00:00')

    def test_created_utc_is_nullable(self):
        """definition_creation_date returns None for metadata-only
        -LH/-CH datasets; that is data, not an error."""
        from prodtools_mcp.tools import discovery
        res = discovery.dataset_details(
            'dig.mu2e.X.Y-LH.art',
            summary_fn=lambda ds: self.SUMMARY,
            created_fn=lambda ds: None)
        self.assertIsNone(res['created_utc'])
        self.assertTrue(res['exists'])

    def test_zero_files_means_not_exists(self):
        from prodtools_mcp.tools import discovery
        res = discovery.dataset_details(
            'dig.mu2e.Nope.Z.art',
            summary_fn=lambda ds: {'file_count': 0, 'total_event_count': 0,
                                   'total_file_size': 0},
            created_fn=lambda ds: None)
        self.assertFalse(res['exists'])

    def test_summary_failure_is_catalog_unavailable(self):
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError

        def boom(ds):
            raise RuntimeError('SAM down')

        with self.assertRaises(ToolError) as ctx:
            discovery.dataset_details('x.y.z.w.art', summary_fn=boom)
        self.assertEqual(ctx.exception.kind, 'catalog_unavailable')

    def test_created_fn_failure_is_tolerated(self):
        """The creation date is decoration, not the answer: a raising
        created_fn yields created_utc=None and does NOT propagate — and
        that tolerance must not extend to summary_fn."""
        from prodtools_mcp.tools import discovery

        def boom(ds):
            raise RuntimeError('SAM down')

        res = discovery.dataset_details('dig.mu2e.X.Y.art',
                                        summary_fn=lambda ds: self.SUMMARY,
                                        created_fn=boom)
        self.assertIsNone(res['created_utc'])
        self.assertTrue(res['exists'])


# ---------------------------------------------------------------------------
# samweb_wrapper.parents_of_file — the fail-loud lineage edge function
# ---------------------------------------------------------------------------

class TestSamwebMetadataChunking(unittest.TestCase):
    """SAM rejects getMultipleMetadata above MAX_METADATA_BATCH names
    outright ('Too many files requested'), so the wrapper chunks. A
    2000-file draining batch hit this as a hard gate failure."""

    def _wrapper(self):
        from utils import samweb_wrapper
        calls = []

        class Client:
            @staticmethod
            def getMultipleMetadata(names):
                calls.append(len(names))
                return [{'file_name': n} for n in names]
        self._patch = patch.object(samweb_wrapper, '_client',
                                   return_value=Client())
        self._patch.start()
        self.addCleanup(self._patch.stop)
        return samweb_wrapper, calls

    def test_oversized_list_is_split_and_concatenated(self):
        from utils.samweb_wrapper import MAX_METADATA_BATCH
        w, calls = self._wrapper()
        names = [f'f{i}.art' for i in range(2000)]
        out = w.metadata_for_files(names)
        self.assertEqual(calls, [MAX_METADATA_BATCH, MAX_METADATA_BATCH])
        self.assertEqual([m['file_name'] for m in out], names)

    def test_ragged_tail_chunk(self):
        w, calls = self._wrapper()
        out = w.metadata_for_files([f'f{i}.art' for i in range(2501)])
        self.assertEqual(calls, [1000, 1000, 501])
        self.assertEqual(len(out), 2501)

    def test_small_list_is_one_round_trip(self):
        w, calls = self._wrapper()
        w.metadata_for_files(['a.art', 'b.art'])
        self.assertEqual(calls, [2])

    def test_empty_list_makes_no_call(self):
        w, calls = self._wrapper()
        self.assertEqual(w.metadata_for_files([]), [])
        self.assertEqual(calls, [])


class TestSamwebParentsOfFile(unittest.TestCase):
    def _wrapper(self, listfiles):
        """The module with a stub client patched in — _client() is the
        seam; a real client needs the Mu2e environment."""
        from utils import samweb_wrapper

        class Client:
            listFiles = staticmethod(listfiles)
        self._patch = patch.object(samweb_wrapper, '_client',
                                   return_value=Client())
        self._patch.start()
        self.addCleanup(self._patch.stop)
        return samweb_wrapper

    def test_query_is_isparentof_on_file_name(self):
        from utils.samweb_wrapper import _q_parents_of_file
        self.assertEqual(_q_parents_of_file('a.art'),
                         'isparentof: (file_name a.art)')

    def test_filters_etc_txt_like_famtree(self):
        w = self._wrapper(
            lambda q: ['sim.mu2e.A.B.art', 'etc.mu2e.index.C.txt'])
        self.assertEqual(w.parents_of_file('x.art'), ['sim.mu2e.A.B.art'])

    def test_raises_instead_of_returning_empty(self):
        """The whole point: file_lineage returns [] on any error, which a
        lineage caller cannot distinguish from a genuine primary."""
        def boom(q):
            raise RuntimeError('403 Forbidden')
        with self.assertRaises(RuntimeError):
            self._wrapper(boom).parents_of_file('x.art')

    def test_file_lineage_raises_on_sam_error(self):
        """file_lineage must not swallow SAM errors: [] is a physics claim
        ("no parents"), not an error state."""
        import inspect
        from utils import samweb_wrapper
        src = inspect.getsource(samweb_wrapper.file_lineage)
        self.assertNotIn('except Exception', src)


class TestMcpLocateFile(unittest.TestCase):
    def test_known_file_exists_with_its_location(self):
        from prodtools_mcp.tools import discovery
        res = discovery.locate_file('cnf.u.T.e470313.0.tar',
                                    locate_fn=lambda n: 'enstore:/pnfs/x')
        self.assertEqual(res, {'name': 'cnf.u.T.e470313.0.tar', 'exists': True,
                               'locations': ['enstore:/pnfs/x']})

    def test_unknown_file_is_not_an_error(self):
        from prodtools_mcp.tools import discovery
        res = discovery.locate_file('cnf.u.T.e470313-001.0.tar', locate_fn=lambda n: '')
        self.assertEqual(res, {'name': 'cnf.u.T.e470313-001.0.tar', 'exists': False,
                               'locations': []})

    def test_catalog_failure_is_classified(self):
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError

        def boom(n):
            raise RuntimeError('SAM down')

        with self.assertRaises(ToolError) as ctx:
            discovery.locate_file('x.y.z.w.0.tar', locate_fn=boom)
        self.assertEqual(ctx.exception.kind, 'catalog_unavailable')


class TestMcpDatasetFiles(unittest.TestCase):
    SIZES = {'nts.u.T.e470313.00000002.root': 20, 'nts.u.T.e470313.00000000.root': 10}

    def test_files_with_sizes_and_hashed_paths_sorted_by_name(self):
        from prodtools_mcp.tools import discovery
        from utils.job_common import Mu2eName
        res = discovery.dataset_files(
            'nts.u.T.e470313.root', 'scratch',
            sizes_fn=lambda ds: dict(self.SIZES),
            dataset_dir_fn=lambda ds, loc: f'/pnfs/{loc}/{ds}')
        self.assertEqual(res['root'], '/pnfs/scratch/nts.u.T.e470313.root')
        self.assertEqual(res['n_files'], 2)
        self.assertEqual(res['total_size'], 30)
        self.assertFalse(res['truncated'])
        self.assertEqual([f['name'] for f in res['files']],
                         ['nts.u.T.e470313.00000000.root', 'nts.u.T.e470313.00000002.root'])
        rel = Mu2eName.parse('nts.u.T.e470313.00000000.root').relpathname()
        self.assertEqual(res['files'][0],
                         {'name': 'nts.u.T.e470313.00000000.root', 'size': 10,
                          'path': f'/pnfs/scratch/nts.u.T.e470313.root/{rel}'})

    def test_limit_truncates_files_but_keeps_exact_totals(self):
        from prodtools_mcp.tools import discovery
        sizes = {'nts.u.T.e470313.00000000.root': 10,
                 'nts.u.T.e470313.00000001.root': 20,
                 'nts.u.T.e470313.00000002.root': 30}
        res = discovery.dataset_files(
            'nts.u.T.e470313.root', 'scratch', limit=2,
            sizes_fn=lambda ds: dict(sizes),
            dataset_dir_fn=lambda ds, loc: f'/pnfs/{loc}/{ds}')
        self.assertEqual(len(res['files']), 2)
        self.assertEqual(res['n_files'], 3)
        self.assertEqual(res['total_size'], 60)
        self.assertTrue(res['truncated'])
        self.assertEqual([f['name'] for f in res['files']],
                         ['nts.u.T.e470313.00000000.root', 'nts.u.T.e470313.00000001.root'])

    def test_rejects_bad_limit(self):
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError
        for bad in (0, discovery.DATASET_FILES_MAX_LIMIT + 1):
            with self.assertRaises(ToolError) as ctx:
                discovery.dataset_files(
                    'nts.u.T.e470313.root', 'scratch', limit=bad,
                    sizes_fn=lambda ds: dict(self.SIZES),
                    dataset_dir_fn=lambda ds, loc: f'/pnfs/{loc}/{ds}')
            self.assertEqual(ctx.exception.kind, 'invalid_argument', bad)

    def test_unknown_location_is_invalid_argument(self):
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError
        with self.assertRaises(ToolError) as ctx:
            discovery.dataset_files('nts.u.T.e470313.root', 'resilient',
                                    sizes_fn=lambda ds: {},
                                    dataset_dir_fn=lambda ds, loc: '')
        self.assertEqual(ctx.exception.kind, 'invalid_argument')
        self.assertIn('resilient', ctx.exception.message)

    def test_listing_failure_is_classified(self):
        from prodtools_mcp.tools import discovery
        from prodtools_mcp.adapters import ToolError

        def boom(ds):
            raise RuntimeError('SAM down')

        with self.assertRaises(ToolError) as ctx:
            discovery.dataset_files('nts.u.T.e470313.root', 'scratch',
                                    sizes_fn=boom,
                                    dataset_dir_fn=lambda ds, loc: '/pnfs/x')
        self.assertEqual(ctx.exception.kind, 'catalog_unavailable')

    def test_registered_on_the_read_server(self):
        from prodtools_mcp import server
        self.assertIn('locate_file', server.TOOL_FUNCTIONS)
        self.assertIn('dataset_files', server.TOOL_FUNCTIONS)
        self.assertIn('locate_file', server.TOOL_NAMES)
        self.assertIn('dataset_files', server.TOOL_NAMES)


# ---------------------------------------------------------------------------
# MCP lineage
# ---------------------------------------------------------------------------

class TestMcpLineage(unittest.TestCase):
    #  a -> b -> d
    #    -> c
    GRAPH = {'a': ['b', 'c'], 'b': ['d'], 'c': [], 'd': []}

    def test_walks_to_depth(self):
        from prodtools_mcp.tools.lineage import walk
        nodes, edges, truncated = walk(
            'a', 'up', 3, lambda n: self.GRAPH.get(n, []))
        self.assertEqual(set(nodes), {'a', 'b', 'c', 'd'})
        self.assertIn({'child': 'a', 'parent': 'b'}, edges)
        self.assertIn({'child': 'b', 'parent': 'd'}, edges)
        self.assertFalse(truncated)

    def test_depth_limit_sets_truncated(self):
        from prodtools_mcp.tools.lineage import walk
        nodes, edges, truncated = walk(
            'a', 'up', 1, lambda n: self.GRAPH.get(n, []))
        self.assertEqual(set(nodes), {'a', 'b', 'c'})
        self.assertTrue(truncated)

    def test_direction_down_reverses_edge_sense(self):
        from prodtools_mcp.tools.lineage import walk
        _, edges, _ = walk('a', 'down', 1, lambda n: self.GRAPH.get(n, []))
        self.assertIn({'child': 'b', 'parent': 'a'}, edges)

    def test_cycle_terminates(self):
        """Without the `seen` set the walk revisits nodes and `nodes` grows
        past the graph's size — assert the list, not the set, so this
        isolates the cycle guard rather than the depth bound."""
        from prodtools_mcp.tools.lineage import walk
        cyclic = {'a': ['b'], 'b': ['a']}
        nodes, _, _ = walk('a', 'up', 10, lambda n: cyclic.get(n, []))
        self.assertEqual(nodes, ['a', 'b'])

    def test_rejects_bad_direction(self):
        from prodtools_mcp.tools import lineage
        from prodtools_mcp.adapters import ToolError
        with self.assertRaises(ToolError) as ctx:
            lineage.trace_provenance('x', direction='sideways')
        self.assertEqual(ctx.exception.kind, 'invalid_argument')

    def test_rejects_bad_depth(self):
        from prodtools_mcp.tools import lineage
        from prodtools_mcp.adapters import ToolError
        with self.assertRaises(ToolError) as ctx:
            lineage.trace_provenance('x', depth=0)
        self.assertEqual(ctx.exception.kind, 'invalid_argument')

    def test_trace_provenance_shape(self):
        from prodtools_mcp.tools import lineage
        res = lineage.trace_provenance(
            'a', direction='up', depth=2,
            parents_fn=lambda n: self.GRAPH.get(n, []))
        self.assertEqual(res['root'], 'a')
        self.assertEqual(res['direction'], 'up')
        self.assertEqual(res['depth'], 2)
        self.assertIn('nodes', res)
        self.assertIn('edges', res)
        self.assertNotIn('mermaid', res)

    def test_parents_cache_is_module_level_and_actually_hits(self):
        """A cache rebuilt per call is inert: walk()'s own `seen` set means
        it can never hit within one call, so it must persist across them."""
        from prodtools_mcp.tools import lineage
        self.assertIs(lineage._default_parents_fn(),
                      lineage._default_parents_fn())
        lineage._cached_parents.cache_clear()
        try:
            with patch('utils.samweb_wrapper.parents_of_file',
                       lambda n: ['p1.art']):
                lineage._cached_parents('f.art')
                lineage._cached_parents('f.art')
            self.assertEqual(lineage._cached_parents.cache_info().hits, 1)
        finally:
            lineage._cached_parents.cache_clear()

    def test_parents_edge_fn_raises_instead_of_returning_empty(self):
        """famtree.get_parents -> file_lineage swallows every exception and
        returns [] (samweb_wrapper.py:260-265). For lineage that reads as
        'no parents' == 'this is a primary'. The edge function must raise."""
        from prodtools_mcp.tools import lineage
        lineage._cached_parents.cache_clear()
        try:
            def boom(name):
                raise RuntimeError('401 Unauthorized')
            with patch('utils.samweb_wrapper.parents_of_file', boom):
                with self.assertRaises(RuntimeError):
                    lineage._cached_parents('f.art')
            # lru_cache does not memoize exceptions: the wrong empty answer
            # cannot outlive the outage.
            self.assertEqual(lineage._cached_parents.cache_info().currsize, 0)
        finally:
            lineage._cached_parents.cache_clear()

    def test_expired_token_on_lineage_is_auth_expired(self):
        from prodtools_mcp.tools import lineage
        from prodtools_mcp.adapters import ToolError

        def boom(name):
            raise RuntimeError('403 Forbidden: bearer token rejected')

        with self.assertRaises(ToolError) as ctx:
            lineage.trace_provenance('f.art', parents_fn=boom)
        self.assertEqual(ctx.exception.kind, 'auth_expired')

    def test_missing_ops_env_on_lineage_is_env_missing(self):
        from prodtools_mcp.tools import lineage
        from prodtools_mcp.adapters import ToolError

        def boom(name):
            raise ImportError("No module named 'samweb_client'")

        with self.assertRaises(ToolError) as ctx:
            lineage.trace_provenance('f.art', parents_fn=boom)
        self.assertEqual(ctx.exception.kind, 'env_missing')

    def test_up_direction_sam_failure_is_error_not_lone_root(self):
        """The symmetric case to the children path: a SAM failure walking
        UP must be catalog_unavailable, NOT nodes=[root] — which a caller
        would read as 'this file is a primary'."""
        from prodtools_mcp.tools import lineage
        from prodtools_mcp.adapters import ToolError

        def boom(name):
            raise RuntimeError('SAM unreachable')

        with self.assertRaises(ToolError) as ctx:
            lineage.trace_provenance('f.art', direction='up',
                                     parents_fn=boom)
        self.assertEqual(ctx.exception.kind, 'catalog_unavailable')

    def test_default_parents_fn_is_the_fail_loud_wrapper(self):
        """Regression guard: pointing this back at famtree.get_parents
        reintroduces the swallow."""
        import inspect
        from prodtools_mcp.tools import lineage
        src = inspect.getsource(lineage._cached_parents.__wrapped__)
        code = '\n'.join(l for l in src.splitlines()
                         if not l.lstrip().startswith('#'))
        self.assertIn('from utils.samweb_wrapper import parents_of_file',
                      code)
        self.assertNotIn('famtree', code)

    def test_depth_bounds_inclusive_at_max(self):
        from prodtools_mcp.tools import lineage
        from prodtools_mcp.adapters import ToolError
        res = lineage.trace_provenance('a', depth=lineage.MAX_DEPTH,
                                       parents_fn=lambda n: [])
        self.assertEqual(res['depth'], lineage.MAX_DEPTH)
        with self.assertRaises(ToolError) as ctx:
            lineage.trace_provenance('a', depth=lineage.MAX_DEPTH + 1,
                                     parents_fn=lambda n: [])
        self.assertEqual(ctx.exception.kind, 'invalid_argument')

    def test_stdout_stays_clean_through_safe_tool(self):
        """Defence in depth: trace_provenance's edge functions are
        samweb_wrapper.parents_of_file/children_of_file, not famtree, so
        famtree.py:71 is no longer on this route. But samweb_wrapper
        itself prints on error at several sites (e.g.
        describe_definition:182, reached from dataset_details via
        definition_creation_date's text fallback), so any edge function
        that prints must still be neutralized here. `chatty_parents`
        stands in for that class of print."""
        from prodtools_mcp.adapters import safe_tool
        from prodtools_mcp.tools import lineage

        def chatty_parents(node):
            print(f"No files found for dataset: {node}")
            return []

        wrapped = safe_tool(lineage.trace_provenance)
        out, err = io.StringIO(), io.StringIO()
        with patch.object(sys, 'stdout', out), patch.object(sys, 'stderr', err):
            res = wrapped('a', parents_fn=chatty_parents)
        self.assertEqual(out.getvalue(), '')
        self.assertIn('No files found', err.getvalue())
        self.assertEqual(res['root'], 'a')

    def test_max_nodes_caps_wide_walk(self):
        from prodtools_mcp.tools.lineage import walk

        def wide(node):
            return [f'{node}.{i}' for i in range(10)]

        nodes, edges, truncated = walk('root', 'up', 3, wide, max_nodes=25)
        self.assertLessEqual(len(nodes), 25)
        self.assertTrue(truncated)

    def test_max_nodes_stops_queries_before_issuing_them(self):
        """The budget must stop work BEFORE issuing more queries, not
        merely trim the result at the end — that is the entire point,
        since one parents_of_file call costs ~0.5s. An unbudgeted 10-way,
        depth-3 walk would call edge_fn once per discovered node: 1 (root)
        + 10 + 100 = 111 calls. A max_nodes=25 budget must cut that off
        after a handful, well before the fan-out reaches depth 2."""
        from prodtools_mcp.tools.lineage import walk
        calls = []

        def wide(node):
            calls.append(node)
            return [f'{node}.{i}' for i in range(10)]

        walk('root', 'up', 3, wide, max_nodes=25)
        self.assertLess(len(calls), 15,
                        'edge_fn was called too many times — the budget '
                        'trimmed the result instead of stopping queries')

    def test_max_nodes_not_hit_is_not_truncated(self):
        from prodtools_mcp.tools.lineage import walk
        nodes, edges, truncated = walk(
            'a', 'up', 3, lambda n: self.GRAPH.get(n, []), max_nodes=100)
        self.assertFalse(truncated)

    def test_rejects_bad_max_nodes(self):
        from prodtools_mcp.tools import lineage
        from prodtools_mcp.adapters import ToolError
        for bad in (0, -1, lineage.MAX_NODES + 1, True, 'many'):
            with self.assertRaises(ToolError) as ctx:
                lineage.trace_provenance('x', max_nodes=bad,
                                         parents_fn=lambda n: [])
            self.assertEqual(ctx.exception.kind, 'invalid_argument', bad)

    def test_max_nodes_echoed_in_response(self):
        from prodtools_mcp.tools import lineage
        res = lineage.trace_provenance(
            'a', max_nodes=42, parents_fn=lambda n: self.GRAPH.get(n, []))
        self.assertEqual(res['max_nodes'], 42)


# ---------------------------------------------------------------------------
# MCP server wiring
# ---------------------------------------------------------------------------

class TestMcpServerInfo(unittest.TestCase):
    def test_declares_read_only(self):
        from prodtools_mcp.server import get_server_info
        info = get_server_info()
        self.assertFalse(info['writes'])
        self.assertIn('read-only', info['description'].lower())

    def test_lists_every_tool(self):
        from prodtools_mcp.server import get_server_info, TOOL_NAMES
        info = get_server_info()
        self.assertEqual(sorted(info['tools']), sorted(TOOL_NAMES))
        self.assertEqual(len(TOOL_NAMES), 8)


class TestMcpToolRegistration(unittest.TestCase):
    def test_every_tool_is_wrapped_in_safe_tool(self):
        """An unwrapped tool could kill the server via SystemExit or
        corrupt the JSON-RPC stream via print()."""
        from prodtools_mcp import server
        for name, fn in server.TOOL_FUNCTIONS.items():
            self.assertTrue(getattr(fn, '__wrapped__', None) is not None,
                            f'{name} is not wrapped in safe_tool')

    def test_tool_names_covers_functions_plus_server_info(self):
        from prodtools_mcp import server
        self.assertEqual(
            sorted(server.TOOL_NAMES),
            sorted(list(server.TOOL_FUNCTIONS) + ['get_server_info']))

    def test_optional_params_are_annotated_optional(self):
        """`str = None` emits {"default": null, "type": "string"}. null is
        not a string; strict validators and other providers'
        function-calling layers reject the schema outright, which defeats
        the spec's 'reach other clients' goal. AST rather than the live
        schema because the suite runs on an interpreter without mcp."""
        import ast
        import pathlib
        path = (pathlib.Path(__file__).resolve().parent.parent /
                'mcp' / 'src' / 'prodtools_mcp' / 'server.py')
        tree = ast.parse(path.read_text())
        offenders = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            a = node.args
            positional = list(a.posonlyargs) + list(a.args)
            defaults = ([None] * (len(positional) - len(a.defaults))
                        + list(a.defaults))
            pairs = list(zip(positional, defaults))
            pairs += list(zip(a.kwonlyargs, a.kw_defaults))
            for arg, default in pairs:
                if not isinstance(default, ast.Constant):
                    continue
                if default.value is not None or arg.annotation is None:
                    continue
                ann = ast.unparse(arg.annotation)
                if 'Optional' not in ann and 'None' not in ann:
                    offenders.append(f'{node.name}({arg.arg}: {ann} = None)')
        self.assertEqual(offenders, [])

    def test_no_tool_can_reach_definition_writers(self):
        """create_definition/delete_definition must never be referenced
        from the server package."""
        import pathlib
        root = pathlib.Path(__file__).resolve().parent.parent / 'mcp' / 'src'
        offenders = []
        for path in root.rglob('*.py'):
            text = path.read_text()
            for bad in ('create_definition', 'delete_definition'):
                if bad in text:
                    offenders.append(f'{path}: {bad}')
        self.assertEqual(offenders, [])


try:
    import importlib
    importlib.import_module('mcp.server.mcpserver')
    _HAVE_FASTMCP = True
except ImportError:
    # The mcp package requires Python >= 3.10; this suite also runs under
    # the system python3.9 (no MCP machinery available there, same reason
    # prodtools_mcp.server defers its own MCPServer import). Skip rather
    # than error so the plain interpreter still gets a clean run; a
    # 3.10+ interpreter exercises the real registration.
    #
    # Every MCP-dependent skip in this file keys on this flag, never on
    # importlib.util.find_spec: find_spec('mcp.server') imports the parent
    # package, so an installed-but-unimportable mcp (the ops
    # typing_extensions shadowing the venv's, say) raises out of a
    # decorator at class-definition time and takes the whole module with
    # it -- 1400 tests lost to skip two.
    _HAVE_FASTMCP = False


class TestWriteServerRegistration(unittest.TestCase):
    def test_tool_names_covers_tool_functions(self):
        """Static coverage check, runs under every interpreter (no
        FastMCP needed): TOOL_NAMES and TOOL_FUNCTIONS cannot drift
        from each other. create_write_mcp_server() registers exactly
        TOOL_FUNCTIONS, so this also pins what gets registered."""
        from prodtools_mcp_write import tools as write_tools
        from prodtools_mcp_write.server import TOOL_FUNCTIONS, TOOL_NAMES
        self.assertEqual(sorted(TOOL_NAMES), sorted(TOOL_FUNCTIONS))
        for name, fn in TOOL_FUNCTIONS.items():
            self.assertTrue(callable(fn), f'{name} is not callable')
            self.assertIs(
                fn, getattr(write_tools, name, None),
                f'{name} is not the function of that name in '
                'prodtools_mcp_write.tools')

    def test_forwarding_reraises_refusals_as_tool_error(self):
        """mcp 2.x hides every exception but ToolError behind "Error
        executing tool <name>", and the write tools refuse with
        ValueError/RuntimeError whose text is the remedy. _forwarding
        turns each into a ToolError carrying the same text, leaves a
        ToolError alone, and keeps the wrapped signature (the SDK builds
        the input schema from it)."""
        import inspect
        from prodtools_mcp_write.server import _forwarding

        class ToolError(Exception):
            pass

        def push_cnf(json: str, run_as: str, confirm: bool = False):
            if json == 'missing':
                raise ValueError('push_cnf: --json config not found: missing')
            if json == 'tool':
                raise ToolError('already a ToolError')
            return {'ok': json}

        wrapped = _forwarding(push_cnf, ToolError)
        self.assertEqual(wrapped('x', 'self'), {'ok': 'x'})
        with self.assertRaises(ToolError) as cm:
            wrapped('missing', 'self')
        self.assertEqual(str(cm.exception), 'push_cnf: --json config not found: missing')
        self.assertIsInstance(cm.exception.__cause__, ValueError)
        with self.assertRaises(ToolError) as cm:
            wrapped('tool', 'self')
        self.assertEqual(str(cm.exception), 'already a ToolError')
        self.assertIsNone(cm.exception.__cause__)
        self.assertEqual(inspect.signature(wrapped), inspect.signature(push_cnf))
        self.assertEqual(wrapped.__name__, 'push_cnf')

    @unittest.skipUnless(_HAVE_FASTMCP, 'mcp package (py3.10+) not installed')
    def test_advertised_names_match_registered_tools(self):
        """Live registration check, in addition to the static one above.
        Only runs where the real mcp package (py3.10+) is available;
        exercised for real by start_write_mcp.sh --check under the venv."""
        import asyncio
        from prodtools_mcp_write.server import create_write_mcp_server, TOOL_NAMES
        server = create_write_mcp_server()
        registered = sorted(t.name for t in asyncio.run(server.list_tools()))
        self.assertEqual(registered, sorted(TOOL_NAMES))

    def test_server_info_declares_the_write_capability(self):
        from prodtools_mcp_write.server import get_write_server_info
        info = get_write_server_info()
        self.assertTrue(info['performs_writes'])
        self.assertIn('mu2epro', info['description'])


class TestWriteToolParameterTypes(unittest.TestCase):
    """FastMCP derives each tool's JSON Schema from the function's
    signature, and an UNANNOTATED parameter is advertised to the model
    as a string. Both consequences were live bugs (2026-08-09):

      - `entry`/`slice_size`/`campaign_id` arrived as str. The
        (since-retired) `enqueue_campaign` tool got as far as running
        submit_map -- creating the campaign -- and then died in its
        entry-lookup helper's `0 <= index < len(entries)` with "'<=' not
        supported between instances of 'int' and 'str'", so a campaign
        existed that the tool had reported as a failure.
      - `confirm` arrived as str, and require_confirmed tests `not
        confirm`. Every non-empty string is truthy, so confirm="false"
        OPENED the production gate.

    These assert the signatures, not the behaviour, because the schema
    is where the defect lives: a correct body cannot repair a parameter
    the model was told to send as text.
    """

    NUMERIC = {'slice_size', 'campaign_id'}

    def test_every_tool_parameter_is_annotated(self):
        import inspect
        from prodtools_mcp_write.server import TOOL_FUNCTIONS
        for name, fn in TOOL_FUNCTIONS.items():
            for pname, param in inspect.signature(fn).parameters.items():
                self.assertIsNot(
                    param.annotation, inspect.Parameter.empty,
                    f'{name}({pname}) has no annotation, so FastMCP will '
                    f'advertise it to the model as a string')

    def test_numeric_and_confirm_parameters_have_the_right_types(self):
        import inspect
        from typing import Optional
        from prodtools_mcp_write.server import TOOL_FUNCTIONS
        seen = set()
        for name, fn in TOOL_FUNCTIONS.items():
            for pname, param in inspect.signature(fn).parameters.items():
                if pname in self.NUMERIC:
                    seen.add(pname)
                    # Optional[int] is equally acceptable, and REQUIRED
                    # where the parameter genuinely defaults to None
                    # (push_cnf's slice_size selects the enqueue mode).
                    # A bare `int = None` would pass a naive identity
                    # check while advertising a schema that disagrees
                    # with the default — the same class of lying
                    # signature this test exists to catch. What matters
                    # is only that the model is never told "string".
                    self.assertIn(
                        param.annotation, (int, Optional[int]),
                        f'{name}({pname}) must be int (or Optional[int] '
                        f'when it defaults to None): it indexes or is '
                        f'compared against ints')
                    if param.default is None:
                        self.assertIs(
                            param.annotation, Optional[int],
                            f'{name}({pname}) defaults to None, so a bare '
                            f'`int` annotation is a lie')
                elif pname == 'confirm':
                    seen.add(pname)
                    self.assertIs(
                        param.annotation, bool,
                        f'{name}(confirm) must be bool: require_confirmed '
                        f'tests `not confirm`, and a non-empty string — '
                        f'including "false" — is truthy')
        self.assertEqual(seen, self.NUMERIC | {'confirm'},
                         'a parameter this test pins has been renamed or '
                         'removed; update the test with the signature')

    def test_require_confirmed_is_defeated_by_a_string(self):
        """Pins WHY confirm must be typed bool. This is not asserting
        desired behaviour — it documents that the gate cannot defend
        itself against a mistyped parameter, so the annotation above is
        the actual control."""
        from prodtools_mcp_write import runner
        runner.require_confirmed('mu2epro', True)          # no raise
        with self.assertRaises(PermissionError):
            runner.require_confirmed('mu2epro', False)
        # The bug: a string the model believed meant "no".
        runner.require_confirmed('mu2epro', 'false')       # no raise


# ---------------------------------------------------------------------------
# Write-server identity dispatch and confirm gate
# ---------------------------------------------------------------------------

class TestCnfPushLocation(unittest.TestCase):
    """The cnf tarball's storage class must follow the dataset owner.

    Measured 2026-08-09: pushing a user-owned cnf to 'disk' resolves to
    /pnfs/mu2e/persistent/datasets/usr-etc/cnf/<user>/... and dies after
    three gfal retries with `DESTINATION MAKE_PARENT HTTP 403`. A user
    token grants storage.modify on scratch/datasets/usr-etc/cnf/<user>,
    never on persistent/datasets.
    """

    def test_production_owner_still_pushes_to_disk(self):
        from utils.json2jobdef import cnf_location
        self.assertEqual(cnf_location('mu2e'), 'disk')

    def test_user_owner_pushes_to_scratch(self):
        from utils.json2jobdef import cnf_location
        for owner in ('oksuzian', 'someone_else'):
            self.assertEqual(cnf_location(owner), 'scratch')

    def test_pushout_passes_the_owner_derived_location(self):
        from unittest import mock
        from utils import json2jobdef
        with mock.patch.object(json2jobdef.Path, 'exists',
                               return_value=True), \
             mock.patch.object(json2jobdef, 'locate_file',
                               return_value=None), \
             mock.patch.object(json2jobdef, 'push_output') as push:
            json2jobdef._pushout_to_sam('cnf.oksuzian.D.C.0.tar', 'oksuzian')
            json2jobdef._pushout_to_sam('cnf.mu2e.D.C.0.tar', 'mu2e')
        self.assertEqual([call.args[0][0][0] for call in push.call_args_list],
                         ['scratch', 'disk'])


class TestTokenClauseIsSelfOnly(unittest.TestCase):
    """getToken refreshes the CALLER's bearer token. Under ksu it would
    refresh mu2epro's, which is a standing hard rule never to do."""

    def setUp(self):
        from prodtools_mcp_write import runner
        self.runner = runner

    def test_self_chain_refreshes_the_token(self):
        script = self.runner._self_wrapper(['bin/submissions'])[-1]
        self.assertIn('getToken', script)
        # Before `muse setup ops`, as .claude/commands/mu2e-run.md has it
        # (getToken is on PATH straight after setupmu2e-art.sh).
        self.assertLess(script.index('getToken'),
                        script.index('muse setup ops'))

    def test_ksu_chain_never_refreshes_a_token(self):
        script = self.runner.ksu_wrapper(['bin/submissions'])[-1]
        self.assertNotIn('getToken', script)
        self.assertNotIn('htgettoken', script)

    def test_both_chains_are_valid_bash(self):
        for argv in (self.runner._self_wrapper(['bin/submissions']),
                     self.runner.ksu_wrapper(['bin/submissions'])):
            proc = subprocess.run(['bash', '-n', '-c', argv[-1]],
                                  capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, proc.stderr)


class TestWriteToolFailureText(unittest.TestCase):
    """A failing CLI puts the traceback on stderr and the DIAGNOSIS on
    stdout. `stderr or stdout` reported only the traceback."""

    def test_both_streams_are_reported(self):
        from prodtools_mcp_write import tools
        text = tools._both_streams(
            {'stdout': 'HTTP 403 : Permission refused',
             'stderr': 'Traceback (most recent call last):'})
        self.assertIn('HTTP 403 : Permission refused', text)
        self.assertIn('Traceback', text)

    def test_empty_streams_say_so_rather_than_reporting_nothing(self):
        from prodtools_mcp_write import tools
        self.assertIn('no output',
                      tools._both_streams({'stdout': '', 'stderr': '  \n'}))


class TestWriteRunnerGate(unittest.TestCase):
    def setUp(self):
        from prodtools_mcp_write import runner
        self.runner = runner

    def test_mu2epro_without_confirm_is_refused(self):
        with self.assertRaises(PermissionError) as ctx:
            self.runner.require_confirmed('mu2epro', False)
        self.assertIn('confirm', str(ctx.exception).lower())

    def test_mu2epro_with_confirm_is_allowed(self):
        self.runner.require_confirmed('mu2epro', True)   # must not raise

    def test_self_needs_no_confirm(self):
        self.runner.require_confirmed('self', False)     # must not raise

    def test_unknown_run_as_is_refused(self):
        with self.assertRaises(ValueError):
            self.runner.require_confirmed('root', True)

    def test_ksu_wrapper_has_every_required_env_export(self):
        # Each of these is a known failure, not a style choice:
        # a caller-owned workdir breaks condor_vault_storer, an
        # unreset USER picks the wrong submitter and tarball, and
        # without the CVMFS sourcing jobsub_submit is not on PATH.
        cmd = ' '.join(self.runner.ksu_wrapper(['bin/submissions', '--map', '/tmp/m.json']))
        self.assertIn('ksu mu2epro', cmd)
        self.assertIn('unset MUSE_WORK_DIR', cmd)
        self.assertIn('USER=mu2epro', cmd)
        self.assertIn('LOGNAME=mu2epro', cmd)
        self.assertIn('HOME=/exp/mu2e/app/home/mu2epro', cmd)
        self.assertIn('XDG_RUNTIME_DIR', cmd)
        self.assertIn('mktemp -d', cmd)
        self.assertIn('setupmu2e-art.sh', cmd)
        self.assertIn('muse setup ops', cmd)

    def test_self_does_not_use_ksu(self):
        with patch('subprocess.run') as run:
            run.return_value = SimpleNamespace(returncode=0, stdout='', stderr='')
            self.runner.run_cli(['bin/submissions', '--map', '/tmp/m.json'], 'self')
        argv = run.call_args[0][0]
        # Token-exact, not a substring check: this checkout's own path
        # (.../oksuzian/...) contains the substring "ksu", so a naive
        # `'ksu' in ' '.join(argv)` would false-positive on REPO_ROOT
        # alone even though no ksu process is ever invoked. Compare
        # basenames too, so an absolute `/usr/bin/ksu` argv element
        # would also be caught, not just a bare `'ksu'` token.
        self.assertFalse(
            any(os.path.basename(a) == 'ksu' for a in argv))

    def test_missing_mu2epro_token_is_reported_never_remediated(self):
        with patch('subprocess.run') as run:
            run.return_value = SimpleNamespace(
                returncode=1, stdout='',
                stderr='kx509: no credentials cache found')
            out = self.runner.run_cli(['bin/submissions'], 'mu2epro')
        self.assertEqual(out['rc'], 1)
        # Nothing in the runner may attempt a refresh.
        joined = ' '.join(' '.join(c[0][0]) for c in run.call_args_list)
        for forbidden in ('htgettoken', 'getToken', 'kinit', 'voms-proxy-init'):
            self.assertNotIn(forbidden, joined)

    # — Round-1 review fixes: injection, allowlist, run_as/cwd validation --

    def test_ksu_wrapper_quotes_the_executable_path_too(self):
        # The Critical: argv[0] used to be interpolated unquoted into
        # the `bash -c` string, so a hostile argv[0] could break out of
        # the intended command and execute arbitrary code as mu2epro.
        # argv[0] must now be quoted exactly like every argv[1:] element.
        cmd = self.runner.ksu_wrapper(['bin/submissions'])
        script = cmd[-1]
        expected = self.runner._quote(
            os.path.join(self.runner.REPO_ROOT, 'bin/submissions'))
        self.assertIn(expected, script)

    def test_ksu_wrapper_quotes_a_hostile_argv_element(self):
        hostile = "a'; id #"
        cmd = self.runner.ksu_wrapper(['bin/json2jobdef', hostile])
        script = cmd[-1]
        expected_quoted = self.runner._quote(hostile)
        self.assertIn(expected_quoted, script)
        # Outside of its one properly-escaped, quoted occurrence, the
        # injected text must not appear as a bare shell word — i.e. it
        # can never form a second, separately-executed command.
        remainder = script.replace(expected_quoted, '', 1)
        self.assertNotIn(' id ', remainder)
        self.assertNotIn(' id\n', remainder)

    def test_ksu_wrapper_rejects_argv0_outside_allowlist(self):
        # Quoting alone makes injection impossible; the allowlist is the
        # separate control that stops a caller running an arbitrary repo
        # script as mu2epro even when it's harmlessly quoted.
        with self.assertRaises(ValueError):
            self.runner.ksu_wrapper(['bin/x; id #'])

    def test_run_cli_rejects_argv0_outside_allowlist(self):
        with self.assertRaises(ValueError):
            self.runner.run_cli(['bin/x; id #'], 'self')

    def test_ksu_block_unsets_only_muse_work_dir(self):
        # Negative half of the MUSE_WORK_DIR constraint: MUSE_DIR must
        # survive (the `muse` shell function needs it), and there must
        # be no `unset MUSE_*` glob that would sweep it up.
        cmd = self.runner.ksu_wrapper(['bin/submissions'])
        script = cmd[-1]
        unset_lines = [ln.strip() for ln in script.splitlines()
                       if ln.strip().startswith('unset ')]
        self.assertEqual(unset_lines, ['unset MUSE_WORK_DIR'])

    def test_setup_chain_is_conjunctive_not_sequential(self):
        # A failed CVMFS source or `muse setup ops` must abort the
        # command — via `|| { ...; exit 1; }` chained onward with &&,
        # not run silently in a broken environment (both setup lines
        # redirect stdout/stderr to /dev/null, so a bare sequence of
        # statements would hide the failure entirely).
        cmd = self.runner.ksu_wrapper(['bin/submissions'])
        script = cmd[-1]
        self.assertIn(
            "setupmu2e-art.sh > /dev/null 2>&1 \\\n"
            "  || { echo 'push_cnf: setupmu2e-art.sh failed' >&2; exit 1; }"
            " \\\n  && setup OfflineOps",
            script)
        self.assertIn(
            "setup OfflineOps > /dev/null 2>&1 \\\n"
            "  || { echo 'push_cnf: setup OfflineOps failed' >&2; exit 1; }"
            " \\\n  && muse setup ops",
            script)
        self.assertIn(
            "muse setup ops > /dev/null 2>&1 \\\n"
            "  || { echo 'push_cnf: muse setup ops failed' >&2; exit 1; }"
            " \\\n  && bash",
            script)

    def test_worker_runjob_sets_up_offlineops_before_muse_ops(self):
        """Same six.moves trap on the worker: bin/runjob.sh must run
        `setup OfflineOps` before `muse setup ops` or every grid job
        dies in runmu2e.py's `import samweb_client` on Python 3.12."""
        import os
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(here, 'bin', 'runjob.sh')) as fh:
            script = fh.read()
        offline = [i for i, l in enumerate(script.splitlines())
                   if l.startswith('setup OfflineOps')]
        muse = [i for i, l in enumerate(script.splitlines())
                if l.strip() == 'muse setup ops']
        self.assertEqual(len(offline), 1, script)
        self.assertEqual(len(muse), 1, script)
        self.assertLess(offline[0], muse[0])

    def test_offlineops_is_set_up_before_muse_ops_on_both_identities(self):
        """OfflineOps' UPS sam_web_client v3_6 bundles six 1.11, which has
        no six.moves on the Python 3.12 that `muse setup ops` provides
        since ops-021 (2026-09-12). `muse setup ops` LAST puts the spack
        view's samweb_client and six 1.16 in front, so json2jobdef's
        `import samweb_client` works. Reversing the order breaks every
        push_cnf at import time."""
        for cmd in (self.runner.ksu_wrapper(['bin/json2jobdef']),
                    self.runner._self_wrapper(['bin/json2jobdef'])):
            script = cmd[-1]
            self.assertLess(script.index('setup OfflineOps'),
                            script.index('muse setup ops'), script)

    def test_setup_chain_syntax_is_valid_bash(self):
        # bash -n (parse-only) on the generated script, so a malformed
        # brace/quote in the per-step error-reporting clauses would
        # fail loudly here instead of only at ksu-run time.
        cmd = self.runner._self_wrapper(
            ['bin/json2jobdef'],
            simjob_setup='/cvmfs/mu2e.opensciencegrid.org/Musings/'
                         'SimJob/Run1Bap/setup.sh')
        script = cmd[-1]
        proc = subprocess.run(['bash', '-n', '-c', script],
                              capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_each_setup_step_announces_its_own_failure_on_stderr(self):
        # Minor fix: every setup line used to redirect BOTH stdout and
        # stderr to /dev/null, so a failed CVMFS source, `muse setup
        # ops`, or Musing source returned rc != 0 with EMPTY stdout and
        # stderr — push_cnf would raise RuntimeError("... rc=1): ")
        # with nothing to debug. Each step must name itself on stderr
        # before exiting.
        setup = '/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/Run1Bap/setup.sh'
        cmd = self.runner.ksu_wrapper(['bin/json2jobdef'], simjob_setup=setup)
        script = cmd[-1]
        for needle in ("echo 'push_cnf: setupmu2e-art.sh failed' >&2; exit 1",
                       "echo 'push_cnf: muse setup ops failed' >&2; exit 1",
                       "echo 'push_cnf: setup OfflineOps failed' >&2; exit 1",
                       "echo 'push_cnf: Musing setup failed' >&2; exit 1"):
            self.assertIn(needle, script)

    def test_mktemp_failure_is_guarded(self):
        cmd = self.runner.ksu_wrapper(['bin/submissions'])
        script = cmd[-1]
        self.assertIn('mktemp -d /tmp/mu2epro_mcp.XXXXXX) || exit 1', script)

    def test_run_cli_rejects_unknown_run_as_rather_than_falling_through(self):
        # A typo like 'mu2Epro' must not silently fall into the 'self'
        # branch: that would run as the CALLER while the caller believes
        # it ran as production. Reject outright instead.
        with patch('subprocess.run') as run:
            with self.assertRaises(ValueError):
                self.runner.run_cli(['bin/submissions'], 'mu2Epro')
        run.assert_not_called()

    def test_run_cli_rejects_explicit_cwd_under_mu2epro(self):
        # The ksu block always cd's into its own mktemp workdir, so a
        # caller-supplied cwd would be silently ignored on this path.
        # Reject it instead of accepting and discarding it.
        with self.assertRaises(ValueError):
            self.runner.run_cli(['bin/submissions'], 'mu2epro',
                                cwd='/tmp/somewhere')

    # — Round-2 review fixes: no Musing on either identity's env chain --

    def test_ksu_wrapper_sources_the_musing_setup_before_the_command(self):
        # setupmu2e-art.sh + setup OfflineOps + muse setup ops alone
        # leaves MUSE_DIR set but `mu2e` NOTFOUND and MU2E_SEARCH_PATH
        # empty — bin/json2jobdef hard-exits in that state. The
        # Musing must be sourced from the entry's own simjob_setup,
        # after OfflineOps and before the command.
        setup = '/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/Run1Bap/setup.sh'
        cmd = self.runner.ksu_wrapper(['bin/json2jobdef'], simjob_setup=setup)
        script = cmd[-1]
        quoted = self.runner._quote(setup)
        offline_ops_idx = script.index('setup OfflineOps')
        musing_idx = script.index(f'source {quoted}')
        command_idx = script.index('bash ')
        self.assertLess(offline_ops_idx, musing_idx)
        self.assertLess(musing_idx, command_idx)
        self.assertIn(f"source {quoted} > /dev/null 2>&1", script)

    def test_ksu_wrapper_without_simjob_setup_is_unchanged(self):
        # No Musing given (e.g. bin/submissions, which needs none) must
        # not grow a stray `source` clause.
        cmd = self.runner.ksu_wrapper(['bin/submissions'])
        self.assertNotIn('source /cvmfs/mu2e.opensciencegrid.org/Musings',
                         cmd[-1])

    def test_self_path_was_a_bare_subprocess_with_no_env_setup_now_fixed(self):
        # Critical: run_as='self' used to be a bare subprocess with NO
        # environment setup at all — no setupmu2e-art.sh, no muse
        # setup ops, no Musing — and failed for the same reason as the
        # ksu path. It must now run the identical setup chain, wrapped
        # in `bash -c` instead of ksu.
        setup = '/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/Run1Bap/setup.sh'
        with patch('subprocess.run') as run:
            run.return_value = SimpleNamespace(returncode=0, stdout='', stderr='')
            self.runner.run_cli(['bin/json2jobdef'], 'self', simjob_setup=setup)
        argv = run.call_args[0][0]
        self.assertEqual(argv[0], 'bash')
        self.assertEqual(argv[1], '-c')
        script = argv[2]
        self.assertIn('setupmu2e-art.sh', script)
        self.assertIn('muse setup ops', script)
        self.assertIn('setup OfflineOps', script)
        self.assertIn(f"source {self.runner._quote(setup)}", script)
        self.assertFalse(any(os.path.basename(a) == 'ksu' for a in argv))

    def test_self_without_a_musing_still_gets_the_base_setup_chain(self):
        with patch('subprocess.run') as run:
            run.return_value = SimpleNamespace(returncode=0, stdout='', stderr='')
            self.runner.run_cli(['bin/submissions'], 'self')
        script = run.call_args[0][0][2]
        self.assertIn('setupmu2e-art.sh', script)
        self.assertIn('muse setup ops', script)
        self.assertIn('setup OfflineOps', script)

    def test_hostile_simjob_setup_cannot_escape_its_quoting(self):
        hostile = "/tmp/x'; id #/setup.sh"
        cmd = self.runner.ksu_wrapper(['bin/json2jobdef'], simjob_setup=hostile)
        script = cmd[-1]
        expected_quoted = self.runner._quote(hostile)
        self.assertIn(expected_quoted, script)
        # Outside of its one properly-escaped, quoted occurrence, the
        # injected text must not appear as a bare shell word.
        remainder = script.replace(expected_quoted, '', 1)
        self.assertNotIn(' id ', remainder)
        self.assertNotIn(' id\n', remainder)


class TestRunCliExitStatusSentinel(unittest.TestCase):
    """`ksu -e` does NOT propagate the child's exit status.

    MIT ksu (krb5 1.21.1) exits with the RAW wait status, and a normal
    exit N encodes as N<<8, which exit() truncates to 0. Measured on
    this host: children exiting 1, 2, 7, 42 and 255 all came back as
    ksu rc=0. Every write tool decides success from that rc, so before
    the stderr sentinel a crashed `submissions run`, a failed
    json2jobdef push and a refused duplicate campaign ALL reported
    success — in production, silently.

    These tests pin the replacement: the rc comes from the
    `__PRODTOOLS_RC__:<n>` line on stderr, and no sentinel is a failure.
    """

    def setUp(self):
        from prodtools_mcp_write import runner
        self.runner = runner

    def _run(self, rc, stderr, run_as='mu2epro'):
        with patch('subprocess.run') as run:
            run.return_value = SimpleNamespace(
                returncode=rc, stdout='', stderr=stderr)
            return self.runner.run_cli(['bin/submissions', 'run'], run_as)

    def test_sentinel_beats_the_process_rc(self):
        # THE bug: ksu says 0, the command really exited 7.
        out = self._run(0, 'trouble\n__PRODTOOLS_RC__:7\n')
        self.assertEqual(out['rc'], 7)

    def test_sentinel_line_is_stripped_from_the_returned_stderr(self):
        out = self._run(0, 'trouble\n__PRODTOOLS_RC__:7\n')
        self.assertEqual(out['stderr'], 'trouble\n')

    def test_missing_sentinel_is_a_failure_not_a_success(self):
        # Fail closed: rc=0 with no sentinel means the shell died before
        # it could report, so the real status is unknown.
        out = self._run(0, 'some output but no sentinel\n')
        self.assertNotEqual(out['rc'], 0)
        self.assertEqual(out['rc'], self.runner.SENTINEL_MISSING_RC)
        self.assertIn('sentinel', out['stderr'])
        self.assertIn('some output but no sentinel', out['stderr'])

    def test_missing_sentinel_never_reports_the_needs_attention_code(self):
        # rc=2 means "the tick ran fine, a human should look". An
        # unknown status must never be laundered into that.
        out = self._run(2, 'no sentinel here\n')
        self.assertNotEqual(out['rc'], 2)

    def test_missing_sentinel_keeps_a_trustworthy_nonzero_process_rc(self):
        # ksu's own failures (and signal deaths) DO come through its rc;
        # only ordinary child statuses are truncated. Keep them.
        out = self._run(1, 'ksu: user not authorized\n')
        self.assertEqual(out['rc'], 1)

    def test_last_sentinel_wins(self):
        # A child that prints a sentinel-shaped line cannot forge the
        # status: ours is echoed after the command has finished.
        out = self._run(0, '__PRODTOOLS_RC__:0\nreal failure\n'
                           '__PRODTOOLS_RC__:3\n')
        self.assertEqual(out['rc'], 3)
        self.assertEqual(out['stderr'], 'real failure\n')

    def test_self_identity_goes_through_the_same_parse(self):
        # run_as='self' is not subject to the ksu truncation, but it
        # must not be a second, rc-unchecked code path either.
        out = self._run(0, '__PRODTOOLS_RC__:4\n', run_as='self')
        self.assertEqual(out['rc'], 4)

    def test_sentinel_tail_reports_the_real_status_under_real_bash(self):
        # The generated tail, run by an actual shell: what the subshell
        # exited with is what the sentinel says and what the script
        # exits with.
        for status in (0, 1, 7, 42):
            proc = subprocess.run(
                ['bash', '-c', f'( exit {status} )\n' + self.runner._RC_TAIL],
                capture_output=True, text=True)
            self.assertEqual(proc.returncode, status)
            self.assertIn(f'__PRODTOOLS_RC__:{status}', proc.stderr)

    def test_a_failing_setup_step_still_reaches_the_sentinel(self):
        # The setup guards are `|| { echo ...; exit 1; }`. They only
        # end the SUBSHELL: the whole chain is wrapped in `( ... )` and
        # the sentinel is echoed outside it. Written as a flat chain the
        # first guard would exit before any sentinel could print, and
        # run_cli would see an rc-less (unknown) failure instead of 1.
        script = self.runner.ksu_wrapper(['bin/submissions'])[-1]
        chain_open = script.index('\n(\n')
        chain_close = script.index('\n)\n')
        guard = script.index("exit 1; }")
        echo = script.index("printf '\\n__PRODTOOLS_RC__:")
        self.assertLess(chain_open, guard)
        self.assertLess(guard, chain_close)
        self.assertLess(chain_close, echo)
        # And it really behaves that way in bash.
        proc = subprocess.run(
            ['bash', '-c',
             "( false || { echo 'step failed' >&2; exit 1; } && true )\n"
             + self.runner._RC_TAIL],
            capture_output=True, text=True)
        self.assertEqual(proc.returncode, 1)
        self.assertIn('step failed', proc.stderr)
        self.assertIn('__PRODTOOLS_RC__:1', proc.stderr)

    def test_sentinel_survives_stderr_with_no_trailing_newline(self):
        # A command whose LAST stderr write has no trailing newline used
        # to glue the sentinel onto that partial line
        # (`no newline here__PRODTOOLS_RC__:0`), which the anchored regex
        # misses -> run_cli reported 125 on a SUCCESSFUL run. The tail
        # emits a leading newline so the sentinel always starts a line.
        proc = subprocess.run(
            ['bash', '-c',
             "( printf 'no newline here' >&2; exit 0 )\n"
             + self.runner._RC_TAIL],
            capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0)
        self.assertIn('no newline here', proc.stderr)
        # The parser, not just the raw text, must agree.
        rc, kept = self.runner._rc_from_sentinel(proc.returncode, proc.stderr)
        self.assertEqual(rc, 0)
        self.assertIn('no newline here', kept)

    def test_generated_scripts_still_parse(self):
        for cmd in (self.runner.ksu_wrapper(['bin/submissions']),
                    self.runner._self_wrapper(['bin/json2jobdef'],
                                              simjob_setup='/cvmfs/a/setup.sh')):
            proc = subprocess.run(['bash', '-n', '-c', cmd[-1]],
                                  capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, proc.stderr)


class TestChildStdinIsClosed(unittest.TestCase):
    """A child of a stdio MCP server must never inherit the server's stdin.

    `capture_output=True` redirects stdout and stderr and says NOTHING
    about stdin, and an unset stdin is inherited. The write server speaks
    JSON-RPC over stdio, so its fd 0 is the client->server socket. A child
    that reads stdin therefore (a) blocks forever, because nothing will
    ever write to it, and (b) consumes bytes the client meant for the
    server, desyncing the session with no error anywhere.

    Measured 2026-08-09 on a real push_cnf: the child's fd 0 and the
    server's fd 0 were the same socket inode. OfflineOps `pushOutput`
    trips it — its unconditional `debugprint` runs `cat
    $BEARER_TOKEN_FILE`, that variable is empty in this chain, the
    argument vanishes, and `cat` reads stdin. The call hung 30 minutes,
    the client gave up and reported failure, and the child kept running.
    Under run_as='mu2epro' that is a live production write proceeding
    past a reported failure.
    """

    def setUp(self):
        from prodtools_mcp_write import runner
        self.runner = runner

    def test_run_cli_closes_the_child_stdin(self):
        with patch('subprocess.run') as run:
            run.return_value = SimpleNamespace(
                returncode=0, stdout='', stderr='__PRODTOOLS_RC__:0\n')
            self.runner.run_cli(['bin/submissions', 'run'], 'mu2epro')
        self.assertEqual(run.call_args.kwargs.get('stdin'),
                         subprocess.DEVNULL,
                         'child stdin must be DEVNULL, never inherited')

    def test_both_identities_close_it(self):
        # self and mu2epro build different commands but share the one
        # subprocess.run call; a future split must not drop this on either.
        for run_as in ('self', 'mu2epro'):
            with patch('subprocess.run') as run:
                run.return_value = SimpleNamespace(
                    returncode=0, stdout='', stderr='__PRODTOOLS_RC__:0\n')
                self.runner.run_cli(['bin/json2jobdef'], run_as,
                                    simjob_setup='/cvmfs/a/setup.sh')
            self.assertEqual(run.call_args.kwargs.get('stdin'),
                             subprocess.DEVNULL, run_as)

    def test_devnull_actually_stops_a_stdin_reading_child(self):
        # Why DEVNULL and not a closed fd or a timeout: a bare `cat` is
        # the exact shape that hung, and with DEVNULL it reads EOF and
        # exits at once. The timeout is the assertion — if this ever
        # blocks, the remedy is wrong, and the suite says so instead of
        # hanging forever.
        proc = subprocess.run(['cat'], capture_output=True, text=True,
                              stdin=subprocess.DEVNULL, timeout=10)
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, '')


# ---------------------------------------------------------------------------
# mcp-write-guard.sh: the PreToolUse hook, exercised as a subprocess so a
# later edit that un-arms the gate (e.g. reverting to fail-open, or a typo
# in the jq filter) fails this suite instead of only being caught by
# someone manually re-running the two `echo | bash` commands from the
# task brief.
# ---------------------------------------------------------------------------

class TestMcpWriteGuardHook(unittest.TestCase):
    HOOK = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        '.claude', 'hooks', 'mcp-write-guard.sh')

    def _run(self, stdin_text):
        result = subprocess.run(['bash', self.HOOK], input=stdin_text,
                                capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0)
        return result.stdout.strip()

    def test_mu2epro_prompts(self):
        out = self._run('{"tool_input":{"run_as":"mu2epro"}}')
        self.assertIn('"permissionDecision":"ask"', out)
        self.assertIn('mu2epro', out)

    def test_self_is_silent(self):
        out = self._run('{"tool_input":{"run_as":"self"}}')
        self.assertEqual(out, '')

    def test_missing_run_as_prompts(self):
        # This is the fail-closed requirement: an empty/absent run_as
        # must NOT be silently treated as safe.
        out = self._run('{}')
        self.assertIn('"permissionDecision":"ask"', out)

    def test_malformed_json_prompts(self):
        out = self._run('not json')
        self.assertIn('"permissionDecision":"ask"', out)

    def test_unrecognised_run_as_prompts(self):
        out = self._run('{"tool_input":{"run_as":"root"}}')
        self.assertIn('"permissionDecision":"ask"', out)


# ---------------------------------------------------------------------------
# push_cnf tool
# ---------------------------------------------------------------------------

class TestPushCnfTool(unittest.TestCase):
    """push_cnf: build the cnf, register it in SAM, create the campaign.

    One call, no map file. The campaign ROW is the artifact this tool
    reads its result back from, so most of these tests are about one
    question: did it hand back the campaign THIS call created, or some
    other campaign that happened to match?
    """

    def setUp(self):
        from prodtools_mcp_write import tools
        self.tools = tools
        tmpdir_ctx = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir_ctx.cleanup)
        self._tmpdir = tmpdir_ctx.name
        self.simjob_setup = (
            '/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/C/setup.sh')
        self.json_path = os.path.join(self._tmpdir, 'entries.json')
        with open(self.json_path, 'w') as f:
            json.dump([{
                'desc': 'D', 'dsconf': 'C', 'owner': 'mu2e',
                'simjob_setup': self.simjob_setup,
                'fcl': 'x.fcl', 'outloc': {'*.art': 'disk'},
            }], f)
        self.tarball = 'cnf.mu2e.D.C.0.tar'

    def _camp(self, cid, tarball=None, state='active', njobs=100,
              datasets=('dig.mu2e.D.C.art',)):
        return {'id': cid, 'tarball': tarball or self.tarball,
                'state': state,
                'entry': {'njobs': njobs,
                          'outputs': [{'dataset': d} for d in datasets]}}

    def _push(self, before, after, *, json_path=None, desc='D', dsconf='C',
              slice_size=500, run_as='self', confirm=False, cli=None,
              prodtools_dir=None):
        """push_cnf with the ledger faked.

        `before`/`after` are what _all_campaigns returns either side of
        run_cli (pass an Exception instance for `before` to simulate a
        ledger that cannot be read yet).
        """
        cli = cli if cli is not None else {'rc': 0, 'stdout': '', 'stderr': ''}
        kw = ({'side_effect': cli} if callable(cli)
              else {'return_value': cli})
        with patch('prodtools_mcp_write.runner.run_cli', **kw) as run, \
             patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value='/db'), \
             patch('prodtools_mcp_write.tools._all_campaigns',
                   side_effect=[before, after]):
            out = self.tools.push_cnf(
                json=json_path or self.json_path, desc=desc, dsconf=dsconf,
                slice_size=slice_size, run_as=run_as, confirm=confirm,
                prodtools_dir=prodtools_dir)
        self.last_run = run
        return out

    # — the production gate ------------------------------------------

    def test_mu2epro_without_confirm_refused_before_running_anything(self):
        with patch('prodtools_mcp_write.runner.run_cli') as run:
            with self.assertRaises(PermissionError):
                self.tools.push_cnf(json=self.json_path, desc='D', dsconf='C',
                                    slice_size=500, run_as='mu2epro')
        run.assert_not_called()

    def test_argument_checks_run_after_the_production_gate(self):
        """An unconfirmed mu2epro call must be refused as a permission
        problem, not reported as malformed arguments — otherwise the
        operator fixes the wrong thing."""
        with patch('prodtools_mcp_write.runner.run_cli') as run:
            with self.assertRaises(PermissionError):
                self.tools.push_cnf(json=self.json_path, desc='D',
                                    dsconf='C', slice_size=0,
                                    run_as='mu2epro')
        run.assert_not_called()

    # — slice_size is checked BEFORE the irreversible push ------------

    def test_bad_slice_size_refused_before_running_anything(self):
        """create_campaign's own `slice_size must be >= 1` fires inside
        the CLI — i.e. AFTER _pushout_to_sam has irreversibly registered
        the cnf. Checking here is the difference between a refused call
        and a pushed cnf with no campaign."""
        for bad in (0, -1, True, 'lots', None):
            with self.subTest(slice_size=bad):
                with patch('prodtools_mcp_write.runner.run_cli') as run:
                    with self.assertRaises(ValueError):
                        self.tools.push_cnf(
                            json=self.json_path, desc='D', dsconf='C',
                            slice_size=bad, run_as='self')
                run.assert_not_called()

    # — the argv, and where the Musing comes from ---------------------

    def test_builds_the_one_command_argv_and_derives_the_musing(self):
        self._push([], [self._camp(7)])
        args, kwargs = self.last_run.call_args
        argv = args[0]
        self.assertEqual(argv[0], 'bin/json2jobdef')
        self.assertIn('--prod', argv)
        self.assertIn('--enqueue', argv)
        self.assertIn('--slice-size', argv)
        self.assertIn('500', argv)
        # The map file is gone from this path entirely.
        self.assertNotIn('--jobdefs', argv)
        # The Musing comes from the entry's own simjob_setup — push_cnf
        # takes no Musing argument, so a caller can never pass one that
        # disagrees with the entry.
        self.assertEqual(kwargs.get('simjob_setup'), self.simjob_setup)

    def test_result_is_read_from_the_ledger_not_stdout(self):
        noisy = 'Enqueued campaign 999: cnf.mu2e.WRONG.C.0.tar'
        out = self._push([], [self._camp(7, njobs=42)],
                         cli={'rc': 0, 'stdout': noisy, 'stderr': ''})
        self.assertEqual(out['campaign_id'], 7)
        self.assertEqual(out['njobs'], 42)
        self.assertEqual(out['tarball'], self.tarball)
        self.assertEqual(out['datasets'], ['dig.mu2e.D.C.art'])
        # No map file exists on this path, so claiming one would be a lie.
        self.assertNotIn('map_path', out)

    def test_nonzero_rc_raises_with_both_streams_and_recovery_advice(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._push([], [], cli={'rc': 2, 'stdout': 'diagnosis here',
                                    'stderr': 'boom'})
        msg = str(ctx.exception)
        self.assertIn('boom', msg)
        self.assertIn('diagnosis here', msg)
        # enqueue_entry runs AFTER _pushout_to_sam, so any failure here
        # may have left the cnf registered with no campaign. Saying so
        # is the operator's whole recovery procedure.
        self.assertIn('list_campaigns', msg)
        self.assertIn('SAM', msg)

    # — which campaign did THIS call create? --------------------------

    def test_prefers_the_campaign_this_call_created(self):
        """A tarball accumulates campaigns over its life (complete,
        cancelled, then a new one). The one this push created is the one
        absent from the before-snapshot.

        The stale campaign is listed AFTER the fresh one on purpose: with
        `[fresh, stale]` ordering, "newest row wins" and "prefer fresh"
        disagree, so this fails against a `return matches[-1]`
        implementation. Ordered the other way it would pass against both
        and pin nothing.
        """
        fresh = self._camp(9, njobs=20)
        stale = self._camp(3, njobs=10)
        out = self._push([stale], [fresh, stale])
        self.assertEqual(out['campaign_id'], 9)

    def test_never_returns_a_campaign_that_already_existed(self):
        """rc=0 with nothing fresh means the snapshot and the write
        disagree about the database. Returning the pre-existing campaign
        would send run_submissions at an unrelated PRODUCTION campaign
        while the new one is never fed — so refuse.

        A successful --enqueue always INSERTs (create_campaign), and a
        duplicate live tarball raises -> rc!=0, so this state is never a
        legitimate re-run.
        """
        stale = self._camp(3)
        with self.assertRaises(RuntimeError) as ctx:
            self._push([stale], [stale])
        msg = str(ctx.exception)
        self.assertIn('already existed', msg)
        self.assertIn('3', msg)
        self.assertIn('list_campaigns', msg)

    def test_several_fresh_campaigns_raises_listing_them(self):
        a, b = self._camp(9), self._camp(10, tarball='cnf.mu2e.D.C.1.tar')
        with self.assertRaises(RuntimeError) as ctx:
            self._push([], [a, b])
        msg = str(ctx.exception)
        self.assertIn('9', msg)
        self.assertIn('10', msg)
        self.assertIn('refusing to guess', msg.lower())

    def test_windowed_versions_are_disambiguated_by_the_snapshot(self):
        """Two live campaigns for the same desc+dsconf are LEGAL:
        _tarball_matches ignores the version index, and cnf_name puts
        `config['version']` there (--extend bumps it). So `...D.C.0.tar`
        and `...D.C.1.tar` both match, and only the snapshot can tell
        which one this call created. This must resolve, not raise."""
        v0 = self._camp(3, tarball='cnf.mu2e.D.C.0.tar')
        v1 = self._camp(9, tarball='cnf.mu2e.D.C.1.tar')
        out = self._push([v0], [v0, v1])
        self.assertEqual(out['campaign_id'], 9)
        self.assertEqual(out['tarball'], 'cnf.mu2e.D.C.1.tar')

    def test_unreadable_ledger_snapshot_falls_back_to_strict_matching(self):
        """A first-ever push creates the DB as it enqueues, so the
        before-read legitimately fails. Recording an EMPTY snapshot there
        would make every pre-existing campaign look freshly created --
        including after a transient 'database is locked'. Unknown means
        'require exactly one live match' instead."""
        out = self._push(RuntimeError('cannot read the submission ledger'),
                         [self._camp(7)])
        self.assertEqual(out['campaign_id'], 7)

    def test_unreadable_snapshot_then_two_matches_raises(self):
        """The strict half of the rule above: with no snapshot to
        disambiguate with, two live matches cannot be resolved."""
        with self.assertRaises(RuntimeError) as ctx:
            self._push(RuntimeError('cannot read the submission ledger'),
                       [self._camp(3), self._camp(9)])
        self.assertIn('uniqueness invariant', str(ctx.exception))

    def test_raises_when_no_campaign_appeared(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._push([], [], cli={'rc': 0, 'stdout': 'looks fine',
                                    'stderr': ''})
        self.assertIn('no live campaign', str(ctx.exception))

    def test_only_live_campaigns_match(self):
        """A completed campaign for the same tarball is not a candidate:
        matching it would resurrect a finished campaign's id."""
        done = self._camp(3, state='complete')
        fresh = self._camp(9)
        out = self._push([done], [done, fresh])
        self.assertEqual(out['campaign_id'], 9)

    def test_failed_push_never_returns_a_pre_existing_campaign(self):
        """The re-run case the read-back could NOT save on its own: a
        live campaign for this tarball already exists, so a FAILED
        json2jobdef would find exactly one candidate. The rc check must
        stop it first."""
        with self.assertRaises(RuntimeError) as ctx:
            self._push([self._camp(3)], [self._camp(3)],
                       cli={'rc': 1, 'stdout': '',
                            'stderr': 'json2jobdef: boom'})
        self.assertIn('boom', str(ctx.exception))

    def test_ksu_truncated_failure_does_not_report_a_push_that_never_ran(self):
        """Same scenario one layer down, with run_cli REAL: ksu returns 0
        for a child that failed, and the only thing that catches it is
        the missing stderr sentinel."""
        with patch('subprocess.run') as run:
            run.return_value = SimpleNamespace(
                returncode=0, stdout='',
                stderr='json2jobdef: no such Musing\n')
            with patch('prodtools_mcp_write.tools._ledger_path_for',
                       return_value='/db'), \
                 patch('prodtools_mcp_write.tools._all_campaigns',
                       side_effect=[[], [self._camp(3)]]):
                with self.assertRaises(RuntimeError) as ctx:
                    self.tools.push_cnf(json=self.json_path, desc='D',
                                        dsconf='C', slice_size=500,
                                        run_as='self')
        self.assertIn('no such Musing', str(ctx.exception))

    # — entry selection (shared with json2jobdef itself) --------------

    def test_no_matching_json_entry_raises_without_guessing(self):
        # find_json_entry's own "found 0" message — reused verbatim
        # (via a ValueError wrapping its SystemExit) rather than
        # rephrased, so this can't drift from what json2jobdef itself
        # reports for the same input.
        with self.assertRaises(ValueError) as ctx:
            self.tools.push_cnf(json=self.json_path, desc='NOPE', dsconf='C',
                                slice_size=500, run_as='self')
        self.assertIn('found 0', str(ctx.exception).lower())

    def test_entry_missing_simjob_setup_raises_without_guessing(self):
        path = os.path.join(self._tmpdir, 'no_simjob.json')
        with open(path, 'w') as f:
            json.dump([{'desc': 'D', 'dsconf': 'C', 'owner': 'mu2e'}], f)
        with self.assertRaises(ValueError) as ctx:
            self.tools.push_cnf(json=path, desc='D', dsconf='C',
                                slice_size=500, run_as='self')
        self.assertIn('simjob_setup', str(ctx.exception))

    def test_find_json_entry_ambiguity_becomes_valueerror_not_systemexit(self):
        # find_json_entry sys.exit()s on 0 or >1 matches — fine for a
        # CLI, fatal for a long-running server process if it leaked
        # through uncaught.
        path = os.path.join(self._tmpdir, 'dup.json')
        with open(path, 'w') as f:
            json.dump([
                {'desc': 'D', 'dsconf': 'C', 'simjob_setup': self.simjob_setup},
                {'desc': 'D', 'dsconf': 'C', 'simjob_setup': self.simjob_setup},
            ], f)
        with self.assertRaises(ValueError):
            self.tools.push_cnf(json=path, desc='D', dsconf='C',
                                slice_size=500, run_as='self')

    def test_mixing_entry_with_no_raw_desc_resolves_end_to_end(self):
        # Mixing entries carry no literal `desc` key in the raw JSON --
        # it is derived from input_data + pbeam by prepare_fields_for_job
        # during expansion. A parallel scan over raw entries (matching
        # the literal `desc` key) can never find this; only json2jobdef's
        # own load_json + find_json_entry can.
        mix_json = str(Path(__file__).resolve().parent.parent /
                       'data' / 'Run1B' / 'mix.json')
        expected_setup = (
            '/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/Run1Bab/setup.sh')
        setup, tarball_desc = self.tools._select_push_params(
            mix_json, 'CeEndpointMixLow', 'Run1Bab_best_v1_2')
        self.assertEqual(setup, expected_setup)
        # No tarball_append on a mixing entry — tarball_desc falls
        # back to the (derived) desc itself.
        self.assertEqual(tarball_desc, 'CeEndpointMixLow')

        tar = 'cnf.mu2e.CeEndpointMixLow.Run1Bab_best_v1_2.0.tar'
        out = self._push([], [self._camp(7, tarball=tar)],
                         json_path=mix_json, desc='CeEndpointMixLow',
                         dsconf='Run1Bab_best_v1_2')
        self.assertEqual(
            self.last_run.call_args.kwargs.get('simjob_setup'),
            expected_setup)
        self.assertEqual(out['tarball'], tar)

    def test_owner_omitted_still_finds_the_right_campaign(self):
        # cnf_name defaults an omitted `owner` to $USER. Computing that
        # default in THIS server process (the caller, e.g. 'oksuzian')
        # while json2jobdef actually ran inside ksu with USER=mu2epro ->
        # owner 'mu2e' would report a push that ACTUALLY SUCCEEDED and
        # irreversibly registered a dataset as a failure. Selecting by
        # desc+dsconf parsed from the ledger's own tarball name never
        # needs to know owner.
        path = os.path.join(self._tmpdir, 'no_owner.json')
        with open(path, 'w') as f:
            json.dump([{
                'desc': 'D', 'dsconf': 'C', 'simjob_setup': self.simjob_setup,
                'fcl': 'x.fcl', 'outloc': {'*.art': 'disk'},
            }], f)
        # 'mu2e' — the identity ksu actually ran as, not this test
        # process's own $USER.
        out = self._push([], [self._camp(7, tarball='cnf.mu2e.D.C.0.tar')],
                         json_path=path, run_as='mu2epro', confirm=True)
        self.assertEqual(out['tarball'], 'cnf.mu2e.D.C.0.tar')

    def test_tarball_append_collision_returns_the_reco_campaign_not_digi(self):
        # Real regression against data/mdc2025/reco.json's
        # CosmicCRYExtracted / MDC2025au_best_v1_5 entry, which has
        # tarball_append='-reco' — its real tarball is
        # cnf.mu2e.CosmicCRYExtracted-reco.MDC2025au_best_v1_5.0.tar.
        # Matching on the bare desc would find a DIFFERENT stage's
        # campaign sharing the same desc+dsconf with no append.
        reco_json = str(Path(__file__).resolve().parent.parent /
                        'data' / 'mdc2025' / 'reco.json')
        digi = self._camp(
            3, tarball='cnf.mu2e.CosmicCRYExtracted.MDC2025au_best_v1_5.0.tar',
            datasets=('dig.mu2e.CosmicCRYExtracted.MDC2025au_best_v1_5.art',))
        reco = self._camp(
            9,
            tarball='cnf.mu2e.CosmicCRYExtracted-reco.'
                    'MDC2025au_best_v1_5.0.tar',
            datasets=('rec.mu2e.CosmicCRYExtracted.MDC2025au_best_v1_5.art',))
        out = self._push([digi], [digi, reco], json_path=reco_json,
                         desc='CosmicCRYExtracted',
                         dsconf='MDC2025au_best_v1_5')
        self.assertEqual(
            out['tarball'],
            'cnf.mu2e.CosmicCRYExtracted-reco.MDC2025au_best_v1_5.0.tar')
        self.assertEqual(
            out['datasets'],
            ['rec.mu2e.CosmicCRYExtracted.MDC2025au_best_v1_5.art'])

    # — prodtools_dir: dev-tarball opt-in ------------------------------

    def test_prodtools_dir_forwarded_for_self(self):
        before, after = [], [self._camp(1)]
        self._push(before, after, prodtools_dir='/exp/mu2e/app/users/u/prodtools')
        argv = self.last_run.call_args[0][0]
        i = argv.index('--prodtools-dir')
        self.assertEqual(argv[i + 1], '/exp/mu2e/app/users/u/prodtools')

    def test_prodtools_dir_absent_by_default(self):
        before, after = [], [self._camp(1)]
        self._push(before, after)
        self.assertNotIn('--prodtools-dir', self.last_run.call_args[0][0])

    def test_prodtools_dir_refused_for_mu2epro(self):
        with patch('prodtools_mcp_write.runner.run_cli') as run:
            with self.assertRaises(ValueError):
                self.tools.push_cnf(json=self.json_path, desc='D', dsconf='C', slice_size=500,
                                    run_as='mu2epro', confirm=True,
                                    prodtools_dir='/exp/mu2e/app/users/u/prodtools')
        run.assert_not_called()


# ---------------------------------------------------------------------------
# run_submissions tool
# ---------------------------------------------------------------------------

class TestPython39Compat(unittest.TestCase):
    """prodtools runs under the Python 3.9 that the Mu2e environment and
    the gpvm nodes ship. A PEP 604 union in an annotation (`int | None`)
    is evaluated at function-definition time and raises TypeError on
    import there -- but imports cleanly on 3.10+, so a developer on a
    newer interpreter never sees it. One such annotation took every
    test that imports prodtools_mcp_write.tools down with it (46 tests)
    and the breakage sat on main unnoticed. Scan the source instead of
    trusting the interpreter the suite happens to run on."""

    ROOTS = ('utils', os.path.join('mcp', 'src'))

    def test_no_pep604_unions_in_annotations(self):
        import ast
        repo = Path(__file__).resolve().parent.parent
        offenders = []
        for root in self.ROOTS:
            for path in sorted((repo / root).rglob('*.py')):
                tree = ast.parse(path.read_text(), filename=str(path))
                for node in ast.walk(tree):
                    anns = []
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        args = node.args
                        anns = [a.annotation for a in
                                args.posonlyargs + args.args + args.kwonlyargs
                                if a.annotation is not None]
                        for extra in (args.vararg, args.kwarg):
                            if extra is not None and extra.annotation is not None:
                                anns.append(extra.annotation)
                        if node.returns is not None:
                            anns.append(node.returns)
                    elif isinstance(node, ast.AnnAssign):
                        anns = [node.annotation]
                    for ann in anns:
                        for sub in ast.walk(ann):
                            if isinstance(sub, ast.BinOp) and isinstance(sub.op, ast.BitOr):
                                offenders.append(
                                    f"{path.relative_to(repo)}:{sub.lineno}")
        self.assertEqual(offenders, [],
                         'PEP 604 unions break import under Python 3.9; '
                         'use typing.Optional / typing.Union')


class TestRunSubmissionsTool(unittest.TestCase):
    """run_submissions and the ledger-identity helpers it (and push_cnf)
    share. The map-based enqueue_campaign tool was retired with
    submit_map --enqueue; campaign creation is push_cnf now."""

    def setUp(self):
        from prodtools_mcp_write import tools
        from utils import submission_ledger as sl
        self.tools = tools
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'submissions.db')

    def test_bare_tick_has_no_campaign_filter(self):
        """campaign_id=None is the bare `submissions run` -- the tick the
        (nonexistent) cron would fire. It is the only way to close open
        recovery rows once every campaign has left 'active': a fully
        submitted campaign is 'complete' while its rows still verify,
        and the scoped form refuses it as 'not active'. a02eafe added
        this form but left behind the guard that pinned campaign_id as
        required; these pins replace that guard."""
        with patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value=self.db):
            with patch('prodtools_mcp_write.tools._all_campaigns') as ledger:
                with patch('prodtools_mcp_write.runner.run_cli',
                           return_value={'rc': 0, 'stdout': 'ok',
                                         'stderr': ''}) as run:
                    out = self.tools.run_submissions(run_as='self')
        self.assertEqual(run.call_args[0][0], ['bin/submissions', 'run'])
        # No id to validate: an empty top-up is a real answer here
        # ("nothing active"), not a masked typo.
        ledger.assert_not_called()
        self.assertIsNone(out['campaign_id'])
        self.assertFalse(out['needs_attention'])

    def test_bare_tick_still_needs_confirm_for_mu2epro(self):
        with patch('prodtools_mcp_write.runner.run_cli') as run:
            with self.assertRaises(PermissionError):
                self.tools.run_submissions(run_as='mu2epro')
        run.assert_not_called()

    def test_bare_tick_other_rc_raises(self):
        with patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value=self.db):
            with patch('prodtools_mcp_write.runner.run_cli',
                       return_value={'rc': 1, 'stdout': '',
                                     'stderr': 'boom'}):
                with self.assertRaises(RuntimeError) as ctx:
                    self.tools.run_submissions(run_as='self')
        self.assertIn('boom', str(ctx.exception))

    def _active_campaign(self, tarball='cnf.mu2e.X.Y.0.tar', njobs=10):
        return self.sl.create_campaign(
            self.db, tarball=tarball,
            entry={'tarball': tarball, 'njobs': njobs}, slice_size=5)

    def test_run_submissions_refuses_mu2epro_without_confirm(self):
        # require_confirmed gates before even the ledger lookup.
        with patch('prodtools_mcp_write.runner.run_cli') as run:
            with self.assertRaises(PermissionError):
                self.tools.run_submissions(campaign_id=1, run_as='mu2epro')
        run.assert_not_called()

    def test_run_submissions_builds_the_expected_argv(self):
        cid = self._active_campaign()
        with patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value=self.db):
            with patch('prodtools_mcp_write.runner.run_cli',
                       return_value={'rc': 0, 'stdout': '',
                                     'stderr': ''}) as run:
                self.tools.run_submissions(campaign_id=cid, run_as='self')
        argv = run.call_args[0][0]
        self.assertEqual(argv,
                         ['bin/submissions', 'run', '--campaign', str(cid)])

    def test_run_submissions_reports_attention_keys(self):
        cid = self._active_campaign()
        with patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value=self.db):
            with patch('prodtools_mcp_write.runner.run_cli',
                       return_value={'rc': 2, 'stdout': '', 'stderr': ''}):
                out = self.tools.run_submissions(campaign_id=cid,
                                                  run_as='self')
        # rc=2 is the documented "needs attention" exit, not a crash.
        self.assertTrue(out['needs_attention'])
        self.assertEqual(out['rc'], 2)
        self.assertEqual(out['campaign_id'], cid)

    def test_run_submissions_rc_zero_is_not_needs_attention(self):
        cid = self._active_campaign()
        with patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value=self.db):
            with patch('prodtools_mcp_write.runner.run_cli',
                       return_value={'rc': 0, 'stdout': 'ok', 'stderr': ''}):
                out = self.tools.run_submissions(campaign_id=cid,
                                                  run_as='self')
        self.assertFalse(out['needs_attention'])
        self.assertEqual(out['rc'], 0)

    def test_run_submissions_other_rc_raises(self):
        cid = self._active_campaign()
        with patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value=self.db):
            with patch('prodtools_mcp_write.runner.run_cli',
                       return_value={'rc': 1, 'stdout': '',
                                     'stderr': 'boom'}):
                with self.assertRaises(RuntimeError) as ctx:
                    self.tools.run_submissions(campaign_id=cid, run_as='self')
        self.assertIn('boom', str(ctx.exception))

    def test_run_submissions_unknown_campaign_raises_before_run_cli(self):
        # A typo'd id must not filter top_up's campaign list to empty
        # and report a no-op tick as a success (rc=0, no attention).
        with patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value=self.db):
            with patch('prodtools_mcp_write.runner.run_cli') as run:
                with self.assertRaises(ValueError) as ctx:
                    self.tools.run_submissions(campaign_id=999, run_as='self')
        run.assert_not_called()
        self.assertIn('no campaign 999', str(ctx.exception))

    def test_run_submissions_non_active_campaign_raises_before_run_cli(self):
        cid = self._active_campaign()
        self.sl.set_campaign_state(self.db, cid, 'paused', note='x')
        with patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value=self.db):
            with patch('prodtools_mcp_write.runner.run_cli') as run:
                with self.assertRaises(ValueError) as ctx:
                    self.tools.run_submissions(campaign_id=cid, run_as='self')
        run.assert_not_called()
        msg = str(ctx.exception)
        self.assertIn('paused', msg)
        # Distinct wording from the "unknown id" case — a paused
        # campaign needs `submissions resume`, a typo'd id needs a
        # different fix, and an operator needs to tell them apart.
        self.assertNotIn('no campaign', msg)

    def test_run_submissions_rc2_survives_the_ksu_truncation(self):
        # End to end with run_cli REAL: the tick exits 2 ("held rows,
        # exhausted recoveries, a paused campaign — a human should
        # look"), ksu truncates that to 0, and only the stderr sentinel
        # carries it out. Without it every such tick reported
        # needs_attention=False.
        cid = self._active_campaign()
        with patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value=self.db):
            with patch('subprocess.run') as run:
                run.return_value = SimpleNamespace(
                    returncode=0, stdout='submissions summary: held=1',
                    stderr='__PRODTOOLS_RC__:2\n')
                out = self.tools.run_submissions(campaign_id=cid,
                                                 run_as='self')
        self.assertEqual(out['rc'], 2)
        self.assertTrue(out['needs_attention'])

    def test_run_submissions_crashed_tick_is_not_reported_as_success(self):
        # A tick that died (lock contention, traceback) leaves no
        # sentinel. That is a failure, not a clean rc=0 tick.
        cid = self._active_campaign()
        with patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value=self.db):
            with patch('subprocess.run') as run:
                run.return_value = SimpleNamespace(
                    returncode=0, stdout='',
                    stderr='another submissions run holds the lock\n')
                with self.assertRaises(RuntimeError) as ctx:
                    self.tools.run_submissions(campaign_id=cid, run_as='self')
        self.assertIn('holds the lock', str(ctx.exception))

    def test_unopenable_ledger_is_named_not_a_bare_sqlite_error(self):
        # A brand-new user has no /exp/mu2e/data/users/<them>/prodtools
        # directory; sqlite reports that as a context-free
        # OperationalError several frames down.
        missing = '/nonexistent-ledger-dir-mcp-test/submissions.db'
        with self.assertRaises(RuntimeError) as ctx:
            self.tools._all_campaigns(missing)
        self.assertIn(missing, str(ctx.exception))

    def test_run_submissions_surfaces_the_named_ledger_error(self):
        with patch('prodtools_mcp_write.tools._ledger_path_for',
                   return_value='/nonexistent-ledger-dir-mcp-test/s.db'):
            with patch('prodtools_mcp_write.runner.run_cli') as run:
                with self.assertRaises(RuntimeError) as ctx:
                    self.tools.run_submissions(campaign_id=1, run_as='self')
        run.assert_not_called()
        self.assertIn('/nonexistent-ledger-dir-mcp-test/s.db',
                      str(ctx.exception))

    def test_ledger_path_for_mu2epro_is_production(self):
        from utils import submission_ledger
        self.assertEqual(self.tools._ledger_path_for('mu2epro'),
                         submission_ledger.PRODUCTION_DB)

    def test_ledger_path_for_self_is_not_production(self):
        from utils import submission_ledger
        self.assertEqual(self.tools._ledger_path_for('self'),
                         submission_ledger.ledger_for())


# ---------------------------------------------------------------------------
# 41. ledger_expected (utils/submissions.py)
# ---------------------------------------------------------------------------

class TestLedgerExpected(unittest.TestCase):
    """Expected job counts per output dataset, sourced from the submission
    ledger. The dataset NAME comes from the cnf tarball; the COUNT comes from
    the ledger entry's njobs (the submitted window)."""

    CRY = 'cnf.mu2e.CosmicCRYAll.MDC2025au_best_v1_5.0.tar'
    MDS = 'cnf.mu2e.ensembleMDS3c.MDC2025au_best_v1_5.0.tar'
    OTHER = 'cnf.mu2e.NoPrimary.MDC2025ar_best_v1_3.0.tar'
    OUT = {
        CRY: ['dig.mu2e.CosmicCRYAllOnSpill.MDC2025au_best_v1_5.art'],
        MDS: ['dig.mu2e.ensembleMDS3cOnSpill.MDC2025au_best_v1_5.art'],
        OTHER: ['dig.mu2e.NoPrimaryOnSpill.MDC2025ar_best_v1_3.art'],
    }
    CRY_DS = 'dig.mu2e.CosmicCRYAllOnSpill.MDC2025au_best_v1_5.art'
    MDS_DS = 'dig.mu2e.ensembleMDS3cOnSpill.MDC2025au_best_v1_5.art'

    def _call(self, camps, dsconfs=None, unlocatable=()):
        """Run ledger_expected with the tarball layer faked out.

        locate is injected; Mu2eJobPars is reduced to identity so the 'path'
        is just the tarball name, which extract_ then maps to datasets."""
        from utils import submissions
        asked = []

        def fake_locate(tarball):
            asked.append(tarball)
            return None if tarball in unlocatable else tarball

        with patch.object(submissions, 'Mu2eJobPars', lambda p: p), \
             patch.object(submissions, 'extract_datasets_from_tarball',
                          lambda job, njobs: self.OUT[job]), \
             patch.object(submissions.submission_ledger, 'all_campaigns',
                          return_value=camps):
            expected, failures = submissions.ledger_expected(
                '/nonexistent.db', dsconfs=dsconfs, locate=fake_locate)
        return expected, failures, asked

    def test_maps_output_dataset_to_njobs(self):
        camps = [{'tarball': self.CRY, 'entry': {'njobs': 2500}},
                 {'tarball': self.MDS, 'entry': {'njobs': 496}}]
        expected, failures, _ = self._call(camps)
        self.assertEqual(expected, {self.CRY_DS: 2500, self.MDS_DS: 496})
        self.assertEqual(failures, {})

    def test_uses_submitted_window_not_cnf_capacity(self):
        """CosmicCRYAll's cnf carries 12500 capacity; the ledger entry says the
        2500 that were actually submitted. The ledger value must win."""
        camps = [{'tarball': self.CRY, 'entry': {'njobs': 2500}}]
        expected, _, _ = self._call(camps)
        self.assertEqual(expected[self.CRY_DS], 2500)

    def test_overlapping_campaigns_take_the_max_not_the_sum(self):
        """A tarball can be enqueued as several index windows, and njobs is
        an ABSOLUTE target index count, not an increment: a later campaign
        resumes via its cursor from where the earlier one stopped, so the
        windows overlap rather than partition. Measured from
        RPCInternalPhysicalMix1BB's real submission rows: campaign 1 covered
        indices 0..249 (njobs=250), campaign 2 covered 0..1666 (njobs=1667,
        already a superset of the first). Expected is max(250, 1667) = 1667,
        not their sum 1917 — summing double-counted the first window and
        made two actually-complete datasets (this one and
        RPCExternalPhysicalMix1BB) report INCOMPLETE. The tarball is
        resolved only once regardless."""
        camps = [{'tarball': self.CRY, 'entry': {'njobs': 250}},
                 {'tarball': self.CRY, 'entry': {'njobs': 1667}}]
        expected, _, asked = self._call(camps)
        self.assertEqual(expected[self.CRY_DS], 1667)
        self.assertEqual(asked, [self.CRY])

    def test_unresolvable_tarball_yields_failure_not_a_number(self):
        camps = [{'tarball': self.CRY, 'entry': {'njobs': 2500}},
                 {'tarball': self.MDS, 'entry': {'njobs': 496}}]
        expected, failures, _ = self._call(camps, unlocatable={self.CRY})
        self.assertNotIn(self.CRY_DS, expected)
        self.assertIn(self.CRY, failures)
        self.assertEqual(expected[self.MDS_DS], 496)   # others unaffected

    def test_dsconf_filter_skips_other_campaigns_without_resolving(self):
        camps = [{'tarball': self.CRY, 'entry': {'njobs': 2500}},
                 {'tarball': self.OTHER, 'entry': {'njobs': 100}}]
        expected, _, asked = self._call(
            camps, dsconfs={'MDC2025au_best_v1_5'})
        self.assertEqual(asked, [self.CRY])
        self.assertEqual(list(expected), [self.CRY_DS])


class TestLedgerExpectedDraining(unittest.TestCase):
    """Denominators for a DRAINING campaign, which has no njobs.

    The honest denominator for a 1:1 direct-input stage is the INPUT
    dataset's current file count: 80 digis in means 80 mcs out. Before
    this, every draining output rendered '—' because ledger_expected
    skipped any campaign without njobs (observed live on campaign 48:
    `80 mcs.mu2e.CeMLeadingLogOnSpill.MDC2025au_best_v1_5.art ... —`)."""

    TB = 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar'
    CAMP = {'tarball': TB, 'entry': {
        'tarball': TB, 'inloc': 'tape',
        'input_pattern': 'dig.mu2e.%OnSpill.MDC2025au_best_v1_5.art',
        'outputs': [{'dataset': 'mcs.*.art', 'location': 'tape'}]}}
    OUT_DS = 'mcs.mu2e.CeMLeadingLogOnSpill.MDC2025au_best_v1_5.art'
    IN_DS = 'dig.mu2e.CeMLeadingLogOnSpill.MDC2025au_best_v1_5.art'

    def _call(self, datasets, counts=None, pars=None):
        from utils import submissions
        counted = []

        def fake_count(ds):
            counted.append(ds)
            return (counts or {}).get(ds, 0)

        with patch.object(submissions, 'Mu2eJobPars',
                          lambda p: pars if pars is not None else _DrainPars(p)), \
             patch.object(submissions.submission_ledger, 'all_campaigns',
                          return_value=[self.CAMP]):
            expected, failures = submissions.ledger_expected(
                '/nonexistent.db', datasets=datasets,
                locate=lambda tb: tb, count_fn=fake_count)
        return expected, failures, counted

    def test_denominator_is_the_input_dataset_file_count(self):
        expected, failures, counted = self._call(
            {self.OUT_DS}, counts={self.IN_DS: 80})
        self.assertEqual(expected, {self.OUT_DS: 80})
        self.assertEqual(counted, [self.IN_DS])
        self.assertEqual(failures, {})

    def test_without_datasets_a_draining_campaign_contributes_nothing(self):
        """Each denominator costs a SAM count, and a draining campaign's desc
        space is large (21 datasets for au reco). A caller that did not name
        what it wants must not pay for them."""
        expected, _, counted = self._call(None, counts={self.IN_DS: 80})
        self.assertEqual(expected, {})
        self.assertEqual(counted, [])

    def test_dataset_outside_the_input_pattern_is_skipped(self):
        """A dataset from some other campaign must not be handed this
        campaign's denominator."""
        other = 'mcs.mu2e.NoPrimaryOnSpill.MDC2025ar_best_v1_3.art'
        expected, _, counted = self._call({other}, counts={other: 999})
        self.assertEqual(expected, {})
        self.assertEqual(counted, [])

    def test_suffixed_output_is_not_guessed_from_the_desc(self):
        """A cnf whose outputs carry a suffix ({desc}-KL) breaks the
        desc==desc assumption. expected_outputs_for is the arbiter: when the
        cnf does not actually produce this dataset, it keeps '—' rather than
        being handed the input count. Guards the trap the contract already
        names — FlatGamma is a prefix of FlatGammaCalo."""
        class SuffixPars:
            def __init__(self, path): pass
            def job_outputs(self, i, override_desc=None, override_seq=None):
                return {'Output': f'mcs.mu2e.{override_desc}-KL.'
                                  f'MDC2025au_best_v1_5.{override_seq}.art'}
        expected, _, counted = self._call(
            {self.OUT_DS}, counts={self.IN_DS: 80}, pars=SuffixPars(None))
        self.assertEqual(expected, {})
        self.assertEqual(counted, [])

    def test_unlocatable_cnf_is_a_failure_not_a_denominator(self):
        from utils import submissions
        with patch.object(submissions, 'Mu2eJobPars', lambda p: _DrainPars(p)), \
             patch.object(submissions.submission_ledger, 'all_campaigns',
                          return_value=[self.CAMP]):
            expected, failures = submissions.ledger_expected(
                '/nonexistent.db', datasets={self.OUT_DS},
                locate=lambda tb: None, count_fn=lambda ds: 80)
        self.assertEqual(expected, {})
        self.assertIn(self.TB, failures)

    def test_index_mode_campaigns_still_use_njobs(self):
        """The draining branch must not disturb the njobs path."""
        from utils import submissions
        cry = 'cnf.mu2e.CosmicCRYAll.MDC2025au_best_v1_5.0.tar'
        cry_ds = 'dig.mu2e.CosmicCRYAllOnSpill.MDC2025au_best_v1_5.art'
        with patch.object(submissions, 'Mu2eJobPars', lambda p: p), \
             patch.object(submissions, 'extract_datasets_from_tarball',
                          lambda job, njobs: [cry_ds]), \
             patch.object(submissions.submission_ledger, 'all_campaigns',
                          return_value=[{'tarball': cry,
                                         'entry': {'njobs': 2500}}]):
            expected, failures = submissions.ledger_expected(
                '/nonexistent.db', datasets={cry_ds}, locate=lambda tb: tb,
                count_fn=lambda ds: 7)
        self.assertEqual(expected, {cry_ds: 2500})


# ---------------------------------------------------------------------------
# 42. listNewDatasets completeness column (ledger-backed)
# ---------------------------------------------------------------------------

class TestListerCompleteness(unittest.TestCase):
    """The COMPLETENESS column formats <landed>/<expected> from the ledger map.
    listNewDatasets uses bare imports, so utils/ must be on sys.path."""

    @classmethod
    def setUpClass(cls):
        d = os.path.join(os.path.dirname(__file__), '..', 'utils')
        if d not in sys.path:
            sys.path.insert(0, d)
        import listNewDatasets
        cls.lnd = listNewDatasets

    DS = 'dig.mu2e.CosmicCRYAllOnSpill.MDC2025au_best_v1_5.art'

    def _lister(self, expected, counts, color='auto'):
        lister = self.lnd.DatasetLister(completeness=True, color=color)
        lister._expected = expected
        lister._total_files = lambda ds: counts.get(ds, 0)
        return lister

    # The tty check gates plain-text vs coloured rendering (see
    # _get_completeness). Patch it explicitly in every test rather than
    # relying on how the test runner happens to be invoked (tty vs piped) —
    # that ambient-state dependency is exactly the flakiness the tty gate
    # is designed to avoid downstream, so the tests must not reintroduce it.

    def test_reports_landed_over_expected_with_incomplete_marker_non_tty(self):
        """Piped/redirected output: today's plain-text marker, no escape
        codes, so grep/awk consumers aren't corrupted."""
        lister = self._lister({self.DS: 2500}, {self.DS: 1432})
        with patch('sys.stdout.isatty', return_value=False):
            self.assertEqual(lister._get_completeness(self.DS),
                             "1432/2500 INCOMPLETE")

    def test_reports_landed_over_expected_in_red_on_tty(self):
        """Interactive output: red ANSI text, ' INCOMPLETE' suffix dropped."""
        lister = self._lister({self.DS: 2500}, {self.DS: 1432})
        with patch('sys.stdout.isatty', return_value=True):
            self.assertEqual(lister._get_completeness(self.DS),
                             "\033[31m1432/2500\033[0m")

    def test_no_marker_once_landed_reaches_expected_non_tty(self):
        lister = self._lister({self.DS: 2500}, {self.DS: 2500})
        with patch('sys.stdout.isatty', return_value=False):
            self.assertEqual(lister._get_completeness(self.DS), "2500/2500")

    def test_no_marker_or_colour_once_landed_reaches_expected_tty(self):
        """Complete rows are never coloured, even interactively."""
        lister = self._lister({self.DS: 2500}, {self.DS: 2500})
        with patch('sys.stdout.isatty', return_value=True):
            self.assertEqual(lister._get_completeness(self.DS), "2500/2500")

    def test_dataset_from_no_campaign_reports_dash(self):
        """The em dash ('no known campaign') is never coloured or marked,
        in either mode."""
        lister = self._lister({}, {self.DS: 17})
        with patch('sys.stdout.isatty', return_value=False):
            self.assertEqual(lister._get_completeness(self.DS), "—")
        with patch('sys.stdout.isatty', return_value=True):
            self.assertEqual(lister._get_completeness(self.DS), "—")

    # --color {auto,always,never}, the ls/grep convention (round 3). 'auto'
    # is exercised by the tty/non-tty pair above (it's _lister's default).
    # 'always' and 'never' must override the tty check in both directions —
    # that's the whole point, since 'auto' alone left colour unreachable
    # for anyone piping through grep.

    def test_color_always_emits_escapes_even_off_a_tty(self):
        """--color always is what makes `| grep` usable with colour: red,
        no INCOMPLETE suffix, regardless of stdout.isatty()."""
        lister = self._lister({self.DS: 2500}, {self.DS: 1432}, color='always')
        with patch('sys.stdout.isatty', return_value=False):
            self.assertEqual(lister._get_completeness(self.DS),
                             "\033[31m1432/2500\033[0m")

    def test_color_never_emits_plain_marker_even_on_a_tty(self):
        """--color never is for reproducible captures: plain text with the
        INCOMPLETE suffix, regardless of stdout.isatty()."""
        lister = self._lister({self.DS: 2500}, {self.DS: 1432}, color='never')
        with patch('sys.stdout.isatty', return_value=True):
            self.assertEqual(lister._get_completeness(self.DS),
                             "1432/2500 INCOMPLETE")


# ---------------------------------------------------------------------------
# 43. Draining campaigns: foundations (is_draining, expected_outputs_for)
# ---------------------------------------------------------------------------

class TestIsDraining(unittest.TestCase):
    """Campaign/row kind is discriminated ONLY by input_pattern presence."""

    def test_pattern_entry_is_draining(self):
        from utils.jobdesc import is_draining
        self.assertTrue(is_draining(
            {'tarball': 't', 'input_pattern': 'dig.mu2e.%.X.art'}))

    def test_index_entry_is_not(self):
        from utils.jobdesc import is_draining
        self.assertFalse(is_draining({'tarball': 't', 'njobs': 100}))


class TestExpectedOutputsFor(unittest.TestCase):
    """The single input->output name mapping, delegating to job_outputs
    (the exact worker-side substitution) so verifier and worker cannot
    drift."""

    IN = 'dig.mu2e.CosmicCRYAllOnSpill.MDC2025au_best_v1_5.001202_00000042.art'

    class FakePars:
        def __init__(self, out):
            self.out = out
            self.calls = []

        def job_outputs(self, index, override_desc=None, override_seq=None):
            self.calls.append((index, override_desc, override_seq))
            return self.out

    def test_delegates_desc_and_sequencer_from_input_name(self):
        from utils.job_common import expected_outputs_for
        jp = self.FakePars({'Output':
            'mcs.mu2e.CosmicCRYAllOnSpill.MDC2025au_best_v1_5.001202_00000042.art'})
        outs = expected_outputs_for(self.IN, jp)
        self.assertEqual(jp.calls, [(0, 'CosmicCRYAllOnSpill',
                                     '001202_00000042')])
        self.assertEqual(outs, ['mcs.mu2e.CosmicCRYAllOnSpill.'
                                'MDC2025au_best_v1_5.001202_00000042.art'])

    def test_filters_non_mu2e_streams_and_sorts(self):
        from utils.job_common import expected_outputs_for
        jp = self.FakePars({'b': 'nts.mu2e.X.C.000_000.root',
                            'null': '/dev/null',
                            'a': 'mcs.mu2e.X.C.000_000.art'})
        self.assertEqual(expected_outputs_for(self.IN, jp),
                         ['mcs.mu2e.X.C.000_000.art',
                          'nts.mu2e.X.C.000_000.root'])

    def test_dataset_name_rejected(self):
        from utils.job_common import expected_outputs_for
        with self.assertRaises(ValueError):
            expected_outputs_for('dig.mu2e.X.C.art', self.FakePars({}))

    def test_junk_name_rejected(self):
        from utils.job_common import expected_outputs_for
        with self.assertRaises(ValueError):
            expected_outputs_for('not-a-mu2e-name', self.FakePars({}))

    def test_no_outputs_is_a_hard_error(self):
        from utils.job_common import expected_outputs_for
        with self.assertRaises(RuntimeError):
            expected_outputs_for(self.IN, self.FakePars({'n': '/dev/null'}))


# ---------------------------------------------------------------------------
# 44. Draining campaigns: enqueue validation
# ---------------------------------------------------------------------------

class TestValidateDrainingEntry(unittest.TestCase):
    BASE = {'tarball': 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar',
            'inloc': 'tape',
            'input_pattern': 'dig.mu2e.%.MDC2025au_best_v1_5.art',
            'outputs': [{'dataset': 'mcs.*.art', 'location': 'tape'}]}

    def _err(self, **over):
        from utils.submit import _validate_draining_entry
        return _validate_draining_entry({**self.BASE, **over})

    def test_valid_entry_passes(self):
        self.assertIsNone(self._err())

    def test_njobs_and_pattern_conflict(self):
        self.assertIn('njobs', self._err(njobs=100))

    def test_firstjob_rejected(self):
        self.assertIn('firstjob', self._err(firstjob=500))

    def test_pattern_must_be_five_fields(self):
        self.assertIn('5-field', self._err(input_pattern='dig.mu2e.%.art'))

    def test_missing_required_key(self):
        entry = {k: v for k, v in self.BASE.items() if k != 'outputs'}
        from utils.submit import _validate_draining_entry
        self.assertIn('outputs', _validate_draining_entry(entry))

    def test_exclude_desc_must_be_string_list(self):
        self.assertIn('exclude_desc', self._err(exclude_desc='NoPrimary'))

    def test_min_age_must_be_nonnegative_int(self):
        self.assertIn('min_age', self._err(min_age_minutes=-5))

    def test_prestage_must_be_bool(self):
        self.assertIn('prestage', self._err(prestage='yes'))

    def test_outputs_glob_matching_pattern_rejected(self):
        """A '*.art' outputs glob matches the input pattern — the worker
        would declare the fetched input for push and pushOutput's orphan
        recovery would try to delete the production input (smoke cluster
        29444911)."""
        err = self._err(outputs=[{'dataset': '*.art', 'location': 'tape'}])
        self.assertIn('input_pattern', err)
        self.assertIn('*.art', err)


class TestEnqueueDraining(unittest.TestCase):
    """enqueue_entry on a draining entry creates a campaign with the
    snapshotted entry; check_inputs is skipped (a generic cnf bakes no
    inputs — the tick gates each batch instead)."""

    ENTRY = {'tarball': 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
             'inloc': 'tape',
             'input_pattern': 'dig.mu2e.%.MDC2025au_best_v1_5.art',
             'outputs': [{'dataset': 'mcs.*.art', 'location': 'tape'}]}

    def test_creates_campaign_without_check_inputs(self):
        from utils import submit
        created = {}

        def fake_create(db, *, tarball, entry, slice_size, origin):
            created.update(tarball=tarball, entry=entry,
                           slice_size=slice_size)
            return 48

        with patch.object(submit, '_ensure_local_tarball',
                          return_value='/tmp/t.tar'), \
             patch.object(submit, 'check_inputs') as ci, \
             patch.object(submit, 'check_code_tarball',
                          return_value=(True, [])), \
             patch.object(submit.submission_ledger, 'create_campaign',
                          fake_create):
            camp_id = submit.enqueue_entry(dict(self.ENTRY),
                                           ledger_db='/x.db',
                                           slice_size=500)
        self.assertEqual(camp_id, 48)
        ci.assert_not_called()
        self.assertEqual(created['slice_size'], 500)
        self.assertEqual(created['entry']['input_pattern'],
                         self.ENTRY['input_pattern'])

    def test_invalid_draining_entry_exits(self):
        from utils import submit
        bad = dict(self.ENTRY, njobs=100)
        with patch.object(submit, '_ensure_local_tarball',
                          return_value='/tmp/t.tar'):
            with self.assertRaises(SystemExit):
                submit.enqueue_entry(bad, ledger_db='/x.db', slice_size=500)

    def test_failing_code_check_blocks_draining_campaign(self):
        """The draining branch skips check_inputs entirely (a generic
        cnf bakes no inputs), so check_code_tarball is the ONLY gate
        standing between a code-mismatched cnf and a live campaign row.
        A (False, ...) result must exit 2 and create nothing."""
        from utils import submit
        created = []
        with patch.object(submit, '_ensure_local_tarball',
                          return_value='/tmp/t.tar'), \
             patch.object(submit, 'check_code_tarball',
                          return_value=(False, [submit.Problem(
                              'code', '/tmp/t.tar', 'code_mismatch',
                              'digest mismatch')])), \
             patch.object(submit.submission_ledger, 'create_campaign',
                          side_effect=lambda *a, **k: created.append(1)):
            with self.assertRaises(SystemExit) as cm:
                submit.enqueue_entry(dict(self.ENTRY), ledger_db='/x.db',
                                     slice_size=500)
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(created, [])   # no campaign row

    def test_dry_run_creates_nothing(self):
        from utils import submit
        with patch.object(submit, '_ensure_local_tarball',
                          return_value='/tmp/t.tar'), \
             patch.object(submit, 'check_code_tarball',
                          return_value=(True, [])), \
             patch.object(submit.submission_ledger,
                          'create_campaign') as cc:
            result = submit.enqueue_entry(dict(self.ENTRY),
                                          ledger_db='/x.db',
                                          slice_size=500, dry_run=True)
        self.assertIsNone(result)
        cc.assert_not_called()


# ---------------------------------------------------------------------------
# 45. Draining campaigns: --files dispatch
# ---------------------------------------------------------------------------

class TestParseFiles(unittest.TestCase):
    F1 = 'dig.mu2e.A.MDC2025au_best_v1_5.001202_00000001.art'
    F2 = 'dig.mu2e.B.MDC2025au_best_v1_5.001202_00000002.art'

    def _parse(self, text):
        from utils.submit import parse_files
        with tempfile.NamedTemporaryFile('w', suffix='.txt',
                                         delete=False) as fh:
            fh.write(text)
        try:
            return parse_files(fh.name)
        finally:
            os.unlink(fh.name)

    def test_none_passthrough(self):
        from utils.submit import parse_files
        self.assertIsNone(parse_files(None))

    def test_sorted_unique_with_comments(self):
        got = self._parse(f"# header\n{self.F2}\n{self.F1}\n{self.F2}\n")
        self.assertEqual(got, [self.F1, self.F2])

    def test_junk_name_raises(self):
        with self.assertRaises(ValueError):
            self._parse("not-a-name\n")

    def test_dataset_name_raises(self):
        with self.assertRaises(ValueError):
            self._parse("dig.mu2e.A.MDC2025au_best_v1_5.art\n")

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            self._parse("# only a comment\n")


class TestBuildOpsJsonFiles(unittest.TestCase):
    def test_build_ops_json_ships_single_jobdesc(self):
        from utils.jobsub_argv import build_ops_json
        entry = {'tarball': 'cnf.mu2e.D.C.0.tar', 'njobs': 10,
                 'inloc': 'tape', 'outputs': []}
        ops = build_ops_json(entry=entry, jobset=[0, 1],
                             input_datasets=[], files=None)
        self.assertIsInstance(ops['jobdesc'], dict)
        self.assertEqual(ops['jobdesc']['tarball'], 'cnf.mu2e.D.C.0.tar')

    def test_files_key_present_only_when_given(self):
        from utils.jobsub_argv import build_ops_json
        entry = {'tarball': 't', 'inloc': 'tape',
                 'outputs': [{'dataset': '*.art', 'location': 'tape'}]}
        ops = build_ops_json(entry=entry, jobset=[0, 1],
                             input_datasets=['dig.mu2e.A.C.art'],
                             files=['f1.art', 'f2.art'])
        self.assertEqual(ops['files'], ['f1.art', 'f2.art'])
        ops2 = build_ops_json(entry=entry, jobset=[0, 1],
                              input_datasets=['dig.mu2e.A.C.art'])
        self.assertNotIn('files', ops2)


class TestSubmitEntryFiles(unittest.TestCase):
    """Files mode: jobset = positions, ledger row stores filenames,
    scopes derive from the mapped outputs of the batch."""

    ENTRY = {'tarball': 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar', 'prodtools_dir': FAKE_PRODTOOLS_DIR,
             'inloc': 'tape',
             'input_pattern': 'dig.mu2e.%.MDC2025au_best_v1_5.art',
             'outputs': [{'dataset': '*.art', 'location': 'tape'}]}
    FILES = ['dig.mu2e.A.MDC2025au_best_v1_5.001202_00000001.art',
             'dig.mu2e.B.MDC2025au_best_v1_5.001202_00000002.art']

    class FakePars:
        def __init__(self, path):
            pass

        def job_outputs(self, index, override_desc=None, override_seq=None):
            return {'Output': f'mcs.mu2e.{override_desc}.'
                              f'MDC2025au_best_v1_5.{override_seq}.art'}

    def _opts(self, **over):
        from utils.submit import SubmitOptions
        base = dict(ledger_db='/x.db', dry_run=False,
                    files=list(self.FILES), origin='/m.json')
        base.update(over)
        return SubmitOptions(**base)

    def test_files_submission_records_filenames_in_ledger(self):
        from utils import submit
        reserved = {}

        def fake_reserve(db, *, tarball, entry, indices, origin=None,
                         parent_id=None):
            reserved['indices'] = indices
            return 99

        with patch.object(submit, '_ensure_local_tarball',
                          return_value=Path('/tmp/t.tar')), \
             patch('utils.jobquery.Mu2eJobPars', self.FakePars), \
             patch.object(submit, 'check_code_tarball',
                          return_value=(True, [])), \
             patch.object(submit, '_run_submit',
                          return_value={'tarball': self.ENTRY['tarball'],
                                        'cluster_id': '123',
                                        'jobsub_id': '123.0@s',
                                        'njobs': 2, 'status': 'submitted',
                                        'raw_output': ''}) as rs, \
             patch.object(submit, '_log_submission'), \
             patch.object(submit.submission_ledger, 'reserve_submission',
                          fake_reserve), \
             patch.object(submit.submission_ledger, 'attach_cluster'):
            result = submit.submit_entry(dict(self.ENTRY), 0,
                                         self._opts())
        self.assertEqual(result['status'], 'submitted')
        self.assertEqual(reserved['indices'], self.FILES)
        # the jobsub argv references an ops JSON (shipped via dropbox)
        cmd = rs.call_args[0][0]
        self.assertEqual(cmd[0], 'jobsub_submit')
        self.assertTrue(any('ops-' in c for c in cmd))

    def test_files_dry_run_submits_nothing(self):
        from utils import submit
        with patch.object(submit, '_ensure_local_tarball',
                          return_value=Path('/tmp/t.tar')), \
             patch('utils.jobquery.Mu2eJobPars', self.FakePars), \
             patch.object(submit, '_run_submit') as rs:
            result = submit.submit_entry(dict(self.ENTRY), 0,
                                         self._opts(dry_run=True))
        self.assertEqual(result['status'], 'dry_run')
        self.assertEqual(result['njobs'], 2)
        rs.assert_not_called()

    def test_files_dry_run_still_resolves_real_tarball(self):
        """Regression: files mode needs the REAL cnf even on a dry run
        (the output-name mapping comes from parsing it via
        Mu2eJobPars/expected_outputs_for). The nonexistent-stand-in
        shortcut is for index mode only. ENTRY['tarball'] does not
        exist relative to cwd, so the old buggy guard (gated on
        `options.dry_run` alone) would have taken the stand-in branch
        and never called `_ensure_local_tarball` at all."""
        from utils import submit
        self.assertFalse(Path(self.ENTRY['tarball']).resolve().is_file())
        with patch.object(submit, '_ensure_local_tarball',
                          return_value=Path('/tmp/t.tar')) as elt, \
             patch('utils.jobquery.Mu2eJobPars', self.FakePars), \
             patch.object(submit, '_run_submit') as rs:
            submit.submit_entry(dict(self.ENTRY), 0,
                                self._opts(dry_run=True))
        elt.assert_called_once_with(self.ENTRY['tarball'])
        rs.assert_not_called()

    def test_two_descs_scopes_cover_both_outputs(self):
        """I2: with a desc-discriminating outputs glob (one desc's
        mapped output goes to tape, the other's to disk), token scopes
        must be derived from EVERY distinct desc in the batch, not just
        files[0]'s. self.FILES carries desc A then desc B."""
        from utils import submit
        entry = {**self.ENTRY, 'outputs': [
            {'dataset': 'mcs.mu2e.A.*', 'location': 'tape'},
            {'dataset': 'mcs.mu2e.B.*', 'location': 'disk'},
        ]}
        with patch.object(submit, '_ensure_local_tarball',
                          return_value=Path('/tmp/t.tar')), \
             patch('utils.jobquery.Mu2eJobPars', self.FakePars), \
             patch.object(submit, 'check_code_tarball',
                          return_value=(True, [])), \
             patch.object(submit, '_run_submit',
                          return_value={'tarball': entry['tarball'],
                                        'cluster_id': '123',
                                        'jobsub_id': '123.0@s',
                                        'njobs': 2, 'status': 'submitted',
                                        'raw_output': ''}) as rs, \
             patch.object(submit, '_log_submission'), \
             patch.object(submit.submission_ledger, 'reserve_submission',
                          lambda *a, **k: 99), \
             patch.object(submit.submission_ledger, 'attach_cluster'):
            submit.submit_entry(entry, 0, self._opts())
        cmd = rs.call_args[0][0]
        scopes = [cmd[i + 1] for i, c in enumerate(cmd)
                 if c == '--need-storage-modify']
        self.assertTrue(any('/tape/' in s for s in scopes),
                        f"missing desc-A (tape) scope in {scopes}")
        self.assertTrue(any('/persistent/datasets/' in s for s in scopes),
                        f"missing desc-B (disk) scope in {scopes}")


class TestSliceOverlapSkipsFileRows(unittest.TestCase):
    def test_file_keyed_row_never_matches_an_index_window(self):
        from utils import submissions
        row = {'tarball': 'cnf.mu2e.reco.X.0.tar',
               'entry': {'input_pattern': 'dig.mu2e.%.X.art'},
               'indices': ['dig.mu2e.A.X.001202_00000001.art']}
        with patch.object(submissions.submission_ledger, 'all_rows',
                          return_value=[row]):
            self.assertFalse(submissions._slice_overlaps_ledger(
                '/x.db', 'cnf.mu2e.reco.X.0.tar', 0, 0, 100))


# ---------------------------------------------------------------------------
# 46. Draining campaigns: worker files branch
# ---------------------------------------------------------------------------

class TestDirectDispatchFiles(unittest.TestCase):
    DRAIN = {'tarball': 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar',
             'inloc': 'tape',
             'input_pattern': 'dig.mu2e.%.MDC2025au_best_v1_5.art',
             'outputs': [{'dataset': '*.art', 'location': 'tape'}]}
    FILES = ['dig.mu2e.A.MDC2025au_best_v1_5.001202_00000001.art',
             'dig.mu2e.B.MDC2025au_best_v1_5.001202_00000002.art']

    def _args(self):
        from argparse import Namespace
        return Namespace(dry_run=False, copy_input=True)

    def _dispatch(self, ops, index):
        from utils import runmu2e
        calls = {}

        def fake_pdi(jobdesc, fname, args):
            calls['fname'] = fname
            # Mirror real process_direct_input: fcl = <fname stem>.fcl, a
            # valid 6-field dot-name (the untouched tail feeds it through
            # replace_file_extensions -> Mu2eName.parse).
            fcl = Path(fname).stem + '.fcl'
            return (fcl, '/cvmfs/setup.sh', fname,
                    ops['jobdesc']['outputs'])

        with patch.object(runmu2e, 'process_direct_input', fake_pdi), \
             patch.object(runmu2e, 'locate_file_strict',
                          return_value=[{'location_type': 'tape'}]) as lfs, \
             patch.object(runmu2e, '_fetch_file_local') as ffl, \
             patch.object(runmu2e, '_execute_mu2e',
                          return_value=False), \
             patch.object(runmu2e, '_push_all'):
            failed = runmu2e._direct_dispatch(self._args(), ops, index)
        return failed, calls, ffl, lfs

    def test_index_selects_the_file(self):
        ops = {'jobs': [0, 1], 'files': list(self.FILES),
               'jobdesc': dict(self.DRAIN)}
        failed, calls, ffl, lfs = self._dispatch(ops, 1)
        self.assertFalse(failed)
        self.assertEqual(calls['fname'], self.FILES[1])
        # The fetch must use the file's RESOLVED SAM location, not
        # _fetch_file_local's 'disk' default — every draining entry
        # example ships inloc='tape', and a wrong-tier `mdh copy-file`
        # fails outright (regression: bare _fetch_file_local(fname)).
        lfs.assert_any_call(self.FILES[1])
        ffl.assert_any_call(self.FILES[1], src_location='tape')

    def test_dry_run_skips_pushes(self):
        """--dry-run must gate the push step (EXAMPLES local-smoke recipe
        depends on it) — regression for the flag being silently ignored
        after the POMS dispatch tail was deleted."""
        from utils import runmu2e
        ops = {'jobs': [0, 1], 'files': list(self.FILES),
               'jobdesc': dict(self.DRAIN)}
        args = self._args()
        args.dry_run = True

        def fake_pdi(jobdesc, fname, _args):
            fcl = Path(fname).stem + '.fcl'
            return (fcl, '/cvmfs/setup.sh', fname,
                    ops['jobdesc']['outputs'])

        with patch.object(runmu2e, 'process_direct_input', fake_pdi), \
             patch.object(runmu2e, 'locate_file_strict',
                          return_value=[{'location_type': 'tape'}]), \
             patch.object(runmu2e, '_fetch_file_local'), \
             patch.object(runmu2e, '_execute_mu2e', return_value=False), \
             patch.object(runmu2e, '_push_all') as pa:
            failed = runmu2e._direct_dispatch(args, ops, 0)
        self.assertFalse(failed)
        pa.assert_not_called()

    def test_index_out_of_range_exits(self):
        from utils import runmu2e
        ops = {'jobs': [0, 1, 2], 'files': list(self.FILES),
               'jobdesc': dict(self.DRAIN)}
        with self.assertRaises(SystemExit):
            runmu2e._direct_dispatch(self._args(), ops, 2)

    def test_files_with_normal_jobdesc_exits(self):
        from utils import runmu2e
        normal = dict(self.DRAIN, njobs=10)
        normal.pop('input_pattern')
        ops = {'jobs': [0], 'files': list(self.FILES),
               'jobdesc': normal}
        with self.assertRaises(SystemExit):
            runmu2e._direct_dispatch(self._args(), ops, 0)

    def test_direct_input_jobdesc_without_files_still_exits(self):
        from utils import runmu2e
        ops = {'jobs': [0], 'jobdesc': dict(self.DRAIN)}
        with self.assertRaises(SystemExit):
            runmu2e._direct_dispatch(self._args(), ops, 0)

    def test_manifest_lands_in_log_materialized_from_jsb_tmp(self):
        """Regression for the production gap: the SAM log used to be
        created by push_logs AFTER _emit_manifest found it missing, so
        no direct-backend art log ever carried `mu2egrid manifest`."""
        from utils import runmu2e
        # validate_outputs=False: read-back validation shells out to
        # `source {simjob_setup} && ...`, and fake_pdi's simjob_setup
        # ('/cvmfs/setup.sh') is a placeholder, not a real script — every
        # other test in this class dodges that subprocess by never
        # matching an output glob against a real file. This test needs a
        # real file (to prove it lands in the manifest), so validation
        # must be turned off explicitly; it isn't what's under test here.
        drain = dict(self.DRAIN,
                     outputs=[{'dataset': 'mcs.*.art', 'location': 'tape'}],
                     validate_outputs=False)
        out_name = 'mcs.mu2e.A.MDC2025au_best_v1_5.001202_00000001.art'
        with tempfile.TemporaryDirectory() as d:
            jsb = Path(d) / 'jsb_tmp'
            jsb.mkdir()
            (jsb / 'JOBSUB_LOG_FILE').write_text('worker stdout\n')
            (Path(d) / out_name).write_bytes(b'x')
            ops = {'jobs': [0], 'files': [self.FILES[0]], 'jobdesc': drain}
            cwd = os.getcwd()
            try:
                os.chdir(d)
                with patch.dict(os.environ, {'JSB_TMP': str(jsb)}):
                    failed, calls, ffl, lfs = self._dispatch(ops, 0)
            finally:
                os.chdir(cwd)
            self.assertFalse(failed)
            log = Path(d) / 'log.mu2e.A.MDC2025au_best_v1_5.001202_00000001.log'
            text = log.read_text()
            self.assertTrue(text.startswith('worker stdout\n'), text[:80])
            self.assertIn('mu2egrid manifest', text)
            self.assertIn(out_name, text)

    def test_normal_mode_routes_through_run_mu2e_job(self):
        """_direct_dispatch only picks the runner; the mu2e runner's
        JobRun goes straight to _finish_job."""
        from utils import runmu2e
        from utils.runmu2e import JobRun
        normal = dict(self.DRAIN, njobs=10)
        normal.pop('input_pattern')
        job = JobRun(outputs=normal['outputs'],
                     log_file='log.mu2e.reco.MDC2025au_best_v1_5.001202_00000000.log',
                     job_failed=False, owner='mu2e', infiles='a.art',
                     simjob_setup='/cvmfs/setup.sh', track_parents=True)
        args = self._args()
        with patch.object(runmu2e, '_run_mu2e_job', return_value=job) as rm, \
             patch.object(runmu2e, '_finish_job', return_value=False) as fj:
            failed = runmu2e._direct_dispatch(args, {'jobs': [0], 'jobdesc': normal}, 0)
        self.assertFalse(failed)
        rm.assert_called_once_with(args, normal, None, False, 0)
        fj.assert_called_once_with(args, job)


# ---------------------------------------------------------------------------
# 47. Draining campaigns: pending predicate + batch gates
# ---------------------------------------------------------------------------

def _mk_file(desc, i):
    return f'dig.mu2e.{desc}.MDC2025au_best_v1_5.001202_{i:08d}.art'


def _mk_out(desc, i):
    return f'mcs.mu2e.{desc}.MDC2025au_best_v1_5.001202_{i:08d}.art'


class _IndexedPars:
    """Fake Mu2eJobPars for an INDEXED campaign.

    job_outputs(0) with no overrides — the indexed form. The dataset name
    is index-independent, so index 0 names every stream the campaign will
    write; only the sequencer varies per job.
    """

    def __init__(self, path):
        pass

    def job_outputs(self, index, override_desc=None, override_seq=None):
        return {'Output': f'dts.mu2e.CosmicCRY.MDC2025au.00{index}.art'}


class _DrainPars:
    """Fake Mu2eJobPars: identity dig->mcs mapping (desc preserved)."""

    def __init__(self, path):
        pass

    def job_outputs(self, index, override_desc=None, override_seq=None):
        return {'Output': f'mcs.mu2e.{override_desc}.'
                          f'MDC2025au_best_v1_5.{override_seq}.art'}


class TestDrainingState(unittest.TestCase):
    CAMP = {'id': 48, 'tarball': 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar',
            'entry': {'tarball': 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar',
                      'inloc': 'tape',
                      'input_pattern': 'dig.mu2e.%.MDC2025au_best_v1_5.art',
                      'outputs': [{'dataset': '*.art', 'location': 'tape'}]},
            'cursor': 0, 'slice_size': 500}

    def _state(self, *, in_files, out_files, rows=(), exclude=None,
               defs=None):
        from utils import submissions
        camp = {**self.CAMP,
                'entry': {**self.CAMP['entry'],
                          **({'exclude_desc': exclude} if exclude else {})}}
        in_ds = 'dig.mu2e.A.MDC2025au_best_v1_5.art'
        out_ds = 'mcs.mu2e.A.MDC2025au_best_v1_5.art'
        listing = {in_ds: list(in_files), out_ds: list(out_files)}
        if defs is None:
            defs = [in_ds]

        def lister(ds):
            return listing.get(ds, [])

        with patch.object(submissions, 'Mu2eJobPars', _DrainPars), \
             patch.object(submissions.os.path, 'exists',
                          return_value=True), \
             patch.object(submissions.submission_ledger, 'all_rows',
                          return_value=list(rows)):
            return submissions.draining_state(
                camp, '/x.db', defs_fn=lambda p: list(defs),
                sam_lister=lister, locate=lambda t: '/tmp/' + t)

    def test_growth_pending_is_inputs_minus_landed(self):
        ins = [_mk_file('A', i) for i in range(4)]
        outs = [_mk_out('A', 0), _mk_out('A', 1)]
        st = self._state(in_files=ins, out_files=outs)
        self.assertEqual(st['pending'], sorted(ins[2:]))
        self.assertEqual(len(st['landed']), 2)

    def test_in_flight_and_parked_excluded(self):
        ins = [_mk_file('A', i) for i in range(4)]
        rows = [{'tarball': self.CAMP['tarball'], 'state': 'active',
                 'entry': self.CAMP['entry'], 'indices': [ins[0]]},
                {'tarball': self.CAMP['tarball'], 'state': 'exhausted',
                 'entry': self.CAMP['entry'], 'indices': [ins[1]]}]
        st = self._state(in_files=ins, out_files=[], rows=rows)
        self.assertEqual(st['pending'], sorted(ins[2:]))
        self.assertEqual(st['in_flight'], {ins[0]})
        self.assertEqual(st['parked'], {ins[1]})

    def test_landed_exhausted_file_is_not_parked(self):
        ins = [_mk_file('A', 0)]
        rows = [{'tarball': self.CAMP['tarball'], 'state': 'exhausted',
                 'entry': self.CAMP['entry'], 'indices': [ins[0]]}]
        st = self._state(in_files=ins, out_files=[_mk_out('A', 0)],
                         rows=rows)
        self.assertEqual(st['parked'], set())
        self.assertEqual(st['pending'], [])

    def test_exclude_desc_drops_whole_dataset_exact_match(self):
        from utils import submissions
        # FlatGamma excluded must NOT drop FlatGammaCalo
        defs = ['dig.mu2e.FlatGamma.MDC2025au_best_v1_5.art',
                'dig.mu2e.FlatGammaCalo.MDC2025au_best_v1_5.art']
        fg = [_mk_file('FlatGamma', 0)]
        fgc = [_mk_file('FlatGammaCalo', 0)]
        listing = {defs[0]: fg, defs[1]: fgc,
                   'mcs.mu2e.FlatGammaCalo.MDC2025au_best_v1_5.art': []}
        with patch.object(submissions, 'Mu2eJobPars', _DrainPars), \
             patch.object(submissions.os.path, 'exists',
                          return_value=True), \
             patch.object(submissions.submission_ledger, 'all_rows',
                          return_value=[]):
            st = submissions.draining_state(
                {**self.CAMP,
                 'entry': {**self.CAMP['entry'],
                           'exclude_desc': ['FlatGamma']}},
                '/x.db', defs_fn=lambda p: defs,
                sam_lister=lambda ds: listing.get(ds, []),
                locate=lambda t: '/tmp/' + t)
        self.assertEqual(st['pending'], fgc)

    def test_non_dataset_definition_names_are_ignored(self):
        # drainingn-era junk (`..._slice_0_stage_2`) still parses as a
        # legal 5-field dataset name — it is caught by the input_pattern
        # field match, not by the is_dataset guard. The fake lister DOES
        # return a file for the junk name, so this fails without the
        # fix (the junk file would leak into inputs/pending).
        from utils import submissions
        ins = [_mk_file('A', 0)]
        in_ds = 'dig.mu2e.A.MDC2025au_best_v1_5.art'
        out_ds = 'mcs.mu2e.A.MDC2025au_best_v1_5.art'
        junk_ds = 'dig.mu2e.A.MDC2025au_best_v1_5.art_slice_0_stage_2'
        junk_file = ('dig.mu2e.A.MDC2025au_best_v1_5.'
                     '001202_00000099.art_slice_0_stage_2')
        listing = {in_ds: list(ins), out_ds: [], junk_ds: [junk_file]}
        with patch.object(submissions, 'Mu2eJobPars', _DrainPars), \
             patch.object(submissions.os.path, 'exists',
                          return_value=True), \
             patch.object(submissions.submission_ledger, 'all_rows',
                          return_value=[]):
            st = submissions.draining_state(
                self.CAMP, '/x.db',
                defs_fn=lambda p: [in_ds, junk_ds],
                sam_lister=lambda ds: listing.get(ds, []),
                locate=lambda t: '/tmp/' + t)
        self.assertEqual(st['pending'], ins)
        self.assertNotIn(junk_file, st['inputs'])

    def test_unlocatable_tarball_raises(self):
        from utils import submissions
        with self.assertRaises(RuntimeError):
            with patch.object(submissions.submission_ledger, 'all_rows',
                              return_value=[]):
                submissions.draining_state(
                    self.CAMP, '/x.db', defs_fn=lambda p: [],
                    sam_lister=lambda ds: [], locate=lambda t: None)


class TestGateBatch(unittest.TestCase):
    ENTRY = {'inloc': 'tape', 'min_age_minutes': 60,
             'input_pattern': 'dig.mu2e.%.MDC2025au_best_v1_5.art'}
    OLD = '2026-08-01T00:00:00+00:00'
    NOW = None  # set in setUp

    def setUp(self):
        from datetime import datetime, timezone
        self.NOW = datetime(2026, 8, 1, 12, 0, 0, tzinfo=timezone.utc)

    def _md(self, files, stamp=OLD):
        # key verified against live SAM metadata 2026-08-02: files carry
        # create_date, not create_datetime.
        return lambda fl: [{'file_name': f, 'create_date': stamp}
                           for f in fl]

    def _md_legacy(self, files, stamp=OLD):
        # Legacy-tolerance fixture: ONLY the old key, no create_date.
        return lambda fl: [{'file_name': f, 'create_datetime': stamp}
                           for f in fl]

    def _gate(self, files, *, states, md=None, loc='enstore'):
        from utils import submissions
        return submissions._gate_batch(
            dict(self.ENTRY), files,
            locality=lambda loc, fl: {f: states.get(f, 'ERROR')
                                      for f in fl},
            metadata_fn=md or self._md(files),
            dataset_location=lambda ds: loc,
            now=self.NOW)

    def test_online_files_dispatch_nearline_withheld(self):
        f1, f2 = _mk_file('A', 1), _mk_file('A', 2)
        dispatch, young, tape = self._gate(
            [f1, f2], states={f1: 'ONLINE_AND_NEARLINE', f2: 'NEARLINE'})
        self.assertEqual(dispatch, [f1])
        self.assertEqual(tape, [f2])
        self.assertEqual(young, [])

    def test_too_young_withheld(self):
        f1 = _mk_file('A', 1)
        fresh = '2026-08-01T11:30:00+00:00'   # 30 min old, min_age 60
        dispatch, young, tape = self._gate(
            [f1], states={f1: 'ONLINE'}, md=self._md([f1], fresh))
        self.assertEqual(dispatch, [])
        self.assertEqual(young, [f1])

    def test_unknown_age_fails_closed(self):
        f1 = _mk_file('A', 1)
        with self.assertRaises(RuntimeError):
            self._gate([f1], states={f1: 'ONLINE'},
                       md=lambda fl: [])   # no metadata returned

    def test_locality_error_fails_closed(self):
        f1 = _mk_file('A', 1)
        with self.assertRaises(RuntimeError):
            self._gate([f1], states={f1: 'ERROR'})

    def test_missing_file_fails_closed(self):
        f1 = _mk_file('A', 1)
        with self.assertRaises(RuntimeError):
            self._gate([f1], states={f1: 'MISSING'})

    def test_unknown_storage_location_fails_closed(self):
        f1 = _mk_file('A', 1)
        with self.assertRaises(RuntimeError):
            self._gate([f1], states={f1: 'ONLINE'}, loc='N/A')

    def test_legacy_create_datetime_key_still_works(self):
        # Tolerance for metadata carrying ONLY the old key name.
        f1 = _mk_file('A', 1)
        dispatch, young, tape = self._gate(
            [f1], states={f1: 'ONLINE'}, md=self._md_legacy([f1]))
        self.assertEqual(dispatch, [f1])
        self.assertEqual(young, [])
        self.assertEqual(tape, [])


# ---------------------------------------------------------------------------
# 48. _request_prestage: never-raises contract + tmpfile hygiene
# ---------------------------------------------------------------------------

class TestRequestPrestage(unittest.TestCase):
    def test_runner_exception_does_not_raise(self):
        from utils import submissions

        def boom(*a, **k):
            raise OSError('mdh not found')

        submissions._request_prestage(['f1'], runner=boom)  # must not raise

    def test_nonzero_returncode_does_not_raise_and_prints(self):
        from utils import submissions

        def fail(*a, **k):
            return types.SimpleNamespace(returncode=1, stderr='boom')

        old_stdout, sys.stdout = sys.stdout, io.StringIO()
        try:
            submissions._request_prestage(['f1'], runner=fail)
            printed = sys.stdout.getvalue()
        finally:
            sys.stdout = old_stdout
        self.assertIn('prestage request failed', printed)

    def test_success_command_and_file_contents_then_unlinked(self):
        from utils import submissions
        calls = {}

        def runner(cmd, **kw):
            calls['cmd'] = cmd
            with open(cmd[2]) as fh:
                calls['content'] = fh.read()
            return types.SimpleNamespace(returncode=0, stderr='')

        submissions._request_prestage(['b', 'a'], runner=runner)
        self.assertEqual(calls['cmd'][:2], ['mdh', 'prestage-files'])
        self.assertEqual(calls['content'], 'a\nb\n')
        self.assertFalse(os.path.exists(calls['cmd'][2]))

    def test_empty_files_returns_early(self):
        from utils import submissions
        calls = []

        def runner(cmd, **kw):
            calls.append(cmd)
            return types.SimpleNamespace(returncode=0)

        submissions._request_prestage([], runner=runner)
        self.assertEqual(calls, [])


# ---------------------------------------------------------------------------
# 49. Draining campaigns: file-keyed verify + recovery
# ---------------------------------------------------------------------------

class _DrainParsTwoStream:
    """Fake Mu2eJobPars with TWO output streams per job (mcs + nts).

    verify_files_row's partial branch (some but not all of an input's
    expected outputs landed) can never fire against the shared
    single-stream _DrainPars fake — every file has exactly one expected
    output, so it is either wholly present or wholly missing. This fake
    gives each input two independently-landable streams.
    """

    def __init__(self, path):
        pass

    def job_outputs(self, index, override_desc=None, override_seq=None):
        return {
            'Output': f'mcs.mu2e.{override_desc}.MDC2025au_best_v1_5.'
                      f'{override_seq}.art',
            'ntuple': f'nts.mu2e.{override_desc}.MDC2025au_best_v1_5.'
                      f'{override_seq}.root',
        }


class TestVerifyFilesRow(unittest.TestCase):
    ROW = {'id': 7, 'tarball': 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar',
           'entry': {'input_pattern': 'dig.mu2e.%.MDC2025au_best_v1_5.art',
                     'inloc': 'tape',
                     'outputs': [{'dataset': '*.art', 'location': 'tape'}]},
           'indices': [_mk_file('A', 1), _mk_file('B', 2)],
           'attempt': 1, 'cluster_id': '123', 'parent_id': None}

    def _verify(self, existing_outputs):
        from utils import submissions
        with patch.object(submissions, 'sam_physical_path_or_none',
                          return_value='/tmp/t.tar'), \
             patch.object(submissions.os.path, 'exists',
                          return_value=True), \
             patch.object(submissions, 'Mu2eJobPars', _DrainPars):
            return submissions.verify_files_row(
                dict(self.ROW), sam_lister=lambda ds: existing_outputs)

    def test_all_outputs_present_is_complete(self):
        missing, partial = self._verify([_mk_out('A', 1), _mk_out('B', 2)])
        self.assertEqual(missing, [])
        self.assertEqual(partial, [])

    def test_missing_output_names_the_input_file(self):
        missing, partial = self._verify([_mk_out('A', 1)])
        self.assertEqual(missing, [_mk_file('B', 2)])
        self.assertEqual(partial, [])

    def test_unlocatable_tarball_raises(self):
        from utils import submissions
        with patch.object(submissions, 'sam_physical_path_or_none',
                          return_value=None):
            with self.assertRaises(RuntimeError):
                submissions.verify_files_row(dict(self.ROW))

    def test_one_stream_missing_flags_partial_both_missing_does_not(self):
        # A: mcs landed, nts didn't -> missing AND partial.
        # B: neither stream landed -> missing only (not partial: ALL
        # expected outputs are absent, not just some).
        from utils import submissions

        def mcs(desc, i):
            return (f'mcs.mu2e.{desc}.MDC2025au_best_v1_5.'
                    f'001202_{i:08d}.art')

        def nts(desc, i):
            return (f'nts.mu2e.{desc}.MDC2025au_best_v1_5.'
                    f'001202_{i:08d}.root')

        listing = {
            'mcs.mu2e.A.MDC2025au_best_v1_5.art': [mcs('A', 1)],
            'nts.mu2e.A.MDC2025au_best_v1_5.root': [],
            'mcs.mu2e.B.MDC2025au_best_v1_5.art': [],
            'nts.mu2e.B.MDC2025au_best_v1_5.root': [],
        }
        with patch.object(submissions, 'sam_physical_path_or_none',
                          return_value='/tmp/t.tar'), \
             patch.object(submissions.os.path, 'exists',
                          return_value=True), \
             patch.object(submissions, 'Mu2eJobPars', _DrainParsTwoStream):
            missing, partial = submissions.verify_files_row(
                dict(self.ROW), sam_lister=lambda ds: listing.get(ds, []))
        a_file, b_file = _mk_file('A', 1), _mk_file('B', 2)
        self.assertEqual(missing, [a_file, b_file])
        self.assertEqual(partial, [a_file])


class TestResubmitFiles(unittest.TestCase):
    def test_child_submission_uses_files_and_ledger_parent(self):
        from utils import submissions
        row = dict(TestVerifyFilesRow.ROW)
        missing = [_mk_file('B', 2)]
        captured = {}

        def fake_submit(entry, idx, options):
            captured['entry'] = entry
            captured['options'] = options
            return {'status': 'submitted'}

        ok = submissions.resubmit_files(row, missing, '/x.db',
                                        submit_fn=fake_submit)
        self.assertTrue(ok)
        options = captured['options']
        self.assertEqual(options.files, missing)
        self.assertEqual(options.ledger_parent, row['id'])
        self.assertEqual(options.ledger_db, '/x.db')
        # recovery resource floor applies (entry names no resources)
        self.assertEqual(options.memory, submissions.RECOVERY_MEMORY)

    def test_a_raising_submit_is_contained(self):
        """TRAP 2 on the recovery path."""
        from utils import submissions
        row = dict(TestVerifyFilesRow.ROW)

        def preflight_fail(entry, idx, options):
            raise SystemExit('input pre-flight FAILED')

        self.assertFalse(
            submissions.resubmit_files(row, [_mk_file('B', 2)], '/x.db',
                                       submit_fn=preflight_fail))


class TestProcessRowKindDispatch(unittest.TestCase):
    def test_draining_row_uses_file_verify_and_file_resubmit(self):
        from utils import submissions
        row = dict(TestVerifyFilesRow.ROW)
        called = {}

        def fake_verify(r):
            called['verify'] = True
            return [], []

        with patch.object(submissions, 'verify_files_row', fake_verify), \
             patch.object(submissions.submission_ledger, 'close_row'):
            action = submissions.process_row(
                row, '/x.db', 3,
                clusters={},   # cluster absent from snapshot -> drained
                dry_run=False)
        self.assertEqual(action, 'complete')
        self.assertTrue(called.get('verify'))

    def test_draining_row_dispatches_resubmit_files_not_resubmit(self):
        # Missing-outputs path: confirms the resubmit_fn half of the
        # per-kind dispatch (untested until now) resolves to
        # resubmit_files, not resubmit, for a draining row.
        from utils import submissions
        row = dict(TestVerifyFilesRow.ROW)
        missing = [_mk_file('B', 2)]
        calls = {}

        def fake_verify(r):
            return list(missing), []

        def fake_resubmit_files(r, miss, db_path):
            calls['resubmit_files'] = (r, list(miss), db_path)
            return True

        def fake_resubmit(r, miss, db_path):
            calls['resubmit'] = True
            return True

        # The resubmit path consults the ledger three times: reserved_rows
        # for the "child reserved, window unproven" guard, then open_rows
        # for the crash-window "child already active" pre-check (no
        # children yet), then open_rows again after resubmit_fn succeeds
        # to find the new child row (see TestRecoverLoop, which fakes this
        # via a real ledger; here it's patched directly since this test
        # uses a fake db_path).
        child_row = {'id': 8, 'parent_id': row['id'], 'state': 'active'}

        with patch.object(submissions, 'verify_files_row', fake_verify), \
             patch.object(submissions, 'resubmit_files', fake_resubmit_files), \
             patch.object(submissions, 'resubmit', fake_resubmit), \
             patch.object(submissions.submission_ledger, 'close_row'), \
             patch.object(submissions.submission_ledger, 'reserved_rows',
                          return_value=[]), \
             patch.object(submissions.submission_ledger, 'open_rows',
                          side_effect=[[], [child_row]]):
            action = submissions.process_row(
                row, '/x.db', 3, clusters={}, dry_run=False)
        self.assertEqual(action, 'resubmitted')
        self.assertIn('resubmit_files', calls)
        self.assertEqual(calls['resubmit_files'], (row, missing, '/x.db'))
        self.assertNotIn('resubmit', calls)


# ---------------------------------------------------------------------------
# 49. Draining campaigns: the drain tick
# ---------------------------------------------------------------------------

class TestDrainTick(unittest.TestCase):
    CAMP = {'id': 48, 'state': 'active',
            'tarball': 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar',
            'entry': {'tarball': 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar',
                      'inloc': 'tape',
                      'input_pattern': 'dig.mu2e.%.MDC2025au_best_v1_5.art',
                      'outputs': [{'dataset': '*.art', 'location': 'tape'}]},
            'cursor': 0, 'slice_size': 2}

    RAISE = object()   # state_map sentinel: this campaign's state_fn raises

    def _tick(self, *, pending=None, cap=100, count=10, dry_run=False,
              gate=None, submit_ok=True, camps=None, prestage_camp=False,
              state_map=None):
        """state_map: optional {camp_id: pending_list | TestDrainTick.RAISE}
        for multi-campaign scenarios — RAISE makes that campaign's
        state_fn raise (pins the fail-closed `continue`). Without it,
        every campaign in `camps` (or the single default CAMP) gets
        `pending` (default []) — the original single-campaign shape.
        Records self.state_calls / self.submit_calls — the campaign ids
        state_fn/submit_fn were actually invoked for, in call order —
        so a campaign after a cap-wait `break` can be asserted as never
        reached, not just inferred from an empty submitted list."""
        from utils import submissions
        if camps is None:
            camp = dict(self.CAMP)
            if prestage_camp:
                camp = {**camp, 'entry': {**camp['entry'], 'prestage': True}}
            camps = [camp]
        if state_map is None:
            state_map = {c['id']: (pending if pending is not None else [])
                         for c in camps}
        self.state_calls = []
        self.submit_calls = []
        submitted = []

        def fake_state(c, db):
            self.state_calls.append(c['id'])
            val = state_map[c['id']]
            if val is self.RAISE:
                raise RuntimeError('state boom')
            return {'inputs': set(val), 'landed': set(),
                    'in_flight': set(), 'parked': set(),
                    'pending': sorted(val)}

        def fake_submit(c, batch, db):
            self.submit_calls.append(c['id'])
            submitted.append(list(batch))
            return submit_ok

        prestaged = []
        with patch.object(submissions.submission_ledger,
                          'active_campaigns', return_value=camps), \
             patch.object(submissions.submission_ledger,
                          'set_campaign_state') as scs:
            summary = submissions.drain_tick(
                '/x.db', cap, dry_run=dry_run,
                count_fn=lambda: count,
                submit_fn=fake_submit,
                state_fn=fake_state,
                gate_fn=gate or (lambda e, cand: (list(cand), [], [])),
                prestage_fn=lambda fl: prestaged.append(list(fl)))
        return summary, submitted, prestaged, scs

    def test_one_gated_batch_per_campaign(self):
        pend = [_mk_file('A', i) for i in range(5)]
        summary, submitted, _, _ = self._tick(pending=pend)
        self.assertEqual(summary.get('drain-batch'), 1)
        self.assertEqual(submitted, [sorted(pend)[:2]])   # slice_size=2

    def test_idle_campaign_reports_and_submits_nothing(self):
        summary, submitted, _, _ = self._tick(pending=[])
        self.assertEqual(summary.get('drain-idle'), 1)
        self.assertEqual(submitted, [])

    def test_cap_stops_the_phase(self):
        pend = [_mk_file('A', i) for i in range(5)]
        summary, submitted, _, _ = self._tick(pending=pend, cap=11,
                                              count=10)
        self.assertEqual(summary.get('drain-cap-wait'), 1)
        self.assertEqual(submitted, [])

    def test_gate_failure_is_fail_closed(self):
        def bad_gate(entry, cand):
            raise RuntimeError('mdh down')
        pend = [_mk_file('A', 1)]
        summary, submitted, _, _ = self._tick(pending=pend, gate=bad_gate)
        self.assertEqual(summary.get('drain-error'), 1)
        self.assertEqual(submitted, [])

    def test_submit_failure_pauses_campaign(self):
        pend = [_mk_file('A', 1)]
        summary, _, _, scs = self._tick(pending=pend, submit_ok=False)
        self.assertEqual(summary.get('campaign-paused'), 1)
        scs.assert_called_once()
        self.assertEqual(scs.call_args[0][2], 'paused')

    def test_dry_run_submits_nothing_but_counts(self):
        pend = [_mk_file('A', 1)]
        summary, submitted, _, _ = self._tick(pending=pend, dry_run=True)
        self.assertEqual(summary.get('would-drain-batch'), 1)
        self.assertEqual(submitted, [])

    def test_prestage_requested_for_tape_only(self):
        pend = [_mk_file('A', 1), _mk_file('A', 2)]

        def gate(entry, cand):
            return [cand[0]], [], [cand[1]]
        summary, submitted, prestaged, _ = self._tick(
            pending=pend, gate=gate, prestage_camp=True)
        self.assertEqual(prestaged, [[sorted(pend)[1]]])
        self.assertEqual(submitted, [[sorted(pend)[0]]])

    def test_index_campaigns_are_ignored(self):
        camps = [{'id': 1, 'state': 'active', 'tarball': 't',
                  'entry': {'njobs': 100}, 'cursor': 0, 'slice_size': 10}]
        summary, submitted, _, _ = self._tick(pending=[], camps=camps)
        self.assertEqual(summary, {})
        self.assertEqual(submitted, [])

    # — multi-campaign control-flow contracts -----------------------

    def test_cap_wait_breaks_before_next_campaign(self):
        # camp_a's batch alone exceeds the cap -> drain-cap-wait AND
        # the loop breaks: camp_b (oldest-first, second in line) must
        # never be evaluated this tick, not merely "not submitted".
        camp_a = {**self.CAMP, 'id': 101}
        camp_b = {**self.CAMP, 'id': 102}
        pend_a = [_mk_file('A', i) for i in range(5)]
        pend_b = [_mk_file('B', i) for i in range(5)]
        summary, submitted, _, _ = self._tick(
            camps=[camp_a, camp_b], cap=11, count=10,
            state_map={101: pend_a, 102: pend_b})
        self.assertEqual(summary.get('drain-cap-wait'), 1)
        self.assertEqual(submitted, [])
        self.assertEqual(self.state_calls, [101])
        self.assertEqual(self.submit_calls, [])

    def test_state_error_continues_to_next_campaign(self):
        # camp_a's state_fn raises (fail-closed, drain-error) but the
        # tick must continue: camp_b still gets evaluated and submits
        # its normal batch.
        camp_a = {**self.CAMP, 'id': 201}
        camp_b = {**self.CAMP, 'id': 202}
        pend_b = [_mk_file('B', i) for i in range(5)]
        summary, submitted, _, _ = self._tick(
            camps=[camp_a, camp_b], cap=100, count=10,
            state_map={201: self.RAISE, 202: pend_b})
        self.assertEqual(summary.get('drain-error'), 1)
        self.assertEqual(summary.get('drain-batch'), 1)
        self.assertEqual(submitted, [sorted(pend_b)[:2]])
        self.assertEqual(self.state_calls, [201, 202])
        self.assertEqual(self.submit_calls, [202])

    def test_dry_run_cap_accumulates_across_campaigns(self):
        # cap=3, count=0, batch=2 each: camp_a's dry-run batch must be
        # ADDED to the running count before camp_b is checked, or
        # camp_b would also read as within cap (would-drain-batch=2,
        # drain-cap-wait=0) instead of hitting cap-wait.
        camp_a = {**self.CAMP, 'id': 301}
        camp_b = {**self.CAMP, 'id': 302}
        pend_a = [_mk_file('A', i) for i in range(5)]
        pend_b = [_mk_file('B', i) for i in range(5)]
        summary, submitted, _, _ = self._tick(
            camps=[camp_a, camp_b], cap=3, count=0, dry_run=True,
            state_map={301: pend_a, 302: pend_b})
        self.assertEqual(summary.get('would-drain-batch'), 1)
        self.assertEqual(summary.get('drain-cap-wait'), 1)
        self.assertEqual(submitted, [])
        self.assertEqual(self.state_calls, [301, 302])

    def test_tape_only_without_prestage_opt_in_skips_request(self):
        # Negative case: tape-only candidates exist but the entry did
        # not opt in with prestage: true -> prestage_fn must not fire.
        pend = [_mk_file('A', 1), _mk_file('A', 2)]

        def gate(entry, cand):
            return [cand[0]], [], [cand[1]]
        summary, submitted, prestaged, _ = self._tick(
            pending=pend, gate=gate)   # prestage_camp defaults False
        self.assertEqual(prestaged, [])
        self.assertEqual(submitted, [[sorted(pend)[0]]])

    def test_dry_run_prestage_does_not_call_but_prints(self):
        # dry-run + prestage: true + tape-only -> prints the "would
        # request" line but never calls prestage_fn (no side effect
        # under --dry-run).
        import contextlib
        pend = [_mk_file('A', 1), _mk_file('A', 2)]

        def gate(entry, cand):
            return [cand[0]], [], [cand[1]]
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            summary, submitted, prestaged, _ = self._tick(
                pending=pend, gate=gate, prestage_camp=True, dry_run=True)
        self.assertEqual(prestaged, [])
        self.assertEqual(submitted, [])
        self.assertIn('would request prestage', buf.getvalue())


class TestTopUpSkipsDraining(unittest.TestCase):
    def test_draining_campaign_never_reaches_index_arithmetic(self):
        from utils import submissions
        camp = dict(TestDrainTick.CAMP)   # no njobs -> would TypeError
        with patch.object(submissions.submission_ledger,
                          'active_campaigns', return_value=[camp]):
            summary = submissions.top_up('/x.db', 100,
                                         count_fn=lambda: 0,
                                         submit_fn=lambda *a: True)
        self.assertEqual(summary, {})


# ---------------------------------------------------------------------------
# 50. Draining campaigns: status + complete verb
# ---------------------------------------------------------------------------

class TestCompleteVerb(unittest.TestCase):
    def test_complete_closes_an_active_campaign(self):
        from utils import submissions
        with patch.object(submissions.submission_ledger,
                          'set_campaign_state') as scs:
            submissions.manage_campaign('/x.db', 48, 'complete')
        scs.assert_called_once()
        self.assertEqual(scs.call_args[0][2], 'complete')

    def test_parser_accepts_complete(self):
        from utils import submissions
        args = submissions.build_parser().parse_args(['complete', '48'])
        self.assertEqual(args.verb, 'complete')
        self.assertEqual(args.camp_id, 48)


class TestStatusDrainingLine(unittest.TestCase):
    def test_draining_campaign_prints_pattern_and_ledger_counts(self):
        from utils import submissions
        import io, contextlib as _ctx
        camp = {**TestDrainTick.CAMP,
                'created_utc': '2026-08-01T00:00:00+00:00'}
        row = {'id': 1, 'state': 'active', 'attempt': 1, 'parent_id': None,
               'tarball': camp['tarball'], 'entry': camp['entry'],
               'indices': [_mk_file('A', 1), _mk_file('A', 2)],
               'created_utc': '2026-08-01T00:00:00+00:00',
               'cluster_id': '123', 'jobsub_id': '1.0@s',
               'origin': None, 'closed_utc': None, 'note': None}
        buf = io.StringIO()
        with patch.object(submissions.submission_ledger, 'all_rows',
                          return_value=[row]), \
             patch.object(submissions.submission_ledger, 'all_campaigns',
                          return_value=[camp]), \
             patch.object(submissions.submission_ledger, 'reserved_rows',
                          return_value=[]), \
             _ctx.redirect_stdout(buf):
            submissions.print_status('/x.db')
        out = buf.getvalue()
        self.assertIn('dig.mu2e.%.MDC2025au_best_v1_5.art', out)
        self.assertIn('in-flight 2', out)
        self.assertIn('draining', out)

    def test_empty_rows_still_shows_draining_campaign(self):
        # Freshly-enqueued campaign, day 1: no ledger rows yet (nothing
        # dispatched this tick), but the campaign itself must not be
        # hidden behind the "Ledger is empty" early return.
        from utils import submissions
        import io, contextlib as _ctx
        camp = {**TestDrainTick.CAMP,
                'created_utc': '2026-08-01T00:00:00+00:00'}
        buf = io.StringIO()
        with patch.object(submissions.submission_ledger, 'all_rows',
                          return_value=[]), \
             patch.object(submissions.submission_ledger, 'all_campaigns',
                          return_value=[camp]), \
             patch.object(submissions.submission_ledger, 'reserved_rows',
                          return_value=[]), \
             _ctx.redirect_stdout(buf):
            submissions.print_status('/x.db')
        out = buf.getvalue()
        self.assertIn('empty', out.lower())
        self.assertIn('dig.mu2e.%.MDC2025au_best_v1_5.art', out)
        self.assertIn('draining', out)


class TestLedgerPathResolution(unittest.TestCase):
    def setUp(self):
        from utils import submission_ledger as sl
        self.sl = sl

    def test_mu2epro_reproduces_the_production_path_exactly(self):
        # This is what makes the change a pure generalization: the
        # existing production path IS what the formula yields for
        # mu2epro, so no migration and no cron change.
        self.assertEqual(self.sl.ledger_for('mu2epro'), self.sl.PRODUCTION_DB)

    def test_ledger_for_named_user(self):
        self.assertEqual(
            self.sl.ledger_for('alice'),
            '/exp/mu2e/data/users/alice/prodtools/submissions.db')

    def test_ledger_for_defaults_to_current_account(self):
        with patch('getpass.getuser', return_value='bob'):
            self.assertEqual(
                self.sl.ledger_for(),
                '/exp/mu2e/data/users/bob/prodtools/submissions.db')

    def test_default_db_still_means_production(self):
        # Readers (ledger_ro, the read-only MCP, listNewDatasets,
        # `submissions status`) resolve to DEFAULT_DB and must keep
        # seeing production.
        import importlib
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('MU2E_SUBMISSION_DB', None)
            reloaded = importlib.reload(self.sl)
            try:
                self.assertEqual(reloaded.DEFAULT_DB, reloaded.PRODUCTION_DB)
            finally:
                importlib.reload(self.sl)

    def test_ensure_ledger_dir_creates_a_derived_parent(self):
        base = _mkdtemp()
        db = os.path.join(base, 'prodtools', 'submissions.db')
        self.assertEqual(self.sl.ensure_ledger_dir(db), db)
        self.assertTrue(os.path.isdir(os.path.dirname(db)))

    def test_ensure_ledger_dir_is_idempotent(self):
        base = _mkdtemp()
        db = os.path.join(base, 'prodtools', 'submissions.db')
        self.sl.ensure_ledger_dir(db)
        self.sl.ensure_ledger_dir(db)   # must not raise

    def test_ensure_ledger_dir_raises_and_never_falls_back(self):
        # A personal ledger that cannot be created must fail loudly.
        # Silently using PRODUCTION_DB would write personal campaigns
        # into the production ledger.
        db = '/proc/cannot/exist/prodtools/submissions.db'
        with self.assertRaises(RuntimeError) as ctx:
            self.sl.ensure_ledger_dir(db)
        self.assertNotIn(self.sl.PRODUCTION_DB, str(ctx.exception))


class TestSubmissionsDbResolution(unittest.TestCase):
    def setUp(self):
        from utils import submissions, submission_ledger as sl
        self.submissions = submissions
        self.sl = sl

    def _opts(self, verb, db=None, mine=False):
        return SimpleNamespace(verb=verb, db=db, mine=mine)

    def test_explicit_db_wins_everywhere(self):
        opts = self._opts('status', db='/tmp/explicit.db', mine=True)
        self.assertEqual(self.submissions.resolve_db(opts), '/tmp/explicit.db')

    def test_status_defaults_to_production(self):
        self.assertEqual(self.submissions.resolve_db(self._opts('status')),
                         self.sl.DEFAULT_DB)

    def test_status_mine_selects_personal(self):
        # resolve_db now mkdir's a derived path (see the dedicated
        # directory-creation tests below) — patched here to a passthrough
        # since this test is only about which PATH wins, not filesystem
        # side effects under the real /exp/mu2e/data/users/bob.
        with patch('getpass.getuser', return_value='bob'), \
             patch.object(self.sl, 'ensure_ledger_dir', side_effect=lambda p: p):
            self.assertEqual(
                self.submissions.resolve_db(self._opts('status', mine=True)),
                '/exp/mu2e/data/users/bob/prodtools/submissions.db')

    def test_mutating_verbs_default_to_personal(self):
        # As a non-mu2epro user you cannot write production at all, so a
        # mutating verb defaulting there is never useful. For mu2epro the
        # two paths are identical. ensure_ledger_dir patched for the same
        # reason as test_status_mine_selects_personal.
        with patch('getpass.getuser', return_value='bob'), \
             patch.object(self.sl, 'ensure_ledger_dir', side_effect=lambda p: p):
            for verb in ('run', 'pause', 'resume', 'cancel', 'complete',
                         'set-slice', 'set-memory', 'reconcile'):
                self.assertEqual(
                    self.submissions.resolve_db(self._opts(verb)),
                    '/exp/mu2e/data/users/bob/prodtools/submissions.db',
                    f'verb {verb}')

    def test_mutating_default_is_production_for_mu2epro(self):
        with patch('getpass.getuser', return_value='mu2epro'), \
             patch.object(self.sl, 'ensure_ledger_dir', side_effect=lambda p: p):
            self.assertEqual(self.submissions.resolve_db(self._opts('run')),
                             self.sl.PRODUCTION_DB)

    def test_mutating_verb_creates_its_derived_ledger_directory(self):
        # Real ensure_ledger_dir (not patched): a mutating verb's
        # DEFAULTED path must get its directory created, so `submissions
        # run` against a never-used personal ledger doesn't die in
        # _connect/_acquire_lock before it can do anything.
        base = _mkdtemp()
        derived = os.path.join(base, 'someuser', 'prodtools',
                               'submissions.db')
        with patch.object(self.sl, 'ledger_for', return_value=derived):
            got = self.submissions.resolve_db(self._opts('run'))
        self.assertEqual(got, derived)
        self.assertTrue(os.path.isdir(os.path.dirname(derived)))

    def test_explicit_db_directory_is_never_created(self):
        # An operator-supplied --db pointing at a typo'd/nonexistent
        # directory must fail loudly downstream, never get silently
        # mkdir'd — only a DERIVED (ledger_for()) path is ever created.
        base = _mkdtemp()
        explicit = os.path.join(base, 'no', 'such', 'dir',
                                'submissions.db')
        got = self.submissions.resolve_db(
            self._opts('run', db=explicit))
        self.assertEqual(got, explicit)
        self.assertFalse(os.path.isdir(os.path.dirname(explicit)))


class TestJson2JobdefEnqueueFlags(unittest.TestCase):
    """argparse-level refusals for `json2jobdef --enqueue`, exercised
    IN-PROCESS via `json2jobdef.main()` with a patched `sys.argv`.

    Not subprocesses: `utils/json2jobdef.py` unconditionally imports
    `utils.prod_utils` -> `utils.samweb_wrapper` -> `samweb_client` at
    module level. This suite stubs `samweb_client`/`ifdh` into
    `sys.modules` before any `utils` import (see the header comment,
    ~line 38) so it runs standalone on bare python3 with no Mu2e
    environment sourced — but that stub lives only in THIS process's
    memory. A `subprocess.run` child inherits none of it and dies with
    `ModuleNotFoundError` before argparse ever runs, which would silently
    turn these into "it failed for some reason" tests. Running in-process
    lets the refusals fail for the reason under test."""

    def _run_main(self, argv_tail):
        with patch.object(sys, 'argv', ['json2jobdef.py'] + argv_tail):
            with self.assertRaises(SystemExit) as cm:
                self.json2jobdef.main()
        return str(cm.exception)

    def setUp(self):
        from utils import json2jobdef
        self.json2jobdef = json2jobdef

    def test_enqueue_requires_prod(self):
        """A campaign whose cnf is not in SAM is broken from birth:
        enqueue_entry resolves the tarball from SAM."""
        msg = self._run_main(
            ['--json', 'data/Run1B/resampler_beam.json',
             '--desc', 'PhysicalPionStops', '--dsconf', 'Run1Bap',
             '--enqueue'])
        self.assertIn('--enqueue requires --prod', msg)

    def test_slice_size_requires_enqueue(self):
        msg = self._run_main(
            ['--json', 'data/Run1B/resampler_beam.json',
             '--desc', 'PhysicalPionStops', '--dsconf', 'Run1Bap',
             '--slice-size', '500'])
        self.assertIn('--slice-size requires --enqueue', msg)

    def test_slice_size_matching_default_still_requires_enqueue(self):
        """Regression pin: the refusal used to compare against the
        literal default (1000) as a sentinel, so an operator who typed
        `--slice-size 1000` explicitly (without --enqueue) got silence
        instead of the refusal. `default=None` + an explicit is-not-None
        check catches this."""
        msg = self._run_main(
            ['--json', 'data/Run1B/resampler_beam.json',
             '--desc', 'PhysicalPionStops', '--dsconf', 'Run1Bap',
             '--slice-size', '1000'])
        self.assertIn('--slice-size requires --enqueue', msg)

    def test_prod_requires_enqueue(self):
        """A bare --prod would push the cnf to SAM and then register no
        campaign — a silent no-op that reports success."""
        msg = self._run_main(
            ['--json', 'data/Run1B/resampler_beam.json',
             '--desc', 'PhysicalPionStops', '--dsconf', 'Run1Bap',
             '--prod'])
        self.assertIn('--prod requires --enqueue', msg)

    def test_prodtools_dir_requires_enqueue(self):
        """A prodtools release is recorded on the entry at enqueue; with
        nothing enqueued the flag would be accepted and silently dropped.
        Moved here from a subprocess test that died on ModuleNotFoundError
        before argparse ran and passed its returncode check for the wrong
        reason (see the class docstring)."""
        msg = self._run_main(
            ['--json', 'data/Run1B/resampler_beam.json',
             '--desc', 'PhysicalPionStops', '--dsconf', 'Run1Bap',
             '--prodtools-dir', '/cvmfs/x'])
        self.assertIn('--prodtools-dir requires --enqueue', msg)

    def test_jobdefs_flag_is_gone(self):
        """`--jobdefs` wrote a submission map for a human to hand-edit
        and feed to submit_map — the POMS-era two-step. It was the only
        thing in prodtools that produced an operator-facing map file, and
        that hand-edit window was an unvalidated door into the ledger.
        argparse must reject it outright rather than ignore it."""
        import contextlib, io
        err = io.StringIO()
        argv = ['json2jobdef.py', '--json', 'data/Run1B/resampler_beam.json',
                '--desc', 'PhysicalPionStops', '--dsconf', 'Run1Bap',
                '--prod', '--jobdefs', '/tmp/map.json']
        with patch.object(sys, 'argv', argv), \
                contextlib.redirect_stderr(err):
            with self.assertRaises(SystemExit) as cm:
                self.json2jobdef.main()
        # argparse's own refusal: exit 2, message on stderr.
        self.assertEqual(cm.exception.code, 2)
        self.assertIn('unrecognized arguments', err.getvalue().lower())
        self.assertIn('--jobdefs', err.getvalue())

    def test_provenance_string_format(self):
        from utils.json2jobdef import _provenance
        self.assertEqual(
            _provenance('data/Run1B/resampler_beam.json',
                        {'desc': 'PhysicalPionStops', 'dsconf': 'Run1Bap'}),
            'data/Run1B/resampler_beam.json#PhysicalPionStops@Run1Bap')

    def test_enqueue_writes_no_map_file(self):
        """json2jobdef writes no submission map at all any more. Runs
        from a scratch cwd so a stray file left by another test/run
        cannot fool the assertion, and checks the whole directory rather
        than one filename — the historical bug was a fall-back to a
        DEFAULT name (./jobdefs_list.json), so naming the file we expect
        to be absent is exactly the assertion that missed it."""
        from utils import json2jobdef
        config = {
            'desc': 'PhysicalPionStops', 'dsconf': 'Run1Bap',
            'simjob_setup': 's', 'fcl': 'f.fcl',
            'outloc': {'*.art': 'tape'}, 'inloc': 'none',
            'njobs': 1, 'owner': 'mu2e',
        }
        tmpdir = _mkdtemp()
        cwd = os.getcwd()
        try:
            os.chdir(tmpdir)
            with patch.object(json2jobdef.shutil, 'which',
                              return_value='/usr/bin/mu2e'), \
                 patch.object(json2jobdef, '_build_job_args', return_value=[]), \
                 patch.object(json2jobdef, 'build_jobdef', return_value=None), \
                 patch.object(json2jobdef, 'get_parfile_name',
                              return_value='cnf.mu2e.PhysicalPionStops.Run1Bap.0.tar'), \
                 patch('utils.submit._resolve_ledger_db', return_value=':memory:'), \
                 patch('utils.submit.enqueue_entry', return_value=1), \
                 patch.object(json2jobdef, 'is_cvmfs_prodtools_dir',
                              return_value=True):
                json2jobdef.process_single_entry(
                    dict(config), pushout=False, no_cleanup=True,
                    enqueue=True, slice_size=1000,
                    json_path='data/Run1B/resampler_beam.json',
                    prodtools_dir=FAKE_PRODTOOLS_DIR)
            self.assertEqual(sorted(os.listdir('.')), [],
                             'json2jobdef --enqueue wrote a file into cwd')
        finally:
            os.chdir(cwd)

    def test_enqueue_joins_build_jobdesc_to_real_enqueue_entry(self):
        """End-to-end join of the branch's headline path: build_jobdesc's
        output must be exactly what the REAL utils.submit.enqueue_entry
        accepts and writes to the ledger. Every other enqueue test patches
        `utils.submit.enqueue_entry` itself out, so a signature drift
        between build_jobdesc and enqueue_entry would pass the whole
        suite and only fail in production, after the cnf was already in
        SAM. Only things that would touch the network or filesystem are
        stubbed: check_inputs, _ensure_local_tarball, get_parfile_name,
        and the cnf build (_build_job_args/build_jobdef) plus
        _pushout_to_sam. The ledger itself is real (a temp sqlite file),
        and enqueue_entry runs unpatched end to end."""
        from utils import json2jobdef
        from utils import submit
        from utils import submission_ledger as sl

        tarball = 'cnf.mu2e.IntegDesc.IntegConf.0.tar'
        config = {
            'desc': 'IntegDesc', 'dsconf': 'IntegConf',
            'simjob_setup': 's', 'fcl': 'f.fcl',
            'outloc': {'*.art': 'tape'}, 'inloc': 'none',
            'njobs': 20, 'owner': 'mu2e',
        }
        db_path = os.path.join(_mkdtemp(), 'submissions.db')
        tmpdir = _mkdtemp()
        cwd = os.getcwd()

        try:
            os.chdir(tmpdir)
            with patch.object(json2jobdef.shutil, 'which',
                              return_value='/usr/bin/mu2e'), \
                 patch.object(json2jobdef, '_build_job_args', return_value=[]), \
                 patch.object(json2jobdef, 'build_jobdef', return_value=None), \
                 patch.object(json2jobdef, 'get_parfile_name',
                              return_value=tarball), \
                 patch.object(json2jobdef, '_pushout_to_sam'), \
                 patch.object(submit, '_ensure_local_tarball',
                              return_value=Path(tarball)), \
                 patch.object(submit, 'check_inputs', return_value=(True, [])), \
                 patch.object(submit, 'check_code_tarball',
                              return_value=(True, [])), \
                 patch.object(submit, '_resolve_ledger_db',
                              return_value=db_path), \
                 patch.object(json2jobdef, 'is_cvmfs_prodtools_dir',
                              return_value=True):
                # Computed under the same patches process_single_entry uses,
                # so this is exactly what build_jobdesc produced for the
                # run under test — not a second, differently-mocked call.
                expected_entry = json2jobdef.build_jobdesc(dict(config))
                # process_single_entry adds the release the campaign runs;
                # passed explicitly so the test does not depend on cvmfs,
                # and declared a release (is_cvmfs_prodtools_dir patched)
                # so the fake tmp dir is not bundled as a dev checkout.
                expected_entry['prodtools_dir'] = FAKE_PRODTOOLS_DIR
                json2jobdef.process_single_entry(
                    dict(config), pushout=True, no_cleanup=True,
                    enqueue=True, slice_size=7,
                    json_path='data/x.json',
                    prodtools_dir=FAKE_PRODTOOLS_DIR)
        finally:
            os.chdir(cwd)

        camps = sl.active_campaigns(db_path)
        self.assertEqual(len(camps), 1)
        camp = camps[0]
        self.assertEqual(camp['entry'], expected_entry)
        self.assertEqual(camp['slice_size'], 7)
        self.assertEqual(camp['origin'], 'data/x.json#IntegDesc@IntegConf')


class TestEraAgreement(unittest.TestCase):
    """validate_era_agreement compares the era letters of a dsconf with
    those of the Musing that processes it. It must compare only when
    BOTH tokens are readable as Mu2e era tags; the docstring promise is
    "tokens with no era letters are skipped, not guessed at". It guessed:
    any trailing lowercase run counted as an era ('MCPTest001' -> 'est',
    'Offline' -> 'ffline'), and the Musing tag was whatever directory
    held setup.sh ('/cvmfs/s.sh' -> 'cvmfs'). Eight unrelated tests died
    on a fixture path with no Musing in it."""

    def setUp(self):
        from utils import json2jobdef
        self.j = json2jobdef

    MUSING = '/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/{}/setup.sh'

    def _cfg(self, dsconf, setup, **over):
        cfg = {'fcl': 'a.fcl', 'dsconf': dsconf, 'outloc': {'dts': 'tape'},
               'simjob_setup': setup}
        cfg.update(over)
        return cfg

    def test_era_suffix_reads_only_mu2e_era_tags(self):
        cases = {'Run1Baw': 'aw', 'MDC2025aw': 'aw', 'Run1Bab2': 'ab',
                 'MDC2020ak': 'ak', 'MDC2030aa': 'aa',
                 'Run1B': None, 'MDC2025': None, 'v02_01_00': None,
                 'MCPTest001': None, 'G4blSmoke': None, 'e470313': None,
                 'x': None, 'cvmfs': None, 'Offline': None}
        for token, era in cases.items():
            with self.subTest(token=token):
                self.assertEqual(self.j._era_suffix(token), era)

    def test_musing_tag_reads_only_the_musings_layout(self):
        self.assertEqual(self.j._musing_tag(self.MUSING.format('Run1Baq')),
                         'Run1Baq')
        self.assertEqual(self.j._musing_tag(
            '/cvmfs/mu2e.opensciencegrid.org/Musings/AnalysisMDC2025/'
            'v02_00_00/setup.sh'), 'v02_00_00')
        for not_a_musing in ('/cvmfs/mu2e.opensciencegrid.org/x/setup.sh',
                             '/cvmfs/s.sh',
                             '/exp/mu2e/app/users/u/Offline/setup.sh',
                             '/cvmfs/mu2e.opensciencegrid.org/Musings/'
                             'SimJob/Run1Baq/other.sh'):
            with self.subTest(path=not_a_musing):
                self.assertIsNone(self.j._musing_tag(not_a_musing))

    def test_mismatch_is_refused_naming_both_eras(self):
        with self.assertRaises(SystemExit) as cm:
            self.j.validate_era_agreement(
                self._cfg('Run1Baw_best_v1_5', self.MUSING.format('Run1Baq')))
        msg = str(cm.exception)
        self.assertIn("'aw'", msg)
        self.assertIn("'aq'", msg)
        self.assertIn('era_mismatch_ok', msg)

    def test_matching_eras_pass_across_families(self):
        self.j.validate_era_agreement(
            self._cfg('Run1Baw_best_v1_5', self.MUSING.format('Run1Baw')))
        self.j.validate_era_agreement(
            self._cfg('Run1Baw', self.MUSING.format('MDC2025aw')))

    def test_stated_reason_permits_a_cross_era_pin(self):
        self.j.validate_era_agreement(
            self._cfg('Run1Baw_best_v1_5', self.MUSING.format('Run1Baq'),
                      era_mismatch_ok='Baq-era digs; aw throws on VD 136'))

    def test_setup_outside_the_musings_layout_is_not_compared(self):
        """A dev checkout's setup.sh has no Musing tag. Before: refused
        as "pins 'Offline' (era 'ffline')"."""
        self.j.validate_era_agreement(
            self._cfg('Run1Baw', '/exp/mu2e/app/users/u/Offline/setup.sh'))
        self.j.validate_era_agreement(self._cfg('Run1Bap', '/cvmfs/s.sh'))

    def test_dsconf_without_era_letters_is_not_compared(self):
        """A personal dsconf under a real Musing. Before: 'MCPTest001'
        read as era 'est' and was refused against 'ap'."""
        self.j.validate_era_agreement(
            self._cfg('MCPTest001', self.MUSING.format('Run1Bap')))
        self.j.validate_era_agreement(
            self._cfg('v02_01_00', self.MUSING.format('Run1Bap')))

    def test_code_tarball_entry_has_nothing_to_compare(self):
        self.j.validate_era_agreement(
            {'fcl': 'a.fcl', 'dsconf': 'Run1Baw', 'outloc': {'dts': 'tape'},
             'code': '/exp/Code.tar.bz2'})


class TestJson2JobdefEntryValueValidation(unittest.TestCase):
    """The entry values a build config supplies are validated where the
    config is READ, not only by `submissions set-entry`.

    A misspelled inloc is the expensive typo. `file_resolver.locate`
    finds no such location and falls through to `_locate_via_sam`, so
    the campaign runs to completion reading from SAM while the operator
    believes it reads from resilient — no error, wrong provenance,
    wrong wall-clock. `set-entry` rejected that spelling; json2jobdef,
    which is how every campaign is BORN, did not.

    Validation is unconditional, not gated on --enqueue: a typo is
    equally wrong on a local smoke, and the whole point is to catch it
    before it becomes invisible."""

    def setUp(self):
        from utils import json2jobdef
        self.json2jobdef = json2jobdef

    def _cfg(self, **over):
        """Minimal config satisfying the required-field check, so each
        test fails (or passes) only on the value under test."""
        cfg = {'simjob_setup': '/cvmfs/mu2e.opensciencegrid.org/x/setup.sh',
               'fcl': 'a.fcl', 'dsconf': 'Run1Bap',
               'outloc': {'dts': 'tape'}}
        cfg.update(over)
        return cfg

    def test_misspelled_inloc_rejected(self):
        with self.assertRaises(SystemExit) as cm:
            self.json2jobdef.validate_required_fields(
                self._cfg(inloc='resiliant'))
        msg = str(cm.exception)
        self.assertIn('inloc', msg)
        self.assertIn('resiliant', msg)

    def test_valid_inloc_forms_accepted(self):
        for good in ('tape', 'disk', 'scratch', 'resilient', 'stash', 'none',
                     'dir:/pnfs/mu2e/tape/phy-sim'):
            with self.subTest(inloc=good):
                self.json2jobdef.validate_required_fields(self._cfg(inloc=good))

    def test_scratch_is_a_legal_inloc(self):
        """Regression pin. 'scratch' was missing from the accepted set
        when the validator was first written, so `set-entry N inloc
        scratch` refused a location the resolver handles
        (jobsub_argv._LOCATION_DEFAULT_PROTOCOL carries a protocol for
        it, and EXAMPLES.md documents it)."""
        from utils import jobdesc
        self.assertIn('scratch', jobdesc.INLOC_SIMPLE)
        jobdesc.validate_entry_value('inloc', 'scratch')

    def test_list_valued_inloc_is_expanded_before_validation(self):
        """The mixing config shape wraps every value in a list
        (`"inloc": ["resilient"]`, `"pbeam": ["Mix1BB"]`, ...). That
        shape is a cross-product template: load_json expands it to
        scalar-valued configs BEFORE process_single_entry validates.

        Pinned because validating a raw, unexpanded config would reject
        49 production entries across data/Run1B, data/mdc2025 and
        data/mdc2030 — so this ordering is what makes the value check
        safe to run unconditionally."""
        import json as _json
        import tempfile
        raw = [{
            'simjob_setup': ['/cvmfs/mu2e.opensciencegrid.org/x/setup.sh'],
            'fcl': ['Production/JobConfig/mixing/Mix.fcl'],
            'dsconf': ['Run1Ban_best_v1_5-000'],
            'inloc': ['resilient'],
            'outloc': [{'dig.mu2e.*.art': 'tape'}],
            'desc': ['Thing'],
        }]
        with tempfile.NamedTemporaryFile('w', suffix='.json',
                                         delete=False) as fh:
            _json.dump(raw, fh)
            path = fh.name
        try:
            configs = self.json2jobdef.load_json(Path(path))
        finally:
            os.unlink(path)
        self.assertEqual(len(configs), 1)
        self.assertEqual(configs[0]['inloc'], 'resilient')
        self.json2jobdef.validate_required_fields(configs[0])

    def test_absent_inloc_accepted(self):
        """inloc is optional — process_single_entry defaults it to
        'none'. Validating a key that isn't there would reject every
        config in data/ that omits it."""
        self.json2jobdef.validate_required_fields(self._cfg())

    def test_malformed_resource_values_rejected(self):
        for key, bad in (('memory', '3000 MB'),
                         ('disk', 'lots'),
                         ('expected_lifetime', '48 hours')):
            with self.subTest(key=key):
                with self.assertRaises(SystemExit) as cm:
                    self.json2jobdef.validate_required_fields(
                        self._cfg(**{key: bad}))
                msg = str(cm.exception)
                self.assertIn(key, msg)
                # The offending value, not just the key: asserting the
                # key alone passes on the message prefix even if the
                # underlying complaint is empty.
                self.assertIn(bad, msg)

    def test_wellformed_resource_values_accepted(self):
        self.json2jobdef.validate_required_fields(
            self._cfg(memory='4000MB', disk='50GB', expected_lifetime='48h'))

    def test_same_validator_as_set_entry(self):
        """One definition, three callers. Cheap wiring check; the
        behavioural equivalence below is what actually pins the
        requirement."""
        from utils import jobdesc, submission_ledger
        self.assertIs(submission_ledger.validate_entry_value,
                      jobdesc.validate_entry_value)

    def test_every_enqueue_boundary_agrees_on_the_grammar(self):
        """The requirement, tested as behaviour rather than as wiring:
        a value one boundary accepts, all of them accept.

        The assertIs above pins a name binding, which a copy-pasted
        second validator would satisfy while diverging. There are three
        doors into the ledger — json2jobdef (where a campaign is born),
        submit_map --enqueue (a foreign map), and set-entry (editing a
        live campaign) — and an operator must not meet three different
        answers.
        """
        from utils import json2jobdef, submit, submission_ledger

        good = ['tape', 'disk', 'scratch', 'resilient', 'stash', 'none',
                'dir:/pnfs/mu2e/tape/phy-sim']
        bad = ['resiliant', 'Tape', 'dir:relative', '', 'tape ']

        def via_json2jobdef(value):
            json2jobdef.validate_required_fields(self._cfg(inloc=value))

        def via_submit_map(value):
            submit._validate_entry_values({'tarball': 't', 'inloc': value})

        def via_set_entry(value):
            submission_ledger.validate_entry_value('inloc', value)

        for door in (via_json2jobdef, via_submit_map, via_set_entry):
            for value in good:
                with self.subTest(door=door.__name__, value=value):
                    door(value)
            for value in bad:
                with self.subTest(door=door.__name__, value=value):
                    with self.assertRaises((SystemExit, ValueError)):
                        door(value)


class TestG4blEntryValidation(unittest.TestCase):
    """g4bl entry detection and boundary validation (json2jobdef)."""

    def _entry(self, **over):
        d = {
            'runner': 'g4bl',
            'desc': 'G4blSmoke', 'dsconf': 'TestConf', 'owner': 'testuser',
            'g4bl_dir': self.g4bl_dir, 'main_input': 'deck.in',
            'events_per_job': 100, 'njobs': 2,
            'outloc': {'nts.*.root': 'scratch'},
        }
        d.update(over)
        return d

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='g4bl_test_')
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.g4bl_dir = os.path.join(self.tmp, 'scripts')
        os.makedirs(os.path.join(self.g4bl_dir, 'Geometry'))
        Path(self.g4bl_dir, 'deck.in').write_text('# deck\n')
        Path(self.g4bl_dir, 'Geometry', 'g.txt').write_text('geom\n')

    def test_determine_job_type_g4bl(self):
        from utils.json2jobdef import determine_job_type
        self.assertEqual(determine_job_type(self._entry()), 'g4bl')

    def test_valid_entry_passes(self):
        from utils.json2jobdef import validate_required_fields
        validate_required_fields(self._entry())  # must not raise

    def test_missing_required_key_fails(self):
        from utils.json2jobdef import validate_required_fields
        for key in ('desc', 'dsconf', 'outloc', 'g4bl_dir',
                    'main_input', 'events_per_job', 'njobs'):
            e = self._entry()
            del e[key]
            with self.assertRaises(SystemExit, msg=key):
                validate_required_fields(e)

    def test_forbidden_key_fails(self):
        from utils.json2jobdef import validate_required_fields
        for key, val in (('fcl', 'x.fcl'), ('simjob_setup', '/cvmfs/x'),
                         ('code', 'c.tar'), ('input_data', {'a': 'b'}),
                         ('resampler_name', 'r'), ('pbeam', '1BB'),
                         ('generic_tarball', True), ('input_pattern', 'p'),
                         ('firstjob', 5), ('inloc', 'tape')):
            with self.assertRaises(SystemExit, msg=key):
                validate_required_fields(self._entry(**{key: val}))

    def test_nonpositive_counts_fail(self):
        from utils.json2jobdef import validate_required_fields
        with self.assertRaises(SystemExit):
            validate_required_fields(self._entry(njobs=0))
        with self.assertRaises(SystemExit):
            validate_required_fields(self._entry(events_per_job=-5))

    def test_missing_dir_and_deck_fail(self):
        from utils.json2jobdef import validate_required_fields
        with self.assertRaises(SystemExit):
            validate_required_fields(self._entry(g4bl_dir='/nonexistent/x'))
        with self.assertRaises(SystemExit):
            validate_required_fields(self._entry(main_input='absent.in'))


class TestG4blBuilder(unittest.TestCase):
    """g4bl cnf tarball contents and jobdesc projection."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='g4bl_build_')
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.g4bl_dir = os.path.join(self.tmp, 'scripts')
        os.makedirs(os.path.join(self.g4bl_dir, 'Geometry'))
        Path(self.g4bl_dir, 'deck.in').write_text('# deck\n')
        Path(self.g4bl_dir, 'Geometry', 'g.txt').write_text('geom\n')
        self.cwd = os.getcwd()
        os.chdir(self.tmp)
        self.addCleanup(os.chdir, self.cwd)
        self.config = {
            'runner': 'g4bl',
            'desc': 'G4blSmoke', 'dsconf': 'TestConf', 'owner': 'testuser',
            'g4bl_dir': self.g4bl_dir, 'main_input': 'deck.in',
            # inloc is FORBIDDEN on the raw entry (Task 1); it appears
            # here because this config simulates the post-default state
            # (process_single_entry sets inloc='none' after validation)
            # and build_jobdesc reads config['inloc'] unconditionally.
            'events_per_job': 100, 'njobs': 2, 'inloc': 'none',
            'outloc': {'nts.*.root': 'scratch'},
        }

    def test_tarball_contents(self):
        from utils.json2jobdef import _build_g4bl_tarball, get_parfile_name
        _build_g4bl_tarball(self.config)
        name = get_parfile_name(self.config)
        self.assertEqual(name, 'cnf.testuser.G4blSmoke.TestConf.0.tar')
        with tarfile.open(name) as t:
            members = set(t.getnames())
            self.assertIn('jobpars.json', members)
            self.assertIn('work/deck.in', members)
            self.assertIn('work/Geometry/g.txt', members)
            jp = json.load(t.extractfile('jobpars.json'))
        self.assertEqual(jp, {'runner': 'g4bl', 'desc': 'G4blSmoke',
                              'dsconf': 'TestConf', 'main_input': 'deck.in',
                              'events_per_job': 100, 'njobs': 2,
                              'owner': 'testuser',
                              'tbs': {'njobs': 2,
                                      'outfiles': {'g4bl':
                                          'nts.owner.G4blSmoke.version.sequencer.root'}}})

    def test_g4bl_params_written_to_jobpars_only_when_configured(self):
        """`g4bl_params` rides in jobpars verbatim so the worker can
        append them to the g4bl command line; an entry without the key
        writes no key (test_tarball_contents pins the exact dict)."""
        from utils.json2jobdef import _build_g4bl_tarball, get_parfile_name
        self.config['g4bl_params'] = {'READ_Beam_File': 1, 'Proton_Dump_kill': 0}
        _build_g4bl_tarball(self.config)
        with tarfile.open(get_parfile_name(self.config)) as t:
            jp = json.load(t.extractfile('jobpars.json'))
        self.assertEqual(jp['g4bl_params'], {'READ_Beam_File': 1, 'Proton_Dump_kill': 0})

    def test_vcs_dirs_excluded_from_tarball(self):
        """g4bl_dir is often a git checkout. tar.add used to sweep the
        whole tree (arcname='work') with no filter, shipping .git/
        internals into a cnf that is pushed to SAM permanently and
        dropbox-staged to every grid job. .git/.svn/.hg must never
        appear under work/, and the real deck must still survive."""
        os.makedirs(os.path.join(self.g4bl_dir, '.git', 'objects'))
        Path(self.g4bl_dir, '.git', 'objects', 'x').write_text('blob\n')
        Path(self.g4bl_dir, '.git', 'HEAD').write_text('ref: refs/heads/main\n')
        from utils.json2jobdef import _build_g4bl_tarball, get_parfile_name
        _build_g4bl_tarball(self.config)
        name = get_parfile_name(self.config)
        with tarfile.open(name) as t:
            members = t.getnames()
        self.assertFalse(any('.git' in m.split('/') for m in members),
                         f"VCS members leaked into cnf: {members}")
        self.assertIn('work/deck.in', members)
        self.assertIn('work/Geometry/g.txt', members)

    def test_readable_via_mu2ejobpars(self):
        """Submit/verify read every cnf through Mu2eJobPars (utils.jobquery),
        never runmu2e directly — this pins the g4bl jobpars.json shape
        byte-for-byte against _run_g4bl_job's own output naming so
        submit._read_cnf_facts (submit.py:495, called at --enqueue) and
        verify_row don't crash or under-count. Before the fix, jobpars.json
        had no tbs block and no top-level owner: sequencer() raised
        ValueError('unsupported JSON content'), job_outputs() returned {},
        and njobs() silently fell back to 0 (open-ended) instead of the
        real count."""
        from utils.json2jobdef import _build_g4bl_tarball, get_parfile_name
        from utils.jobquery import Mu2eJobPars
        _build_g4bl_tarball(self.config)
        name = get_parfile_name(self.config)
        jp = Mu2eJobPars(name)
        self.assertEqual(jp.njobs(), 2)
        self.assertEqual(jp.sequencer(3), '00000003')
        self.assertEqual(list(jp.job_outputs(0).values()),
                         ['nts.testuser.G4blSmoke.TestConf.00000000.root'])
        # Flat worker keys survive alongside the new tbs/owner additions.
        self.assertEqual(jp.json_data['runner'], 'g4bl')
        self.assertEqual(jp.json_data['main_input'], 'deck.in')
        self.assertEqual(jp.json_data['events_per_job'], 100)
        self.assertEqual(jp.json_data['owner'], 'testuser')

    def test_recipe_identifies_g4bl_not_code_tarball(self):
        """--recipe is the tool people point at a mystery cnf. A g4bl
        cnf has no embedded mu2e.fcl (same symptom as a code-tarball
        cnf), and used to print the code-tarball line verbatim —
        actively misleading for a g4bl job, which carries no code
        tarball at all. The recipe must identify it as g4bl instead."""
        from utils.json2jobdef import _build_g4bl_tarball, get_parfile_name
        from utils.jobquery import Mu2eJobPars
        _build_g4bl_tarball(self.config)
        name = get_parfile_name(self.config)
        out = Mu2eJobPars(name).recipe()
        self.assertIn('g4bl', out)
        self.assertNotIn('code-tarball', out)

    def test_build_jobdesc_carries_runner(self):
        from utils.json2jobdef import build_jobdesc
        entry = build_jobdesc(self.config)
        self.assertEqual(entry['runner'], 'g4bl')
        self.assertEqual(entry['njobs'], 2)
        self.assertEqual(entry['tarball'],
                         'cnf.testuser.G4blSmoke.TestConf.0.tar')
        self.assertEqual(entry['outputs'],
                         [{'dataset': 'nts.*.root', 'location': 'scratch'}])

    def test_extend_not_supported(self):
        """process_single_entry refuses --extend for g4bl entries (no SAM
        inputs to exclude): plan-mandated behavior, pinned here so a future
        refactor of process_single_entry can't silently drop the check."""
        from utils.json2jobdef import process_single_entry
        config = dict(self.config)
        del config['inloc']  # forbidden on the raw entry (Task 1)
        with self.assertRaises(SystemExit):
            process_single_entry(config, extend=True)


class TestG4blVerifyRow(unittest.TestCase):
    """verify_row on a real g4bl cnf. Before the output_datasets() fix,
    the derived dataset was a phantom '...art' name that never matched
    any real '.root' filename: build_file_maps's per-dataset map stayed
    empty, every index fell into `expected`-less 'unverifiable', and
    verify_row raised RuntimeError on every g4bl row — a g4bl campaign
    could never verify, complete, or recover."""

    def setUp(self):
        from utils import submissions as recover
        self.recover = recover
        self.tmp = tempfile.mkdtemp(prefix='g4bl_verify_')
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        g4bl_dir = os.path.join(self.tmp, 'scripts')
        os.makedirs(g4bl_dir)
        Path(g4bl_dir, 'deck.in').write_text('# deck\n')
        cwd = os.getcwd()
        os.chdir(self.tmp)
        self.addCleanup(os.chdir, cwd)
        self.config = {
            'runner': 'g4bl', 'desc': 'G4blSmoke', 'dsconf': 'TestConf',
            'owner': 'testuser', 'g4bl_dir': g4bl_dir,
            'main_input': 'deck.in', 'events_per_job': 100, 'njobs': 3,
        }
        from utils.json2jobdef import _build_g4bl_tarball, get_parfile_name
        _build_g4bl_tarball(self.config)
        self.tarpath = os.path.join(self.tmp, get_parfile_name(self.config))
        self.row = {'id': 1, 'tarball': get_parfile_name(self.config),
                    'indices': [0, 1, 2], 'entry': {}, 'attempt': 1,
                    'jobsub_id': 'x'}

    def test_all_present_reports_none_missing(self):
        from utils.jobquery import Mu2eJobPars
        jp = Mu2eJobPars(self.tarpath)
        self.assertEqual(jp.output_datasets(),
                         ['nts.testuser.G4blSmoke.TestConf.root'])
        all_files = {f for i in range(3) for f in jp.job_outputs(i).values()}

        def fake_lister(ds):
            return list(all_files)

        with patch.object(self.recover, 'sam_physical_path_or_none',
                          return_value=self.tarpath):
            missing, partial = self.recover.verify_row(
                self.row, sam_lister=fake_lister)
        self.assertEqual((missing, partial), ([], []))

    def test_one_absent_reports_that_index_missing(self):
        from utils.jobquery import Mu2eJobPars
        jp = Mu2eJobPars(self.tarpath)
        absent = jp.job_outputs(1)['g4bl']

        def fake_lister(ds):
            return [f for i in range(3) for f in jp.job_outputs(i).values()
                    if f != absent]

        with patch.object(self.recover, 'sam_physical_path_or_none',
                          return_value=self.tarpath):
            missing, partial = self.recover.verify_row(
                self.row, sam_lister=fake_lister)
        self.assertEqual(missing, [1])
        self.assertEqual(partial, [])


class TestG4blPreflight(unittest.TestCase):
    """A g4bl cnf must sail through the enqueue input gate: no tbs
    block means no inputs to check. Pinned so a future check_inputs
    change that assumes a mu2ejobdef shape fails HERE, not at enqueue."""

    def test_check_inputs_passes_g4bl_cnf(self):
        from utils.json2jobdef import _build_g4bl_tarball
        from utils.check_inputs import check_inputs
        tmp = tempfile.mkdtemp(prefix='g4bl_pre_')
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        g4bl_dir = os.path.join(tmp, 'scripts')
        os.makedirs(g4bl_dir)
        Path(g4bl_dir, 'deck.in').write_text('# deck\n')
        cwd = os.getcwd()
        os.chdir(tmp)
        self.addCleanup(os.chdir, cwd)
        _build_g4bl_tarball({
            'runner': 'g4bl', 'desc': 'G4blSmoke', 'dsconf': 'TestConf',
            'owner': 'testuser', 'g4bl_dir': g4bl_dir,
            'main_input': 'deck.in', 'events_per_job': 10, 'njobs': 1,
        })
        ok, problems = check_inputs(
            'cnf.testuser.G4blSmoke.TestConf.0.tar', 'none',
            sam_sizes=lambda ds: {})
        self.assertTrue(ok, problems)
        self.assertEqual(problems, [])


class TestG4blParamsValidation(unittest.TestCase):
    """`g4bl_params` is validated at the entry boundary: a dict of
    g4bl parameter names to scalars, never one of the params the worker
    owns (First_Event, Num_Events, histoFile, viewer) — a collision
    there would silently change every job's event range or output."""

    def _config(self, params):
        d = _mkdtemp()
        Path(d, 'deck.in').write_text('# deck\n')
        return {'runner': 'g4bl', 'desc': 'D', 'dsconf': 'C',
                'g4bl_dir': d, 'main_input': 'deck.in',
                'events_per_job': 10, 'njobs': 1,
                'outloc': {'nts.*.root': 'scratch'}, 'g4bl_params': params}

    def _refused(self, params, needle):
        from utils.json2jobdef import _validate_g4bl_entry
        with self.assertRaises(SystemExit) as cm:
            _validate_g4bl_entry(self._config(params))
        self.assertIn('g4bl_params', str(cm.exception))
        self.assertIn(needle, str(cm.exception))

    def test_valid_params_pass(self):
        from utils.json2jobdef import _validate_g4bl_entry
        _validate_g4bl_entry(self._config({'READ_Beam_File': 1, 'Target_Color': 'red',
                                           'BsigmaT': 0.5}))

    def test_not_a_dict_refused(self):
        self._refused(['READ_Beam_File=1'], 'dict')

    def test_reserved_worker_param_refused(self):
        self._refused({'First_Event': 5}, 'First_Event')
        self._refused({'histoFile': 'x.root'}, 'histoFile')

    def test_bad_name_refused(self):
        self._refused({'bad-name': 1}, 'bad-name')

    def test_non_scalar_value_refused(self):
        self._refused({'Target_Color': [1, 2]}, 'Target_Color')


class TestG4blWorker(unittest.TestCase):
    """Worker-side g4bl mode: jobdesc validation, command construction,
    run mechanics with a stubbed g4bl, dispatch tail routing."""

    def _jobdesc(self, **over):
        d = {'runner': 'g4bl',
             'tarball': 'cnf.testuser.G4blSmoke.TestConf.0.tar',
             'outputs': [{'dataset': 'nts.*.root', 'location': 'scratch'}],
             'njobs': 2}
        d.update(over)
        return d

    def test_validate_jobdesc_g4bl(self):
        from utils import runmu2e
        self.assertEqual(runmu2e.validate_jobdesc(self._jobdesc()), 'g4bl')

    def test_validate_jobdesc_g4bl_missing_field(self):
        from utils import runmu2e
        bad = self._jobdesc()
        del bad['njobs']
        with self.assertRaises(SystemExit):
            runmu2e.validate_jobdesc(bad)

    def test_g4bl_script_form(self):
        from utils.runmu2e import _g4bl_script
        s = _g4bl_script('deck.in', 101, 100, '/abs/nts.x.root')
        self.assertIn('unset SPACK_ENV PYTHONHOME PYTHONPATH '
                      'PYTHONNOUSERSITE', s)
        self.assertIn('eval "$(spack load --sh g4beamline)"', s)
        self.assertIn('viewer=none', s)
        self.assertIn('First_Event=101', s)
        self.assertIn('Num_Events=100', s)
        self.assertIn('histoFile=/abs/nts.x.root', s)
        self.assertNotIn('param ', s)

    def test_g4bl_script_appends_entry_params_after_the_worker_ones(self):
        """Entry `g4bl_params` become extra `key=value` CLI overrides
        (the only form g4bl 3.08b accepts on the command line), sorted
        for a stable command, values shell-quoted."""
        from utils.runmu2e import _g4bl_script
        s = _g4bl_script('deck.in', 101, 100, '/abs/nts.x.root',
                         params={'READ_Beam_File': 1, 'Target_Color': 'a b'})
        tail = s.split('histoFile=/abs/nts.x.root', 1)[1]
        self.assertEqual(tail, " READ_Beam_File=1 Target_Color='a b'")
        self.assertEqual(_g4bl_script('deck.in', 101, 100, '/abs/nts.x.root'),
                         _g4bl_script('deck.in', 101, 100, '/abs/nts.x.root', params={}))

    def _make_cnf(self, tmp, params=None):
        g4bl_dir = os.path.join(tmp, 'scripts')
        os.makedirs(g4bl_dir)
        Path(g4bl_dir, 'deck.in').write_text('# deck\n')
        from utils.json2jobdef import _build_g4bl_tarball
        _build_g4bl_tarball({
            'runner': 'g4bl', 'desc': 'G4blSmoke', 'dsconf': 'TestConf',
            'owner': 'testuser', 'g4bl_dir': g4bl_dir,
            'main_input': 'deck.in', 'events_per_job': 100, 'njobs': 2,
            **({'g4bl_params': params} if params else {})})

    def test_run_g4bl_job_appends_jobpars_params_to_the_command(self):
        """jobpars.g4bl_params reach the g4bl command line, after the
        worker's own First_Event/Num_Events/histoFile."""
        from utils import runmu2e
        tmp = tempfile.mkdtemp(prefix='g4bl_params_')
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        cwd = os.getcwd()
        os.chdir(tmp)
        self.addCleanup(os.chdir, cwd)
        self._make_cnf(tmp, params={'READ_Beam_File': 1})
        captured = {}

        def fake_run(cmd, shell=False, **kw):
            captured['script'] = cmd[2]
            return 0

        with patch.object(runmu2e, 'run', fake_run):
            runmu2e._run_g4bl_job(self._jobdesc(), 0)
        cmdline = captured['script'].splitlines()[-1]
        self.assertTrue(cmdline.endswith(' READ_Beam_File=1'), cmdline)
        self.assertLess(cmdline.index('histoFile='), cmdline.index('READ_Beam_File='))

    def test_recipe_prints_g4bl_params(self):
        from utils.jobquery import Mu2eJobPars
        tmp = tempfile.mkdtemp(prefix='g4bl_recipe_')
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        cwd = os.getcwd()
        os.chdir(tmp)
        self.addCleanup(os.chdir, cwd)
        self._make_cnf(tmp, params={'READ_Beam_File': 1, 'BsigmaT': 0.5})
        text = Mu2eJobPars('cnf.testuser.G4blSmoke.TestConf.0.tar').recipe()
        self.assertIn('# g4bl_params: BsigmaT=0.5 READ_Beam_File=1', text)

    def test_run_g4bl_job_names_and_first_event(self):
        from utils import runmu2e
        tmp = tempfile.mkdtemp(prefix='g4bl_run_')
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        cwd = os.getcwd()
        os.chdir(tmp)
        self.addCleanup(os.chdir, cwd)
        self._make_cnf(tmp)
        captured = {}

        def fake_run(cmd, shell=False, **kw):
            captured['script'] = cmd[2]
            return 0

        with patch.object(runmu2e, 'run', fake_run):
            job = runmu2e._run_g4bl_job(self._jobdesc(), 3)
        # Owner comes from the cnf tarball name (Mu2eName), never a
        # literal 'mu2e' — see the 2026-09-01 live-smoke 403 finding.
        self.assertEqual(job.owner, 'testuser')
        self.assertEqual(job.log_file, 'log.testuser.G4blSmoke.TestConf.00000003.log')
        self.assertIn('histoFile=', captured['script'])
        self.assertIn('nts.testuser.G4blSmoke.TestConf.00000003.root',
                      captured['script'])
        self.assertNotIn('.mu2e.', captured['script'])
        self.assertNotIn('.mu2e.', job.log_file)
        self.assertIn('First_Event=301', captured['script'])
        self.assertFalse(job.job_failed)
        self.assertEqual(job.outputs, self._jobdesc()['outputs'])
        self.assertEqual(job.infiles, '')
        self.assertIsNone(job.simjob_setup)
        self.assertFalse(job.track_parents)

    def test_run_g4bl_job_failure_is_reported_not_raised(self):
        """A non-zero g4bl exit marks the JobRun failed so the tail still
        pushes the log — it must not propagate as an exception."""
        from utils import runmu2e
        tmp = tempfile.mkdtemp(prefix='g4bl_fail_')
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        cwd = os.getcwd()
        os.chdir(tmp)
        self.addCleanup(os.chdir, cwd)
        self._make_cnf(tmp)

        def failing_run(cmd, shell=False, **kw):
            raise subprocess.CalledProcessError(3, cmd, output='g4bl: boom')

        with patch.object(runmu2e, 'run', failing_run):
            job = runmu2e._run_g4bl_job(self._jobdesc(), 0)
        self.assertTrue(job.job_failed)
        self.assertEqual(job.log_file, 'log.testuser.G4blSmoke.TestConf.00000000.log')

    def test_dispatch_g4bl_rejects_draining(self):
        from utils import runmu2e
        ops = {'jobdesc': self._jobdesc(), 'files': ['a.art']}
        args = types.SimpleNamespace(dry_run=True)
        with self.assertRaises(SystemExit):
            runmu2e._direct_dispatch(args, ops, 0)

    def _in_tmp(self):
        """Same pattern as TestFinishJob._in_tmp: this test runs the real
        _finish_job tail (only _push_all is stubbed), which globs cwd for
        manifest files and calls _materialize_log — both must not touch
        the test-process cwd or pick up a stray JSB_TMP from the real
        environment."""
        d = tempfile.mkdtemp(prefix='g4bl_dispatch_')
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        cwd = os.getcwd()
        os.chdir(d)
        self.addCleanup(os.chdir, cwd)
        env = {k: v for k, v in os.environ.items() if k != 'JSB_TMP'}
        patcher = patch.dict(os.environ, env, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        return Path(d)

    def test_dispatch_g4bl_dry_run_skips_pushes(self):
        from utils import runmu2e
        from utils.runmu2e import JobRun
        self._in_tmp()
        args = types.SimpleNamespace(dry_run=True)
        jobdesc = self._jobdesc()
        job = JobRun(outputs=jobdesc['outputs'],
                     log_file='log.testuser.G4blSmoke.TestConf.00000000.log',
                     job_failed=False, owner='testuser')
        with patch.object(runmu2e, '_run_g4bl_job', return_value=job) as rj, \
             patch.object(runmu2e, '_push_all') as push_all:
            failed = runmu2e._direct_dispatch(args, {'jobdesc': jobdesc}, 0)
        self.assertFalse(failed)
        push_all.assert_not_called()
        rj.assert_called_once_with(jobdesc, 0)


class TestG4blPushCnfParams(unittest.TestCase):
    """MCP push_cnf parameter selection for g4bl entries: no Musing
    to source, so simjob_setup comes back None (falsy -> _musing_clause
    emits no source step)."""

    def test_select_push_params_g4bl(self):
        from prodtools_mcp_write import tools
        tmp = tempfile.mkdtemp(prefix='g4bl_push_')
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        cfg = os.path.join(tmp, 'g4bl.json')
        Path(cfg).write_text(json.dumps([{
            'runner': 'g4bl', 'desc': 'G4blSmoke', 'dsconf': 'TestConf',
            'owner': 'testuser', 'g4bl_dir': tmp, 'main_input': 'deck.in',
            'events_per_job': 10, 'njobs': 1,
            'outloc': {'nts.*.root': 'scratch'}}]))
        setup, tarball_desc = tools._select_push_params(
            cfg, 'G4blSmoke', 'TestConf')
        self.assertIsNone(setup)
        self.assertEqual(tarball_desc, 'G4blSmoke')


class TestEnqueueDoorClosed(unittest.TestCase):
    """The only campaign-creation path is json2jobdef --prod --enqueue.

    submit_map's --enqueue was a second door into campaign creation, and
    a rule enforced on one door only is how campaign 54 lost 239 of 500
    jobs to an unvalidated inloc.
    """

    def test_enqueue_flags_are_gone_from_argv(self):
        import utils.submit as submit
        src = Path(submit.__file__).read_text()
        for flag in ("'--enqueue'", "'--slice-size'", "'--entry'",
                     "'--no-ledger'"):
            self.assertNotIn(
                flag, src,
                f"{flag} still registered in submit.py argparse")

    def test_enqueue_entries_helper_is_gone(self):
        import utils.submit as submit
        self.assertFalse(hasattr(submit, '_enqueue_entries'))

    def test_enqueue_entry_survives_for_json2jobdef(self):
        import utils.submit as submit
        self.assertTrue(callable(submit.enqueue_entry))

    def test_no_ledger_attribute_is_not_consulted(self):
        import utils.submit as submit
        src = Path(submit.__file__).read_text()
        self.assertNotIn('no_ledger', src)

    def test_mcp_enqueue_campaign_tool_is_gone(self):
        import prodtools_mcp_write.tools as tools
        self.assertFalse(hasattr(tools, 'enqueue_campaign'))


class TestSubmitOptions(unittest.TestCase):
    """SubmitOptions replaces the argparse namespace the engine used to
    reach into, so submissions.py can call submit_entry without building
    a fake CLI object."""

    def test_defaults_allow_a_minimal_construction(self):
        from utils.submit import SubmitOptions
        o = SubmitOptions(ledger_db='/tmp/x.db')
        self.assertEqual(o.ledger_db, '/tmp/x.db')
        self.assertFalse(o.dry_run)
        self.assertIsNone(o.indices)
        self.assertIsNone(o.files)
        self.assertIsNone(o.origin)

    def test_carries_first_and_num(self):
        """TRAP 1: submit_slice feeds EVERY campaign slice through these.
        They are not the retired operator flags."""
        from utils.submit import SubmitOptions
        o = SubmitOptions(ledger_db='/tmp/x.db', first=100, num=50)
        self.assertEqual((o.first, o.num), (100, 50))

    def test_is_immutable(self):
        from utils.submit import SubmitOptions
        o = SubmitOptions(ledger_db='/tmp/x.db')
        with self.assertRaises(AttributeError):
            o.dry_run = True


class TestSubmitEntryRenamed(unittest.TestCase):
    """The `direct` suffix named a backend distinction retired
    2026-07-19; utils/submit.py:3 already says 'single backend'."""

    def test_submit_entry_exists(self):
        import utils.submit as submit
        self.assertTrue(callable(submit.submit_entry))

    def test_old_name_is_gone(self):
        import utils.submit as submit
        self.assertFalse(hasattr(submit, 'submit_entry_direct'))

    def test_live_direct_input_sense_is_untouched(self):
        """TRAP 3: `direct input` is a DIFFERENT, living concept — one
        named input file per job. A blanket rename would break draining."""
        import utils.runmu2e as runmu2e
        for name in ('_is_direct_mode', '_load_direct_ops',
                     '_resolve_direct_index', '_synthesize_direct_fname',
                     '_direct_dispatch', '_direct_main'):
            self.assertTrue(hasattr(runmu2e, name),
                            f"runmu2e.{name} was renamed — TRAP 3 violated")


class TestSubmitContainment(unittest.TestCase):
    """The process boundary bin/submit_map provided is what stopped one
    bad campaign from killing the whole tick. Calling in-process removes
    it; _guarded_submit puts it back."""

    def test_exception_is_contained_and_reported_false(self):
        from utils import submissions
        def boom():
            raise RuntimeError('jobsub exploded')
        self.assertFalse(submissions._guarded_submit('campaign 7', boom))

    def test_system_exit_is_contained(self):
        """TRAP 2: submit_entry RAISES SystemExit on a pre-flight failure,
        and SystemExit derives from BaseException — `except Exception`
        lets it through and ends the tick."""
        from utils import submissions
        def preflight_fail():
            raise SystemExit('input pre-flight FAILED')
        self.assertFalse(
            submissions._guarded_submit('campaign 7', preflight_fail))

    def test_keyboard_interrupt_is_NOT_contained(self):
        """Ctrl-C must still stop the tick — swallowing it would make the
        process unkillable from the terminal."""
        from utils import submissions
        def interrupted():
            raise KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            submissions._guarded_submit('campaign 7', interrupted)

    def test_success_returns_true(self):
        from utils import submissions
        self.assertTrue(submissions._guarded_submit('campaign 7',
                                                    lambda: None))


class TestCallSitesContainFailures(unittest.TestCase):
    """Containment must live INSIDE submit_slice/submit_drain_batch.

    top_up already handles a False return by pausing the campaign and
    continuing, so the only new failure mode is an exception escaping the
    call site. Test that boundary directly — wrapping _guarded_submit by
    hand in the test would prove nothing about the real code path.
    """

    def test_submit_slice_contains_a_raising_engine(self):
        from utils import submissions

        def preflight_fail(entry, idx, options):
            raise SystemExit('input pre-flight FAILED')

        camp = {'id': 1, 'cursor': 0, 'tarball': 'a.tar',
                'entry': {'tarball': 'a.tar', 'njobs': 10}}
        self.assertFalse(
            submissions.submit_slice(camp, 5, '/tmp/x.db',
                                     submit_fn=preflight_fail))

    def test_submit_drain_batch_contains_a_raising_engine(self):
        from utils import submissions

        def boom(entry, idx, options):
            raise RuntimeError('jobsub exploded')

        camp = {'id': 2, 'tarball': 'b.tar',
                'entry': {'tarball': 'b.tar', 'input_pattern': 'dts.*.art'}}
        self.assertFalse(
            submissions.submit_drain_batch(camp, ['dts.mu2e.a.v.art'],
                                           '/tmp/x.db', submit_fn=boom))


class TestRecoveryResourceKwargs(unittest.TestCase):
    """Recoveries get a 4000MB/48h FLOOR when the row's own entry names
    no value — an unset memory is what earns the floor."""

    def test_absent_keys_get_the_floor(self):
        from utils.submissions import (recovery_resource_kwargs,
                                       RECOVERY_MEMORY, RECOVERY_LIFETIME)
        kw = recovery_resource_kwargs({'tarball': 'x.tar'})
        self.assertEqual(kw['memory'], RECOVERY_MEMORY)
        self.assertEqual(kw['expected_lifetime'], RECOVERY_LIFETIME)

    def test_present_keys_are_left_alone(self):
        from utils.submissions import recovery_resource_kwargs
        kw = recovery_resource_kwargs(
            {'tarball': 'x.tar', 'memory': '8000MB'})
        self.assertNotIn('memory', kw)

    def test_bare_entry_gets_both_floors(self):
        from utils.submissions import (recovery_resource_kwargs,
                                       RECOVERY_MEMORY, RECOVERY_LIFETIME)
        self.assertEqual(recovery_resource_kwargs({}),
                         {'memory': RECOVERY_MEMORY,
                          'expected_lifetime': RECOVERY_LIFETIME})

    def test_floor_never_downgrades_a_larger_request(self):
        """submit_entry's SubmitOptions are built with **kwargs from this
        function, so an unconditional key would SILENTLY DOWNGRADE an
        entry asking for more. That is the hazard _snapshot_entry exists
        to prevent."""
        from utils.submissions import recovery_resource_kwargs
        kw = recovery_resource_kwargs({'memory': '8000MB'})
        self.assertNotIn('memory', kw)
        self.assertIn('expected_lifetime', kw)

    def test_entry_choice_respected_even_when_smaller(self):
        from utils.submissions import recovery_resource_kwargs
        self.assertNotIn(
            'memory', recovery_resource_kwargs({'memory': '1000MB'}))

    def test_entry_naming_both_gets_no_keys(self):
        from utils.submissions import recovery_resource_kwargs
        self.assertEqual(
            recovery_resource_kwargs({'memory': '8000MB',
                                      'expected_lifetime': '72h'}), {})

    def test_resubmit_passes_the_floors_to_submit_options(self):
        """End-to-end: the floors actually reach SubmitOptions."""
        from utils import submissions
        captured = {}

        def fake_submit(entry, idx, options):
            captured['options'] = options
            return {'status': 'submitted'}

        row = {'id': 7, 'tarball': 'cnf.mu2e.X.Y.0.tar',
               'entry': {'njobs': 10, 'outputs': []}}
        submissions.resubmit(row, [3], '/tmp/none.db',
                             submit_fn=fake_submit)
        options = captured['options']
        self.assertEqual(options.memory, submissions.RECOVERY_MEMORY)
        self.assertEqual(options.expected_lifetime,
                         submissions.RECOVERY_LIFETIME)


class TestResubmitDropsFirstjob(unittest.TestCase):
    """--indices values are ABSOLUTE cnf indices, so the shipped entry
    must sit at firstjob=0 for the worker's `local == global` to hold."""

    def test_firstjob_is_stripped_from_the_shipped_entry(self):
        from utils import submissions
        captured = {}

        def fake_submit(entry, idx, options):
            captured['entry'] = entry
            captured['options'] = options
            return {'status': 'submitted', 'cluster_id': '1', 'njobs': 3,
                    'tarball': 'x.tar'}

        row = {'id': 9, 'tarball': 'x.tar',
               'entry': {'tarball': 'x.tar', 'firstjob': 400, 'njobs': 100}}
        ok = submissions.resubmit(row, [401, 402], '/tmp/x.db',
                                  submit_fn=fake_submit)
        self.assertTrue(ok)
        self.assertNotIn('firstjob', captured['entry'])
        self.assertEqual(captured['options'].indices, [401, 402])
        self.assertEqual(captured['options'].ledger_parent, 9)

    def test_a_raising_submit_is_contained(self):
        """TRAP 2 on the recovery path."""
        from utils import submissions

        def preflight_fail(entry, idx, options):
            raise SystemExit('input pre-flight FAILED')

        row = {'id': 9, 'tarball': 'x.tar',
               'entry': {'tarball': 'x.tar', 'njobs': 100}}
        self.assertFalse(
            submissions.resubmit(row, [1], '/tmp/x.db',
                                 submit_fn=preflight_fail))


class TestMapScratchDirIsGone(unittest.TestCase):
    def test_no_scratch_map_dir(self):
        from utils import submissions
        self.assertFalse(hasattr(submissions, '_scratch_map_dir'))

    def test_no_submit_map_constant(self):
        from utils import submissions
        self.assertFalse(hasattr(submissions, 'SUBMIT_MAP'))


# ---------------------------------------------------------------------------
# submissions resubmit verb (utils/submission_ledger.py, utils/submissions.py)
# ---------------------------------------------------------------------------
class TestRowById(unittest.TestCase):
    def test_returns_the_row(self):
        from utils import submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            rid = submission_ledger.reserve_submission(
                db, tarball='x.tar', entry={'tarball': 'x.tar'},
                indices=[1, 2, 3])
            row = submission_ledger.row_by_id(db, rid)
        self.assertEqual(row['id'], rid)
        self.assertEqual(row['indices'], [1, 2, 3])

    def test_missing_row_is_none(self):
        from utils import submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            self.assertIsNone(submission_ledger.row_by_id(db, 999))


class TestResubmitOverlapGuard(unittest.TestCase):
    """Deterministic payloads make an unverified resubmit the Run1Ban
    failure mode: re-sending indices that are still live duplicates
    physics. A live row blocks; a closed one does not."""

    def _db_with_row(self, td, state, indices):
        from utils import submission_ledger
        db = os.path.join(td, 'submissions.db')
        submission_ledger.ensure_ledger_dir(db)
        rid = submission_ledger.reserve_submission(
            db, tarball='x.tar', entry={'tarball': 'x.tar'},
            indices=indices)
        if state != 'submitting':
            submission_ledger.attach_cluster(db, rid, jobsub_id='j',
                                             cluster_id='1')
        if state in ('complete', 'recovered', 'exhausted'):
            submission_ledger.close_row(db, rid, state)
        return db, rid

    def test_active_row_blocks(self):
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, rid = self._db_with_row(td, 'active', [5, 6, 7])
            blocking = submissions._rows_blocking_indices(db, 'x.tar', [6])
        self.assertIsNotNone(blocking)
        self.assertEqual(blocking['id'], rid)

    def test_submitting_row_blocks(self):
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, _ = self._db_with_row(td, 'submitting', [5, 6, 7])
            self.assertIsNotNone(
                submissions._rows_blocking_indices(db, 'x.tar', [6]))

    def test_complete_row_does_not_block(self):
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, _ = self._db_with_row(td, 'complete', [5, 6, 7])
            self.assertIsNone(
                submissions._rows_blocking_indices(db, 'x.tar', [6]))

    def test_other_tarball_does_not_block(self):
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, _ = self._db_with_row(td, 'active', [5, 6, 7])
            self.assertIsNone(
                submissions._rows_blocking_indices(db, 'other.tar', [6]))

    def test_disjoint_indices_do_not_block(self):
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, _ = self._db_with_row(td, 'active', [5, 6, 7])
            self.assertIsNone(
                submissions._rows_blocking_indices(db, 'x.tar', [99]))

    def test_failed_row_blocks(self):
        """'failed' is deliberately NOT in _SETTLED_STATES: a
        jobsub_submit that exits non-zero can still have created a
        cluster, so a failed row's window is not proven free. Pinned
        separately from the 'submitting'/'active' cases because it is
        the one a future hygiene pass is most likely to mistake for
        terminal-hence-safe (fail_reservation's own docstring: "the
        window is not proven free and must keep blocking until a human
        reconciles it")."""
        from utils import submissions, submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            rid = submission_ledger.reserve_submission(
                db, tarball='x.tar', entry={'tarball': 'x.tar'},
                indices=[5, 6, 7])
            submission_ledger.fail_reservation(
                db, rid, 'jobsub_submit exit 1 (cluster may exist)')
            blocking = submissions._rows_blocking_indices(db, 'x.tar', [6])
        self.assertIsNotNone(blocking)
        self.assertEqual(blocking['id'], rid)


class TestResubmitVerb(unittest.TestCase):
    def test_refuses_a_missing_row(self):
        from utils import submissions, submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            with self.assertRaises(SystemExit) as cm:
                submissions.main(['--db', db, 'resubmit', '999',
                                  '--indices', '1'])
            self.assertIn('no ledger row 999', str(cm.exception))

    def test_refuses_when_a_live_row_overlaps(self):
        from utils import submissions, submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            rid = submission_ledger.reserve_submission(
                db, tarball='x.tar', entry={'tarball': 'x.tar'},
                indices=[1, 2, 3])
            submission_ledger.attach_cluster(db, rid, jobsub_id='j',
                                             cluster_id='1')
            with self.assertRaises(SystemExit) as cm:
                submissions.main(['--db', db, 'resubmit', str(rid),
                                  '--indices', '2'])
            self.assertIn('refusing', str(cm.exception))
            self.assertIn('reconcile', str(cm.exception))

    def test_rejects_indices_on_a_draining_row(self):
        from utils import submissions, submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            rid = submission_ledger.reserve_submission(
                db, tarball='x.tar',
                entry={'tarball': 'x.tar', 'input_pattern': 'dts.*.art'},
                indices=['dts.mu2e.a.v.art'])
            submission_ledger.attach_cluster(db, rid, jobsub_id='j',
                                             cluster_id='1')
            submission_ledger.close_row(db, rid, 'complete')
            with self.assertRaises(SystemExit) as cm:
                submissions.main(['--db', db, 'resubmit', str(rid),
                                  '--indices', '1'])
            self.assertIn('draining', str(cm.exception))

    def test_files_selector_dispatches_to_resubmit_files_not_resubmit(self):
        """The --files branch had zero coverage: every existing test here
        drives --indices, so nothing pinned that a draining row's
        file-keyed selector reaches resubmit_files (the file-keyed
        submitter) rather than resubmit (the index submitter, which
        would treat filenames as cnf indices)."""
        from utils import submissions, submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            rid = submission_ledger.reserve_submission(
                db, tarball='x.tar',
                entry={'tarball': 'x.tar', 'input_pattern': 'dts.*.art'},
                indices=['dts.mu2e.a.v.art'])
            submission_ledger.attach_cluster(db, rid, jobsub_id='j',
                                             cluster_id='1')
            files_path = os.path.join(td, 'files.txt')
            with open(files_path, 'w') as fh:
                fh.write('dts.mu2e.CosmicCORSIKA.MDC2020az.'
                         '001202_00000002.art\n')
            with patch.object(submissions, 'resubmit_files',
                              return_value=True) as fake_files, \
                 patch.object(submissions, 'resubmit') as fake_indices:
                submissions.main(['--db', db, 'resubmit', str(rid),
                                  '--files', files_path, '--dry-run'])
            fake_files.assert_called_once()
            fake_indices.assert_not_called()

    def test_a_non_raising_but_unconfirmed_submit_exits_nonzero(self):
        """_guarded_submit returns True for ANY non-raising call,
        including submit_entry returning {'status': 'failed'} — the
        shape jobsub_submit produces on a non-zero exit, possibly after
        already creating a cluster (see submit._run_submit), or on exit
        0 with no parseable cluster id. The verb must not report success
        on that truthy return alone: it must see a new ACTIVE child
        ledger row, mirroring process_row's own 'child-missing' check."""
        from utils import submissions, submission_ledger, submit
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            rid = submission_ledger.reserve_submission(
                db, tarball='x.tar', entry={'tarball': 'x.tar'},
                indices=[1, 2, 3])
            submission_ledger.attach_cluster(db, rid, jobsub_id='j',
                                             cluster_id='1')
            fake_result = {'status': 'failed', 'tarball': 'x.tar',
                           'cluster_id': None, 'njobs': 1}
            with patch.object(submit, 'submit_entry',
                              return_value=fake_result):
                with self.assertRaises(SystemExit) as cm:
                    submissions.main(['--db', db, 'resubmit', str(rid),
                                      '--indices', '99'])
            self.assertIn('did NOT confirm', str(cm.exception))

    def test_bad_indices_spec_exits_cleanly_not_a_traceback(self):
        from utils import submissions, submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            rid = submission_ledger.reserve_submission(
                db, tarball='x.tar', entry={'tarball': 'x.tar'},
                indices=[1, 2, 3])
            submission_ledger.attach_cluster(db, rid, jobsub_id='j',
                                             cluster_id='1')
            with self.assertRaises(SystemExit) as cm:
                submissions.main(['--db', db, 'resubmit', str(rid),
                                  '--indices', 'abc'])
            self.assertIn('submissions:', str(cm.exception))

    def test_missing_indices_file_exits_cleanly_not_a_traceback(self):
        from utils import submissions, submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            rid = submission_ledger.reserve_submission(
                db, tarball='x.tar', entry={'tarball': 'x.tar'},
                indices=[1, 2, 3])
            submission_ledger.attach_cluster(db, rid, jobsub_id='j',
                                             cluster_id='1')
            with self.assertRaises(SystemExit) as cm:
                submissions.main(['--db', db, 'resubmit', str(rid),
                                  '--indices-file', '/no/such/file.txt'])
            self.assertIn('submissions:', str(cm.exception))


# ---------------------------------------------------------------------------
# bin/submit_map retirement (Task 6) — the command is gone, the engine
# (utils/submit.py) survives as a library
# ---------------------------------------------------------------------------

class TestSubmitMapCommandRetired(unittest.TestCase):
    def test_bin_submit_map_is_gone(self):
        import pathlib
        repo = pathlib.Path(__file__).resolve().parent.parent
        self.assertFalse((repo / 'bin' / 'submit_map').exists())

    def test_submit_map_function_is_gone(self):
        import utils.submit as submit
        self.assertFalse(hasattr(submit, 'submit_map'))

    def test_submit_py_has_no_cli(self):
        import utils.submit as submit
        self.assertFalse(hasattr(submit, 'main'))

    def test_engine_is_still_exported(self):
        import utils.submit as submit
        for name in ('SubmitOptions', 'submit_entry', 'enqueue_entry'):
            self.assertTrue(hasattr(submit, name), f"lost {name}")

    def test_runner_allowlist_drops_the_deleted_script(self):
        """ALLOWED_ENTRY_POINTS is a security allowlist; an entry naming a
        script that no longer exists is dead surface."""
        from prodtools_mcp_write.runner import ALLOWED_ENTRY_POINTS
        self.assertNotIn('bin/submit_map', ALLOWED_ENTRY_POINTS)
        self.assertIn('bin/submissions', ALLOWED_ENTRY_POINTS)
        self.assertIn('bin/json2jobdef', ALLOWED_ENTRY_POINTS)

    def test_mu2ejobsub_helpers_gone(self):
        """Regression guard restored from the deleted TestSingleBackend
        (2026-07-19's retirement of the INTERNAL mu2ejobsub backend —
        build_mu2ejobsub_argv/_submit_entry_mu2ejobsub — a DIFFERENT
        retirement from this branch's map-file removal). Nothing else
        pins this: .claude/commands/mu2ejobsub-submit.md deliberately
        survives because the UPSTREAM mu2ejobsub CLI is still in use,
        which makes accidental reintroduction of these internal helpers
        more plausible, not less."""
        from utils import submit
        self.assertFalse(hasattr(submit, 'build_mu2ejobsub_argv'))
        self.assertFalse(hasattr(submit, '_submit_entry_mu2ejobsub'))


# ---------------------------------------------------------------------------
# Ledger map_path -> origin column migration (Task 7)
# ---------------------------------------------------------------------------

class TestOriginColumnMigration(unittest.TestCase):
    """map_path named a file that no longer exists. The column is free-text
    provenance; nothing dispatches from it."""

    def _columns(self, db, table):
        con = sqlite3.connect(db)
        try:
            return [r[1] for r in con.execute(f'PRAGMA table_info({table})')]
        finally:
            con.close()

    def test_fresh_db_has_origin_not_map_path(self):
        from utils import submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            submission_ledger.all_rows(db)
            for table in ('submissions', 'campaigns'):
                cols = self._columns(db, table)
                self.assertIn('origin', cols, table)
                self.assertNotIn('map_path', cols, table)

    def test_legacy_db_is_migrated_preserving_values(self):
        from utils import submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            con = sqlite3.connect(db)
            con.executescript("""
                CREATE TABLE submissions (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_utc TEXT NOT NULL,
                  state TEXT NOT NULL DEFAULT 'active',
                  attempt INTEGER NOT NULL DEFAULT 1,
                  parent_id INTEGER,
                  map_path TEXT, tarball TEXT NOT NULL,
                  entry_json TEXT NOT NULL, indices_json TEXT NOT NULL,
                  jobsub_id TEXT, cluster_id TEXT, closed_utc TEXT, note TEXT);
                INSERT INTO submissions
                  (created_utc, map_path, tarball, entry_json, indices_json)
                  VALUES ('2026-01-01T00:00:00Z', '/tmp/legacy.json',
                          'x.tar', '{}', '[]');
            """)
            con.commit()
            con.close()
            rows = submission_ledger.all_rows(db)
            self.assertIn('origin', self._columns(db, 'submissions'))
            self.assertEqual(rows[0]['origin'], '/tmp/legacy.json')

    def test_migration_is_idempotent(self):
        from utils import submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            for _ in range(3):
                submission_ledger.all_rows(db)
            self.assertIn('origin', self._columns(db, 'submissions'))


# ---------------------------------------------------------------------------
# Task 7 fix round 1 (2026-08-11): the read-only MCP path never runs the
# migration (mcp/src/prodtools_mcp/ledger_ro.py opens mode=ro and issues
# no DDL by design), so status.py's unconditional camp['origin'] used to
# raise KeyError on a ledger no WRITE connection has touched since the
# rename shipped. Reproduced by the reviewer against a hand-built legacy
# DB, never through submission_ledger's own writer (which would migrate
# it on connect).
# ---------------------------------------------------------------------------

class TestLedgerRoOriginShimOnLegacyLedger(unittest.TestCase):
    """ledger_ro must tolerate a ledger still keyed map_path — the
    normalizing shim in _shape_campaign/_shape_row is a transition
    measure, deletable once every ledger has been touched by a writer at
    least once post-rename."""

    def _make_legacy_db(self, td):
        db = os.path.join(td, 'submissions.db')
        con = sqlite3.connect(db)
        con.executescript("""
            CREATE TABLE submissions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_utc TEXT NOT NULL,
              state TEXT NOT NULL DEFAULT 'active',
              attempt INTEGER NOT NULL DEFAULT 1,
              parent_id INTEGER,
              map_path TEXT, tarball TEXT NOT NULL,
              entry_json TEXT NOT NULL, indices_json TEXT NOT NULL,
              jobsub_id TEXT, cluster_id TEXT, closed_utc TEXT, note TEXT);
            CREATE TABLE campaigns (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_utc TEXT NOT NULL,
              state TEXT NOT NULL DEFAULT 'active',
              map_path TEXT, tarball TEXT NOT NULL,
              entry_json TEXT NOT NULL, cursor INTEGER NOT NULL DEFAULT 0,
              slice_size INTEGER NOT NULL, closed_utc TEXT, note TEXT);
            INSERT INTO submissions
              (created_utc, map_path, tarball, entry_json, indices_json)
              VALUES ('2026-01-01T00:00:00Z', '/tmp/legacy-row.json',
                      'cnf.mu2e.X.C.0.tar', '{}', '[]');
            INSERT INTO campaigns
              (created_utc, map_path, tarball, entry_json, slice_size)
              VALUES ('2026-01-01T00:00:00Z', '/tmp/legacy-map.json',
                      'cnf.mu2e.X.C.0.tar', '{"njobs": 4}', 4);
        """)
        con.commit()
        con.close()
        return db

    def test_ledger_ro_campaigns_normalizes_map_path_to_origin(self):
        from prodtools_mcp import ledger_ro
        with tempfile.TemporaryDirectory() as td:
            db = self._make_legacy_db(td)
            camps = ledger_ro.campaigns(db)
        self.assertEqual(len(camps), 1)
        self.assertEqual(camps[0]['origin'], '/tmp/legacy-map.json')
        self.assertNotIn('map_path', camps[0])

    def test_ledger_ro_rows_normalizes_map_path_to_origin(self):
        from prodtools_mcp import ledger_ro
        with tempfile.TemporaryDirectory() as td:
            db = self._make_legacy_db(td)
            rows = ledger_ro.rows(db)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['origin'], '/tmp/legacy-row.json')
        self.assertNotIn('map_path', rows[0])

    def test_ledger_ro_snapshot_normalizes_both_tables(self):
        from prodtools_mcp import ledger_ro
        with tempfile.TemporaryDirectory() as td:
            db = self._make_legacy_db(td)
            camps, rows = ledger_ro.snapshot(db)
        self.assertEqual(camps[0]['origin'], '/tmp/legacy-map.json')
        self.assertEqual(rows[0]['origin'], '/tmp/legacy-row.json')

    def test_campaign_status_does_not_raise_on_unmigrated_ledger(self):
        """REGRESSION: this is the exact failure the reviewer reproduced
        — status.py's dict construction does camp['origin']
        unconditionally, which raised KeyError (caught by safe_tool and
        surfaced as a generic 'internal' error) against a ledger no
        writer had reconnected to since the rename."""
        from prodtools_mcp.tools import status
        with tempfile.TemporaryDirectory() as td:
            db = self._make_legacy_db(td)
            result = status.campaign_status(db_path=db)
        self.assertEqual(len(result['campaigns']), 1)
        self.assertEqual(result['campaigns'][0]['origin'],
                         '/tmp/legacy-map.json')

    def test_list_campaigns_does_not_raise_on_unmigrated_ledger(self):
        from prodtools_mcp.tools import status
        with tempfile.TemporaryDirectory() as td:
            db = self._make_legacy_db(td)
            result = status.list_campaigns(db_path=db)
        self.assertEqual(result['count'], 1)
        self.assertEqual(result['campaigns'][0]['origin'],
                         '/tmp/legacy-map.json')

    def test_readonly_path_still_issues_no_ddl(self):
        """The whole point of the shim is to avoid needing the write
        connection the migration requires — confirm the DB on disk is
        STILL legacy (map_path, not origin) after every read above."""
        from prodtools_mcp import ledger_ro
        with tempfile.TemporaryDirectory() as td:
            db = self._make_legacy_db(td)
            ledger_ro.campaigns(db)
            ledger_ro.rows(db)
            ledger_ro.snapshot(db)
            con = sqlite3.connect(db)
            try:
                cols = [r[1] for r in
                       con.execute('PRAGMA table_info(submissions)')]
            finally:
                con.close()
        self.assertIn('map_path', cols)
        self.assertNotIn('origin', cols)


# ---------------------------------------------------------------------------
# Task 7 fix round 1 (2026-08-11): the migration's check-then-act
# (PRAGMA table_info, then ALTER TABLE) is not atomic across processes.
# Reviewer raced 6 OS processes against a never-migrated legacy DB and
# hit "no such column: map_path" in 2/15 trials: process A's PRAGMA saw
# map_path, process B committed the rename first, A's own ALTER then
# failed. A real multi-process race is nondeterministic by nature — these
# tests instead deterministically FORCE the exact failure mode by
# injecting the concurrent winner's rename in between our own PRAGMA
# check and our own ALTER attempt, via a thin delegating wrapper around
# the connection _connect creates (sqlite3.Connection is a built-in type
# and cannot be patched directly — "can't set attributes of
# built-in/extension type").
# ---------------------------------------------------------------------------

class TestOriginMigrationRace(unittest.TestCase):
    def _make_legacy_submissions_only_db(self, td, origin_value):
        db = os.path.join(td, 'submissions.db')
        con = sqlite3.connect(db)
        con.executescript(f"""
            CREATE TABLE submissions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_utc TEXT NOT NULL,
              state TEXT NOT NULL DEFAULT 'active',
              attempt INTEGER NOT NULL DEFAULT 1,
              parent_id INTEGER,
              map_path TEXT, tarball TEXT NOT NULL,
              entry_json TEXT NOT NULL, indices_json TEXT NOT NULL,
              jobsub_id TEXT, cluster_id TEXT, closed_utc TEXT, note TEXT);
            INSERT INTO submissions
              (created_utc, map_path, tarball, entry_json, indices_json)
              VALUES ('2026-01-01T00:00:00Z', '{origin_value}',
                      'x.tar', '{{}}', '[]');
        """)
        con.commit()
        con.close()
        return db

    def test_migration_survives_a_concurrent_winner(self):
        """Deterministic reproduction: our OWN ALTER TABLE is made to
        fail with sqlite3's real "no such column: map_path" by having a
        SEPARATE, genuine connection win the rename first — exactly the
        race the reviewer hit, forced instead of timed."""
        from utils import submission_ledger as sl
        with tempfile.TemporaryDirectory() as td:
            db = self._make_legacy_submissions_only_db(
                td, '/tmp/race-winner.json')

            real_connect = sqlite3.connect
            fired = {'done': False}
            target_sql = ('ALTER TABLE submissions RENAME COLUMN '
                         'map_path TO origin')

            class _RacyConn:
                def __init__(self, real):
                    object.__setattr__(self, '_real', real)

                def __getattr__(self, name):
                    return getattr(self._real, name)

                def __setattr__(self, name, value):
                    setattr(self._real, name, value)

                def execute(self, sql, *a, **kw):
                    if not fired['done'] and sql == target_sql:
                        fired['done'] = True
                        # The "other process" — a genuine second
                        # connection — wins the rename BEFORE our own
                        # ALTER (below) gets to run.
                        winner = real_connect(db)
                        try:
                            winner.execute(target_sql)
                            winner.commit()
                        finally:
                            winner.close()
                    return self._real.execute(sql, *a, **kw)

            def fake_connect(*a, **kw):
                return _RacyConn(real_connect(*a, **kw))

            with patch.object(sl.sqlite3, 'connect',
                              side_effect=fake_connect):
                rows = sl.all_rows(db)

            self.assertTrue(fired['done'], "race hook never fired")
            self.assertEqual(rows[0]['origin'], '/tmp/race-winner.json')

    def test_unrelated_operational_error_still_raises(self):
        """The except clause must not swallow a genuinely different
        failure — only the specific 'someone else already migrated it
        first' case. Force the ALTER to fail for an UNRELATED reason
        (origin never gets created); the re-check must find no origin
        column and re-raise rather than silently continuing."""
        from utils import submission_ledger as sl
        with tempfile.TemporaryDirectory() as td:
            db = self._make_legacy_submissions_only_db(
                td, '/tmp/unrelated-failure.json')

            real_connect = sqlite3.connect
            target_sql = ('ALTER TABLE submissions RENAME COLUMN '
                         'map_path TO origin')

            class _BrokenConn:
                def __init__(self, real):
                    object.__setattr__(self, '_real', real)

                def __getattr__(self, name):
                    return getattr(self._real, name)

                def __setattr__(self, name, value):
                    setattr(self._real, name, value)

                def execute(self, sql, *a, **kw):
                    if sql == target_sql:
                        raise sqlite3.OperationalError(
                            'disk I/O error (simulated, unrelated to '
                            'the migration race)')
                    return self._real.execute(sql, *a, **kw)

            def fake_connect(*a, **kw):
                return _BrokenConn(real_connect(*a, **kw))

            with patch.object(sl.sqlite3, 'connect',
                              side_effect=fake_connect):
                with self.assertRaises(sqlite3.OperationalError) as ctx:
                    sl.all_rows(db)
            self.assertIn('disk I/O error', str(ctx.exception))


# ---------------------------------------------------------------------------
# Whole-branch review fix wave (2026-08-11), FIX 1: the production ledger
# is -rw-r--r-- owned by mu2epro, so a non-mu2epro reader's _connect hits
# "attempt to write a readonly database" on the map_path->origin ALTER,
# not the migration-race "no such column" seen above. Before this fix
# that OperationalError re-raised (cols_now still lacked 'origin'),
# killing `submissions status` — the DEFAULT verb, documented safe under
# any account — for every collaborator until a writer happened to touch
# the ledger first.
# ---------------------------------------------------------------------------

class TestOriginMigrationReadOnlyDb(unittest.TestCase):
    def _make_legacy_db(self, td):
        db = os.path.join(td, 'submissions.db')
        con = sqlite3.connect(db)
        con.executescript("""
            CREATE TABLE submissions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_utc TEXT NOT NULL,
              state TEXT NOT NULL DEFAULT 'active',
              attempt INTEGER NOT NULL DEFAULT 1,
              parent_id INTEGER,
              map_path TEXT, tarball TEXT NOT NULL,
              entry_json TEXT NOT NULL, indices_json TEXT NOT NULL,
              jobsub_id TEXT, cluster_id TEXT, closed_utc TEXT, note TEXT);
            CREATE TABLE campaigns (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_utc TEXT NOT NULL,
              state TEXT NOT NULL DEFAULT 'active',
              map_path TEXT, tarball TEXT NOT NULL,
              entry_json TEXT NOT NULL, cursor INTEGER NOT NULL DEFAULT 0,
              slice_size INTEGER NOT NULL, closed_utc TEXT, note TEXT);
            CREATE UNIQUE INDEX campaigns_live_tarball
              ON campaigns(tarball) WHERE state IN ('active','paused');
            INSERT INTO submissions
              (created_utc, map_path, tarball, entry_json, indices_json)
              VALUES ('2026-01-01T00:00:00Z', '/tmp/ro-legacy.json',
                      'x.tar', '{}', '[]');
        """)
        con.commit()
        con.close()
        return db

    def test_readonly_legacy_db_is_readable_and_left_unmigrated(self):
        from utils import submission_ledger as sl
        with tempfile.TemporaryDirectory() as td:
            db = self._make_legacy_db(td)
            os.chmod(db, 0o444)
            try:
                rows = sl.all_rows(db)
            finally:
                # Allow TemporaryDirectory cleanup to remove the file.
                os.chmod(db, 0o644)
            # The origin shim now lives in the shared shapers
            # (2026-08-28 un-fork), so even the CLI reader sees the
            # normalized key on an un-migrated ledger — previously it
            # got raw map_path and any origin consumer KeyError'd.
            self.assertEqual(rows[0]['origin'], '/tmp/ro-legacy.json')
            self.assertNotIn('map_path', rows[0])
            con = sqlite3.connect(db)
            try:
                cols = [r[1] for r in
                       con.execute('PRAGMA table_info(submissions)')]
            finally:
                con.close()
        self.assertIn('map_path', cols)
        self.assertNotIn('origin', cols)


# ---------------------------------------------------------------------------
# Whole-branch review fix wave (2026-08-11), FIX 5: resubmit()/
# resubmit_files() hardcoded origin=f"recovery of row {id}" — the SAME
# string the automatic recovery loop writes. The ledger column was
# renamed map_path -> origin specifically to make it audit-trail
# provenance; a hand re-fire through `submissions resubmit` writing the
# identical string as an automatic recovery defeated that on day one.
# ---------------------------------------------------------------------------

class TestResubmitOriginIsDistinguishable(unittest.TestCase):
    def test_resubmit_default_origin_is_the_recovery_string(self):
        """Unchanged default: the automatic recovery loop (process_row)
        never passes origin, so it must keep getting the original
        string."""
        from utils import submissions
        captured = {}

        def fake_submit(entry, idx, options):
            captured['options'] = options
            return {'status': 'submitted'}

        row = {'id': 5, 'tarball': 'x.tar', 'entry': {'njobs': 10}}
        submissions.resubmit(row, [1], '/tmp/x.db', submit_fn=fake_submit)
        self.assertEqual(captured['options'].origin, 'recovery of row 5')

    def test_resubmit_honors_an_explicit_origin(self):
        from utils import submissions
        captured = {}

        def fake_submit(entry, idx, options):
            captured['options'] = options
            return {'status': 'submitted'}

        row = {'id': 5, 'tarball': 'x.tar', 'entry': {'njobs': 10}}
        submissions.resubmit(row, [1], '/tmp/x.db', submit_fn=fake_submit,
                             origin='operator resubmit of row 5')
        self.assertEqual(captured['options'].origin,
                         'operator resubmit of row 5')

    def test_resubmit_files_honors_an_explicit_origin(self):
        from utils import submissions
        captured = {}

        def fake_submit(entry, idx, options):
            captured['options'] = options
            return {'status': 'submitted'}

        row = {'id': 6, 'tarball': 'x.tar',
              'entry': {'input_pattern': 'dts.*.art'}}
        submissions.resubmit_files(row, ['f.art'], '/tmp/x.db',
                                   submit_fn=fake_submit,
                                   origin='operator resubmit of row 6')
        self.assertEqual(captured['options'].origin,
                         'operator resubmit of row 6')

    def test_resubmit_verb_passes_an_operator_origin_distinct_from_recovery(self):
        """End-to-end through the CLI dispatch (mirrors
        test_files_selector_dispatches_to_resubmit_files_not_resubmit's
        pattern of patching the module-level resubmit/resubmit_files
        names and driving via --dry-run, which returns right after the
        call without needing a confirmed ledger child)."""
        from utils import submissions, submission_ledger
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            submission_ledger.ensure_ledger_dir(db)
            rid = submission_ledger.reserve_submission(
                db, tarball='x.tar', entry={'tarball': 'x.tar'},
                indices=[1, 2, 3])
            submission_ledger.attach_cluster(db, rid, jobsub_id='j',
                                             cluster_id='1')
            with patch.object(submissions, 'resubmit',
                              return_value=True) as fake_resubmit:
                submissions.main(['--db', db, 'resubmit', str(rid),
                                  '--indices', '99', '--dry-run'])
            fake_resubmit.assert_called_once()
            _, kwargs = fake_resubmit.call_args
            self.assertEqual(kwargs['origin'],
                             f'operator resubmit of row {rid}')
            self.assertNotEqual(kwargs['origin'], f'recovery of row {rid}')


class TestTickAdvanceRequiresEvidence(unittest.TestCase):
    """FIX A (2026-08-11 wave 2): submit_entry RETURNS {'status':
    'failed'} — it does NOT raise — when jobsub_submit exits non-zero,
    or exits 0 with no parseable cluster id. _guarded_submit alone only
    catches exceptions, so submit_slice/submit_drain_batch used to
    report success on that returned failure, and top_up/drain_tick
    would advance the cursor / count the batch as delivered past work
    that was never actually submitted. The fix requires a new ACTIVE
    ledger row for the tarball as evidence, same principle as the
    `submissions resubmit` verb's own child-row check."""

    def setUp(self):
        import tempfile
        from utils import submission_ledger as sl
        self.sl = sl
        self.db = os.path.join(_mkdtemp(), 'submissions.db')

    def _campaign(self, tarball='cnf.mu2e.A.C.0.tar', njobs=10, slice=4):
        entry = {'tarball': tarball, 'njobs': njobs, 'inloc': 'tape',
                 'outputs': []}
        return self.sl.create_campaign(self.db, tarball=tarball,
                                       entry=entry, slice_size=slice)

    # — submit_slice, direct --------------------------------------------

    def test_submit_slice_false_on_returned_failed_status(self):
        """A submit_fn that returns the non-raising 'failed' shape and
        leaves no ledger trace must be reported as failure."""
        from utils import submissions
        tarball = 'cnf.mu2e.A.C.0.tar'
        camp = {'id': 1, 'cursor': 0, 'tarball': tarball,
                'entry': {'tarball': tarball, 'njobs': 10}}
        def fake_submit_entry(entry, idx, options):
            return {'status': 'failed', 'tarball': tarball,
                    'cluster_id': None, 'njobs': 0}
        ok = submissions.submit_slice(camp, 4, self.db,
                                      submit_fn=fake_submit_entry)
        self.assertFalse(ok)

    def test_submit_slice_true_on_genuine_ledger_evidence(self):
        from utils import submissions
        tarball = 'cnf.mu2e.A.C.0.tar'
        camp = {'id': 1, 'cursor': 0, 'tarball': tarball,
                'entry': {'tarball': tarball, 'njobs': 10}}
        def fake_submit_entry(entry, idx, options):
            self.sl.record_submission(
                self.db, tarball=tarball, entry=entry,
                indices=[0, 1, 2, 3], jobsub_id='5.0@js', cluster_id='5')
            return {'status': 'submitted', 'cluster_id': '5'}
        ok = submissions.submit_slice(camp, 4, self.db,
                                      submit_fn=fake_submit_entry)
        self.assertTrue(ok)

    # — submit_drain_batch, direct ---------------------------------------

    def test_submit_drain_batch_false_on_returned_failed_status(self):
        from utils import submissions
        tarball = 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar'
        camp = {'id': 2, 'tarball': tarball,
                'entry': {'tarball': tarball, 'input_pattern': 'dts.*.art'}}
        def fake_submit_entry(entry, idx, options):
            return {'status': 'failed', 'cluster_id': None}
        ok = submissions.submit_drain_batch(
            camp, ['dts.mu2e.a.v.art'], self.db,
            submit_fn=fake_submit_entry)
        self.assertFalse(ok)

    def test_submit_drain_batch_true_on_genuine_ledger_evidence(self):
        from utils import submissions
        tarball = 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar'
        camp = {'id': 2, 'tarball': tarball,
                'entry': {'tarball': tarball, 'input_pattern': 'dts.*.art'}}
        def fake_submit_entry(entry, idx, options):
            self.sl.record_submission(
                self.db, tarball=tarball, entry=entry,
                indices=['dts.mu2e.a.v.art'],
                jobsub_id='6.0@js', cluster_id='6')
            return {'status': 'submitted', 'cluster_id': '6'}
        ok = submissions.submit_drain_batch(
            camp, ['dts.mu2e.a.v.art'], self.db,
            submit_fn=fake_submit_entry)
        self.assertTrue(ok)

    # — top_up integration: cursor must not advance -----------------------

    def test_top_up_pauses_without_advancing_on_returned_failure(self):
        """End-to-end through the REAL submit_slice (top_up's own
        default submit_fn), with submit.submit_entry itself patched to
        return the returned-not-raised failure shape. The cursor must
        stay put and the campaign must pause — exactly the same handling
        top_up already gives an exception-raising submit_fn."""
        from utils.submissions import top_up
        from utils import submit
        tarball = 'cnf.mu2e.A.C.0.tar'
        self._campaign(tarball=tarball, njobs=10, slice=4)
        def fake_submit_entry(entry, idx, options):
            return {'status': 'failed', 'cluster_id': None}
        with patch.object(submit, 'submit_entry', fake_submit_entry):
            s = top_up(self.db, cap=100, count_fn=lambda: 0)
        c = self.sl.all_campaigns(self.db)[0]
        self.assertEqual(c['cursor'], 0)
        self.assertEqual(c['state'], 'paused')
        self.assertEqual(s['campaign-paused'], 1)

    def test_top_up_advances_on_genuine_success(self):
        """top_up loops slices within one call until the cap or the
        campaign is exhausted (see TestTopUp.test_feeds_until_complete),
        so njobs=10/slice=4 with a fixed cap=100 count_fn drains the
        whole campaign in this one call: three genuinely-evidenced
        slices, cursor to njobs, closed complete."""
        from utils.submissions import top_up
        from utils import submit
        tarball = 'cnf.mu2e.A.C.0.tar'
        self._campaign(tarball=tarball, njobs=10, slice=4)
        counter = [0]
        def fake_submit_entry(entry, idx, options):
            counter[0] += 1
            self.sl.record_submission(
                self.db, tarball=tarball, entry=entry,
                indices=[counter[0]], jobsub_id=f'{counter[0]}.0@js',
                cluster_id=str(counter[0]))
            return {'status': 'submitted', 'cluster_id': str(counter[0])}
        with patch.object(submit, 'submit_entry', fake_submit_entry):
            s = top_up(self.db, cap=100, count_fn=lambda: 0)
        c = self.sl.all_campaigns(self.db)[0]
        self.assertEqual(c['cursor'], 10)
        self.assertEqual(c['state'], 'complete')
        self.assertEqual(s['slice'], 3)
        self.assertEqual(counter[0], 3)

    # — drain_tick integration: no cursor, but must still pause ----------

    def test_drain_tick_pauses_on_returned_failure(self):
        """The draining analog: no cursor to protect, but the campaign
        must still pause rather than count a never-submitted batch as
        delivered."""
        from utils.submissions import drain_tick
        from utils import submit
        tarball = 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar'
        entry = {'tarball': tarball, 'inloc': 'tape',
                 'input_pattern': 'dig.mu2e.%.MDC2025au_best_v1_5.art',
                 'outputs': [{'dataset': '*.art', 'location': 'tape'}]}
        self.sl.create_campaign(self.db, tarball=tarball, entry=entry,
                                slice_size=2)
        pending = ['dig.mu2e.A.MDC2025au_best_v1_5.001202_00000001.art']
        def fake_state(camp, db):
            return {'inputs': set(pending), 'landed': set(),
                    'in_flight': set(), 'parked': set(),
                    'pending': list(pending)}
        def fake_submit_entry(entry, idx, options):
            return {'status': 'failed', 'cluster_id': None}
        with patch.object(submit, 'submit_entry', fake_submit_entry):
            s = drain_tick(self.db, cap=100, count_fn=lambda: 0,
                           state_fn=fake_state,
                           gate_fn=lambda e, cand: (list(cand), [], []))
        c = self.sl.all_campaigns(self.db)[0]
        self.assertEqual(c['state'], 'paused')
        self.assertEqual(s['campaign-paused'], 1)

    def test_drain_tick_delivers_on_genuine_success(self):
        from utils.submissions import drain_tick
        from utils import submit
        tarball = 'cnf.mu2e.reco.MDC2025au_best_v1_5.0.tar'
        entry = {'tarball': tarball, 'inloc': 'tape',
                 'input_pattern': 'dig.mu2e.%.MDC2025au_best_v1_5.art',
                 'outputs': [{'dataset': '*.art', 'location': 'tape'}]}
        self.sl.create_campaign(self.db, tarball=tarball, entry=entry,
                                slice_size=2)
        pending = ['dig.mu2e.A.MDC2025au_best_v1_5.001202_00000001.art']
        def fake_state(camp, db):
            return {'inputs': set(pending), 'landed': set(),
                    'in_flight': set(), 'parked': set(),
                    'pending': list(pending)}
        def fake_submit_entry(entry, idx, options):
            self.sl.record_submission(
                self.db, tarball=tarball, entry=entry, indices=pending,
                jobsub_id='8.0@js', cluster_id='8')
            return {'status': 'submitted', 'cluster_id': '8'}
        with patch.object(submit, 'submit_entry', fake_submit_entry):
            s = drain_tick(self.db, cap=100, count_fn=lambda: 0,
                           state_fn=fake_state,
                           gate_fn=lambda e, cand: (list(cand), [], []))
        c = self.sl.all_campaigns(self.db)[0]
        self.assertEqual(c['state'], 'active')
        self.assertEqual(s['drain-batch'], 1)


class TestResubmitCursorBound(unittest.TestCase):
    """FIX B (2026-08-11 wave 2): `submissions resubmit --indices`
    must refuse an index at or above the live (active/paused)
    campaign's cursor for the row's tarball. Below the cursor is a
    legitimate recovery of already-submitted work; at or above it is
    ground the campaign has not reached yet, and a hand-fired child
    that lands and closes there permanently blocks the tick
    (_slice_overlaps_ledger skips only 'reconciled', and reconcile_row
    refuses anything not 'failed'/'submitting' — no CLI escape)."""

    def _setup(self, td, cursor, camp_state='active'):
        from utils import submission_ledger as sl
        db = os.path.join(td, 'submissions.db')
        sl.ensure_ledger_dir(db)
        tarball = 'cnf.mu2e.A.C.0.tar'
        entry = {'tarball': tarball, 'njobs': 10000, 'inloc': 'tape',
                 'outputs': []}
        cid = sl.create_campaign(db, tarball=tarball, entry=entry,
                                 slice_size=100)
        sl.advance_campaign(db, cid, cursor)   # must be 'active' to advance
        if camp_state == 'paused':
            sl.set_campaign_state(db, cid, 'paused', note='operator pause')
        rid = sl.reserve_submission(db, tarball=tarball, entry=entry,
                                    indices=[4231])
        sl.attach_cluster(db, rid, jobsub_id='j', cluster_id='1')
        return db, rid, tarball

    def test_below_cursor_allowed(self):
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, rid, _ = self._setup(td, cursor=1000)
            with patch.object(submissions, 'resubmit',
                              return_value=True) as fake:
                submissions.main(['--db', db, 'resubmit', str(rid),
                                  '--indices', '999', '--dry-run'])
            fake.assert_called_once()

    def test_at_cursor_refused(self):
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, rid, tarball = self._setup(td, cursor=1000)
            with patch.object(submissions, 'resubmit') as fake:
                with self.assertRaises(SystemExit) as cm:
                    submissions.main(['--db', db, 'resubmit', str(rid),
                                      '--indices', '1000', '--dry-run'])
            fake.assert_not_called()
            self.assertIn('cursor', str(cm.exception))
            self.assertIn('1000', str(cm.exception))
            self.assertIn(tarball, str(cm.exception))

    def test_above_cursor_refused(self):
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, rid, _ = self._setup(td, cursor=1000)
            with patch.object(submissions, 'resubmit') as fake:
                with self.assertRaises(SystemExit) as cm:
                    submissions.main(['--db', db, 'resubmit', str(rid),
                                      '--indices', '5000,5001',
                                      '--dry-run'])
            fake.assert_not_called()
            self.assertIn('cursor', str(cm.exception))
            self.assertIn('5000', str(cm.exception))

    def test_no_live_campaign_allowed(self):
        """No active/paused campaign owns the tarball's index space —
        there is no cursor to bound against, so nothing is refused
        here (a closed/complete/cancelled campaign does not count)."""
        from utils import submission_ledger as sl, submissions
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            sl.ensure_ledger_dir(db)
            tarball = 'cnf.mu2e.A.C.0.tar'
            rid = sl.reserve_submission(
                db, tarball=tarball, entry={'tarball': tarball},
                indices=[1, 2, 3])
            sl.attach_cluster(db, rid, jobsub_id='j', cluster_id='1')
            with patch.object(submissions, 'resubmit',
                              return_value=True) as fake:
                submissions.main(['--db', db, 'resubmit', str(rid),
                                  '--indices', '999999', '--dry-run'])
            fake.assert_called_once()

    def test_paused_campaign_also_bounds(self):
        """A paused campaign still owns its index space (same live set
        as the campaigns_live_tarball unique index) — its cursor bounds
        a resubmit exactly like an active campaign's."""
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, rid, _ = self._setup(td, cursor=1000, camp_state='paused')
            with patch.object(submissions, 'resubmit') as fake:
                with self.assertRaises(SystemExit) as cm:
                    submissions.main(['--db', db, 'resubmit', str(rid),
                                      '--indices', '1000', '--dry-run'])
            fake.assert_not_called()
            self.assertIn('cursor', str(cm.exception))

    def test_indices_file_variant_also_bounded(self):
        """--indices-file goes through the same payload/refusal path as
        --indices; pin it separately since it is a different argparse
        branch."""
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, rid, _ = self._setup(td, cursor=1000)
            idx_path = os.path.join(td, 'idx.txt')
            with open(idx_path, 'w') as fh:
                fh.write('1500\n')
            with patch.object(submissions, 'resubmit') as fake:
                with self.assertRaises(SystemExit) as cm:
                    submissions.main(['--db', db, 'resubmit', str(rid),
                                      '--indices-file', idx_path,
                                      '--dry-run'])
            fake.assert_not_called()
            self.assertIn('cursor', str(cm.exception))

    def test_files_selector_not_bounded_by_cursor(self):
        """--files is file-keyed with no index space — the cursor bound
        must not touch it, even when a live index campaign exists for
        the same tarball name."""
        from utils import submission_ledger as sl, submissions
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            sl.ensure_ledger_dir(db)
            tarball = 'cnf.mu2e.A.C.0.tar'
            entry = {'tarball': tarball, 'input_pattern': 'dts.*.art'}
            rid = sl.reserve_submission(
                db, tarball=tarball, entry=entry,
                indices=['dts.mu2e.a.v.art'])
            sl.attach_cluster(db, rid, jobsub_id='j', cluster_id='1')
            files_path = os.path.join(td, 'files.txt')
            with open(files_path, 'w') as fh:
                fh.write('dts.mu2e.CosmicCORSIKA.MDC2020az.'
                         '001202_00000002.art\n')
            with patch.object(submissions, 'resubmit_files',
                              return_value=True) as fake:
                submissions.main(['--db', db, 'resubmit', str(rid),
                                  '--files', files_path, '--dry-run'])
            fake.assert_called_once()


class TestResubmitCursorBoundWindowed(unittest.TestCase):
    """FIX 1 (2026-08-11 wave 3): the cursor bound compared ABSOLUTE
    --indices values against an ENTRY-RELATIVE `cursor` — omitting
    `firstjob`. None of TestResubmitCursorBound's fixtures set
    firstjob, so the omission never showed up there. On a windowed
    campaign (firstjob=200000, njobs=1000, cursor=500 — i.e. absolute
    [200000, 200500) already submitted) the bare-cursor bug refused
    200100 (legitimate recovery of already-submitted work) while
    letting 200600 through only by coincidence. The fix bounds against
    firstjob + cursor (200500), matching _slice_overlaps_ledger's own
    `lo = firstjob + cursor` idiom."""

    def _setup(self, td):
        from utils import submission_ledger as sl
        db = os.path.join(td, 'submissions.db')
        sl.ensure_ledger_dir(db)
        tarball = 'cnf.mu2e.A.C.0.tar'
        entry = {'tarball': tarball, 'firstjob': 200000, 'njobs': 1000,
                 'inloc': 'tape', 'outputs': []}
        cid = sl.create_campaign(db, tarball=tarball, entry=entry,
                                 slice_size=100)
        sl.advance_campaign(db, cid, 500)   # entry-relative cursor
        rid = sl.reserve_submission(db, tarball=tarball, entry=entry,
                                    indices=[200999])
        sl.attach_cluster(db, rid, jobsub_id='j', cluster_id='1')
        return db, rid, tarball

    def test_absolute_index_below_absolute_cursor_allowed(self):
        """200100 sits inside the already-submitted absolute window
        [200000, 200500) — legitimate recovery, must be ALLOWED. The
        bare-cursor bug compared 200100 against the raw entry-relative
        cursor (500) and wrongly REFUSED this."""
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, rid, _ = self._setup(td)
            with patch.object(submissions, 'resubmit',
                              return_value=True) as fake:
                submissions.main(['--db', db, 'resubmit', str(rid),
                                  '--indices', '200100', '--dry-run'])
            fake.assert_called_once()

    def test_absolute_index_above_absolute_cursor_refused(self):
        """200600 is past the absolute bound (200500) — ground the
        campaign has not reached yet — REFUSED, and the message must
        name the ABSOLUTE bound (200500), not the raw cursor (500),
        since 500 is index space this tarball's campaign does not
        own."""
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, rid, tarball = self._setup(td)
            with patch.object(submissions, 'resubmit') as fake:
                with self.assertRaises(SystemExit) as cm:
                    submissions.main(['--db', db, 'resubmit', str(rid),
                                      '--indices', '200600', '--dry-run'])
            fake.assert_not_called()
            msg = str(cm.exception)
            self.assertIn('200600', msg)
            self.assertIn('200500', msg)
            self.assertIn(tarball, msg)

    def test_index_outside_campaign_window_allowed(self):
        """400 is below firstjob entirely — an index this campaign's
        [200000, 201000) window will never contain. It is still
        numerically below the absolute cursor bound (200500), so the
        bound (scoped to this fix — the domain-membership check is a
        separate, out-of-scope concern) allows it, same as before and
        after the fix."""
        from utils import submissions
        with tempfile.TemporaryDirectory() as td:
            db, rid, _ = self._setup(td)
            with patch.object(submissions, 'resubmit',
                              return_value=True) as fake:
                submissions.main(['--db', db, 'resubmit', str(rid),
                                  '--indices', '400', '--dry-run'])
            fake.assert_called_once()


class TestGuardedSubmitEvidenceReadsContained(unittest.TestCase):
    """FIX 2 (2026-08-11 wave 3): _guarded_submit_with_evidence's two
    open_rows() reads (pre- and post-submit, utils/submissions.py
    around what were lines 749/753) sat OUTSIDE any try. submit_slice
    and submit_drain_batch are called from top_up/_run_pass and
    drain_tick with no outer try (see TestCallSitesContainFailures),
    so a raising open_rows — e.g. a locked ledger — escaped the call
    site raw and aborted the whole tick. The POST-read case is the
    damaging one: the slice has already gone to the grid, so the
    escape happens before both the cursor advance and the pause,
    wedging every remaining campaign that tick with no physics
    duplicated (the next tick's overlap guard still blocks on the
    active row) but no self-heal either.

    Both cases must yield False, not an escaping exception, from both
    submit_slice and submit_drain_batch."""

    @staticmethod
    def _raise_on_call(n_to_raise):
        """An open_rows stand-in that raises on call number `n_to_raise`
        (1-indexed) and otherwise returns an empty active-row list."""
        calls = {'n': 0}

        def fn(db_path):
            calls['n'] += 1
            if calls['n'] == n_to_raise:
                raise sqlite3.OperationalError('database is locked')
            return []
        fn.calls = calls
        return fn

    def test_submit_slice_pre_read_raise_contained(self):
        from utils import submissions, submission_ledger
        camp = {'id': 1, 'cursor': 0, 'tarball': 'a.tar',
                'entry': {'tarball': 'a.tar', 'njobs': 10}}
        raiser = self._raise_on_call(1)   # PRE-READ
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            with patch.object(submission_ledger, 'open_rows',
                              side_effect=raiser):
                self.assertFalse(submissions.submit_slice(
                    camp, 5, db,
                    submit_fn=lambda entry, idx, options:
                        {'status': 'submitted'}))
        self.assertEqual(raiser.calls['n'], 1)

    def test_submit_slice_post_read_raise_contained(self):
        from utils import submissions, submission_ledger
        camp = {'id': 1, 'cursor': 0, 'tarball': 'a.tar',
                'entry': {'tarball': 'a.tar', 'njobs': 10}}
        raiser = self._raise_on_call(2)   # POST-READ
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            with patch.object(submission_ledger, 'open_rows',
                              side_effect=raiser):
                self.assertFalse(submissions.submit_slice(
                    camp, 5, db,
                    submit_fn=lambda entry, idx, options:
                        {'status': 'submitted'}))
        self.assertEqual(raiser.calls['n'], 2)

    def test_submit_drain_batch_pre_read_raise_contained(self):
        from utils import submissions, submission_ledger
        camp = {'id': 2, 'tarball': 'b.tar',
                'entry': {'tarball': 'b.tar', 'input_pattern': 'dts.*.art'}}
        raiser = self._raise_on_call(1)   # PRE-READ
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            with patch.object(submission_ledger, 'open_rows',
                              side_effect=raiser):
                self.assertFalse(submissions.submit_drain_batch(
                    camp, ['dts.mu2e.a.v.art'], db,
                    submit_fn=lambda entry, idx, options:
                        {'status': 'submitted'}))
        self.assertEqual(raiser.calls['n'], 1)

    def test_submit_drain_batch_post_read_raise_contained(self):
        from utils import submissions, submission_ledger
        camp = {'id': 2, 'tarball': 'b.tar',
                'entry': {'tarball': 'b.tar', 'input_pattern': 'dts.*.art'}}
        raiser = self._raise_on_call(2)   # POST-READ
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, 'submissions.db')
            with patch.object(submission_ledger, 'open_rows',
                              side_effect=raiser):
                self.assertFalse(submissions.submit_drain_batch(
                    camp, ['dts.mu2e.a.v.art'], db,
                    submit_fn=lambda entry, idx, options:
                        {'status': 'submitted'}))
        self.assertEqual(raiser.calls['n'], 2)


# ---------------------------------------------------------------------------
# outstage outputs — write to $MU2EGRID_WFOUTSTAGE, declare nothing
# ---------------------------------------------------------------------------


class TestOutlocVocabulary(unittest.TestCase):
    """outloc values are checked where the build config enters.

    Nothing validated them before: a misspelled location travelled all
    the way to pushOutput, which knows four actions and treats anything
    else as an error on the worker, after the job has already run.
    """

    def test_known_locations_accepted(self):
        from utils.jobdesc import validate_outloc
        validate_outloc({'*.art': 'tape', '*.root': 'disk'})
        validate_outloc({'*.art': 'scratch'})
        validate_outloc({'*.art': 'outstage'})

    def test_typo_rejected(self):
        from utils.jobdesc import validate_outloc
        with self.assertRaises(ValueError):
            validate_outloc({'*.art': 'presistent'})

    def test_non_dict_rejected(self):
        from utils.jobdesc import validate_outloc
        with self.assertRaises(ValueError):
            validate_outloc('disk')


class TestOutstageDir(unittest.TestCase):
    """`$MU2EGRID_WFOUTSTAGE/$CLUSTER/$PROCESS` — the mu2egrid layout,
    which `mu2eClusterCheckAndMove` already knows how to walk."""

    def test_dir_from_env(self):
        from utils import runmu2e
        env = {'MU2EGRID_WFOUTSTAGE': '/pnfs/mu2e/scratch/users/o/workflow/p/outstage',
               'CLUSTER': '12345', 'PROCESS': '7'}
        with patch.dict(os.environ, env, clear=False):
            self.assertEqual(
                runmu2e._outstage_dir(),
                '/pnfs/mu2e/scratch/users/o/workflow/p/outstage/12345/7')

    def test_missing_wfoutstage_is_a_hard_error(self):
        """Silently defaulting would scatter output somewhere nobody
        looks; the submitter always exports this var."""
        from utils import runmu2e
        env = dict(os.environ)
        env.pop('MU2EGRID_WFOUTSTAGE', None)
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(RuntimeError):
                runmu2e._outstage_dir()


class TestPushDataOutstage(unittest.TestCase):
    """Outstage specs bypass pushOutput entirely — no copy to a dataset
    path, no SAM declare."""

    OUT = 'mcs.mu2e.CePLeadingLogOnSpill.MDC2025au_best_v1_1.001430_00000000.art'
    NTD = 'nts.mu2e.CePLeadingLogOnSpill.MDC2025au_best_v1_1.001430_00000000.root'

    def _run(self, outputs, files, infiles=''):
        from utils import runmu2e
        seen = {'push': None, 'outstage': None}

        def fake_push_output(output_specs, output_file="output.txt",
                             simjob_setup=None):
            seen['push'] = [s[1] for s in output_specs]
            return 0

        def fake_copy(filenames):
            seen['outstage'] = list(filenames)
            return 0

        d = _mkdtemp()
        for f in files:
            (Path(d) / f).write_text('x\n')
        cwd = os.getcwd()
        try:
            os.chdir(d)
            with patch.object(runmu2e, 'push_output', fake_push_output), \
                 patch.object(runmu2e, '_copy_to_outstage', fake_copy):
                rc = runmu2e.push_data(outputs, infiles)
        finally:
            os.chdir(cwd)
        return seen, rc, d

    def test_outstage_only_never_calls_push_output(self):
        seen, rc, _ = self._run(
            [{'dataset': '*.art', 'location': 'outstage'}], [self.OUT])
        self.assertEqual(seen['outstage'], [self.OUT])
        self.assertIsNone(seen['push'])
        self.assertEqual(rc, 0)

    def test_outstage_only_writes_no_parents_list(self):
        """parents_list.txt exists solely to feed pushOutput the SAM
        parents for a declare. Nothing declares here."""
        seen, _, d = self._run(
            [{'dataset': '*.art', 'location': 'outstage'}], [self.OUT],
            infiles='dig.mu2e.X.Y.001430_00000000.art')
        self.assertFalse((Path(d) / 'parents_list.txt').exists())

    def test_mixed_outputs_partition_by_location(self):
        seen, rc, _ = self._run(
            [{'dataset': '*.art', 'location': 'outstage'},
             {'dataset': '*.root', 'location': 'disk'}],
            [self.OUT, self.NTD])
        self.assertEqual(seen['outstage'], [self.OUT])
        self.assertEqual(seen['push'], [self.NTD])

    def test_mixed_outputs_still_write_parents_list(self):
        """A surviving declared output still needs its parents."""
        infiles = 'dig.mu2e.X.Y.001430_00000000.art'
        seen, _, d = self._run(
            [{'dataset': '*.art', 'location': 'outstage'},
             {'dataset': '*.root', 'location': 'disk'}],
            [self.OUT, self.NTD], infiles=infiles)
        self.assertEqual((Path(d) / 'parents_list.txt').read_text(),
                         infiles + '\n')


class TestLogFollowsOutstage(unittest.TestCase):
    """A log declared to SAM would name outstage files as its parents —
    files SAM has never heard of. The log goes where the data goes."""

    def test_log_storage_location_outstage(self):
        from utils.job_common import log_storage_location
        self.assertEqual(
            log_storage_location([{'location': 'outstage', 'dataset': '*.art'}]),
            'outstage')

    def test_push_logs_outstage_skips_push_output(self):
        from utils import runmu2e
        seen = {'push': False, 'outstage': None}

        def fake_push_output(*a, **kw):
            seen['push'] = True
            return 0

        d = _mkdtemp()
        logname = 'log.mu2e.X.Y.001430_00000000.log'
        (Path(d) / logname).write_text('log\n')
        cwd = os.getcwd()
        try:
            os.chdir(d)
            with patch.object(runmu2e, 'push_output', fake_push_output), \
                 patch.object(runmu2e, '_copy_to_outstage',
                              lambda fs: seen.__setitem__('outstage', list(fs)) or 0):
                rc = runmu2e.push_logs(log_file=logname, location='outstage')
        finally:
            os.chdir(cwd)
        self.assertEqual(seen['outstage'], [logname])
        self.assertFalse(seen['push'])
        self.assertEqual(rc, 0)


class TestOutstageRequestsNoDatasetScope(unittest.TestCase):
    """The worker token already carries storage.modify on WFOUTSTAGE
    (jobsub_argv adds it unconditionally). An outstage output must not
    ALSO request a `/mu2e/<area>/datasets/...` scope — htvault rejects
    scopes it has not pre-allocated, which would fail the submission."""

    def test_no_scope_for_outstage_output(self):
        from utils.jobsub_argv import output_storage_dirs
        dirs = output_storage_dirs(
            ['mcs.mu2e.D.C.001430_00000000.art'],
            [{'dataset': '*.art', 'location': 'outstage'}])
        self.assertEqual(dirs, [])


class TestEnqueueRefusesOutstage(unittest.TestCase):
    """verify_row is fail-closed against SAM. An outstage campaign
    declares nothing, so every index reads as missing and the next tick
    recovers the whole row — forever. Refuse at the door."""

    def setUp(self):
        from utils import submit
        self.db = os.path.join(_mkdtemp(), 'submissions.db')
        tb = patch.object(submit, '_ensure_local_tarball',
                          return_value=Path('cnf.mu2e.O.C.0.tar'))
        ci = patch.object(submit, 'check_inputs', return_value=(True, []))
        cc = patch.object(submit, 'check_code_tarball',
                          return_value=(True, []))
        tb.start()
        ci.start()
        cc.start()
        self.addCleanup(tb.stop)
        self.addCleanup(ci.stop)
        self.addCleanup(cc.stop)

    def test_outstage_entry_refused(self):
        from utils.submit import enqueue_entry
        entry = {'tarball': 'cnf.mu2e.O.C.0.tar', 'njobs': 10, 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                 'inloc': 'tape',
                 'outputs': [{'dataset': '*.art', 'location': 'outstage'}]}
        with self.assertRaises(SystemExit):
            enqueue_entry(entry, ledger_db=self.db, slice_size=10)

    def test_outstage_among_several_outputs_refused(self):
        from utils.submit import enqueue_entry
        entry = {'tarball': 'cnf.mu2e.O.C.0.tar', 'njobs': 10, 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                 'inloc': 'tape',
                 'outputs': [{'dataset': '*.root', 'location': 'disk'},
                             {'dataset': '*.art', 'location': 'outstage'}]}
        with self.assertRaises(SystemExit):
            enqueue_entry(entry, ledger_db=self.db, slice_size=10)

    def test_ordinary_entry_still_enqueues(self):
        from utils.submit import enqueue_entry
        entry = {'tarball': 'cnf.mu2e.O.C.0.tar', 'njobs': 10, 'prodtools_dir': FAKE_PRODTOOLS_DIR,
                 'inloc': 'tape',
                 'outputs': [{'dataset': '*.art', 'location': 'disk'}]}
        self.assertIsNotNone(
            enqueue_entry(entry, ledger_db=self.db, slice_size=10))


# ---------------------------------------------------------------------------
# runlocal — running cnf jobs on this node, several at a time
# ---------------------------------------------------------------------------

def _runlocal_args(**over):
    """A parsed-args stand-in for the runlocal driver."""
    args = SimpleNamespace(
        jobdef='cnf.mu2e.Test.TestConf.0.tar', inloc='tape',
        indices=[0], parallel=1, workdir='.', nevts=-1,
        mu2e_options='', copy_input=False, one=None, json=None,
        timeout=0, entry_point='/repo/utils/runlocal.py')
    for key, value in over.items():
        setattr(args, key, value)
    return args


def _fake_popen(fake):
    """Adapt a simple `(argv, cwd, env, stdout) -> returncode` stand-in
    to the Popen object the driver drives.

    The driver uses Popen, not subprocess.run, because it must be able to
    signal a job's whole process GROUP on timeout — run() would kill the
    launcher and leave mu2e behind. `pid` is deliberately not 0 or 1:
    os.killpg(0, ...) signals the CALLER's own group, which in a test run
    is the test runner.
    """
    def popen(argv, cwd=None, env=None, stdout=None, stderr=None, **kwargs):
        result = fake(argv, cwd=cwd, env=env, stdout=stdout, stderr=stderr)
        return SimpleNamespace(pid=2 ** 30, returncode=result.returncode,
                               wait=lambda timeout=None: result.returncode)
    return popen


class TestRunLocalOutputGlobs(unittest.TestCase):
    """A job's outputs are globbed, not computed: the sequencer of an
    input-driven job isn't known until its inputs resolve."""

    def _tarball(self, outfiles):
        return _make_tarball({'owner': 'mu2e', 'dsconf': 'TestConf',
                              'tbs': {'outfiles': outfiles}})

    def test_resolves_owner_and_version_wildcards_sequencer(self):
        from utils.runlocal import output_globs
        tar = self._tarball({'o': 'dts.owner.CeEndpoint.version.sequencer.art'})
        self.assertEqual(output_globs(tar),
                         ['dts.mu2e.CeEndpoint.TestConf.*.art'])

    def test_real_jobpars_spell_the_placeholder_sequence(self):
        """Live cnfs write '.sequence.', not '.sequencer.' — a string
        replace on one spelling silently reported zero outputs."""
        from utils.runlocal import output_globs
        tar = self._tarball(
            {'outputs.compressedOutput101.fileName':
             'dts.mu2e.STMBeamToVDTarget101.MDC2025au.sequence.art'})
        self.assertEqual(output_globs(tar),
                         ['dts.mu2e.STMBeamToVDTarget101.MDC2025au.*.art'])

    def test_skips_sinks(self):
        """/dev/null is a legal outfile target and produces no file."""
        from utils.runlocal import output_globs
        tar = self._tarball({'o': 'dts.owner.X.version.sequencer.art',
                             'n': '/dev/null'})
        self.assertEqual(output_globs(tar), ['dts.mu2e.X.TestConf.*.art'])

    def test_generic_desc_placeholder_becomes_a_wildcard(self):
        from utils.runlocal import output_globs
        tar = self._tarball({'o': 'dts.owner.{desc}.version.sequencer.art'})
        self.assertEqual(output_globs(tar), ['dts.mu2e.*.TestConf.*.art'])

    def test_no_outfiles_is_not_an_error(self):
        from utils.runlocal import output_globs
        self.assertEqual(output_globs(self._tarball({})), [])


class TestRunLocalJobdesc(unittest.TestCase):
    """The synthesized jobdesc has ONE index space — the cnf's."""

    def setUp(self):
        self.tar = _make_tarball(
            {'owner': 'mu2e', 'dsconf': 'TestConf',
             'tbs': {'outfiles': {'o': 'dts.owner.X.version.sequencer.art'}}})

    def test_njobs_covers_the_window(self):
        from utils.runlocal import synth_jobdesc
        from utils.prod_utils import resolve_entry_index
        jobdesc = synth_jobdesc(self.tar, 'tape', [5, 6, 7])
        self.assertEqual(jobdesc['njobs'], 8)
        # An index in the window maps to ITSELF — baseSeed = 1 + index.
        entry, local = resolve_entry_index(jobdesc, 7)
        self.assertIsNotNone(entry)
        self.assertEqual(local, 7)
        # And one past it is refused rather than silently rerunning index 0.
        self.assertEqual(resolve_entry_index(jobdesc, 8), (None, None))

    def test_carries_no_firstjob(self):
        """Two index spaces is the trap this runner avoids; a firstjob
        key would shift every index and change the seeds."""
        from utils.runlocal import synth_jobdesc
        self.assertNotIn('firstjob',
                         synth_jobdesc(self.tar, 'tape', [5, 6, 7]))

    def test_a_gapped_list_still_reaches_its_largest_index(self):
        """`--indices 0,9` must not stop at njobs=2 — resolve_entry_index
        rejects anything >= njobs, which would refuse index 9."""
        from utils.runlocal import synth_jobdesc
        from utils.prod_utils import resolve_entry_index
        jobdesc = synth_jobdesc(self.tar, 'tape', [0, 9])
        self.assertEqual(jobdesc['njobs'], 10)
        self.assertEqual(resolve_entry_index(jobdesc, 9)[1], 9)

    def test_outputs_are_globs_marked_undeclared(self):
        from utils.runlocal import synth_jobdesc
        from utils.jobdesc import OUTSTAGE_LOCATION
        outputs = synth_jobdesc(self.tar, 'tape', [0])['outputs']
        self.assertEqual(outputs,
                         [{'dataset': 'dts.mu2e.X.TestConf.*.art',
                           'location': OUTSTAGE_LOCATION}])


class TestRunLocalChildArgv(unittest.TestCase):
    """The rerun command printed for a failed job must be runnable."""

    def test_carries_index_and_window(self):
        from utils.runlocal import child_argv
        argv = child_argv(7, _runlocal_args(indices=[5, 6, 7, 8],
                                            jobdef='/t/c.tar'))
        self.assertIn('--one', argv)
        self.assertEqual(argv[argv.index('--one') + 1], '7')
        # One spelling reaches the child whichever flag the user used.
        self.assertEqual(argv[argv.index('--indices') + 1], '5-8')
        self.assertNotIn('--first', argv)
        self.assertNotIn('--num', argv)
        self.assertEqual(argv[argv.index('--jobdef') + 1], '/t/c.tar')

    def test_a_gapped_window_survives_the_round_trip(self):
        """The child rebuilds njobs from this spec; a collapsed range
        that lost an index would give the rerun a different jobdesc."""
        from utils.runlocal import child_argv, parse_index_spec
        indices = [0, 3, 7, 8, 9]
        argv = child_argv(3, _runlocal_args(indices=indices))
        spec = argv[argv.index('--indices') + 1]
        self.assertEqual(spec, '0,3,7-9')
        self.assertEqual(parse_index_spec(spec), indices)

    def test_optional_flags_only_when_set(self):
        from utils.runlocal import child_argv
        self.assertNotIn('--copy-input', child_argv(0, _runlocal_args()))
        self.assertNotIn('--mu2e-options', child_argv(0, _runlocal_args()))
        argv = child_argv(0, _runlocal_args(copy_input=True,
                                            mu2e_options='--no-timing'))
        self.assertIn('--copy-input', argv)
        # `=` form: mu2e options start with a dash, and argparse would
        # otherwise read the value as the next flag and reject it.
        self.assertIn('--mu2e-options=--no-timing', argv)


class TestRunlocalCode(unittest.TestCase):
    """The driver unpacks the code tarball ONCE and hands children the
    directory. 3.6 GB per job times four parallel jobs is not viable."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.code = _make_code_tarball(os.path.join(self.dir, 'Code.tar.bz2'))

    def test_unpack_creates_code_setup(self):
        from utils.runlocal import unpack_code
        root = unpack_code(self.code, self.dir)
        self.assertTrue(os.path.isfile(os.path.join(root, 'Code', 'setup.sh')))

    def test_unpack_is_idempotent(self):
        from utils.runlocal import unpack_code
        root = unpack_code(self.code, self.dir)
        marker = os.path.join(root, 'Code', 'setup.sh')
        with open(marker, 'a') as fh:
            fh.write('# touched\n')
        before = os.path.getsize(marker)
        self.assertEqual(unpack_code(self.code, self.dir), root)
        # Second call must not re-extract over an existing tree.
        self.assertEqual(os.path.getsize(marker), before)

    def test_child_argv_carries_code_root(self):
        from utils.runlocal import child_argv
        args = SimpleNamespace(entry_point='/repo/utils/runlocal.py',
                               jobdef='/tmp/cnf.tar', inloc='tape',
                               indices=[0, 1], nevts=10, mu2e_options='',
                               copy_input=False, code_root='/w/code')
        argv = child_argv(0, args)
        self.assertIn('--code-root', argv)
        self.assertEqual(argv[argv.index('--code-root') + 1], '/w/code')

    def test_child_argv_omits_code_root_when_absent(self):
        from utils.runlocal import child_argv
        args = SimpleNamespace(entry_point='/repo/utils/runlocal.py',
                               jobdef='/tmp/cnf.tar', inloc='tape',
                               indices=[0, 1], nevts=10, mu2e_options='',
                               copy_input=False, code_root=None)
        self.assertNotIn('--code-root', child_argv(0, args))

    def test_parser_accepts_both_flags(self):
        from utils.runlocal import build_parser
        args = build_parser().parse_args(
            ['--jobdef', 'cnf.tar', '--code', '/exp/Code.tar.bz2'])
        self.assertEqual(args.code, '/exp/Code.tar.bz2')
        self.assertIsNone(args.code_root)
        child = build_parser().parse_args(
            ['--jobdef', 'cnf.tar', '--one', '3', '--code-root', '/w/code'])
        self.assertEqual(child.code_root, '/w/code')

    def test_sentinel_not_setup_sh_gates_reuse(self):
        """`Code/setup.sh` is payload, extracted partway through a run
        that could still be killed. Only the sentinel — written last —
        proves the extract finished; a run that trusted setup.sh alone
        would silently reuse a truncated tree forever."""
        from utils.runlocal import unpack_code
        root = unpack_code(self.code, self.dir)
        lib = os.path.join(root, 'Code', 'lib', 'libFake.so')
        self.assertTrue(os.path.isfile(lib))
        os.remove(lib)  # simulate a partial extract that got past setup.sh
        self.assertEqual(unpack_code(self.code, self.dir), root)
        # Sentinel was still present, so the second call trusted it and
        # did not re-extract — the missing file stays missing.
        self.assertFalse(os.path.isfile(lib))

    def test_missing_sentinel_forces_re_extraction(self):
        from utils.runlocal import unpack_code
        root = unpack_code(self.code, self.dir)
        lib = os.path.join(root, 'Code', 'lib', 'libFake.so')
        os.remove(lib)
        os.remove(os.path.join(root, '.unpack-complete'))
        self.assertEqual(unpack_code(self.code, self.dir), root)
        # No sentinel meant a real re-extract, which restored the file --
        # proving the sentinel, not setup.sh, is what actually gates.
        self.assertTrue(os.path.isfile(lib))


class TestRunLocalDrive(unittest.TestCase):
    """Each job runs as a child in its own directory."""

    def setUp(self):
        self.workdir = _mkdtemp()
        self.tar = _make_tarball(
            {'owner': 'mu2e', 'dsconf': 'TestConf',
             'tbs': {'outfiles': {'o': 'dts.owner.X.version.sequencer.art'}}})

    def _args(self, **over):
        over.setdefault('jobdef', self.tar)
        over.setdefault('workdir', self.workdir)
        return _runlocal_args(**over)

    @staticmethod
    def _index_of(argv):
        return int(argv[argv.index('--one') + 1])

    def _drive(self, args, fake):
        from utils import runlocal
        buf = io.StringIO()
        with patch.object(runlocal.subprocess, 'Popen', _fake_popen(fake)):
            with contextlib.redirect_stdout(buf):
                rc = runlocal.drive(args)
        return rc, buf.getvalue()

    def test_each_job_gets_its_own_directory(self):
        """process_jobdef works in cwd and its copy-input branch runs
        `mv *.art indir/` — shared directories would cross-contaminate."""
        seen = []

        def fake(argv, cwd=None, env=None, stdout=None, stderr=None):
            seen.append((self._index_of(argv), cwd))
            Path(cwd, 'dts.mu2e.X.TestConf.001430_00000000.art').touch()
            return SimpleNamespace(returncode=0)

        rc, _ = self._drive(self._args(indices=[3, 4]), fake)
        self.assertEqual(rc, 0)
        self.assertEqual([i for i, _ in seen], [3, 4])
        self.assertEqual(sorted(Path(c).name for _, c in seen),
                         ['job_000003', 'job_000004'])

    def test_child_output_is_captured_per_job(self):
        def fake(argv, cwd=None, env=None, stdout=None, stderr=None):
            stdout.write("mu2e chatter\n")
            return SimpleNamespace(returncode=0)

        self._drive(self._args(indices=[0, 1]), fake)
        for index in (0, 1):
            log = Path(self.workdir) / f"job_{index:06d}" / 'stdout.log'
            self.assertIn("mu2e chatter", log.read_text())

    def test_a_failure_does_not_stop_the_rest(self):
        ran = []

        def fake(argv, cwd=None, env=None, stdout=None, stderr=None):
            index = self._index_of(argv)
            ran.append(index)
            return SimpleNamespace(returncode=1 if index == 1 else 0)

        rc, out = self._drive(self._args(indices=[0, 1, 2, 3]), fake)
        self.assertEqual(sorted(ran), [0, 1, 2, 3])
        self.assertEqual(rc, 1)
        self.assertIn('3/4 succeeded', out)
        self.assertIn('rerun index 1', out)

    def test_never_exceeds_the_parallel_limit(self):
        import threading
        import time
        lock = threading.Lock()
        state = {'now': 0, 'peak': 0}

        def fake(argv, cwd=None, env=None, stdout=None, stderr=None):
            with lock:
                state['now'] += 1
                state['peak'] = max(state['peak'], state['now'])
            time.sleep(0.05)
            with lock:
                state['now'] -= 1
            return SimpleNamespace(returncode=0)

        self._drive(self._args(indices=list(range(6)), parallel=2), fake)
        self.assertEqual(state['peak'], 2)

    def test_a_preset_muse_does_not_reach_the_job(self):
        """Each job sources the cnf's own simjob_setup, and museSetup
        refuses when MUSE_WORK_DIR is already set — a caller who ran
        `muse setup SimJob <tag>` first would lose every job."""
        from utils import runlocal
        seen = {}

        def fake(argv, cwd=None, env=None, stdout=None, stderr=None):
            seen.update(env)
            return SimpleNamespace(returncode=0)

        with patch.dict(runlocal.os.environ,
                        {'MUSE_WORK_DIR': '/some/other/build',
                         'MUSE_DIR': '/muse'}):
            self._drive(self._args(), fake)
        self.assertNotIn('MUSE_WORK_DIR', seen)
        # Only that one variable: MUSE_DIR carries the `muse` function's
        # own home, and PATH carries everything else.
        self.assertEqual(seen.get('MUSE_DIR'), '/muse')
        self.assertIn('PATH', seen)

    def test_counts_the_outputs_each_job_produced(self):
        def fake(argv, cwd=None, env=None, stdout=None, stderr=None):
            if self._index_of(argv) == 0:
                Path(cwd, 'dts.mu2e.X.TestConf.001430_00000000.art').touch()
            Path(cwd, 'unrelated.txt').touch()
            return SimpleNamespace(returncode=0)

        _, out = self._drive(self._args(indices=[0, 1]), fake)
        # Only files matching the cnf's declared outputs count.
        self.assertIn(' 1 output(s)', out)
        self.assertIn(' 0 output(s)', out)
        self.assertNotIn('unrelated.txt', out)


class TestRunLocalTimeout(unittest.TestCase):
    """A wedged job must not hold the driver open forever: with no
    limit there is no summary, no exit, and nothing to distinguish a
    hung run from a slow one."""

    def setUp(self):
        self.workdir = _mkdtemp()
        self.tar = _make_tarball(
            {'owner': 'mu2e', 'dsconf': 'TestConf',
             'tbs': {'outfiles': {'o': 'dts.owner.X.version.sequencer.art'}}})
        self.killed = []

    def _hang_popen(self, hang_indices):
        """Popen stand-in whose listed indices never finish."""
        import subprocess as sp

        def popen(argv, cwd=None, env=None, stdout=None, stderr=None, **kw):
            index = int(argv[argv.index('--one') + 1])

            def wait(timeout=None):
                # Stops hanging once it has been signalled — the SIGTERM
                # half of kill_job is what a real art job answers.
                if index in hang_indices and not self.killed:
                    raise sp.TimeoutExpired(argv, timeout or 0)
                return 0
            return SimpleNamespace(pid=2 ** 30 + index, wait=wait)
        return popen

    def _drive(self, popen, **over):
        from utils import runlocal
        args = _runlocal_args(jobdef=self.tar, workdir=self.workdir, **over)
        killers = []

        def killpg(pid, sig):
            killers.append((pid, sig))
            self.killed.append((pid, sig))
        with patch.object(runlocal.subprocess, 'Popen', popen):
            with patch.object(runlocal.os, 'killpg', killpg):
                with contextlib.redirect_stdout(io.StringIO()) as buf:
                    rc = runlocal.drive(args)
        return rc, buf.getvalue(), killers

    def test_a_hung_job_is_killed_and_the_rest_still_run(self):
        path = os.path.join(self.workdir, 'summary.json')
        rc, out, _ = self._drive(self._hang_popen({1}), indices=[0, 1, 2],
                                 timeout=0.01, json=path)
        self.assertEqual(rc, 1)
        self.assertIn('TIMEOUT', out)
        with open(path) as handle:
            data = json.load(handle)
        self.assertEqual(data['failed'], [1])
        self.assertEqual(data['ok'], 2)
        hung = data['jobs'][1]
        self.assertTrue(hung['timed_out'])
        self.assertEqual(hung['rc'], 124)
        self.assertFalse(data['jobs'][0]['timed_out'])

    def test_the_whole_process_group_is_signalled(self):
        """mu2e is a GRANDchild: signalling only the direct child would
        leave it running with nothing left to reap it."""
        import signal as sig_module
        _, _, killers = self._drive(self._hang_popen({0}), indices=[0],
                                    timeout=0.01)
        self.assertTrue(killers)
        # SIGTERM first — art traps it to close its output files.
        self.assertEqual(killers[0][1], sig_module.SIGTERM)
        self.assertEqual(killers[0][0], 2 ** 30)

    def test_zero_disables_the_limit(self):
        """The escape hatch for a job legitimately longer than a day."""
        seen, waited = [], []

        def popen(argv, cwd=None, env=None, stdout=None, stderr=None, **kw):
            seen.append(kw.get('start_new_session'))
            return SimpleNamespace(pid=2 ** 30, wait=lambda timeout=None:
                                   waited.append(timeout) or 0)
        rc, _, killers = self._drive(popen, timeout=0)
        self.assertEqual(rc, 0)
        self.assertEqual(killers, [])
        # 0 reaches Popen.wait as None, which is how it spells "forever".
        self.assertIn(None, waited)
        # Every job owns a process group, so a kill can reach mu2e.
        self.assertEqual(seen, [True])

    def test_a_negative_timeout_is_refused(self):
        from utils.runlocal import main
        with self.assertRaises(SystemExit):
            main(['--jobdef', self.tar, '--timeout', '-1'])

    def test_default_is_the_grid_lease(self):
        from utils.runlocal import build_parser, DEFAULT_TIMEOUT
        self.assertEqual(DEFAULT_TIMEOUT, 24 * 3600)
        args = build_parser().parse_args(['--jobdef', 'c.tar'])
        self.assertEqual(args.timeout, DEFAULT_TIMEOUT)


class TestRunLocalJsonSummary(unittest.TestCase):
    """`--json` exists because the printed table is lossy: it shows a
    COUNT of outputs, never their names, and one exit code for the whole
    run. A caller that chains stages needs the names, and a caller that
    measures a rate needs to know which indices contributed."""

    def setUp(self):
        self.workdir = _mkdtemp()
        self.tar = _make_tarball(
            {'owner': 'mu2e', 'dsconf': 'TestConf',
             'tbs': {'outfiles': {'o': 'dts.owner.X.version.sequencer.art'}}})
        self.path = os.path.join(self.workdir, 'summary.json')

    def _run(self, fake, **over):
        from utils import runlocal
        args = _runlocal_args(jobdef=self.tar, workdir=self.workdir,
                              json=self.path, **over)
        with patch.object(runlocal.subprocess, 'Popen', _fake_popen(fake)):
            with contextlib.redirect_stdout(io.StringIO()):
                rc = runlocal.drive(args)
        return rc

    @staticmethod
    def _index_of(argv):
        return int(argv[argv.index('--one') + 1])

    def _emit(self, index_rcs):
        def fake(argv, cwd=None, env=None, stdout=None, stderr=None):
            index = self._index_of(argv)
            rc = index_rcs[index]
            if rc == 0:
                Path(cwd, f'dts.mu2e.X.TestConf.001430_{index:08d}.art').touch()
            Path(cwd, 'unrelated.txt').touch()
            return SimpleNamespace(returncode=rc)
        return fake

    def test_outputs_are_absolute_paths_the_next_stage_can_open(self):
        """The driver stores basenames; a consumer must not have to know
        the job_<index> layout to turn them into openable paths."""
        self._run(self._emit({0: 0}))
        with open(self.path) as handle:
            data = json.load(handle)
        outputs = data['jobs'][0]['outputs']
        self.assertEqual(len(outputs), 1)
        self.assertTrue(os.path.isabs(outputs[0]))
        self.assertTrue(os.path.isfile(outputs[0]))
        # Only the cnf's declared outputs, same rule the table follows.
        self.assertNotIn('unrelated.txt', json.dumps(data))

    def test_written_even_when_a_job_failed(self):
        """The partial run is exactly the case the file exists for: 2 of
        3 succeeded is indistinguishable from 0 of 3 by exit code."""
        rc = self._run(self._emit({0: 0, 1: 1, 2: 0}), indices=[0, 1, 2])
        self.assertEqual(rc, 1)
        with open(self.path) as handle:
            data = json.load(handle)
        self.assertEqual(data['ok'], 2)
        self.assertEqual(data['failed'], [1])
        self.assertEqual([j['index'] for j in data['jobs']], [0, 1, 2])
        self.assertEqual([j['rc'] for j in data['jobs']], [0, 1, 0])
        self.assertEqual(data['jobs'][1]['outputs'], [])

    def test_no_flag_writes_no_file(self):
        from utils import runlocal
        args = _runlocal_args(jobdef=self.tar, workdir=self.workdir)
        with patch.object(runlocal.subprocess, 'Popen',
                          _fake_popen(self._emit({0: 0}))):
            with contextlib.redirect_stdout(io.StringIO()):
                runlocal.drive(args)
        self.assertFalse(os.path.exists(self.path))

    def test_a_bad_directory_is_refused_before_any_job_runs(self):
        """Losing an hour-long run's summary to a directory typo is not
        recoverable — the path is checked up front."""
        from utils.runlocal import main
        with self.assertRaises(SystemExit):
            main(['--jobdef', self.tar, '--json',
                  os.path.join(self.workdir, 'nope', 'summary.json')])


class TestRunLocalArgValidation(unittest.TestCase):
    """Windows that would run nothing, or a nonsense pool size, are
    rejected up front rather than producing an empty summary."""

    def test_rejects_empty_window(self):
        from utils.runlocal import main
        with self.assertRaises(SystemExit):
            main(['--jobdef', 'c.tar', '--num', '0'])

    def test_rejects_negative_first(self):
        from utils.runlocal import main
        with self.assertRaises(SystemExit):
            main(['--jobdef', 'c.tar', '--first', '-1'])

    def test_rejects_zero_parallel(self):
        from utils.runlocal import main
        with self.assertRaises(SystemExit):
            main(['--jobdef', 'c.tar', '-j', '0'])

    def test_rejects_indices_together_with_a_window(self):
        """Clipping a list to a window, or ignoring the window, would
        each surprise someone — so neither is offered."""
        from utils.runlocal import main
        with self.assertRaises(SystemExit):
            main(['--jobdef', 'c.tar', '--indices', '0,1', '--first', '5'])
        with self.assertRaises(SystemExit):
            main(['--jobdef', 'c.tar', '--indices', '0,1', '--num', '2'])

    def test_rejects_a_malformed_index_list(self):
        from utils.runlocal import main
        for spec in ('', '1,,2', '3-1', '1-', 'a', '-2', '1 2'):
            with self.assertRaises(SystemExit, msg=spec):
                main(['--jobdef', 'c.tar', '--indices', spec])


class TestRunLocalIndexSpec(unittest.TestCase):
    """`--indices` exists for reruns of the exact jobs a grid pass
    lost, which are rarely contiguous."""

    def test_parses_singles_ranges_and_normalizes(self):
        from utils.runlocal import parse_index_spec
        self.assertEqual(parse_index_spec('0,3,7-9'), [0, 3, 7, 8, 9])
        # Inclusive at both ends, sorted, deduplicated, space-tolerant.
        self.assertEqual(parse_index_spec('5-5'), [5])
        self.assertEqual(parse_index_spec('4, 2 , 4'), [2, 4])

    def test_formats_runs_back_into_ranges(self):
        from utils.runlocal import format_indices
        self.assertEqual(format_indices([0, 3, 7, 8, 9]), '0,3,7-9')
        self.assertEqual(format_indices([5]), '5')
        self.assertEqual(format_indices(list(range(200))), '0-199')

    def test_window_flags_still_produce_a_contiguous_list(self):
        from utils.runlocal import resolve_indices
        self.assertEqual(
            resolve_indices(SimpleNamespace(indices=None, first=5, num=3)),
            [5, 6, 7])
        # Both unset is the one-job default, not zero jobs.
        self.assertEqual(
            resolve_indices(SimpleNamespace(indices=None, first=None,
                                            num=None)),
            [0])


class TestResolveSetup(unittest.TestCase):
    """resolve_setup is the single relative->absolute step for a
    code-mode cnf. Absolute setup = a /cvmfs Musing; relative setup =
    the Offline build travels as a separate tarball."""

    def test_absolute_setup_passes_through(self):
        from utils.runmu2e import resolve_setup
        path = '/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/Run1Baq/setup.sh'
        self.assertEqual(resolve_setup(path), path)

    def test_absolute_setup_ignores_code_root(self):
        from utils.runmu2e import resolve_setup
        path = '/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/Run1Baq/setup.sh'
        self.assertEqual(resolve_setup(path, code_root='/srv/rcds'), path)

    def test_relative_setup_joins_code_root(self):
        from utils.runmu2e import resolve_setup
        self.assertEqual(resolve_setup('Code/setup.sh', code_root='/srv/rcds'),
                         '/srv/rcds/Code/setup.sh')

    def test_relative_setup_without_code_root_raises(self):
        from utils.runmu2e import resolve_setup
        with self.assertRaises(ValueError) as ctx:
            resolve_setup('Code/setup.sh')
        # The message must name both recovery paths, because the two
        # callers fail for different reasons.
        self.assertIn('INPUT_TAR_DIR_LOCAL', str(ctx.exception))
        self.assertIn('--code', str(ctx.exception))

    def test_relative_setup_with_empty_code_root_raises(self):
        # os.environ.get returns '' for an exported-but-empty variable;
        # that must fail like a missing one, not join to 'Code/setup.sh'.
        from utils.runmu2e import resolve_setup
        with self.assertRaises(ValueError):
            resolve_setup('Code/setup.sh', code_root='')

    def test_empty_setup_raises(self):
        from utils.runmu2e import resolve_setup
        with self.assertRaises(ValueError):
            resolve_setup('')


def _make_code_tarball(path, with_setup=True, bzip2=True):
    """Build a tiny stand-in for `muse tarball` output. Few KB, no
    /cvmfs, no network — safe for the in-process suite."""
    import tarfile as _tf
    mode = 'w:bz2' if bzip2 else 'w'
    with _tf.open(path, mode) as tar:
        payload = io.BytesIO(b'# fake muse setup\n')
        if with_setup:
            info = _tf.TarInfo('Code/setup.sh')
            info.size = len(payload.getvalue())
            tar.addfile(info, io.BytesIO(payload.getvalue()))
        other = _tf.TarInfo('Code/lib/libFake.so')
        other.size = 3
        tar.addfile(other, io.BytesIO(b'abc'))
    return path


class TestCodeTarballValidation(unittest.TestCase):
    """jobdef refuses a code tarball that cannot work on a worker,
    mirroring the checks Perl mu2ejobdef does at lines 808-828."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def test_good_tarball_accepted(self):
        from utils.jobdef import validate_code_tarball
        path = _make_code_tarball(os.path.join(self.dir, 'Code.tar.bz2'))
        validate_code_tarball(path)  # must not raise

    def test_missing_file_rejected(self):
        from utils.jobdef import validate_code_tarball
        with self.assertRaises(ValueError) as ctx:
            validate_code_tarball(os.path.join(self.dir, 'nope.tar.bz2'))
        self.assertIn('readable', str(ctx.exception))

    def test_uncompressed_tar_rejected(self):
        from utils.jobdef import validate_code_tarball
        path = _make_code_tarball(os.path.join(self.dir, 'plain.tar'),
                                  bzip2=False)
        with self.assertRaises(ValueError) as ctx:
            validate_code_tarball(path)
        self.assertIn('bzip2', str(ctx.exception))

    def test_tarball_without_code_setup_rejected(self):
        from utils.jobdef import validate_code_tarball
        path = _make_code_tarball(os.path.join(self.dir, 'nosetup.tar.bz2'),
                                  with_setup=False)
        with self.assertRaises(ValueError) as ctx:
            validate_code_tarball(path)
        self.assertIn('Code/setup.sh', str(ctx.exception))
        self.assertIn('muse tarball', str(ctx.exception))

    def test_name_does_not_decide(self):
        # Content decides, not the filename: a correctly built tarball
        # under any name is accepted.
        from utils.jobdef import validate_code_tarball
        path = _make_code_tarball(os.path.join(self.dir, 'my-build.tbz'))
        validate_code_tarball(path)


class TestSha256File(unittest.TestCase):

    def test_digest_and_size(self):
        from utils.job_common import sha256_file
        with tempfile.NamedTemporaryFile(delete=False) as fh:
            fh.write(b'hello')
            name = fh.name
        self.addCleanup(os.unlink, name)
        digest, size = sha256_file(name)
        self.assertEqual(digest, hashlib.sha256(b'hello').hexdigest())
        self.assertEqual(size, 5)


class TestCodeModeJobpars(unittest.TestCase):
    """A code-mode cnf says setup='Code/setup.sh' and carries code_ref;
    upstream's `code` key stays empty because nothing is embedded."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.tarball = _make_code_tarball(
            os.path.join(self.dir, 'Code.tar.bz2'))

    def test_code_mode_shape(self):
        from utils.jobdef import _build_jobpars_json
        config = {'code': self.tarball, 'desc': 'Demo',
                  'dsconf': 'Run1Baq', 'owner': 'mu2e'}
        pars = _build_jobpars_json(config, {'outfiles': {}})
        self.assertEqual(pars['setup'], 'Code/setup.sh')
        self.assertEqual(pars['code'], '')
        self.assertEqual(pars['code_ref']['size'],
                         os.path.getsize(self.tarball))
        self.assertEqual(pars['code_ref']['source_path'],
                         os.path.abspath(self.tarball))
        self.assertEqual(len(pars['code_ref']['sha256']), 64)

    def test_setup_mode_shape_unchanged(self):
        from utils.jobdef import _build_jobpars_json
        setup = '/cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/Run1Baq/setup.sh'
        config = {'simjob_setup': setup, 'desc': 'Demo',
                  'dsconf': 'Run1Baq', 'owner': 'mu2e'}
        pars = _build_jobpars_json(config, {'outfiles': {}})
        self.assertEqual(pars['setup'], setup)
        self.assertEqual(pars['code'], '')
        self.assertNotIn('code_ref', pars)

    def test_both_setup_and_code_rejected(self):
        from utils.jobdef import _build_jobpars_json
        config = {'simjob_setup': '/cvmfs/x/setup.sh', 'code': self.tarball,
                  'desc': 'Demo', 'dsconf': 'Run1Baq', 'owner': 'mu2e'}
        with self.assertRaises(ValueError):
            _build_jobpars_json(config, {'outfiles': {}})

    def test_neither_setup_nor_code_rejected(self):
        from utils.jobdef import _build_jobpars_json
        config = {'desc': 'Demo', 'dsconf': 'Run1Baq', 'owner': 'mu2e'}
        with self.assertRaises(ValueError):
            _build_jobpars_json(config, {'outfiles': {}})


class TestEntryCodeKey(unittest.TestCase):
    """The entry carries the code tarball's absolute path; the cnf
    carries only the relative setup and the digest."""

    def test_code_of_returns_path(self):
        from utils.jobdesc import code_of
        self.assertEqual(code_of({'code': '/exp/build/Code.tar.bz2'}),
                         '/exp/build/Code.tar.bz2')

    def test_code_of_none_for_musing_entry(self):
        from utils.jobdesc import code_of
        self.assertIsNone(code_of({'tarball': 'cnf.mu2e.X.Run1Baq.0.tar'}))

    def test_absolute_path_accepted(self):
        from utils.jobdesc import validate_entry_value
        validate_entry_value('code', '/exp/build/Code.tar.bz2')

    def test_any_suffix_accepted(self):
        # Content decides whether a tarball is usable (jobdef checks the
        # bzip2 magic); the name must not.
        from utils.jobdesc import validate_entry_value
        validate_entry_value('code', '/exp/build/my-build.tbz')

    def test_relative_path_rejected(self):
        from utils.jobdesc import validate_entry_value
        with self.assertRaises(ValueError) as ctx:
            validate_entry_value('code', 'Code.tar.bz2')
        self.assertIn('absolute', str(ctx.exception))

    def test_non_string_rejected(self):
        from utils.jobdesc import validate_entry_value
        with self.assertRaises(ValueError):
            validate_entry_value('code', 17)

    def test_code_is_editable_on_a_live_campaign(self):
        # A rebuilt tarball must be reachable without a new cnf.
        from utils.submission_ledger import EDITABLE_ENTRY_KEYS
        self.assertIn('code', EDITABLE_ENTRY_KEYS)


class TestJson2JobdefCodeConfig(unittest.TestCase):

    def test_exactly_one_of_setup_and_code(self):
        from utils.json2jobdef import validate_required_fields
        base = {'fcl': 'x.fcl', 'dsconf': 'Run1Baq', 'outloc': {'dts.*': 'disk'}}
        with self.assertRaises(SystemExit):
            validate_required_fields(dict(base))                    # neither
        with self.assertRaises(SystemExit):
            validate_required_fields(dict(base, simjob_setup='/cvmfs/s.sh',
                                          code='/exp/Code.tar.bz2'))  # both
        validate_required_fields(dict(base, simjob_setup='/cvmfs/s.sh'))
        validate_required_fields(dict(base, code='/exp/Code.tar.bz2'))

    def test_code_reaches_the_entry(self):
        from utils.json2jobdef import build_jobdesc
        config = {'code': '/exp/build/Code.tar.bz2', 'inloc': 'tape',
                  'desc': 'Demo', 'dsconf': 'Run1Baq', 'owner': 'mu2e',
                  'fcl': 'x.fcl', 'outloc': {}, 'njobs': 5}
        entry = build_jobdesc(config)
        self.assertEqual(entry['code'], '/exp/build/Code.tar.bz2')

    def test_musing_entry_has_no_code_key(self):
        from utils.json2jobdef import build_jobdesc
        config = {'simjob_setup': '/cvmfs/s.sh', 'inloc': 'tape',
                  'desc': 'Demo', 'dsconf': 'Run1Baq', 'owner': 'mu2e',
                  'fcl': 'x.fcl', 'outloc': {}, 'njobs': 5}
        self.assertNotIn('code', build_jobdesc(config))


class TestBuildJobdefCodeModeReturnValue(unittest.TestCase):
    """build_jobdef's diagnostic perl_commands dict must not KeyError on
    a code-mode config (no simjob_setup). That dict is metadata for
    test/parity_test.py's Perl comparison, which only runs in --setup
    mode, so `None` is the truthful value for simjob_setup in code
    mode — not a placeholder and not the code path's own value."""

    def test_code_mode_reaches_return_without_keyerror(self):
        from unittest.mock import patch
        from utils import json2jobdef
        with patch.object(json2jobdef, 'write_fcl_template'), \
             patch.object(json2jobdef, 'create_jobdef'), \
             patch.object(json2jobdef, 'get_parfile_name',
                          return_value='cnf.x.0.tar'), \
             patch.object(json2jobdef, 'validate_output_filenames'):
            cfg = {'desc': 'reco', 'dsconf': 'D', 'owner': 'mu2e',
                   'code': '/exp/build/Code.tar.bz2', 'inloc': 'tape',
                   'fcl': 'f.fcl', 'outloc': {'*.art': 'tape'}}
            result = json2jobdef.build_jobdef(cfg, job_args=[])
        entry = result['perl_commands'][0]
        self.assertIsNone(entry['simjob_setup'])
        self.assertIn('--code', entry['command'])


class TestCommonIncludePrecedence(unittest.TestCase):
    """A campaign default must never beat an entry that pins the key.

    Reversed, `data/Run1B/common.json` would move the 42 frozen entries
    (geom_run1_b_v01/v03/v06) onto v40 — a job that runs to completion
    and produces a wrong world, which is the failure this file exists to
    make impossible. write_fcl_template owns the ordering so no caller
    can get it wrong; dict order is not relied on."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.cwd = os.getcwd()
        os.chdir(self.dir)
        self.addCleanup(os.chdir, self.cwd)

    def _write(self, overrides, **kwargs):
        from utils.prod_utils import write_fcl_template
        write_fcl_template('Production/JobConfig/mixing/Mix.fcl',
                           overrides, **kwargs)
        return Path('template.fcl').read_text().splitlines()

    def test_common_include_precedes_the_entrys_own_keys(self):
        from utils.prod_utils import COMMON_INCLUDE_KEY
        lines = self._write({
            COMMON_INCLUDE_KEY: ['Production/JobConfig/common/Run1B.fcl'],
            'services.GeometryService.inputFile':
                'Offline/Mu2eG4/geom/geom_run1_b_v06.txt',
        })
        common = lines.index('#include "Production/JobConfig/common/Run1B.fcl"')
        pinned = [i for i, l in enumerate(lines) if 'geom_run1_b_v06' in l]
        self.assertEqual(lines[0],
                         '#include "Production/JobConfig/mixing/Mix.fcl"')
        self.assertEqual(common, 1)
        self.assertTrue(pinned and min(pinned) > common,
                        'frozen v06 must be stated after the campaign default')
        # The reserved key is a directive, never emitted as a FHiCL key.
        self.assertNotIn(COMMON_INCLUDE_KEY, '\n'.join(lines))

    def test_common_include_also_precedes_pre_lines(self):
        """pre_lines carry the mixing pbeam include, which entries may
        override. The campaign default sits below even that."""
        from utils.prod_utils import COMMON_INCLUDE_KEY
        lines = self._write(
            {COMMON_INCLUDE_KEY: ['Production/JobConfig/common/Run1B.fcl']},
            pre_lines=['#include "Production/JobConfig/mixing/OneBB.fcl"'])
        self.assertLess(
            lines.index('#include "Production/JobConfig/common/Run1B.fcl"'),
            lines.index('#include "Production/JobConfig/mixing/OneBB.fcl"'))

    def test_absent_key_writes_the_same_template_as_before(self):
        lines = self._write({'services.GeometryService.inputFile': 'g.txt'})
        self.assertEqual(lines, ['#include "Production/JobConfig/mixing/Mix.fcl"',
                                 'services.GeometryService.inputFile: "g.txt"'])


class TestCommonOverlayScope(unittest.TestCase):
    """common.json states which stage files and which dsconf it covers.
    Both filters are load-bearing: data/Run1B holds 15 MDC2025* entries
    and three merge/ntuple stage files whose FCLs configure no
    GeometryService."""

    COMMON = {
        'applies_to': ['mix.json'],
        'dsconf_prefix': 'Run1B',
        'fcl_overrides': {
            '#include': ['Production/JobConfig/common/Run1B.fcl']},
    }

    def setUp(self):
        from utils import json2jobdef
        self.json2jobdef = json2jobdef
        self.dir = Path(tempfile.mkdtemp())
        (self.dir / 'common.json').write_text(json.dumps(self.COMMON))

    def _load(self, name, entries):
        path = self.dir / name
        path.write_text(json.dumps(entries))
        return self.json2jobdef.load_json(path)

    def _entry(self, dsconf='Run1Ban', **overrides):
        return {'desc': 'Thing', 'dsconf': dsconf,
                'fcl': 'Production/JobConfig/mixing/Mix.fcl',
                'fcl_overrides': overrides}

    def _includes(self, config):
        from utils.prod_utils import COMMON_INCLUDE_KEY
        return config['fcl_overrides'].get(COMMON_INCLUDE_KEY, [])

    def test_listed_file_and_matching_dsconf_takes_the_default(self):
        configs = self._load('mix.json', [self._entry()])
        self.assertEqual(self._includes(configs[0]),
                         ['Production/JobConfig/common/Run1B.fcl'])

    def test_unlisted_stage_file_is_untouched(self):
        configs = self._load('merge_filter.json', [self._entry()])
        self.assertEqual(self._includes(configs[0]), [])

    def test_foreign_dsconf_in_a_listed_file_is_untouched(self):
        """data/Run1B/resampler_beam.json really does hold MDC2025ac and
        MDC2025af entries; a Run1B geometry default there is wrong."""
        configs = self._load('mix.json', [self._entry(dsconf='MDC2025ac')])
        self.assertEqual(self._includes(configs[0]), [])

    def test_entry_without_fcl_overrides_gets_the_key(self):
        configs = self._load('mix.json', [{'desc': 'T', 'dsconf': 'Run1Ban',
                                           'fcl': 'x.fcl'}])
        self.assertEqual(self._includes(configs[0]),
                         ['Production/JobConfig/common/Run1B.fcl'])

    def test_pinned_key_survives_the_overlay(self):
        pin = 'Offline/Mu2eG4/geom/geom_run1_b_v06.txt'
        configs = self._load('mix.json', [self._entry(
            **{'services.GeometryService.inputFile': pin})])
        self.assertEqual(
            configs[0]['fcl_overrides']['services.GeometryService.inputFile'],
            pin)

    def test_overlay_is_idempotent(self):
        """Expansion can hand several expanded entries one dict; applying
        the default twice must not stack duplicate includes."""
        path = self.dir / 'mix.json'
        path.write_text(json.dumps([self._entry()]))
        configs = self.json2jobdef.load_json(path)
        self.json2jobdef.apply_common_overlay(configs, path)
        self.assertEqual(self._includes(configs[0]),
                         ['Production/JobConfig/common/Run1B.fcl'])

    def test_directory_without_common_json_is_untouched(self):
        (self.dir / 'common.json').unlink()
        configs = self._load('mix.json', [self._entry()])
        self.assertEqual(self._includes(configs[0]), [])


class TestCommonPlainKeyDefaults(unittest.TestCase):
    """common.json may state its defaults as plain keys instead of an
    include — what a campaign uses while its FCL is still an unmerged
    Production PR, since including a FCL that does not exist aborts every
    build in fhicl-get and no entry-level override can suppress it.

    Plain keys carry the same direction as the include: applied only where
    the entry is silent, so a pinned entry still wins."""

    COMMON = {
        'applies_to': ['mix.json'],
        'dsconf_prefix': 'Run1B',
        'fcl_overrides': {
            'services.GeometryService.inputFile':
                'Offline/Mu2eG4/geom/geom_run1_b_v40.txt',
            'services.GeometryService.bFieldFile':
                'Offline/Mu2eG4/geom/bfgeom_DSOff.txt'},
    }
    GEO = 'services.GeometryService.inputFile'
    BF = 'services.GeometryService.bFieldFile'

    def setUp(self):
        from utils import json2jobdef
        self.json2jobdef = json2jobdef
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        (self.dir / 'common.json').write_text(json.dumps(self.COMMON))

    def _load(self, entries):
        path = self.dir / 'mix.json'
        path.write_text(json.dumps(entries))
        return self.json2jobdef.load_json(path)

    def _entry(self, dsconf='Run1Ban', **overrides):
        return {'desc': 'Thing', 'dsconf': dsconf,
                'fcl': 'Production/JobConfig/mixing/Mix.fcl',
                'fcl_overrides': overrides}

    def test_silent_entry_takes_the_default(self):
        ov = self._load([self._entry()])[0]['fcl_overrides']
        self.assertEqual(ov[self.GEO],
                         'Offline/Mu2eG4/geom/geom_run1_b_v40.txt')
        self.assertEqual(ov[self.BF], 'Offline/Mu2eG4/geom/bfgeom_DSOff.txt')

    def test_pinned_entry_keeps_its_own_value(self):
        """The whole safety property: a frozen entry on v06 must not be
        moved onto v40 by the campaign default."""
        pinned = {self.GEO: 'Offline/Mu2eG4/geom/geom_run1_b_v06.txt'}
        ov = self._load([self._entry(**pinned)])[0]['fcl_overrides']
        self.assertEqual(ov[self.GEO],
                         'Offline/Mu2eG4/geom/geom_run1_b_v06.txt')
        # The key it left silent still takes the default.
        self.assertEqual(ov[self.BF], 'Offline/Mu2eG4/geom/bfgeom_DSOff.txt')

    def test_out_of_scope_dsconf_is_untouched(self):
        ov = self._load([self._entry(dsconf='MDC2025au')])[0]['fcl_overrides']
        self.assertNotIn(self.GEO, ov)
        self.assertNotIn(self.BF, ov)

    def test_no_reserved_include_key_is_emitted(self):
        """With no '#include' in common.json the reserved key must not
        appear at all — an empty one would write a stray blank include."""
        from utils.prod_utils import COMMON_INCLUDE_KEY
        ov = self._load([self._entry()])[0]['fcl_overrides']
        self.assertNotIn(COMMON_INCLUDE_KEY, ov)


class TestRun1BCommonJson(unittest.TestCase):
    """Pins the shipped data/Run1B/common.json against the two mistakes
    that would make it dangerous."""

    ROOT = Path(__file__).resolve().parent.parent

    def setUp(self):
        self.common = json.loads(
            (self.ROOT / 'data/Run1B/common.json').read_text())

    def test_merge_and_ntuple_stages_are_excluded(self):
        """artcat.fcl and the EventNtuple FCLs configure no
        GeometryService; a detector default there is meaningless at best."""
        for name in ('merge_filter.json', 'merge_beam_pileup.json',
                     'evntuple.json'):
            self.assertNotIn(name, self.common['applies_to'])

    def test_every_listed_file_exists(self):
        for name in self.common['applies_to']:
            self.assertTrue((self.ROOT / 'data/Run1B' / name).exists(), name)

    def test_no_covered_entry_leaves_the_detector_keys_implicit(self):
        """The default must be redundant everywhere it lands, so adding it
        changes no job. An entry that states neither key inherits one from
        its base FCL's epilog (pileup/epilog.fcl sets bfgeom_no_tsu_ps_v01,
        beam/POT.fcl sets bfgeom_no_ds_v01) and WOULD change — six entries
        did, until they were pinned. A new such entry must be pinned too,
        or removed from applies_to."""
        prefix = self.common['dsconf_prefix']
        implicit = []
        for name in self.common['applies_to']:
            for entry in json.loads(
                    (self.ROOT / 'data/Run1B' / name).read_text()):
                dsconf = entry.get('dsconf')
                dsconf = dsconf[0] if isinstance(dsconf, list) else dsconf
                if not str(dsconf or '').startswith(prefix):
                    continue
                ov = entry.get('fcl_overrides') or {}
                ov = ov[0] if isinstance(ov, list) and ov else ov
                stated = set(ov) | ({'#include'} if '#include' in ov else set())
                if 'services.GeometryService.bFieldFile' in stated:
                    continue
                # An entry may instead pull the field in via its own include
                # (stage1's Run1Bai POT_Run1_b uses beam/epilog_1b.fcl).
                if '#include' in ov:
                    continue
                implicit.append(f"{name}:{entry.get('desc')}/{dsconf}")
        self.assertEqual(implicit, [], 'entries inheriting bFieldFile')


class TestSamplingInputTableIsWritten(unittest.TestCase):
    """The samplinginput table must be built for the source type that
    REQUIRES it.

    The writer used to live inside the `PBISequence` branch, which the
    validator forbids --samplinginput on; SamplingInput therefore passed
    validation (which requires the option) and produced a cnf with no
    samplinginput section at all — a job that resamples nothing, silently.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.flist = os.path.join(self.tmp, 'files.txt')
        with open(self.flist, 'w') as fh:
            fh.write('dts.mu2e.A.v1.001.art\ndts.mu2e.A.v1.002.art\n')

    def _parse(self, source_type, job_args):
        import utils.jobdef as jd
        with patch.object(jd, '_get_source_type', return_value=source_type), \
             patch.object(jd, '_get_output_modules', return_value=[]), \
             patch.object(jd, '_seed_needed', return_value=False), \
             patch.object(jd, '_get_fcl_value',
                          side_effect=subprocess.CalledProcessError(1, 'fhicl-get')):
            return jd._parse_job_args(job_args, 'template.fcl', {})

    def test_samplinginput_source_gets_the_table(self):
        tbs = self._parse('SamplingInput', [
            '--samplinginput', f'2:mydataset:{self.flist}',
            '--run-number', '1201',
        ])
        self.assertIn('samplinginput', tbs)
        self.assertEqual(
            tbs['samplinginput'],
            {'source.dataSets.mydataset.fileNames':
                [2, ['dts.mu2e.A.v1.001.art', 'dts.mu2e.A.v1.002.art']]})

    def test_all_requested_datasets_are_written(self):
        second = os.path.join(self.tmp, 'files2.txt')
        with open(second, 'w') as fh:
            fh.write('dts.mu2e.B.v1.001.art\n')
        tbs = self._parse('SamplingInput', [
            '--samplinginput', f'2:dsA:{self.flist}',
            '--samplinginput', f'all:dsB:{second}',
            '--run-number', '1201',
        ])
        self.assertEqual(sorted(tbs['samplinginput']), [
            'source.dataSets.dsA.fileNames',
            'source.dataSets.dsB.fileNames'])
        self.assertEqual(tbs['samplinginput']['source.dataSets.dsB.fileNames'][0], 1)

    def test_no_sampling_requested_writes_no_table(self):
        tbs = self._parse('EmptyEvent', [
            '--run-number', '1201', '--events-per-job', '10'])
        self.assertNotIn('samplinginput', tbs)

    def test_pbisequence_still_rejects_samplinginput(self):
        # The validator's veto is the reason the writer could not live in
        # that branch; it must stay a veto.
        with self.assertRaises(ValueError) as cm:
            self._parse('PBISequence', [
                '--samplinginput', f'2:dsA:{self.flist}',
                '--run-number', '1201'])
        self.assertIn('not compatible', str(cm.exception))


class TestOutputDatasetsDerivedFromTbs(unittest.TestCase):
    """`output_datasets()` must answer from tbs.outfiles.

    It used to read a TOP-LEVEL `output_datasets` key of jobpars.json
    that nothing has ever written (jobdef emits only code/setup/code_ref/
    tbs/jobname), so it always returned [] on every real cnf. That made
    the --output-files gate reject every dataset and left its whole
    implementation unreachable.
    """

    def setUp(self):
        from utils.jobquery import Mu2eJobPars
        self.Mu2eJobPars = Mu2eJobPars

    def _tar(self, **kw):
        return _make_tarball(_root_input_jobpars(**kw))

    def test_output_datasets_from_outfiles(self):
        tar = self._tar(files=['dts.mu2e.In.CampA.001430_00000000.art'])
        self.addCleanup(os.unlink, tar)
        jp = self.Mu2eJobPars(tar)
        self.assertEqual(jp.output_datasets(), ['sim.mu2e.TestDesc.TestConf.art'])

    def test_g4bl_output_dataset_not_coerced_to_art(self):
        """A g4bl cnf's only output is a .root ntuple. output_datasets()
        used to force every parsed name through .with_extension('art'),
        so a g4bl cnf reported a phantom '...art' dataset that SAM never
        holds a file under — build_file_maps then never matches the real
        .root filenames, and verify_row raises 'no expected output
        files' on every g4bl row forever. The dataset must come back
        with its real extension."""
        from utils.json2jobdef import _build_g4bl_tarball, get_parfile_name
        tmp = tempfile.mkdtemp(prefix='g4bl_outds_')
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        g4bl_dir = os.path.join(tmp, 'scripts')
        os.makedirs(g4bl_dir)
        Path(g4bl_dir, 'deck.in').write_text('# deck\n')
        cwd = os.getcwd()
        os.chdir(tmp)
        self.addCleanup(os.chdir, cwd)
        config = {
            'runner': 'g4bl', 'desc': 'G4blSmoke', 'dsconf': 'TestConf',
            'owner': 'testuser', 'g4bl_dir': g4bl_dir,
            'main_input': 'deck.in', 'events_per_job': 100, 'njobs': 2,
        }
        _build_g4bl_tarball(config)
        name = get_parfile_name(config)
        jp = self.Mu2eJobPars(name)
        self.assertEqual(jp.output_datasets(),
                         ['nts.testuser.G4blSmoke.TestConf.root'])

    def test_multiple_output_streams_are_all_reported(self):
        pars = _root_input_jobpars(files=['dts.mu2e.In.CampA.001430_00000000.art'])
        pars['tbs']['outfiles'] = {
            'outputs.A.fileName': 'dig.mu2e.TestDesc.TestConf.sequencer.art',
            'outputs.B.fileName': 'mcs.mu2e.TestDesc.TestConf.sequencer.art',
        }
        tar = _make_tarball(pars)
        self.addCleanup(os.unlink, tar)
        self.assertEqual(sorted(self.Mu2eJobPars(tar).output_datasets()),
                         ['dig.mu2e.TestDesc.TestConf.art',
                          'mcs.mu2e.TestDesc.TestConf.art'])

    def test_output_files_gate_now_passes(self):
        # The end-to-end symptom: --output-files used to exit 1 for every
        # dataset because the gate consulted the always-empty list.
        tar = self._tar(files=['dts.mu2e.In.CampA.001430_00000000.art',
                               'dts.mu2e.In.CampA.001430_00000001.art'])
        self.addCleanup(os.unlink, tar)
        jp = self.Mu2eJobPars(tar)
        ds = jp.output_datasets()[0]
        files = jp.output_files(ds, list_size=2)
        self.assertEqual(len(files), 2)
        for f in files:
            self.assertTrue(f.startswith('sim.mu2e.TestDesc.TestConf.'), f)

    def test_no_outfiles_returns_empty(self):
        pars = _root_input_jobpars(files=['dts.mu2e.In.CampA.001430_00000000.art'])
        del pars['tbs']['outfiles']
        tar = _make_tarball(pars)
        self.addCleanup(os.unlink, tar)
        self.assertEqual(self.Mu2eJobPars(tar).output_datasets(), [])


class TestCopyToStashExitCode(unittest.TestCase):
    """A stash copy that lost files must not report success.

    `_copy_dataset` counted `n_fail`, printed it, then returned `n_ok`
    alone — and bin/copy_to_stash did `return 0 if n_copied >= 0 else 1`,
    a condition that cannot be false. A dataset copy that failed on every
    file exited 0, so any wrapping script read it as done.
    """

    MOCK_FILES = ["dts.mu2e.CeEndpoint.Run1Bab.001440_00000000.art",
                  "dts.mu2e.CeEndpoint.Run1Bab.001440_00000001.art"]
    MOCK_LOC = [{'location_type': 'disk',
                 'full_path': '/pnfs/mu2e/persistent/datasets/phy-sim/dts/mu2e/'
                              'CeEndpoint/Run1Bab/art'}]

    def _run(self, copyfile_side_effect=None):
        from utils import stash_utils
        cm = patch('utils.stash_utils.shutil.copyfile')
        with patch('utils.stash_utils.files_in_dataset',
                   return_value=self.MOCK_FILES), \
             patch('utils.stash_utils.locate_files_strict',
                   side_effect=lambda fns: {f: self.MOCK_LOC for f in fns}), \
             patch('os.makedirs'), \
             patch('utils.stash_utils.shutil.copyfile',
                   side_effect=copyfile_side_effect):
            return stash_utils.copy_dataset_to_stash(
                "dts.mu2e.CeEndpoint.Run1Bab.art",
                source_loc='disk', dry_run=False, verbose=False)

    def test_failure_count_is_returned(self):
        res = self._run(OSError(28, "No space left on device"))
        self.assertEqual(res.copied, 0)
        self.assertEqual(res.failed, 2)

    def test_clean_copy_reports_no_failures(self):
        res = self._run(None)
        self.assertEqual(res.copied, 2)
        self.assertEqual(res.failed, 0)

    def test_result_is_falsy_only_when_nothing_copied(self):
        # Keeps the old `if not n:` idiom meaningful for any caller.
        self.assertFalse(self._run(OSError(28, "nope")).copied)
        self.assertTrue(self._run(None).copied)


class TestCopyToStashCliExit(unittest.TestCase):
    """bin/copy_to_stash's exit code must follow the failure count."""

    @staticmethod
    def _load_cli():
        import importlib.util
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(root, 'bin', 'copy_to_stash')
        spec = importlib.util.spec_from_loader(
            'copy_to_stash_cli',
            importlib.machinery.SourceFileLoader('copy_to_stash_cli', path))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _main(self, result):
        cli = self._load_cli()
        argv = ['--dataset', 'dts.mu2e.CeEndpoint.Run1Bab.art', '--quiet']
        with patch.object(sys, 'argv', ['copy_to_stash'] + argv), \
             patch.object(cli, 'copy_dataset_to_stash', return_value=result), \
             patch.object(cli, 'stash_write_root', return_value='/stash/w'), \
             patch.object(cli, 'stash_read_root', return_value='/stash/r'):
            return cli.main()

    def test_exit_nonzero_when_files_failed(self):
        from utils.stash_utils import CopyResult
        self.assertNotEqual(self._main(CopyResult(copied=3, failed=2)), 0)

    def test_exit_zero_on_clean_copy(self):
        from utils.stash_utils import CopyResult
        self.assertEqual(self._main(CopyResult(copied=5, failed=0)), 0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


class TestProdtoolsReleaseIsTheOnlyWorkerPath(unittest.TestCase):
    """Every grid job runs prodtools from a cvmfs release recorded on the
    entry at enqueue, or — explicit opt-in, TestDevProdtoolsTarball — a
    digest-pinned checkout tarball. Nothing else, no fallback: each
    boundary fails loudly."""

    def test_current_symlink_resolves_to_a_version_dir(self):
        import os, tempfile
        from utils.jobdesc import resolve_prodtools_dir
        base = tempfile.mkdtemp()
        os.symlink(FAKE_PRODTOOLS_DIR, os.path.join(base, 'current'))
        self.assertEqual(resolve_prodtools_dir(os.path.join(base, 'current')),
                         os.path.realpath(FAKE_PRODTOOLS_DIR))

    def test_validator_rejects_a_dir_that_is_not_a_release(self):
        import tempfile
        from utils.jobdesc import validate_entry_value
        with self.assertRaises(ValueError) as cm:
            validate_entry_value('prodtools_dir', tempfile.mkdtemp())
        self.assertIn('bin/setup.sh', str(cm.exception))
        with self.assertRaises(ValueError):
            validate_entry_value('prodtools_dir', 'relative/path')

    def test_validator_rejects_a_release_older_than_the_bootstrap(self):
        """v3.2.0 and earlier ship a runjob.sh that untars a dropbox
        tarball; pointing a campaign at one would kill every job in
        setup. The layout check alone passes such a release."""
        import os, shutil, tempfile
        from utils.jobdesc import validate_entry_value
        old = tempfile.mkdtemp()
        shutil.copytree(FAKE_PRODTOOLS_DIR, old, dirs_exist_ok=True)
        with open(os.path.join(old, 'bin', 'runjob.sh'), 'w') as fh:
            fh.write('tar xf "$CONDOR_DIR_INPUT/$PRODTOOLS_TAR"\n')
        with self.assertRaises(ValueError) as cm:
            validate_entry_value('prodtools_dir', old)
        self.assertIn('predates', str(cm.exception))

    def test_entry_without_release_names_the_set_entry_fix(self):
        from utils.jobdesc import prodtools_dir_of
        with self.assertRaises(ValueError) as cm:
            prodtools_dir_of({'tarball': 'cnf.mu2e.X.Y.0.tar'})
        self.assertIn('set-entry', str(cm.exception))
        self.assertIn('prodtools_dir', str(cm.exception))

    def test_enqueue_refuses_an_entry_without_release(self):
        from utils import submit
        entry = {'tarball': 'cnf.mu2e.NoRel.C.0.tar', 'njobs': 3, 'inloc': 'tape',
                 'outputs': [{'dataset': '*.art', 'location': 'tape'}]}
        with patch.object(submit.submission_ledger, 'create_campaign') as cc, \
             self.assertRaises(SystemExit) as cm:
            submit.enqueue_entry(entry, ledger_db='/tmp/never.db', slice_size=1)
        self.assertIn('prodtools_dir', str(cm.exception))
        cc.assert_not_called()

    def test_submit_refuses_a_row_without_release(self):
        from utils import submit
        entry = {'tarball': 'cnf.mu2e.NoRel.C.0.tar', 'njobs': 3, 'inloc': 'tape',
                 'outputs': [{'dataset': '*.art', 'location': 'tape'}]}
        options = submit.SubmitOptions(ledger_db='/tmp/never.db', dry_run=True,
                                       origin='/tmp/m.json')
        with patch.object(submit, '_run_submit') as rs, \
             self.assertRaises(SystemExit):
            submit.submit_entry(entry, 0, options)
        rs.assert_not_called()

    def test_set_entry_accepts_prodtools_dir(self):
        """An old open row is given a release with the documented fix."""
        import os
        from utils import submission_ledger as sl
        db = os.path.join(_mkdtemp(), 'submissions.db')
        cid = sl.create_campaign(
            db, tarball='cnf.mu2e.Old.C.0.tar', slice_size=5,
            entry={'tarball': 'cnf.mu2e.Old.C.0.tar', 'njobs': 5, 'inloc': 'tape',
                   'outputs': [{'dataset': '*.art', 'location': 'tape'}]})
        previous, rows = sl.set_campaign_entry_key(db, cid, 'prodtools_dir',
                                                   FAKE_PRODTOOLS_DIR)
        self.assertIsNone(previous)
        camp = [c for c in sl.active_campaigns(db) if c['id'] == cid][0]
        self.assertEqual(camp['entry']['prodtools_dir'], FAKE_PRODTOOLS_DIR)

    def test_runjob_sh_refuses_to_run_without_a_release(self):
        """The worker script has one path. No env var → exit 1 before any
        setup; a dir that is not a release on this worker → exit 1."""
        import os, subprocess
        script = os.path.join(os.path.dirname(__file__), '..', 'bin', 'runjob.sh')
        env = {k: v for k, v in os.environ.items() if k != 'MU2EGRID_PRODTOOLS_DIR'}
        r = subprocess.run(['bash', script], env=env, capture_output=True, text=True)
        self.assertEqual(r.returncode, 1)
        self.assertIn('MU2EGRID_PRODTOOLS_DIR is not set', r.stderr)
        env['MU2EGRID_PRODTOOLS_DIR'] = _mkdtemp()
        r = subprocess.run(['bash', script], env=env, capture_output=True, text=True)
        self.assertEqual(r.returncode, 1)
        self.assertIn('is not a prodtools release on this worker', r.stderr)



class TestDevProdtoolsTarball(unittest.TestCase):
    """Opt-in second worker path, restored from 4314038^: a checkout named
    by `--prodtools-dir` (anything outside the cvmfs release root) is
    tarred ONCE at enqueue, content-addressed, and shipped per job via
    -f dropbox://. The entry records the tar and its digest; every submit
    re-hashes and refuses a mismatch. A cvmfs release entry is untouched:
    no tar key, no MU2EGRID_PRODTOOLS_TAR, byte-identical argv."""

    def _checkout(self):
        """A fake checkout: the three worker files plus a pyc that must
        not ship."""
        import shutil
        d = _mkdtemp()
        shutil.copytree(FAKE_PRODTOOLS_DIR, d, dirs_exist_ok=True)
        pyc = Path(d) / 'utils' / '__pycache__' / 'runmu2e.cpython-39.pyc'
        pyc.parent.mkdir(parents=True)
        pyc.write_bytes(b'\x00')
        return d

    def _bundled_entry(self):
        from utils.submit import bundle_prodtools
        tar, ref = bundle_prodtools(self._checkout(), _mkdtemp())
        return {'tarball': 'cnf.oksuzian.Dev.C.0.tar', 'njobs': 2,
                'inloc': 'none', 'outputs': [{'location': 'disk'}],
                'prodtools_dir': FAKE_PRODTOOLS_DIR,
                'prodtools_tar': tar, 'prodtools_ref': ref}

    def test_bundle_is_content_addressed_and_skips_pycache(self):
        import tarfile
        from utils.submit import bundle_prodtools
        from utils.job_common import sha256_file
        checkout, dest = self._checkout(), _mkdtemp()
        tar, ref = bundle_prodtools(checkout, dest)
        digest, size = sha256_file(tar)
        self.assertEqual(ref, {'sha256': digest, 'size': size,
                               'source_path': checkout})
        self.assertEqual(os.path.basename(tar), f'prodtools-{digest[:12]}.tar')
        self.assertEqual(os.path.dirname(tar), dest)
        with tarfile.open(tar) as t:
            names = t.getnames()
        self.assertIn('prodtools/bin/runjob.sh', names)
        self.assertIn('prodtools/utils/runmu2e.py', names)
        self.assertFalse([n for n in names if '__pycache__' in n or n.endswith('.pyc')])
        # Same bytes in, same file out: a second enqueue reuses it.
        self.assertEqual(bundle_prodtools(checkout, dest)[0], tar)

    def test_release_entry_has_no_tar(self):
        from utils.jobdesc import prodtools_tar_of
        self.assertIsNone(prodtools_tar_of({'prodtools_dir': FAKE_PRODTOOLS_DIR}))

    def test_tar_of_returns_the_verified_path(self):
        from utils.jobdesc import prodtools_tar_of
        entry = self._bundled_entry()
        self.assertEqual(prodtools_tar_of(entry), entry['prodtools_tar'])

    def test_tar_of_refuses_a_digest_mismatch(self):
        from utils.jobdesc import prodtools_tar_of
        entry = self._bundled_entry()
        with open(entry['prodtools_tar'], 'ab') as fh:
            fh.write(b'tampered')
        with self.assertRaises(ValueError) as cm:
            prodtools_tar_of(entry)
        self.assertIn('sha256', str(cm.exception))

    def test_tar_of_refuses_a_tar_without_ref_or_file(self):
        from utils.jobdesc import prodtools_tar_of
        entry = self._bundled_entry()
        with self.assertRaises(ValueError) as cm:
            prodtools_tar_of({**entry, 'prodtools_ref': None})
        self.assertIn('prodtools_ref', str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            prodtools_tar_of({**entry, 'prodtools_tar': '/nonexistent/p.tar'})
        self.assertIn('/nonexistent/p.tar', str(cm.exception))

    def test_argv_ships_the_tar_and_names_it_instead_of_the_dir(self):
        """Worker gets exactly one of the two env vars. The executable is
        still the checkout's own runjob.sh (jobsub copies it into the
        sandbox), and the cnf + ops JSON still ship beside the tar."""
        from utils.jobsub_argv import build_jobsub_argv
        argv = build_jobsub_argv(
            entry={'tarball': 'cnf.oksuzian.Dev.C.0.tar', 'outputs': []},
            jobset=[0], jobdef_path='/tmp/cnf.oksuzian.Dev.C.0.tar',
            ops_json_path='/tmp/ops.json', submitter='me',
            prodtools_dir='/exp/checkout/prodtools',
            prodtools_tar='/exp/data/prodtools-tarballs/prodtools-abc123def456.tar')
        envs = [argv[j + 1] for j, a in enumerate(argv) if a == '-e']
        self.assertIn('MU2EGRID_PRODTOOLS_TAR=prodtools-abc123def456.tar', envs)
        self.assertFalse([e for e in envs if e.startswith('MU2EGRID_PRODTOOLS_DIR')])
        shipped = [argv[i + 1] for i, a in enumerate(argv) if a == '-f']
        self.assertEqual(len(shipped), 3)
        self.assertIn('dropbox:///exp/data/prodtools-tarballs/prodtools-abc123def456.tar', shipped)
        self.assertEqual(argv[-1], 'file:///exp/checkout/prodtools/bin/runjob.sh')

    def test_submit_passes_the_verified_tar(self):
        from utils.submit import submit_entry, SubmitOptions
        entry = self._bundled_entry()
        options = SubmitOptions(ledger_db='/tmp/never.db', dry_run=True,
                                origin='/tmp/m.json')
        captured = {}
        def fake_build(**kw):
            captured.update(kw)
            return ['--fake-argv']
        with patch('utils.submit._jobsub_argv.build_jobsub_argv', side_effect=fake_build):
            submit_entry(entry, 0, options)
        self.assertEqual(captured['prodtools_tar'], entry['prodtools_tar'])

    def test_submit_refuses_a_tampered_tar_before_anything_runs(self):
        from utils import submit
        entry = self._bundled_entry()
        with open(entry['prodtools_tar'], 'ab') as fh:
            fh.write(b'x')
        options = submit.SubmitOptions(ledger_db='/tmp/never.db', dry_run=True,
                                       origin='/tmp/m.json')
        with patch.object(submit, '_run_submit') as rs, \
             patch('utils.submit._jobsub_argv.build_jobsub_argv') as bj, \
             self.assertRaises(SystemExit) as cm:
            submit.submit_entry(entry, 0, options)
        self.assertIn('sha256', str(cm.exception))
        rs.assert_not_called()
        bj.assert_not_called()

    def test_release_entry_submits_with_no_tar(self):
        from utils.submit import submit_entry, SubmitOptions
        entry = {'tarball': 'cnf.mu2e.NoSuchTarballXYZ.TestConf.0.tar',
                 'prodtools_dir': FAKE_PRODTOOLS_DIR, 'njobs': 5, 'inloc': 'tape',
                 'outputs': [{'location': 'tape'}]}
        options = SubmitOptions(ledger_db='/tmp/never.db', dry_run=True,
                                origin='/tmp/m.json')
        captured = {}
        def fake_build(**kw):
            captured.update(kw)
            return ['--fake-argv']
        with patch('utils.submit._jobsub_argv.build_jobsub_argv', side_effect=fake_build):
            submit_entry(entry, 0, options)
        self.assertIsNone(captured['prodtools_tar'])

    def test_enqueue_keys_for_a_cvmfs_release_are_just_the_dir(self):
        from utils.json2jobdef import prodtools_entry_keys
        d = '/cvmfs/mu2e.opensciencegrid.org/bin/prodtools/v3.3.1'
        self.assertEqual(prodtools_entry_keys(d, user='oksuzian'),
                         {'prodtools_dir': d})

    def test_enqueue_keys_for_a_checkout_bundle_it(self):
        from utils import json2jobdef
        checkout = self._checkout()
        with patch.object(json2jobdef, 'bundle_prodtools',
                          return_value=('/data/prodtools-aaaaaaaaaaaa.tar',
                                        {'sha256': 'a' * 64, 'size': 1,
                                         'source_path': checkout})) as bp:
            keys = json2jobdef.prodtools_entry_keys(checkout, user='oksuzian')
        bp.assert_called_once()
        self.assertEqual(bp.call_args[0][0], checkout)
        self.assertEqual(bp.call_args[0][1],
                         '/exp/mu2e/data/users/oksuzian/prodtools/prodtools-tarballs')
        self.assertEqual(keys, {'prodtools_dir': checkout,
                                'prodtools_tar': '/data/prodtools-aaaaaaaaaaaa.tar',
                                'prodtools_ref': {'sha256': 'a' * 64, 'size': 1,
                                                  'source_path': checkout}})

    def test_enqueue_refuses_a_checkout_for_mu2epro(self):
        """Production runs a release only: the provenance rule from
        4314038 still binds the production account."""
        from utils.json2jobdef import prodtools_entry_keys
        with self.assertRaises(SystemExit) as cm:
            prodtools_entry_keys(self._checkout(), user='mu2epro')
        self.assertIn('mu2epro', str(cm.exception))
        self.assertIn('cvmfs', str(cm.exception))

    def _runjob(self, env_extra):
        import subprocess
        script = os.path.join(os.path.dirname(__file__), '..', 'bin', 'runjob.sh')
        env = {k: v for k, v in os.environ.items()
               if k not in ('MU2EGRID_PRODTOOLS_DIR', 'MU2EGRID_PRODTOOLS_TAR')}
        env.update(env_extra)
        return subprocess.run(['bash', script], env=env, capture_output=True, text=True)

    def test_runjob_sh_refuses_both_env_vars(self):
        r = self._runjob({'MU2EGRID_PRODTOOLS_DIR': FAKE_PRODTOOLS_DIR,
                          'MU2EGRID_PRODTOOLS_TAR': 'prodtools-x.tar'})
        self.assertEqual(r.returncode, 1)
        self.assertIn('both', r.stderr)

    def test_runjob_sh_fails_when_the_shipped_tar_is_missing(self):
        r = self._runjob({'MU2EGRID_PRODTOOLS_TAR': 'prodtools-x.tar',
                          'CONDOR_DIR_INPUT': _mkdtemp(),
                          '_CONDOR_SCRATCH_DIR': _mkdtemp()})
        self.assertEqual(r.returncode, 1)
        self.assertIn('ERROR: tar xf', r.stderr)
        self.assertIn('prodtools-x.tar failed', r.stderr)
        self.assertNotIn('is not a prodtools release on this worker', r.stderr)

    def test_runjob_sh_extracts_the_tar_and_runs_from_scratch_dir(self):
        """Extraction proven without reaching cvmfs: a tar lacking
        utils/runmu2e.py trips the release check AT the extracted path."""
        import tarfile
        inp, scratch = _mkdtemp(), _mkdtemp()
        src = _mkdtemp()
        (Path(src) / 'prodtools' / 'bin').mkdir(parents=True)
        (Path(src) / 'prodtools' / 'bin' / 'setup.sh').write_text('# x\n')
        with tarfile.open(os.path.join(inp, 'prodtools-x.tar'), 'w') as t:
            t.add(os.path.join(src, 'prodtools'), arcname='prodtools')
        r = self._runjob({'MU2EGRID_PRODTOOLS_TAR': 'prodtools-x.tar',
                          'CONDOR_DIR_INPUT': inp, '_CONDOR_SCRATCH_DIR': scratch})
        self.assertEqual(r.returncode, 1)
        self.assertIn(f'{scratch}/prodtools is not a prodtools release on this worker',
                      r.stderr)
        self.assertTrue(os.path.isfile(os.path.join(scratch, 'prodtools', 'bin', 'setup.sh')))


class TestReadBackValidation(unittest.TestCase):
    """After mu2e exits 0 and before anything is pushed, every .art/.root
    output is read back entry by entry (utils/validate_root_outputs.py).
    It is the only gate that looks at the payload — exit code, SHA256
    manifest and pushOutput CRC all certify the bytes as written."""

    def _run_capture(self, side_effect=None):
        from utils import runmu2e
        calls = []
        def fake_run(cmd, shell=False, **kw):
            calls.append(cmd)
            if side_effect:
                raise side_effect
        return runmu2e, calls, fake_run

    def test_only_root_files_are_read_back_and_none_means_no_run(self):
        from utils import runmu2e
        runmu2e_, calls, fake_run = self._run_capture()
        with patch.object(runmu2e, 'run', fake_run):
            self.assertFalse(runmu2e._validate_outputs(
                ['out.log', 'etc.mu2e.index.000.0000001.txt'], '/setup.sh'))
        self.assertEqual(calls, [])

    def test_command_sources_the_job_setup_and_names_every_file(self):
        from utils import runmu2e
        _, calls, fake_run = self._run_capture()
        with patch.object(runmu2e, 'run', fake_run):
            failed = runmu2e._validate_outputs(
                ['mcs.mu2e.A.B.001.art', 'nts.mu2e.A.B.001.root', 'x.log'],
                '/cvmfs/m/setup.sh')
        self.assertFalse(failed)
        self.assertEqual(len(calls), 1)
        argv = calls[0]
        self.assertEqual(argv[:2], ['bash', '-c'])
        self.assertTrue(argv[2].startswith('source /cvmfs/m/setup.sh && python3 '))
        self.assertIn('validate_root_outputs.py', argv[2])
        self.assertIn('mcs.mu2e.A.B.001.art', argv[2])
        self.assertIn('nts.mu2e.A.B.001.root', argv[2])
        self.assertNotIn('x.log', argv[2])

    def test_unreadable_output_marks_the_job_failed(self):
        from utils import runmu2e
        _, calls, fake_run = self._run_capture(
            subprocess.CalledProcessError(1, 'validate'))
        with patch.object(runmu2e, 'run', fake_run):
            self.assertTrue(runmu2e._validate_outputs(
                ['mcs.mu2e.A.B.001.art'], '/setup.sh'))

    def test_entry_key_wins_over_the_cli_and_must_be_bool(self):
        from types import SimpleNamespace
        from utils import runmu2e
        cli_on = SimpleNamespace(no_validate=False)
        cli_off = SimpleNamespace(no_validate=True)
        self.assertTrue(runmu2e._validation_enabled({}, cli_on))
        self.assertFalse(runmu2e._validation_enabled({}, cli_off))
        self.assertFalse(runmu2e._validation_enabled({'validate_outputs': False}, cli_on))
        self.assertTrue(runmu2e._validation_enabled({'validate_outputs': True}, cli_off))
        with self.assertRaises(SystemExit):
            runmu2e._validation_enabled({'validate_outputs': 'yes'}, cli_on)

    def test_runlocal_forwards_no_validate_to_the_child(self):
        from types import SimpleNamespace
        from utils import runlocal
        base = dict(entry_point='/x/runlocal.py', jobdef='cnf.tar', inloc='tape',
                    indices=[3], nevts=-1, mu2e_options='', copy_input=False,
                    code=None, code_root=None, no_validate=False)
        argv = runlocal.child_argv(3, SimpleNamespace(**base))
        self.assertNotIn('--no-validate', argv)
        argv = runlocal.child_argv(3, SimpleNamespace(**{**base, 'no_validate': True}))
        self.assertIn('--no-validate', argv)

    def test_scan_finds_a_scrambled_block(self):
        """End to end with real ROOT (skipped in an environment without
        it, e.g. `muse setup ops`): a healthy tree reads OK; the same file
        with 4 KiB overwritten mid-way reads BAD and exits 1 — the
        fnpc18003 signature."""
        try:
            import ROOT  # noqa: F401
        except ImportError:
            self.skipTest('ROOT not importable here')
        import os, random, io, contextlib
        from array import array
        from utils import validate_root_outputs as v
        d = _mkdtemp(); p = os.path.join(d, 'nts.mu2e.T.C.001.root')
        f = ROOT.TFile(p, 'RECREATE'); t = ROOT.TTree('Events', 'Events')
        x = array('d', [0.0]); t.Branch('x', x, 'x/D')
        rnd = random.Random(1)
        for i in range(200000):
            x[0] = rnd.random(); t.Fill()
        t.Write(); f.Close()
        self.assertEqual(v.main([p]), 0)
        size = os.path.getsize(p)
        with open(p, 'r+b') as fh:
            fh.seek(size // 2); fh.write(bytes(rnd.getrandbits(8) for _ in range(4096)))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = v.main([p])
        self.assertEqual(rc, 1)
        self.assertIn('BAD', out.getvalue())

if __name__ == '__main__':
    unittest.main(verbosity=2)


class TestInferDatasetLocationImports(unittest.TestCase):
    """The function body imports SAMError lazily; it must execute under the
    deployed samweb_client, which is a single module (no `.exceptions`).
    Every _gate_batch test injects dataset_location=, so nothing else
    runs this line."""

    def test_real_function_runs_with_sam_patched(self):
        from utils import file_resolver
        with patch('utils.samweb_wrapper.first_file_in_definition',
                   return_value='sim.mu2e.X.v1.001.art'), \
             patch('utils.samweb_wrapper.locate_file_strict',
                   return_value=[{'location': 'enstore:/pnfs/mu2e/tape/x',
                                  'location_type': 'tape'}]):
            self.assertEqual(file_resolver.infer_dataset_location('sim.mu2e.X.v1.art'),
                             'enstore')

    def test_sam_error_is_caught_not_import_error(self):
        from utils import file_resolver
        from utils.samweb_wrapper import SAMError
        with patch('utils.samweb_wrapper.first_file_in_definition',
                   side_effect=SAMError('SAM down')):
            self.assertEqual(file_resolver.infer_dataset_location('sim.mu2e.X.v1.art'),
                             'N/A')


# ---------------------------------------------------------------------------
# push_file: publish one already-built file to SAM through pushOutput
# ---------------------------------------------------------------------------

class TestPushFileValidate(unittest.TestCase):
    """The one rule for what may be pushed, shared by the CLI (owner from
    $USER) and the MCP tool (owner from run_as)."""

    def setUp(self):
        self.d = _mkdtemp()
        self.path = self._file('etc.u.TBeam-bm.e470313.0.txt')

    def _file(self, name):
        p = os.path.join(self.d, name)
        Path(p).write_text('#BLTrackFile: Source file\n')
        return p

    def _validate(self, path=None, location='scratch', parents=(), owner='u'):
        from utils.push_file import validate
        return validate(path or self.path, location, list(parents), owner=owner)

    def test_accepts_a_six_field_name_owned_by_the_identity(self):
        name = self._validate(parents=['nts.u.T.e470313.00000000.root'])
        self.assertEqual((name.tier, name.owner, name.sequencer, name.extension),
                         ('etc', 'u', '0', 'txt'))

    def test_refuses_a_missing_file(self):
        with self.assertRaisesRegex(ValueError, 'not a file'):
            self._validate(path=os.path.join(self.d, 'etc.u.X.e470313.0.txt'))

    def test_refuses_a_dataset_shaped_name(self):
        """Five fields is a DATASET name; pushOutput needs the sequencer."""
        with self.assertRaisesRegex(ValueError, 'sequencer'):
            self._validate(path=self._file('etc.u.TBeam-bm.e470313.txt'))

    def test_refuses_a_name_that_is_not_mu2e_shaped(self):
        with self.assertRaisesRegex(ValueError, 'Mu2e file name'):
            self._validate(path=self._file('beam.txt'))

    def test_refuses_another_owner(self):
        with self.assertRaisesRegex(ValueError, "owned by 'u'.*'mu2e'"):
            self._validate(owner='mu2e')

    def test_refuses_outstage_and_unknown_locations(self):
        for loc in ('outstage', 'presistent', ''):
            with self.subTest(location=loc):
                with self.assertRaisesRegex(ValueError, 'location'):
                    self._validate(location=loc)

    def test_refuses_a_parent_that_is_not_a_file_name(self):
        with self.assertRaisesRegex(ValueError, 'parent'):
            self._validate(parents=['nts.u.T.e470313.root'])


class TestPushFileCli(unittest.TestCase):
    """utils/push_file.py main: validate, refuse a name already in SAM,
    write parents_list.txt, hand pushOutput one output.txt line."""

    def setUp(self):
        self.src = _mkdtemp()
        self.path = os.path.join(self.src, 'etc.u.TBeam-bm.e470313.0.txt')
        Path(self.path).write_text('#BLTrackFile: Source file\n')

    def _run(self, argv, *, in_sam='', push=0):
        from utils import push_file
        seen = {}

        def fake_push_output(output_specs, output_file='output.txt', simjob_setup=None):
            seen['specs'] = output_specs
            seen['output_file'] = output_file
            if isinstance(push, Exception):
                raise push
            return push

        work = _mkdtemp()
        cwd = os.getcwd()
        try:
            os.chdir(work)
            with patch.object(push_file, 'push_output', fake_push_output), \
                 patch.object(push_file, 'locate_file', return_value=in_sam), \
                 patch.object(push_file, 'default_owner', return_value='u'):
                rc = push_file.main(argv)
        finally:
            os.chdir(cwd)
        return rc, seen, Path(work)

    def test_writes_parents_list_and_one_output_line(self):
        rc, seen, work = self._run(['--file', self.path, '--location', 'scratch',
                                    '--parent', 'nts.u.T.e470313.00000000.root',
                                    '--parent', 'nts.u.T.e470313.00000001.root'])
        self.assertEqual(rc, 0)
        self.assertEqual(seen['specs'], [('scratch', self.path, 'parents_list.txt')])
        self.assertEqual(seen['output_file'], 'output.txt')
        self.assertEqual((work / 'parents_list.txt').read_text(),
                         'nts.u.T.e470313.00000000.root\nnts.u.T.e470313.00000001.root\n')

    def test_no_parents_means_none_and_no_parents_file(self):
        rc, seen, work = self._run(['--file', self.path, '--location', 'tape'])
        self.assertEqual(rc, 0)
        self.assertEqual(seen['specs'], [('tape', self.path, 'none')])
        self.assertFalse((work / 'parents_list.txt').exists())

    def test_refuses_a_name_already_in_sam_before_pushing(self):
        rc, seen, _ = self._run(['--file', self.path, '--location', 'scratch'],
                                in_sam='enstore:/pnfs/x')
        self.assertEqual(rc, 2)
        self.assertNotIn('specs', seen)

    def test_validation_failure_exits_2_before_pushing(self):
        bad = os.path.join(self.src, 'etc.u.TBeam-bm.e470313.txt')
        Path(bad).write_text('x\n')
        rc, seen, _ = self._run(['--file', bad, '--location', 'scratch'])
        self.assertEqual(rc, 2)
        self.assertNotIn('specs', seen)

    def test_pushoutput_failure_is_the_exit_code(self):
        import subprocess as sp
        rc, _, _ = self._run(['--file', self.path, '--location', 'scratch'],
                             push=sp.CalledProcessError(3, 'pushOutput output.txt'))
        self.assertEqual(rc, 3)


class TestPushFileTool(unittest.TestCase):
    """prodtools_mcp_write.tools.push_file: the same gates as push_cnf,
    every refusal before run_cli, argv composed for bin/push_file."""

    def setUp(self):
        from prodtools_mcp_write import tools
        self.tools = tools
        self.src = _mkdtemp()
        self.work = _mkdtemp()
        self.name = 'etc.u.TBeam-bm.e470313.0.txt'
        self.path = os.path.join(self.src, self.name)
        Path(self.path).write_text('#BLTrackFile: Source file\n')
        self.parents = ['nts.u.T.e470313.00000000.root']

    def _push(self, *, path=None, location='scratch', parents=None, run_as='self',
              confirm=False, cli=None):
        cli = cli if cli is not None else {'rc': 0, 'stdout': 'pushed', 'stderr': ''}
        with patch('prodtools_mcp_write.runner.run_cli', return_value=cli) as run, \
             patch('prodtools_mcp_write.tools.getpass.getuser', return_value='u'), \
             patch('prodtools_mcp_write.tools._self_workdir', return_value=self.work):
            self.last_run = run
            out = self.tools.push_file(
                path=path or self.path, location=location,
                parents=self.parents if parents is None else parents,
                run_as=run_as, confirm=confirm)
        return out

    def test_entry_point_is_allowed(self):
        from prodtools_mcp_write import runner
        self.assertIn('bin/push_file', runner.ALLOWED_ENTRY_POINTS)

    def test_mu2epro_without_confirm_refused_before_running_anything(self):
        with patch('prodtools_mcp_write.runner.run_cli') as run:
            with self.assertRaises(PermissionError):
                self.tools.push_file(path=self.path, location='tape',
                                     parents=self.parents, run_as='mu2epro')
        run.assert_not_called()

    def test_argv_names_the_file_location_and_every_parent(self):
        out = self._push(parents=['nts.u.T.e470313.00000000.root',
                                  'nts.u.T.e470313.00000001.root'])
        argv, run_as = self.last_run.call_args.args
        self.assertEqual(argv, ['bin/push_file', '--file', self.path, '--location', 'scratch',
                                '--parent', 'nts.u.T.e470313.00000000.root',
                                '--parent', 'nts.u.T.e470313.00000001.root'])
        self.assertEqual(run_as, 'self')
        self.assertEqual(self.last_run.call_args.kwargs, {'cwd': self.work})
        self.assertEqual(out, {'name': self.name, 'location': 'scratch', 'parents': 2, 'rc': 0})

    def test_self_runs_in_its_own_workdir_and_removes_it_on_success(self):
        self._push()
        self.assertFalse(os.path.exists(self.work))

    def test_mu2epro_gets_no_cwd_and_expects_the_mu2e_owner(self):
        with self.assertRaisesRegex(ValueError, "owned by 'u'.*'mu2e'"):
            self._push(run_as='mu2epro', confirm=True)
        self.last_run.assert_not_called()
        prod = os.path.join(self.src, 'etc.mu2e.TBeam-bm.e470313.0.txt')
        Path(prod).write_text('x\n')
        self._push(path=prod, location='tape', run_as='mu2epro', confirm=True)
        self.assertEqual(self.last_run.call_args.kwargs, {'cwd': None})
        self.assertEqual(self.last_run.call_args.args[1], 'mu2epro')

    def test_refusals_happen_before_run_cli(self):
        cases = {
            'missing file': dict(path=os.path.join(self.src, 'etc.u.X.e470313.0.txt')),
            'dataset-shaped name': dict(path=self._write('etc.u.TBeam-bm.e470313.txt')),
            'outstage': dict(location='outstage'),
            'parents not a list': dict(parents='nts.u.T.e470313.00000000.root'),
            'empty parent': dict(parents=['']),
        }
        for label, kw in cases.items():
            with self.subTest(label):
                with self.assertRaises(ValueError):
                    self._push(**kw)
                self.last_run.assert_not_called()

    def _write(self, name):
        p = os.path.join(self.src, name)
        Path(p).write_text('x\n')
        return p

    def test_nonzero_rc_raises_with_both_streams_and_keeps_the_workdir(self):
        with self.assertRaises(RuntimeError) as cm:
            self._push(cli={'rc': 1, 'stdout': 'gfal-copy error: HTTP 403',
                            'stderr': 'Traceback'})
        msg = str(cm.exception)
        self.assertIn('HTTP 403', msg)
        self.assertIn('Traceback', msg)
        self.assertIn(self.work, msg)
        self.assertTrue(os.path.exists(self.work))


# ---------------------------------------------------------------------------
# Shared (streamable-http) mode: transport flags and caller identity
# ---------------------------------------------------------------------------

class TestMcpSharedRuntime(unittest.TestCase):
    """One process-wide fact: is this server serving one caller or many."""

    def setUp(self):
        from prodtools_mcp import runtime
        self.runtime = runtime
        self.addCleanup(runtime.set_shared, False)

    def test_stdio_is_the_default(self):
        self.assertFalse(self.runtime.is_shared())

    def test_set_shared_flips_it(self):
        self.runtime.set_shared(True)
        self.assertTrue(self.runtime.is_shared())


class TestMcpUserParameter(unittest.TestCase):
    """`user` names whose ledger and queue to read, for both transports.

    Over stdio the process user IS the caller, so `mine` is meaningful.
    Over streamable-http the process user is the HOST: `mine` would hand
    every reader the host's ledger, and an empty answer from the wrong
    ledger reads exactly like "no campaigns".
    """

    def setUp(self):
        from prodtools_mcp import condor, ledger_ro, runtime
        from prodtools_mcp.adapters import ToolError
        from prodtools_mcp.tools import status
        self.status = status
        self.condor = condor
        self.ledger_ro = ledger_ro
        self.runtime = runtime
        self.ToolError = ToolError
        self.addCleanup(runtime.set_shared, False)

    def test_user_resolves_both_axes(self):
        db, owner = self.status._resolve_identity(False, user='alice')
        self.assertEqual(db,
                         '/exp/mu2e/data/users/alice/prodtools/submissions.db')
        self.assertEqual(owner, 'alice')

    def test_user_wins_over_mine(self):
        with patch('getpass.getuser', return_value='bob'):
            db, owner = self.status._resolve_identity(True, user='alice')
        self.assertEqual(owner, 'alice')
        self.assertIn('/users/alice/', db)

    def test_user_works_on_a_shared_server(self):
        self.runtime.set_shared(True)
        db, owner = self.status._resolve_identity(False, user='alice')
        self.assertEqual(owner, 'alice')
        self.assertIn('/users/alice/', db)

    def test_mine_without_user_is_refused_when_shared(self):
        self.runtime.set_shared(True)
        with self.assertRaises(self.ToolError) as ctx:
            self.status._resolve_identity(True)
        self.assertEqual(ctx.exception.kind, 'invalid_argument')
        self.assertIn('user=', ctx.exception.remedy)

    def test_mine_without_user_still_works_over_stdio(self):
        with patch('getpass.getuser', return_value='bob'):
            db, owner = self.status._resolve_identity(True)
        self.assertEqual(owner, 'bob')
        self.assertIn('/users/bob/', db)

    def test_production_is_still_the_default_when_shared(self):
        # The shared server's whole point. Only `mine` is refused.
        self.runtime.set_shared(True)
        db, owner = self.status._resolve_identity(False)
        self.assertIsNone(db)
        self.assertEqual(owner, self.condor.OWNER)

    def test_user_must_be_a_login_not_a_path(self):
        # `user` becomes a path component on a server other people reach.
        for bad in ('../mu2epro', 'a/b', '', '.', 'alice; rm'):
            with self.assertRaises(self.ToolError) as ctx:
                self.status._resolve_identity(False, user=bad)
            self.assertEqual(ctx.exception.kind, 'invalid_argument')

    def test_campaign_status_reads_the_named_users_ledger(self):
        seen = []

        def snapshot(db_path):
            seen.append(db_path)
            return [], []

        with patch.object(self.ledger_ro, 'snapshot', snapshot):
            out = self.status.campaign_status(user='alice')
        self.assertEqual(seen,
                         ['/exp/mu2e/data/users/alice/prodtools/submissions.db'])
        self.assertEqual(out['db_path'], seen[0])

    def test_list_campaigns_reads_the_named_users_ledger(self):
        seen = []

        def campaigns(db_path, state=None):
            seen.append(db_path)
            return []

        with patch.object(self.ledger_ro, 'campaigns', campaigns):
            out = self.status.list_campaigns(user='alice')
        self.assertEqual(seen,
                         ['/exp/mu2e/data/users/alice/prodtools/submissions.db'])
        self.assertEqual(out['db_path'], seen[0])


class TestMcpTransportCli(unittest.TestCase):
    """`--transport streamable-http` is what makes the server shareable."""

    def setUp(self):
        from prodtools_mcp import runtime, server
        self.server = server
        self.runtime = runtime
        self.addCleanup(runtime.set_shared, False)

    def test_defaults_are_stdio_on_localhost(self):
        args = self.server._parse_args([])
        self.assertEqual(args.transport, 'stdio')
        self.assertEqual(args.host, '127.0.0.1')
        self.assertEqual(args.port, self.server.DEFAULT_PORT)

    def test_http_flags_parse(self):
        args = self.server._parse_args(
            ['--transport', 'streamable-http', '--host', '0.0.0.0',
             '--port', '9001'])
        self.assertEqual(args.transport, 'streamable-http')
        self.assertEqual(args.host, '0.0.0.0')
        self.assertEqual(args.port, 9001)

    def test_unknown_transport_is_refused(self):
        with open(os.devnull, 'w') as devnull:
            with contextlib.redirect_stderr(devnull):
                with self.assertRaises(SystemExit):
                    self.server._parse_args(['--transport', 'carrier-pigeon'])

    def _run_main(self, argv):
        calls = {}

        class FakeMcp:
            def run(self, transport='stdio', **kwargs):
                calls['transport'] = transport
                calls['run_kwargs'] = kwargs
                calls['shared_at_run'] = _runtime.is_shared()

        from prodtools_mcp import runtime as _runtime
        def fake_create(**kw):
            calls['kwargs'] = kw
            return FakeMcp()

        with patch.object(self.server, 'create_mcp_server', fake_create):
            self.server.main(argv)
        return calls

    def test_http_marks_the_server_shared_before_it_serves(self):
        calls = self._run_main(['--transport', 'streamable-http'])
        self.assertEqual(calls['transport'], 'streamable-http')
        self.assertTrue(calls['shared_at_run'])

    def test_stdio_does_not_mark_the_server_shared(self):
        calls = self._run_main([])
        self.assertEqual(calls['transport'], 'stdio')
        self.assertFalse(calls['shared_at_run'])
        # stdio takes no bind address: mcp 2.x would reject the kwargs.
        self.assertEqual(calls['run_kwargs'], {})

    def test_http_passes_the_bind_address_to_run(self):
        # mcp 2.x: host/port belong to run(), not to the server object.
        calls = self._run_main(['--transport', 'streamable-http',
                                '--host', '0.0.0.0', '--port', '9001'])
        self.assertEqual(calls['run_kwargs']['host'], '0.0.0.0')
        self.assertEqual(calls['run_kwargs']['port'], 9001)
        self.assertNotIn('transport_security', calls['run_kwargs'])

    @unittest.skipUnless(_HAVE_FASTMCP,
                         'needs the MCP venv (mcp/scripts/install.sh); the '
                         'rest of this suite runs on the plain ops python')
    def test_allowed_host_turns_on_rebinding_protection(self):
        kwargs = self.server.http_run_kwargs(
            '0.0.0.0', 9001, ['mu2eaigpvm01.fnal.gov:9001'])
        sec = kwargs['transport_security']
        self.assertTrue(sec.enable_dns_rebinding_protection)
        self.assertIn('mu2eaigpvm01.fnal.gov:9001', sec.allowed_hosts)

    @unittest.skipUnless(_HAVE_FASTMCP,
                         'needs the MCP venv (mcp/scripts/install.sh)')
    def test_no_allowed_host_leaves_protection_off(self):
        # An empty allowlist WITH protection on rejects every request
        # (421). Left out, the SDK decides: localhost binds get a
        # localhost allowlist, anything else gets none — what the other
        # central Mu2e servers run.
        kwargs = self.server.http_run_kwargs('0.0.0.0', 9001)
        self.assertNotIn('transport_security', kwargs)

    def test_server_info_tells_a_shared_reader_to_pass_user(self):
        self.runtime.set_shared(True)
        ident = self.server.get_server_info()['identity']
        self.assertIn('user', ident)
        self.assertIn('user', ident['mine_true'])
