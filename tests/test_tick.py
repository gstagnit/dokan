"""End-to-end tests of `nnlojet-run tick` against a fake HTCondor.

The batch system is `tests/fakecondor` (symlinked `condor_*` commands sharing a
JSON queue), NNLOJET never runs: a job "finishes" when the test drops a real
NNLOJET log (and, for production, a real histogram file) into the batch
directory and removes the job from the fake queue.  Everything else -- the
database, the dispatcher, the merges, the Luigi builds -- is the real thing,
driven through the command line exactly as a scheduler would.

Run with the dokan environment's interpreter, e.g.
    PYTHONPATH=src python -m pytest tests/test_tick.py -x
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from dokan.config import read_config_json

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures"
FAKECONDOR = HERE / "fakecondor"
SRC = HERE.parent / "src"

PROC_RUN = "WmJunsym.WmC_ATLAS_2026"  # <PROCESS>.<RUN> prefix NNLOJET puts on its output files

# > job.status codes (see dokan.db.JobStatus)
QUEUED, DISPATCHED, RUNNING, DONE, MERGED, FAILED = 0, 1, 2, 3, 4, -1


_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _plain(text: str) -> str:
    """Strip terminal escape codes (rich colours the output when the environment asks for it)."""
    return _ANSI.sub("", text)


class FakeCondor:
    """Handle on the fake queue: inspect it, and make jobs finish."""

    def __init__(self, state_path: Path):
        self.state_path = state_path
        self.state_path.write_text("")

    @property
    def state(self) -> dict:
        raw = self.state_path.read_text()
        return json.loads(raw) if raw.strip() else {"next_cluster": 1000, "clusters": {}, "calls": []}

    def _write(self, state: dict) -> None:
        self.state_path.write_text(json.dumps(state, indent=1))

    @property
    def clusters(self) -> dict[str, dict]:
        return self.state["clusters"]

    @property
    def n_queued(self) -> int:
        return sum(len(c["procs"]) for c in self.clusters.values())

    def calls(self, command: str) -> list:
        return [c for c in self.state["calls"] if c[0] == command]

    def clear_calls(self) -> None:
        state = self.state
        state["calls"] = []
        self._write(state)

    def hold(self, cluster: str, proc: str) -> None:
        state = self.state
        state["clusters"][cluster]["procs"][proc] = 5
        self._write(state)

    def finish(self, cluster: str, proc: str | None = None, *, success: bool = True) -> None:
        """Make proc(s) of `cluster` leave the queue, writing NNLOJET output when `success`."""
        state = self.state
        info = state["clusters"][cluster]
        procs = [proc] if proc is not None else list(info["procs"])
        iwd = Path(info["iwd"])
        for p in procs:
            seed = info["start_seed"] + int(p)
            if success:
                shutil.copyfile(FIXTURES / "nnlojet_LO.log", iwd / f"{PROC_RUN}.s{seed}.log")
                if "production" in iwd.parts:
                    shutil.copyfile(
                        FIXTURES / "nnlojet_LO.histos.dat", iwd / f"{PROC_RUN}.LO.histos.s{seed}.dat"
                    )
            (iwd / f"job.s{seed}.out").write_text("")
            del info["procs"][p]
        if not info["procs"]:
            del state["clusters"][cluster]
        self._write(state)

    def finish_all(self) -> None:
        for cluster in list(self.clusters):
            self.finish(cluster)


class Run:
    """A run directory plus the means to tick it and to look at its database."""

    def __init__(self, path: Path, condor: FakeCondor):
        self.path = path
        self.condor = condor
        self.env = dict(os.environ)
        self.env["PATH"] = f"{FAKECONDOR}:{self.env.get('PATH', '')}"
        self.env["PYTHONPATH"] = str(SRC)
        self.env["FAKE_CONDOR_STATE"] = str(condor.state_path)

    @classmethod
    def create(cls, tmp_path: Path) -> Run:
        run_path = tmp_path / "run"
        run_path.mkdir()
        shutil.copyfile(FIXTURES / "template.run", run_path / "template.run")
        shutil.copyfile(
            SRC / "dokan" / "exe" / "htcondor" / "htcondor.template", run_path / "htcondor.template"
        )
        config = json.loads((FIXTURES / "config.json").read_text())
        (run_path / "config.json").write_text(json.dumps(config, indent=1))
        return cls(run_path, FakeCondor(tmp_path / "condor_state.json"))

    def cli(self, *args: str, check: bool = True, timeout: float = 600.0) -> subprocess.CompletedProcess:
        proc = subprocess.run(
            [sys.executable, "-m", "dokan", *args],
            env=self.env,
            cwd=self.path.parent,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if check and proc.returncode != 0:
            raise AssertionError(
                f"nnlojet-run {' '.join(args)} failed ({proc.returncode}):\n{proc.stdout}\n{proc.stderr}"
            )
        return proc

    def tick(self, *args: str, **kwargs) -> str:
        proc = self.cli("tick", str(self.path), *args, **kwargs)
        assert not (self.path / "tick.lease").exists(), "lease left behind"
        return _plain(proc.stdout)

    def config(self, **updates) -> None:
        config = read_config_json(self.path / "config.json")
        for dotted, value in updates.items():
            section, key = dotted.split(".")
            config[section][key] = value
        (self.path / "config.json").write_text(json.dumps(config, indent=1))

    def sql(self, query: str, db: str = "db.sqlite") -> list[tuple]:
        conn = sqlite3.connect(f"file:{self.path / db}?mode=ro", uri=True, timeout=30)
        try:
            return conn.execute(query).fetchall()
        finally:
            conn.close()

    def jobs(self, **where) -> list[tuple]:
        """`(id, part, mode, status, seed, rel_path)` rows, optionally filtered by column equality."""
        clause = " and ".join(f"job.{k} = {v!r}" for k, v in where.items())
        return self.sql(
            "select job.id, part.name, job.mode, job.status, job.seed, job.rel_path from job join part"
            " on job.part_id = part.id" + (f" where {clause}" if clause else "") + " order by job.id"
        )

    def count(self, **where) -> int:
        return len(self.jobs(**where))

    def signals(self) -> list[tuple[int, str]]:
        return self.sql("select level, message from log where level < 0 order by id", db="log.sqlite")

    def parts(self) -> dict[str, tuple[float, float, float, float]]:
        return {
            name: (ntot, result, error, ts)
            for name, ntot, result, error, ts in self.sql(
                "select name, ntot, result, error, timestamp from part where active = 1"
            )
        }

    def exe_data(self, rel_path: str) -> dict:
        batch = self.path / rel_path
        final = batch / "job.json"
        return json.loads((final if final.is_file() else batch / "job.tmp").read_text())


@pytest.fixture(scope="module")
def run(tmp_path_factory) -> Run:
    return Run.create(tmp_path_factory.mktemp("tick"))


# ---------------------------------------------------------------------------
# > the campaign, one tick at a time.  The tests are ordered and share the run.
# ---------------------------------------------------------------------------


def test_01_first_tick_starts_warmups(run: Run):
    out = run.tick()
    assert "phase=preproduction" in out, out
    # > two parts, one single-seed warmup step each, each its own cluster
    assert len(run.condor.clusters) == 2
    assert run.condor.n_queued == 2
    assert run.count(mode=1, status=RUNNING) == 2
    rel_paths = {row[5] for row in run.jobs(status=RUNNING)}
    assert rel_paths == {"raw/warmup/LO_1/s1", "raw/warmup/LO_2/s1"}
    # > the submission recorded its cluster and its schedd
    for rel_path in rel_paths:
        settings = run.exe_data(rel_path)["policy_settings"]
        assert settings["htcondor_id"] >= 1000
        assert settings["htcondor_schedd"] == "fakeschedd.example.org"
    # > campaign state exists, the lease does not linger
    assert (run.path / "tick.json").is_file()
    assert (run.path / "tick.lease").exists() is False
    assert run.signals()[0][0] == -4  # SIG_SUB


def test_02_idle_tick_changes_nothing(run: Run):
    run.condor.clear_calls()
    out = run.tick()
    assert "phase=preproduction" in out
    assert "queue=2i" in out
    assert run.condor.calls("condor_submit") == []
    # > exactly one queue query for the whole tick (one schedd)
    assert len(run.condor.calls("condor_q")) == 1
    assert run.count(status=RUNNING) == 2


def test_03_lease_blocks_a_concurrent_tick(run: Run):
    lease = run.path / "tick.lease"
    lease.write_text(json.dumps({"host": "elsewhere.example.org", "pid": 1, "started": time.time()}))
    proc = run.cli("tick", str(run.path))
    assert "skipped (lease held by pid 1 on elsewhere.example.org" in _plain(proc.stdout)
    lease.unlink()
    # > a stale lease (older than the timeout) is taken over
    lease.write_text(json.dumps({"host": "elsewhere.example.org", "pid": 1, "started": time.time() - 10}))
    out = run.tick("--lease-timeout", "5s")
    assert "skipped" not in out
    assert not lease.exists()


def test_04_finished_warmups_advance_to_the_second_step(run: Run):
    run.condor.finish_all()
    out = run.tick()
    assert "finalized=2" in out and "done=2" in out, out
    assert run.count(mode=1, status=DONE) == 2
    for rel_path in ("raw/warmup/LO_1/s1", "raw/warmup/LO_2/s1"):
        assert (run.path / rel_path / "job.json").is_file()
        entry = next(iter(run.exe_data(rel_path)["jobs"].values()))
        assert entry["result"] == pytest.approx(43641.199)
    # > step 2: the free slots (4) are shared by the parts still warming up: the first
    # > part takes 2 seeds, the second sees those queued and gets 1 (as in a live run)
    assert run.count(mode=1, status=RUNNING) == 3
    assert {row[5] for row in run.jobs(mode=1, status=RUNNING)} == {
        "raw/warmup/LO_1/s2-3",
        "raw/warmup/LO_2/s2",
    }
    assert run.condor.n_queued == 3


def test_05_held_jobs_are_released(run: Run):
    cluster = next(iter(run.condor.clusters))
    run.condor.hold(cluster, "0")
    out = run.tick()
    assert "released=1" in out
    assert run.condor.clusters[cluster]["procs"]["0"] == 1


def test_06_warmup_complete_starts_preproduction(run: Run):
    run.condor.finish_all()
    out = run.tick()
    assert "finalized=2" in out and "done=3" in out, out
    # > max_increment_steps = 2: the QC is done, one pre-production per part
    assert run.count(mode=2, status=RUNNING) == 2
    assert {row[5] for row in run.jobs(mode=2)} == {"raw/production/LO_1/s1", "raw/production/LO_2/s1"}


def test_07_preproduction_merges_then_production_dispatches(run: Run):
    run.condor.finish_all()
    out = run.tick()
    assert "phase=production" in out, out
    # > every part carries a merged result now
    for name, (ntot, result, error, ts) in run.parts().items():
        assert ntot > 0 and result != 0.0 and error > 0.0 and ts > 0.0, (name, ntot, result, error, ts)
    assert run.count(mode=2, status=MERGED) == 2
    assert (run.path / "result" / "merge" / "cross.dat").is_file()
    # > the production wave respects the concurrency limit across the campaign
    assert run.count(mode=2, status=RUNNING) == 4
    assert run.condor.n_queued == 4
    assert "dispatched=4" in out


def test_08_early_seeds_free_slots(run: Run):
    # > one seed of one batch finishes; its batch stays in the queue
    cluster = next(c for c, info in run.condor.clusters.items() if len(info["procs"]) > 1)
    run.condor.finish(cluster, "0")
    out = run.tick()
    assert "early 1" in out, out
    # > ... and its data was merged in the same tick (2 pre-productions + 1 early seed)
    assert run.count(mode=2, status=DONE) + run.count(mode=2, status=MERGED) == 3
    # > the slot it freed is refilled at once, still within the limit
    assert run.count(mode=2, status=RUNNING) == 4
    assert run.condor.n_queued == 4


def test_09_no_dispatch_drains(run: Run):
    run.condor.finish_all()
    out = run.tick("--no-dispatch")
    assert "dispatch disabled" in out
    assert run.count(mode=2, status=RUNNING) == 0
    assert run.condor.n_queued == 0
    # > the finished jobs were merged into their parts
    assert run.count(mode=2, status=DONE) == 0
    assert run.count(mode=2, status=MERGED) >= 5


def test_10_status_reads_the_database(run: Run):
    proc = run.cli("status", str(run.path), "-n", "5")
    out = _plain(proc.stdout)
    assert "LO" in out and "cross =" in out, out


def test_11_target_reached_settles_and_completes(run: Run):
    run.config(**{"run.target_rel_acc": 10.0})
    # > rows planned by an earlier wave but held back by the concurrency cap are
    # > dispatched before the target is re-evaluated (as in a live run): drain them
    for _ in range(5):
        run.condor.finish_all()
        out = run.tick()
        if "settled" in out:
            break
    assert "settled" in out, out
    levels = [level for level, _ in run.signals()]
    assert -5 in levels  # SIG_DISPATCH_DONE
    assert levels[-1] == -1  # SIG_COMP
    assert (run.path / "result" / "final" / "lo.cross.dat").is_file()
    # > a further tick does nothing
    out = run.tick()
    assert "phase=complete" in out


def test_12_reopen_continues_the_campaign(run: Run):
    run.config(**{"run.target_rel_acc": 0.02})
    out = run.tick("--reopen")
    assert "phase=production" in out, out
    assert run.count(mode=2, status=RUNNING) == 4


def test_13_unsubmitted_batch_is_retried_and_adopted(run: Run):
    # > a submission that fails leaves the batch staged; the next tick re-submits it
    run.condor.finish_all()
    run.env["FAKE_CONDOR_SUBMIT_FAILS"] = "1"
    out = run.tick()
    assert "dispatched=" not in out and "staged but not submitted" in out, out
    staged = [row for row in run.jobs(mode=2, status=RUNNING)]
    assert staged, "runner should have staged batches even though the submission failed"
    for rel_path in {row[5] for row in staged}:
        assert "htcondor_id" not in run.exe_data(rel_path)["policy_settings"]
    del run.env["FAKE_CONDOR_SUBMIT_FAILS"]
    out = run.tick()
    for rel_path in {row[5] for row in staged}:
        assert run.exe_data(rel_path)["policy_settings"]["htcondor_id"] >= 1000
    assert run.condor.n_queued == run.count(mode=2, status=RUNNING) == 4
    # > a batch whose id was lost is recognised by its directory rather than re-submitted
    rel_path = next(iter({row[5] for row in staged}))
    tmp = run.path / rel_path / "job.tmp"
    data = json.loads(tmp.read_text())
    lost = data["policy_settings"].pop("htcondor_id")
    tmp.write_text(json.dumps(data))
    run.condor.clear_calls()
    run.tick()
    assert run.condor.calls("condor_submit") == []
    assert run.exe_data(rel_path)["policy_settings"]["htcondor_id"] == lost


def test_14_queue_failure_aborts_before_touching_anything(run: Run):
    run.env["FAKE_CONDOR_Q_FAILS"] = "1"
    before = run.jobs()
    proc = run.cli("tick", str(run.path), check=False)
    del run.env["FAKE_CONDOR_Q_FAILS"]
    assert proc.returncode != 0
    assert run.jobs() == before
    assert not (run.path / "tick.lease").exists()


def test_15_config_detached_is_never_persisted(run: Run):
    config = read_config_json(run.path / "config.json")
    assert "detached" not in config["run"]
    assert re.search(r'"jobs_batch_size"', (run.path / "config.json").read_text()) is None


def test_16_bare_environment_aborts_before_touching_anything(run: Run, tmp_path):
    # > an executable that cannot load: what a scheduler-launched tick sees when the
    # > compiler module of the user's shell is missing.  The fake fails like the loader.
    exe = tmp_path / "NNLOJET"
    exe.write_text(
        '#!/bin/sh\necho "$0: /lib64/libstdc++.so.6: version \\`GLIBCXX_3.4.32\' not found" >&2\nexit 1\n'
    )
    exe.chmod(0o755)
    before = run.jobs()
    proc = run.cli("--exe", str(exe), "tick", str(run.path), check=False)
    assert proc.returncode != 0
    assert "cannot start in this environment" in _plain(proc.stdout + proc.stderr)
    assert run.jobs() == before
    assert not (run.path / "tick.lease").exists()
