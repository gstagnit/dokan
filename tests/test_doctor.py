"""`nnlojet-run doctor` on a tick-mode run with batches in flight (fake HTCondor, see `test_tick.py`)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from test_tick import DONE, FAILED, MERGED, RUNNING, Run, _plain


def production_run(tmp_path: Path) -> Run:
    run = Run.create(tmp_path)
    # > warmups (single seed, then parallel seeds), pre-production, production wave
    run.tick()
    for _ in range(3):
        run.condor.finish_all()
        out = run.tick()
    assert "phase=production" in out, out
    return run


@pytest.fixture
def run(tmp_path) -> Run:
    return production_run(tmp_path)


def doctor(run: Run) -> str:
    proc = subprocess.run(
        [sys.executable, "-m", "dokan", "doctor", str(run.path)],
        env=run.env,
        cwd=run.path.parent,
        input="y\n" * 20,  # > accept the prompts (clear log, ...)
        capture_output=True,
        text=True,
        timeout=600.0,
    )
    out = _plain(proc.stdout + proc.stderr)
    assert proc.returncode == 0, out
    return out


def finished_batch(run: Run) -> Path:
    """A collected multi-seed production batch, to copy orphans from."""
    for job_json in sorted((run.path / "raw" / "production").glob("*/s*/job.json")):
        if len(json.loads(job_json.read_text())["jobs"]) >= 2:
            return job_json.parent
    raise AssertionError("no finished multi-seed production batch")


def plant_orphan(run: Run, src: Path, job_ids: list[int], seed_shift: int) -> Path:
    """Copy batch `src` to a new directory of the same part, with the given job ids and shifted seeds.

    This is what a database restore leaves behind: a batch directory whose job ids the
    database no longer knows -- or, worse, has since handed to other jobs.
    """
    exe = json.loads((src / "job.json").read_text())
    entries = list(exe["jobs"].values())[: len(job_ids)]
    assert len(entries) == len(job_ids)
    seeds = [int(e["seed"]) + seed_shift for e in entries]
    dst = src.parent / (f"s{seeds[0]}-{seeds[-1]}" if len(seeds) > 1 else f"s{seeds[0]}")
    dst.mkdir()
    jobs = {}
    for job_id, entry in zip(job_ids, entries, strict=True):
        old_seed = int(entry["seed"])
        new_seed = old_seed + seed_shift
        jobs[str(job_id)] = {**entry, "seed": new_seed}
        for f in src.glob(f"*.s{old_seed}.*"):
            (dst / f.name.replace(f".s{old_seed}.", f".s{new_seed}.")).write_bytes(f.read_bytes())
    exe["jobs"] = jobs
    (dst / "job.json").write_text(json.dumps(exe, indent=1))
    return dst


def test_doctor_leaves_jobs_in_flight_alone(run: Run):
    in_flight = {row[0] for row in run.jobs(mode=2, status=RUNNING)}
    assert in_flight
    doctor(run)
    # > the doctor has no view of the queue: settling these is the tick's job
    assert {row[0] for row in run.jobs(mode=2, status=RUNNING)} == in_flight
    run.condor.finish_all()
    run.tick("--no-dispatch")
    assert all(row[3] in (DONE, MERGED) for row in run.jobs(mode=2) if row[0] in in_flight)
    assert run.count(status=FAILED) == 0


def test_doctor_adopts_orphans_and_ignores_reused_ids(run: Run):
    run.condor.finish_all()
    run.tick("--no-dispatch")
    src = finished_batch(run)
    part = src.parent.name
    # > an id the database never saw, and one it has since given to a job of *another* part
    other = next(row for row in run.jobs(mode=2) if row[1] != part)
    unknown_id = max(row[0] for row in run.jobs()) + 1000
    before = {row[0]: row for row in run.jobs()}
    orphan = plant_orphan(run, src, [unknown_id, other[0]], seed_shift=5000)
    rel_path = str(orphan.relative_to(run.path))

    out = doctor(run)
    assert "Traceback" not in out, out

    # > the job that owns the reused id is untouched, and the orphan seed behind it is
    # > reported rather than adopted (adopting it under a fresh id would orphan it again)
    assert run.jobs(id=other[0]) == [before[other[0]]]
    assert f"job {other[0]} of {rel_path} belongs to {other[5]}" in " ".join(out.split()), out
    # > the unknown id is adopted under its own id, part and directory, with its result
    adopted = run.jobs(rel_path=rel_path)
    assert [(row[0], row[1]) for row in adopted] == [(unknown_id, part)], adopted
    assert adopted[0][3] in (DONE, MERGED), adopted

    # > a second doctor finds nothing more to do: no duplicate rows, no status flips, no crash
    snapshot = run.jobs()
    doctor(run)
    assert run.jobs() == snapshot
