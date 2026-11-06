"""A failed seed's partial histogram must not enter the merge (fake HTCondor, see `test_tick.py`)."""

from __future__ import annotations

import shutil
from pathlib import Path

import h5py
import pytest
from test_tick import DONE, FAILED, FIXTURES, MERGED, PROC_RUN, RUNNING, Run


@pytest.fixture(scope="module")
def run(tmp_path_factory) -> Run:
    return Run.create(tmp_path_factory.mktemp("failed_seed"))


def test_failed_seed_histogram_is_not_merged(run: Run):
    # > warmups (single seed, then parallel seeds), pre-production, production wave
    run.tick()
    for _ in range(3):
        run.condor.finish_all()
        out = run.tick()
    assert "phase=production" in out, out
    assert run.count(mode=2, status=RUNNING) == 4

    # > a multi-seed production batch: seed 0 fails after NNLOJET wrote a partial histogram
    cluster, info = next((c, i) for c, i in run.condor.clusters.items() if len(i["procs"]) > 1)
    iwd = Path(info["iwd"])
    failed_seed = info["start_seed"]
    partial = iwd / f"{PROC_RUN}.LO.histos.s{failed_seed}.dat"
    shutil.copyfile(FIXTURES / "nnlojet_LO.histos.dat", partial)
    run.condor.finish(cluster, "0", success=False)
    run.condor.finish_all()
    run.tick("--no-dispatch")

    rel_path = str(iwd.relative_to(run.path))
    seeds = {row[4]: row[3] for row in run.jobs(mode=2) if row[5] == rel_path}
    assert seeds[failed_seed] == FAILED, seeds
    assert all(status in (DONE, MERGED) for seed, status in seeds.items() if seed != failed_seed), seeds

    part = iwd.parts[-2]
    with h5py.File(run.path / "raw" / f"{part}.hdf5", "r") as h5f:
        grp = h5f[f"{part}/cross"]
        stored = set(grp["files"].asstr()[: int(grp.attrs["ndat_valid"])])
    assert str(partial.relative_to(run.path)) not in stored
    # > the batch's successful seeds did make it in
    assert any(f.startswith(rel_path + "/") for f in stored), stored


def test_legacy_cache_with_a_failed_seed_is_rebuilt(run: Run):
    """A cache that an older version filled with a failed seed's rows is rebuilt on the next merge."""
    from dokan.config import read_config_json
    from dokan.merge._core import build_obs_group

    failed = [row for row in run.jobs(mode=2, status=FAILED)]
    assert failed, "needs the previous test's failed seed"
    _, part, _, _, seed, rel_path = failed[0]
    partial = f"{rel_path}/{PROC_RUN}.LO.histos.s{seed}.dat"
    hdf5 = run.path / "raw" / f"{part}.hdf5"

    # > what an older version did: ingest the failed seed's histogram too
    config = read_config_json(run.path / "config.json")
    with h5py.File(hdf5, "r") as h5f:
        grp = h5f[f"{part}/cross"]
        stored = list(grp["files"].asstr()[: int(grp.attrs["ndat_valid"])])
    histograms = config["run"]["histograms"]
    build_obs_group(hdf5, part, {"histos": [*stored, partial]}, histograms, run.path, single_file="histos")
    with h5py.File(hdf5, "r") as h5f:
        assert partial in set(h5f[f"{part}/cross/files"].asstr()[:])

    # > new production data for the part triggers its next merge (the dispatcher decides
    # > which part gets jobs, so keep going until this one had a merge)
    n_merged = len(run.jobs(mode=2, status=MERGED))
    part_merged = lambda: sum(1 for row in run.jobs(mode=2, status=MERGED) if row[1] == part)  # noqa: E731
    n_part = part_merged()
    for _ in range(6):
        run.tick()
        run.condor.finish_all()
        run.tick("--no-dispatch")
        if part_merged() > n_part:
            break
    assert part_merged() > n_part and len(run.jobs(mode=2, status=MERGED)) > n_merged

    with h5py.File(hdf5, "r") as h5f:
        grp = h5f[f"{part}/cross"]
        stored_after = set(grp["files"].asstr()[: int(grp.attrs["ndat_valid"])])
    assert partial not in stored_after
    assert set(stored) < stored_after  # > everything valid was re-ingested, plus the new jobs
    messages = [m for (m,) in run.sql("select message from log", db="log.sqlite")]
    assert any("rebuilding from raw job output" in m for m in messages)
