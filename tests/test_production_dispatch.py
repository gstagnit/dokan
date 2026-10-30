"""How production jobs are sized.

Run with the dokan environment's interpreter, e.g.
    PYTHONPATH=src python -m pytest tests/test_production_dispatch.py
"""

from pathlib import Path

import pytest

from dokan.config import Config
from dokan.db._dbdispatch import DBDispatch
from dokan.db._dbtask import _DBRole
from dokan.db._jobstatus import JobStatus
from dokan.db._sqla import DokanDB, DokanLog, Job, Part
from dokan.exe import ExecutionMode

RUN_TAG: float = 2.0
OLD_TAG: float = 1.0  # > an earlier submission


def _make(path: Path, **run) -> DBDispatch:
    config = Config(default_ok=True)
    config["run"]["path"] = str(path)
    config["run"]["opt_target"] = "cross"
    config["run"]["target_rel_acc"] = 1e-6  # > never reached: dispatch keeps going
    config["run"]["job_max_runtime"] = 102.0
    config["run"]["job_fill_max_runtime"] = False
    config["run"]["jobs_batch_size"] = 3  # > the failure limit
    config["run"]["jobs_batch_unit_size"] = 1
    config["run"]["jobs_max_total"] = 100
    config["run"]["jobs_max_concurrent"] = 10
    config["production"]["niter"] = 2
    config["production"]["ncall_start"] = 5
    config["production"]["min_number"] = 1
    config["ui"]["monitor"] = True  # > keep the log in the database
    config["run"].update(run)
    task = DBDispatch(config=config, run_tag=RUN_TAG)
    DokanDB.metadata.create_all(task._engine(_DBRole.JOB))
    DokanLog.metadata.create_all(task._engine(_DBRole.LOG))
    return task


@pytest.fixture
def dispatch(tmp_path: Path) -> DBDispatch:
    return _make(tmp_path)


def _part(task: DBDispatch, name: str, result: float = 1.0, error: float = 0.1, ntot: int = 100) -> int:
    with task.session as session:
        pt = Part(
            name=name,
            string=name,
            part="V",
            part_num=1,
            order=0,
            active=True,
            result=result,
            error=error,
            ntot=ntot,
        )
        session.add(pt)
        session.commit()
        return pt.id


def _jobs(
    task: DBDispatch,
    part_id: int,
    status: JobStatus,
    n: int = 1,
    *,
    run_tag: float = RUN_TAG,
    ncall: int = 10,
    elapsed: float = 20.0,  # > 1s per event at ncall=10, niter=2
) -> None:
    with task.session as session:
        session.add_all(
            Job(
                run_tag=run_tag,
                part_id=part_id,
                mode=ExecutionMode.PRODUCTION,
                policy=task.config["exe"]["policy"],
                status=status,
                ncall=ncall,
                niter=2,
                elapsed_time=elapsed,
            )
            for _ in range(n)
        )
        session.commit()


def test_jobs_are_whole_iterations_within_the_runtime_limit(dispatch):
    pid = _part(dispatch, "V_1")
    _jobs(dispatch, pid, JobStatus.MERGED, 2)  # > tau = 1s exactly, no spread: 50% buffer
    with dispatch.session as session:
        opt = dispatch._distribute_time(session, 1000.0)["part"][pid]
    # > 1000s over 20 jobs of at most 102s * 50% = 51 events: 50 events = 25 calls x 2
    # > (not one call per iteration on top, as `ntot // niter + 1` had it)
    assert (opt["njobs"], opt["ncall"], opt["niter"], opt["ntot_job"]) == (20, 25, 2, 50)
    assert opt["T_job"] == pytest.approx(50.0)


def test_a_part_without_new_runtime_is_sized_for_a_start_job(dispatch):
    pid = _part(dispatch, "V_1", error=1e-9)  # > already precise: excluded by the optimisation
    other = _part(dispatch, "V_2")
    _jobs(dispatch, pid, JobStatus.MERGED, 2)
    _jobs(dispatch, other, JobStatus.MERGED, 2)
    with dispatch.session as session:
        opt = dispatch._distribute_time(session, 1.0)["part"][pid]
    assert opt["njobs"] == 0 and opt["ncall"] * opt["niter"] == 10  # > ncall_start x niter
