"""Parts that cannot take another production job, and how production jobs are sized.

Run with the dokan environment's interpreter, e.g.
    PYTHONPATH=src python -m pytest tests/test_production_dispatch.py
"""

import json
import math
from pathlib import Path

import pytest
from sqlalchemy import select

from dokan.config import Config
from dokan.db._dbdispatch import DBDispatch
from dokan.db._dbtask import _DBRole
from dokan.db._jobstatus import JobStatus
from dokan.db._loglevel import LogLevel
from dokan.db._sqla import DokanDB, DokanLog, Job, Log, Part
from dokan.exe import ExecutionMode
from dokan.tick import failures_from_job, forgive_failures

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


def _stuck(task: DBDispatch) -> tuple[dict[int, str], bool]:
    with task.session as session:
        return task._stuck_parts(session)


def test_a_part_with_successes_is_not_stuck(dispatch):
    pid = _part(dispatch, "V_1")
    _jobs(dispatch, pid, JobStatus.MERGED)
    _jobs(dispatch, pid, JobStatus.FAILED, 2)  # > below the limit of 3
    assert _stuck(dispatch) == ({}, False)


def test_consecutive_failures_of_this_submission_make_a_part_stuck(dispatch):
    pid = _part(dispatch, "V_1")
    _jobs(dispatch, pid, JobStatus.FAILED, 5, run_tag=OLD_TAG)  # > an earlier submission: forgotten
    _jobs(dispatch, pid, JobStatus.MERGED, run_tag=OLD_TAG)
    _jobs(dispatch, pid, JobStatus.FAILED, 3)
    stuck, stalled = _stuck(dispatch)
    assert list(stuck) == [pid] and "3 production jobs failed in a row" in stuck[pid]
    assert stalled  # > the only part


def test_a_success_resets_the_failure_count(dispatch):
    pid = _part(dispatch, "V_1")
    _jobs(dispatch, pid, JobStatus.FAILED, 3)
    _jobs(dispatch, pid, JobStatus.DONE)
    _jobs(dispatch, pid, JobStatus.FAILED, 2)
    assert _stuck(dispatch) == ({}, False)


def test_a_minimum_job_beyond_the_runtime_limit_makes_a_part_stuck(dispatch):
    pid = _part(dispatch, "V_1")
    _jobs(dispatch, pid, JobStatus.MERGED, ncall=1, elapsed=300.0)  # > 150s per event, 2 events per job
    stuck, _ = _stuck(dispatch)
    assert "job_max_runtime" in stuck[pid]


def test_stalled_only_when_no_part_can_progress_or_the_total_has_no_error(dispatch):
    ok = _part(dispatch, "V_1")
    bad = _part(dispatch, "V_2")
    _jobs(dispatch, ok, JobStatus.MERGED)
    _jobs(dispatch, bad, JobStatus.MERGED)
    _jobs(dispatch, bad, JobStatus.FAILED, 3)
    stuck, stalled = _stuck(dispatch)
    assert set(stuck) == {bad} and not stalled

    no_result = _part(dispatch, "V_3", error=math.inf)
    _jobs(dispatch, no_result, JobStatus.FAILED, 3)
    stuck, stalled = _stuck(dispatch)
    assert set(stuck) == {bad, no_result} and stalled
    assert "no result yet" in stuck[no_result]


def test_a_stuck_part_gets_no_runtime(dispatch):
    ok = _part(dispatch, "V_1")
    bad = _part(dispatch, "V_2", error=10.0)  # > would otherwise get most of the runtime
    _jobs(dispatch, ok, JobStatus.MERGED)
    _jobs(dispatch, bad, JobStatus.MERGED)
    _jobs(dispatch, bad, JobStatus.FAILED, 3)
    with dispatch.session as session:
        opt = dispatch._distribute_time(session, 1000.0)
    assert set(opt["stuck"]) == {bad} and not opt["stalled"]
    assert opt["part"][bad]["T_opt"] == 0.0 and opt["part"][bad]["njobs"] == 0
    assert opt["part"][ok]["T_opt"] == pytest.approx(1000.0)


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


def test_dispatch_stops_when_production_stalls(dispatch):
    pid = _part(dispatch, "V_1")
    _jobs(dispatch, pid, JobStatus.FAILED, 3)
    _jobs(dispatch, pid, JobStatus.QUEUED, 4)
    with dispatch.session as session:
        assert dispatch._repopulate(session) is False
        assert dispatch.part_id == 0
        assert not session.scalars(select(Job).where(Job.status == JobStatus.QUEUED)).all()
        signals = session.scalars(select(Log).where(Log.level == LogLevel.SIG_DISPATCH_DONE)).all()
        assert len(signals) == 1 and "cannot reach the target accuracy" in signals[0].message
        # > stopped: a second pass neither queues nor signals again
        assert dispatch._repopulate(session) is False
        assert len(session.scalars(select(Log).where(Log.level == LogLevel.SIG_DISPATCH_DONE)).all()) == 1


def test_a_stuck_part_loses_its_queue_and_the_others_carry_on(dispatch):
    ok = _part(dispatch, "V_1")
    bad = _part(dispatch, "V_2")
    _jobs(dispatch, ok, JobStatus.MERGED)
    _jobs(dispatch, bad, JobStatus.MERGED)
    _jobs(dispatch, bad, JobStatus.FAILED, 3)
    _jobs(dispatch, bad, JobStatus.QUEUED, 4)
    with dispatch.session as session:
        dispatch._repopulate(session)
        queued = session.scalars(select(Job).where(Job.status == JobStatus.QUEUED)).all()
        assert queued and {job.part_id for job in queued} == {ok}
        assert dispatch.part_id == ok
        warnings = session.scalars(select(Log).where(Log.level == LogLevel.WARN)).all()
        assert any("discarded 4 queued" in log.message for log in warnings)
        assert not session.scalars(select(Log).where(Log.level == LogLevel.SIG_DISPATCH_DONE)).all()


def test_failures_before_the_mark_are_forgiven(tmp_path):
    task = _make(tmp_path)
    pid = _part(task, "V_1")
    _jobs(task, pid, JobStatus.MERGED)
    _jobs(task, pid, JobStatus.FAILED, 3)  # > job ids 2-4
    assert list(_stuck(task)[0]) == [pid]
    reopened = _make(tmp_path, failures_from_job=5)
    assert _stuck(reopened) == ({}, False)
    _jobs(reopened, pid, JobStatus.FAILED, 3)  # > after the reopen: these count
    assert list(_stuck(reopened)[0]) == [pid]


def test_the_mark_lives_in_the_campaign_state(tmp_path):
    assert failures_from_job(tmp_path) == 0  # > no campaign: every failure counts
    (tmp_path / "tick.json").write_text(json.dumps({"run_tag": RUN_TAG}))
    forgive_failures(tmp_path, 42)
    assert failures_from_job(tmp_path) == 42
    assert json.loads((tmp_path / "tick.json").read_text())["run_tag"] == RUN_TAG
