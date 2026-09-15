"""`nnlojet-run tick`: one round of a campaign, without a live orchestrator.

The live `submit` keeps a process alive for the whole campaign so that it can
*watch* batch jobs finish.  A tick instead *asks* what finished, does everything
that can be done without waiting, and exits -- to be run again by a scheduler
(`acron` at CERN, `cron` elsewhere) every 15-30 minutes.  `doc/tick_mode.md` has
the reasoning; this module has the mechanics:

1. take the lease (`tick.lease`) so ticks never overlap;
2. reconcile: one batch-system query, then every in-flight batch is either
   finalized (gone from the queue), advanced (seeds that finished early are
   marked done) or left alone (still queued);
3. merge whatever gained data, through the same Luigi tasks the live run uses;
4. dispatch: warmup / pre-production steps or one production wave, through the
   same dispatcher, with executors that submit and return (`Executor.detached`);
5. once dispatch is settled, the final merge and the completion signal.

Everything a tick decides is derived from the database and the execution
directories; the only state of its own is the campaign tag in `tick.json` and the
lease.  Killing a tick at any point loses nothing: the next one re-derives.
"""

from __future__ import annotations

import datetime
import json
import multiprocessing
import os
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path

import luigi
from luigi.execution_summary import LuigiStatusCode
from rich.console import Console
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .__about__ import __version__
from .db import DBTask, Job, JobStatus, Log, MergeAll, MergeFinal, MergePart, Part
from .db._dbdispatch import DBDispatch
from .db._dbrunner import _log_line_is_current
from .db._loglevel import LogLevel
from .entry import merge_config_reset_tag
from .exe import ExecutionMode, ExecutionPolicy, Executor, ExeData, QueueSnapshot, QueueStatus
from .monitor import Monitor
from .preproduction import JobRef, PreProduction
from .scheduler import WorkerSchedulerFactory
from .util import read_json_sidecar, write_json_sidecar

_STATE_FILE: str = "tick.json"
_LEASE_FILE: str = "tick.lease"

_console = Console()


# ---------------------------------------------------------------------------
# > lease & campaign state
# ---------------------------------------------------------------------------


class TickLease:
    """Mutual exclusion between ticks, as a file created with `O_EXCL`.

    `flock` is not relied upon: ticks may run on different hosts (a scheduler
    picks a login node), and advisory locks across AFS clients are not something
    to build a campaign on.  Exclusive creation is atomic on every filesystem
    dokan runs on, and the lease carries enough to recognise a dead owner: a
    process on this host that no longer exists, or any owner older than
    `timeout` seconds (a tick that died on another host, or one hung beyond what
    a tick may reasonably take).
    """

    def __init__(self, path: Path, timeout: float):
        self.path: Path = path
        self.timeout: float = timeout
        self.held: bool = False

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _stale(self, info: dict | None) -> bool:
        if not isinstance(info, dict):
            # > unreadable lease: judge by the file's age alone
            try:
                return (time.time() - self.path.stat().st_mtime) > self.timeout
            except FileNotFoundError:
                return True
        started: float = float(info.get("started") or 0.0)
        if (time.time() - started) > self.timeout:
            return True
        if info.get("host") == socket.gethostname():
            try:
                return not self._pid_alive(int(info.get("pid") or 0))
            except (TypeError, ValueError):
                return True
        return False

    def acquire(self) -> str | None:
        """Take the lease; return a reason string when it is legitimately held elsewhere."""
        payload = {"host": socket.gethostname(), "pid": os.getpid(), "started": time.time()}
        for _ in range(2):
            try:
                fd: int = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            except FileExistsError:
                info = read_json_sidecar(self.path)
                if self._stale(info):
                    self.path.unlink(missing_ok=True)
                    continue
                since: str = (
                    datetime.datetime.fromtimestamp(float(info["started"])).strftime("%Y-%m-%d %H:%M:%S")
                    if isinstance(info, dict) and info.get("started")
                    else "?"
                )
                owner: str = f"pid {info.get('pid')} on {info.get('host')}" if isinstance(info, dict) else "?"
                return f"lease held by {owner} since {since}"
            with os.fdopen(fd, "w") as lease:
                json.dump(payload, lease)
            self.held = True
            return None
        return "lease could not be acquired"

    def release(self) -> None:
        if self.held:
            self.path.unlink(missing_ok=True)
            self.held = False

    def __enter__(self) -> TickLease:
        return self

    def __exit__(self, *_exc) -> None:
        self.release()


def campaign_tag(run_path: Path) -> tuple[float, bool]:
    """The campaign's `run_tag`, created on first use: `(tag, created_now)`.

    A live `submit` mints a fresh `run_tag` per process, and the job rows it
    creates carry it: the dispatcher only ever looks at rows of its own tag, the
    job cap is counted per tag.  Ticks are many short processes standing in for
    one long one, so they must share a tag -- and it must survive across ticks,
    hence the sidecar.  Deleting `tick.json` starts a new "submission" the same
    way a new `submit` would (active jobs are adopted, queued ones re-planned).
    """
    state_path: Path = run_path / _STATE_FILE
    state = read_json_sidecar(state_path)
    if isinstance(state, dict) and isinstance(state.get("run_tag"), (int, float)):
        return float(state["run_tag"]), False
    tag: float = time.time()
    write_json_sidecar(state_path, {"run_tag": tag, "created": tag, "dokan_version": __version__})
    return tag, True


# ---------------------------------------------------------------------------
# > the tick
# ---------------------------------------------------------------------------


@dataclass
class TickReport:
    """What one tick did, for the summary line and for tests."""

    phase: str = ""
    n_batches: int = 0  # in flight at the start of the tick
    n_finalized: int = 0  # batches gone from the queue and collected
    n_done: int = 0  # jobs flipped to DONE (finalized batches + early seeds)
    n_failed: int = 0
    n_early: int = 0  # seeds marked DONE while their batch is still queued
    n_skipped: int = 0  # batches that could not be reconciled this tick
    n_held_released: int = 0
    queue: dict[QueueStatus, int] = field(default_factory=dict)
    n_dispatched: int = 0
    n_merged_parts: int = 0
    settled: bool = False
    messages: list[str] = field(default_factory=list)

    @property
    def n_in_queue(self) -> int:
        return sum(self.queue.values())


class Tick(DBTask):
    """One tick of a campaign.  Not scheduled by Luigi: `run_once()` drives it."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger_prefix: str = "Tick"

    def complete(self) -> bool:  # > never "done": every tick starts over
        return False

    # -----------------------------------------------------------------------
    # > helpers
    # -----------------------------------------------------------------------

    def _log(self, message: str, level: LogLevel = LogLevel.INFO) -> None:
        with self.session as session:
            self._logger(session, message, level=level)

    def _executor(self, policy: ExecutionPolicy, exe_dir: Path) -> Executor:
        """A detached executor for `exe_dir`, with its metadata as it is on disk *now*.

        Luigi's task registry hands back the instance it already built for these
        parameters, and that instance loaded its `ExeData` when it was first
        constructed -- before the forked worker that ran it submitted anything.
        Reload, or a batch this very tick submitted reads as unsubmitted.
        """
        exe: Executor = Executor.factory(
            policy=policy,
            path=str(exe_dir.absolute()),
            log_level=self.config["ui"]["log_level"],
            detached=True,
        )
        exe.exe_data.load()
        return exe

    def _luigi(
        self, tasks: list[luigi.Task], *, workers: int, jobs_concurrent: int, merge_concurrent: int
    ) -> bool:
        """Run `tasks` to completion in this process's Luigi worker pool."""
        if not tasks:
            return True
        result = luigi.build(
            tasks,
            worker_scheduler_factory=WorkerSchedulerFactory(
                resources={
                    "local_ncores": max(1, workers),
                    "jobs_concurrent": max(1, jobs_concurrent),
                    "DBTask": workers + 2,
                    "DBDispatch": 1,
                    "merge_concurrent": max(1, merge_concurrent),
                },
                cache_task_completion=False,  # needed for MergePart
                check_complete_on_run=False,
                check_unfulfilled_deps=True,
                # > the factory's short intervals: a tick's tasks are seconds long and
                # > the process does not live long enough for the idle overhead to matter
                wait_interval=0.1,
                ping_interval=0.1,
            ),
            detailed_summary=True,
            workers=max(1, workers),
            local_scheduler=True,
            log_level="WARNING",
        )
        ok: bool = result.status in (LuigiStatusCode.SUCCESS, LuigiStatusCode.SUCCESS_WITH_RETRY)
        if not ok:
            self._log(
                f"{self._logger_prefix}::luigi:  {result.status.value[1]}\n{result.summary_text}",
                LogLevel.WARN,
            )
        return ok

    def campaign_complete(self, session: Session) -> bool:
        last_sig = session.scalars(select(Log).where(Log.level < 0).order_by(Log.id.desc())).first()
        return last_sig is not None and last_sig.level == LogLevel.SIG_COMP

    def reopen(self) -> int:
        """Downgrade the terminal signals so dispatch can resume (budget raised, target lowered).

        The live `submit` achieves the same by clearing the log at startup; a tick
        never clears the log, so the signals are demoted in place, audit trail kept.
        """
        n: int = 0
        with self.session as session:
            for sig in session.scalars(
                select(Log).where(Log.level.in_([LogLevel.SIG_DISPATCH_DONE, LogLevel.SIG_COMP]))
            ):
                sig.level = LogLevel.INFO
                sig.message = "[reopened] " + sig.message
                n += 1
            if n:
                self._safe_commit(session)
                self._logger(session, f"{self._logger_prefix}::reopen:  demoted {n} terminal signal(s)")
        return n

    # -----------------------------------------------------------------------
    # > 1. normalise leftovers of previous ticks / a dead submit
    # -----------------------------------------------------------------------

    def _normalise(self, session: Session) -> None:
        """Make the job table say what is true before anything is derived from it.

        * seeds assigned but never staged (`DISPATCHED`, no `rel_path`): the
          runner never ran -- back to `QUEUED`, seed released;
        * queued rows of *another* submission: a dead `submit`'s plan, purged the
          way `submit` itself purges them at startup;
        * active rows of another submission *with* a directory: real batch jobs,
          adopted into this campaign so the dispatcher counts and settles on them
          (`submit` keeps production resurrections on their old tag to spare its
          per-submission job cap; a tick's tag is the campaign, so there is
          nothing to spare);
        * `RECOVER` rows: a `submit` died mid-resurrection; they are running batch
          jobs like any other.
        """
        n_requeued = n_purged = n_adopted = 0
        for job in session.scalars(
            select(Job).where(Job.rel_path.is_(None)).where(Job.status.in_(JobStatus.active_list()))
        ):
            if job.run_tag != self.run_tag:
                session.delete(job)
                n_purged += 1
            elif job.status != JobStatus.QUEUED:
                job.status = JobStatus.QUEUED
                job.seed = None
                n_requeued += 1
        for job in session.scalars(
            select(Job).where(Job.rel_path.is_not(None)).where(Job.status.in_(JobStatus.active_list()))
        ):
            if job.status == JobStatus.QUEUED:
                continue  # > cannot happen (rel_path is set together with RUNNING); leave it to doctor
            if job.status == JobStatus.RECOVER:
                job.status = JobStatus.RUNNING
            if job.run_tag != self.run_tag:
                job.run_tag = self.run_tag
                n_adopted += 1
        if n_requeued or n_purged or n_adopted:
            self._safe_commit(session)
            self._logger(
                session,
                f"{self._logger_prefix}::normalise:  re-queued {n_requeued}, purged {n_purged} stale queued,"
                f" adopted {n_adopted} active job(s) from another submission",
            )

    # -----------------------------------------------------------------------
    # > 2. reconcile
    # -----------------------------------------------------------------------

    def _inflight_batches(
        self, session: Session
    ) -> dict[str, tuple[ExecutionPolicy, ExecutionMode, list[int]]]:
        rows = session.execute(
            select(Job.rel_path, Job.policy, Job.mode, Job.id)
            .where(Job.rel_path.is_not(None))
            .where(Job.status.in_([JobStatus.DISPATCHED, JobStatus.RUNNING]))
            .order_by(Job.rel_path, Job.id)
        ).all()
        batches: dict[str, tuple[ExecutionPolicy, ExecutionMode, list[int]]] = {}
        for rel_path, policy, mode, job_id in rows:
            entry = batches.setdefault(rel_path, (ExecutionPolicy(policy), ExecutionMode(mode), []))
            entry[2].append(job_id)
        return batches

    def _echo_exe_log(self, session: Session, exe_data: ExeData, label: str) -> None:
        """Forward the executor's own log for this batch into the workflow log (as `DBRunner` does)."""
        exe_log: Path = exe_data.path / Executor._file_log
        if not exe_log.exists():
            return
        job_start: float = float(exe_data.get("timestamp") or 0.0)
        with open(exe_log) as f:
            lines = [ln for ln in f.readlines() if _log_line_is_current(ln, job_start)]
        if lines:
            self._logger(
                session,
                f"{self._logger_prefix}::reconcile:  {label} executor log [dim]({exe_log})[/dim]:\n"
                + "\n".join(f" | [dim]{ln.strip()}[/dim]" for ln in lines),
            )

    def _count_status(self, session: Session, ids: list[int]) -> tuple[int, int]:
        rows = session.execute(
            select(Job.status, func.count(Job.id)).where(Job.id.in_(ids)).group_by(Job.status)
        ).all()
        n_done = sum(n for status, n in rows if status in JobStatus.success_list())
        n_failed = sum(n for status, n in rows if status == JobStatus.FAILED)
        return n_done, n_failed

    def reconcile(self, report: TickReport) -> list[Executor]:
        """Bring the job table up to date with the batch system and the disk.

        Returns the executors of staged batches that are *not* in the queue and
        carry no batch handle: their submission never happened (or failed) and the
        dispatch step re-submits them.
        """
        with self.session as session:
            self._normalise(session)
            batches = self._inflight_batches(session)
        report.n_batches = len(batches)

        # > load every in-flight batch; sort out what cannot be reconciled at all
        executors: dict[str, Executor] = {}
        with self.session as session:
            for rel_path, (policy, _mode, ids) in batches.items():
                exe_dir: Path = self._local(rel_path)
                if not exe_dir.is_dir() or not (
                    (exe_dir / ExeData._file_tmp).exists() or (exe_dir / ExeData._file_fin).exists()
                ):
                    self._logger(
                        session,
                        f"{self._logger_prefix}::reconcile:  {rel_path}: execution directory or"
                        f" metadata missing -> {len(ids)} job(s) FAILED",
                        LogLevel.WARN,
                    )
                    for job in session.scalars(select(Job).where(Job.id.in_(ids))):
                        job.status = JobStatus.FAILED
                    self._safe_commit(session)
                    report.n_failed += len(ids)
                    continue
                if policy == ExecutionPolicy.LOCAL:
                    self._logger(
                        session,
                        f"{self._logger_prefix}::reconcile:  {rel_path}: local execution cannot be reconciled"
                        " by a tick (use `submit` or `doctor --scan-dir`)",
                        LogLevel.WARN,
                    )
                    report.n_skipped += 1
                    continue
                executors[rel_path] = self._executor(policy, exe_dir)

        # > one query per policy (one per schedd inside it)
        snapshots: dict[ExecutionPolicy, QueueSnapshot] = {}
        for policy in {ExecutionPolicy(batches[rp][0]) for rp in executors}:
            exe_datas = [exe.exe_data for rp, exe in executors.items() if batches[rp][0] == policy]
            snapshots[policy] = Executor.get_cls(policy).queue_snapshot(
                exe_datas, lambda m, lvl: self._log(m, lvl)
            )
        for policy, snapshot in snapshots.items():
            counts = snapshot.count()
            for status, n in counts.items():
                report.queue[status] = report.queue.get(status, 0) + n
            self._log(
                f"{self._logger_prefix}::reconcile:  {policy!s} queue"
                f" [dim]({', '.join(snapshot.queried) or 'default'})[/dim]:"
                f" {snapshot.n_jobs} job(s)"
                f" [idle {counts[QueueStatus.IDLE]}, running {counts[QueueStatus.RUNNING]},"
                f" held {counts[QueueStatus.HELD]}, other {counts[QueueStatus.OTHER]}]",
            )

        unsubmitted: list[Executor] = []
        for rel_path, exe in executors.items():
            policy, mode, ids = batches[rel_path]
            snapshot = snapshots[policy]
            label: str = f"{Path(rel_path).parent.name}/{Path(rel_path).name}"

            if exe.exe_data.is_final:
                # > collected already (a previous tick died between finalize and the DB update)
                with self.session as session:
                    self._update_job(session, exe.exe_data, {job_id: None for job_id in ids})
                    n_done, n_failed = self._count_status(session, ids)
                    self._logger(
                        session,
                        f"{self._logger_prefix}::reconcile:  {label}: already final"
                        f" -> {n_done} done, {n_failed} failed",
                    )
                report.n_finalized += 1
                report.n_done += n_done
                report.n_failed += n_failed
                continue

            if not exe.is_submitted():
                # > no handle on disk: either the submission never happened (or failed),
                # > or it happened and the write-back did not -- the queue tells which
                exe.adopt_submission(snapshot)
            try:
                seeds = exe.seeds_in_queue(snapshot)
            except LookupError as exc:
                self._log(f"{self._logger_prefix}::reconcile:  {label}: skipped ({exc})", LogLevel.WARN)
                report.n_skipped += 1
                continue
            if seeds is None:
                unsubmitted.append(exe)
                continue

            if not seeds:
                # > gone from the queue: collect, finalize, record
                try:
                    exe.finish()
                except Exception as exc:
                    self._log(
                        f"{self._logger_prefix}::reconcile:  {label}: collecting outputs failed: {exc!r}",
                        LogLevel.ERROR,
                    )
                    report.n_skipped += 1
                    continue
                with self.session as session:
                    self._echo_exe_log(session, exe.exe_data, label)
                    self._update_job(session, exe.exe_data, {job_id: None for job_id in ids})
                    n_done, n_failed = self._count_status(session, ids)
                    self._logger(
                        session,
                        f"{self._logger_prefix}::reconcile:  {label}: finished"
                        f" -> {n_done} done, {n_failed} failed [dim](job_ids = {ids})[/dim]",
                    )
                report.n_finalized += 1
                report.n_done += n_done
                report.n_failed += n_failed
                continue

            # > still queued (at least partly)
            report.n_held_released += exe.release_held(snapshot)
            if mode != ExecutionMode.PRODUCTION:
                continue  # > a warmup step is collected whole (its grid is adapted from all seeds)
            # > seeds no longer queued with a parseable result: mark them DONE now.  The
            # > in-flight count then tracks the queue seed by seed, and their data joins
            # > the next merge instead of waiting for the batch's slowest job.  A seed
            # > gone *without* a result is left alone: the full collection, with its
            # > filesystem retries, decides once the batch is gone.
            exe.exe_data.scan_dir([Executor._file_log])
            early: dict[int, None] = {}
            for raw_id, entry in exe.exe_data.get("jobs", {}).items():
                job_id = int(raw_id)
                if job_id in ids and int(entry["seed"]) not in seeds and "result" in entry:
                    early[job_id] = None
            if early:
                exe.exe_data.write()
                with self.session as session:
                    self._update_job(session, exe.exe_data, early)
                    n_done, _ = self._count_status(session, list(early))
                    self._logger(
                        session,
                        f"{self._logger_prefix}::reconcile:  {label}: {n_done}/{len(ids)} seed(s)"
                        f" finished early [dim]({len(seeds)} still queued)[/dim]",
                    )
                report.n_early += n_done
                report.n_done += n_done
        return unsubmitted

    # -----------------------------------------------------------------------
    # > 3. merge
    # -----------------------------------------------------------------------

    def _preproductions(self, session: Session) -> list[PreProduction]:
        return [
            self.clone(cls=PreProduction, part_id=pt.id)
            for pt in session.scalars(select(Part).where(Part.active.is_(True)).order_by(Part.id))
        ]

    def _needs_preproduction_merge(self, session: Session) -> bool:
        """Every part must carry a merged result before production can be planned.

        `Entry` stage 2 forces one `MergeAll` once all pre-productions are in; the
        equivalent here is derivable: a part without a completed merge has
        `ntot == 0` (or the in-progress sentinel `timestamp < 0`), and
        `_distribute_time` cannot allocate to a part whose error is still infinite.
        """
        return any(
            pt.ntot <= 0 or pt.timestamp < 0.0
            for pt in session.scalars(select(Part).where(Part.active.is_(True)))
        )

    def merge_tasks(self, *, production: bool) -> list[luigi.Task]:
        reset_tag: float = merge_config_reset_tag(
            self.config, self._path / "result" / "merge-config.json", time.time()
        )
        tasks: list[luigi.Task] = []
        with self.session as session:
            if not production:
                return tasks
            if self._needs_preproduction_merge(session):
                # > `Entry` stage 2: the one forced merge of every part
                tasks.append(self.clone(MergeAll, force=True, reset_tag=reset_tag))
                return tasks
            for pt in session.scalars(select(Part).where(Part.active.is_(True)).order_by(Part.id)):
                candidate = self.clone(MergePart, force=False, reset_tag=reset_tag, part_id=pt.id)
                if not candidate.complete():
                    tasks.append(candidate)
            # > out-of-band requests: `signal merge`, the periodic finalize
            dispatcher = self.clone(DBDispatch, id=0, _n=0)
            tasks.extend(dispatcher._consume_dispatch_signals(session))
        return tasks

    # -----------------------------------------------------------------------
    # > 4. dispatch
    # -----------------------------------------------------------------------

    def preproduction_tasks(self) -> tuple[list[luigi.Task], int]:
        """Bounded dispatches for every part still in warmup / pre-production.

        `PreProduction.run()` is a generator that yields a dispatch and then a
        *resurrection* of the step -- which would track it.  Its decision methods
        are reused directly instead: they queue the next step when the part is
        ready for one, and say which job to dispatch.  Returns `(tasks, pending)`
        with `pending` the number of parts not yet through pre-production.
        """
        tasks: list[luigi.Task] = []
        pending: int = 0
        with self.session as session:
            preprods = self._preproductions(session)
        for preprod in preprods:
            if preprod.complete():
                continue
            pending += 1
            with self.session as session:
                preprod._part_name(preprod.part_id, session)
                step = preprod._warmup_step(session)
                job_ref: JobRef | None = (
                    step if isinstance(step, JobRef) else preprod._production_step(session)
                )
                if job_ref is None:
                    continue
                job = session.get(Job, job_ref.id)
                if job is not None and job.status == JobStatus.QUEUED:
                    tasks.append(self.clone(cls=DBDispatch, id=job_ref.id))
        return tasks, pending

    def settled(self, session: Session) -> bool:
        return self.clone(DBDispatch, id=0, _n=0)._dispatch_settled(session)

    def _count_dispatched(self, report: TickReport, before: set[str]) -> None:
        """Count the jobs of the batches staged by this tick that actually reached the queue.

        A runner flips its rows to RUNNING once the directory is staged, before the
        executor submits; a batch whose submission failed is therefore RUNNING in the
        database and unsubmitted on disk.  It is not "dispatched": the next tick
        re-submits it (`reconcile` returns it as such).
        """
        with self.session as session:
            batches = self._inflight_batches(session)
        n_staged: int = 0
        for rel_path, (policy, _mode, ids) in batches.items():
            if rel_path in before or policy == ExecutionPolicy.LOCAL:
                continue
            exe_dir: Path = self._local(rel_path)
            if not exe_dir.is_dir():
                continue
            exe = self._executor(policy, exe_dir)
            if exe.exe_data.is_final or exe.is_submitted():
                report.n_dispatched += len(ids)
            else:
                n_staged += len(ids)
        if n_staged:
            report.messages.append(f"{n_staged} job(s) staged but not submitted (retried next tick)")

    # -----------------------------------------------------------------------
    # > the tick
    # -----------------------------------------------------------------------

    def run_once(
        self,
        *,
        dispatch: bool = True,
        workers: int | None = None,
        merge_concurrent: int | None = None,
    ) -> TickReport:
        report = TickReport()
        cpu_count: int = multiprocessing.cpu_count()
        workers = workers if workers is not None else max(2, min(cpu_count, 8))
        merge_concurrent = merge_concurrent if merge_concurrent is not None else max(1, min(cpu_count, 8))
        jobs_concurrent: int = int(self.config["run"]["jobs_max_concurrent"])

        with self.session as session:
            if self.campaign_complete(session):
                report.phase = "complete"
                report.messages.append("campaign complete (SIG_COMP); use `tick --reopen` to continue it")
                return report
            self._logger(
                session, f"{self._logger_prefix}::run:  tick on {socket.gethostname()}", LogLevel.DEBUG
            )

        # > 2. reconcile
        unsubmitted: list[Executor] = self.reconcile(report)
        jobs_concurrent = max(jobs_concurrent, *(len(exe.exe_data["jobs"]) for exe in unsubmitted), 1)

        # > 3. merge -- the phase is decided after reconciliation, since a finished
        # > pre-production may just have completed the phase
        with self.session as session:
            production: bool = all(pp.complete() for pp in self._preproductions(session))
        report.phase = "production" if production else "preproduction"
        merges = self.merge_tasks(production=production)
        report.n_merged_parts = sum(1 for t in merges if isinstance(t, MergePart)) + sum(
            len(t.requires()) for t in merges if isinstance(t, MergeAll)
        )
        if merges:
            self._luigi(
                merges, workers=workers, jobs_concurrent=jobs_concurrent, merge_concurrent=merge_concurrent
            )

        # > 4. dispatch
        if dispatch:
            tasks: list[luigi.Task] = list(unsubmitted)
            if production:
                with self.session as session:
                    blocked: bool = self._needs_preproduction_merge(session)
                    settled: bool = self.settled(session)
                if blocked:
                    report.messages.append(
                        "pre-production merge did not complete; production dispatch deferred"
                    )
                elif not settled:
                    tasks.append(self.clone(DBDispatch, id=0, _n=0))
            else:
                preprod_tasks, pending = self.preproduction_tasks()
                tasks.extend(preprod_tasks)
                report.messages.append(f"{pending} part(s) in warmup / pre-production")
            if tasks:
                with self.session as session:
                    before: set[str] = set(self._inflight_batches(session))
                self._luigi(
                    tasks, workers=workers, jobs_concurrent=jobs_concurrent, merge_concurrent=merge_concurrent
                )
                self._count_dispatched(report, before)
        else:
            report.messages.append("dispatch disabled (--no-dispatch)")

        # > 5. settled: the end-of-run merge and the completion signal (`Entry` stage 4).
        # > This is the forced merge of what is DONE plus the per-order files, not the
        # > from-scratch re-merge of `finalize`, which stays an explicit command.
        if production:
            with self.session as session:
                report.settled = self.settled(session) and not self._needs_preproduction_merge(session)
            if report.settled:
                self._log(f"{self._logger_prefix}::run:  dispatch settled -> final merge")
                self._luigi(
                    [self.clone(MergeFinal, force=True)],
                    workers=workers,
                    jobs_concurrent=jobs_concurrent,
                    merge_concurrent=merge_concurrent,
                )
                with self.session as session:
                    if self.campaign_complete(session):
                        report.phase = "complete"
        return report


# ---------------------------------------------------------------------------
# > status (the live board, once)
# ---------------------------------------------------------------------------


def print_status(config, run_tag: float, *, n_log: int = 20, console: Console | None = None) -> None:
    """The live monitor's board and log tail, rendered once from the database."""
    console = console or _console
    monitor = Monitor(config=config, run_tag=run_tag)
    with monitor.session as session:
        monitor._init_board(session)
        last_xs = session.scalars(
            select(Log).where(Log.level == LogLevel.SIG_UPDXS).order_by(Log.id.desc())
        ).first()
        if last_xs is not None:
            monitor.cross_line = last_xs.message
            monitor.cross_time = last_xs.timestamp
        table = monitor._generate_table(session)
        logs = list(
            session.scalars(
                select(Log).where(Log.level != LogLevel.SIG_UPDXS).order_by(Log.id.desc()).limit(n_log)
            )
        )
        n_active: int = (
            session.scalar(select(func.count(Job.id)).where(Job.status.in_(JobStatus.active_list()))) or 0
        )
    console.print(table)
    console.print(f"[dim]active jobs in the database: {n_active}[/dim]")
    lease = read_json_sidecar(Path(config["run"]["path"]) / _LEASE_FILE)
    if isinstance(lease, dict):
        since = datetime.datetime.fromtimestamp(float(lease.get("started") or 0.0)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        console.print(f"[dim]tick lease: pid {lease.get('pid')} on {lease.get('host')} since {since}[/dim]")
    for log in reversed(logs):
        dt_str: str = datetime.datetime.fromtimestamp(log.timestamp).strftime("%Y-%m-%d %H:%M:%S")
        console.print(f"[dim][{dt_str}][/dim]({LogLevel(log.level)!r}): {log.message}")


def format_report(report: TickReport) -> str:
    """One line a scheduler's mail or a log file can hold."""
    parts: list[str] = [f"phase={report.phase}"]
    if report.n_batches:
        parts.append(f"batches={report.n_batches}")
    if report.n_finalized:
        parts.append(f"finalized={report.n_finalized}")
    if report.n_done:
        parts.append(f"done={report.n_done}" + (f" (early {report.n_early})" if report.n_early else ""))
    if report.n_failed:
        parts.append(f"failed={report.n_failed}")
    if report.n_skipped:
        parts.append(f"skipped={report.n_skipped}")
    if report.n_held_released:
        parts.append(f"released={report.n_held_released}")
    if report.queue:
        parts.append(
            "queue="
            + "/".join(
                f"{report.queue.get(s, 0)}{s.name[0].lower()}" for s in QueueStatus if report.queue.get(s, 0)
            )
            if report.n_in_queue
            else "queue=empty"
        )
    if report.n_merged_parts:
        parts.append(f"merged_parts={report.n_merged_parts}")
    if report.n_dispatched:
        parts.append(f"dispatched={report.n_dispatched}")
    if report.settled:
        parts.append("settled")
    return "tick: " + "  ".join(parts) + ("  |  " + "; ".join(report.messages) if report.messages else "")
