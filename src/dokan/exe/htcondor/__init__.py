"""NNLOJET execution on HTCondor

implementation of the backend for ExecutionPolicy.HTCONDOR
"""

import json
import os
import re
import string
import subprocess
import time
from collections.abc import Callable
from pathlib import Path

from ..._types import GenericPath
from ...db._loglevel import LogLevel
from .._exe_data import ExeData
from .._executor import Executor
from .._queue import QueueSnapshot, QueueStatus

# > HTCondor `JobStatus` codes -> what dokan cares about.  Removed (3) and
# > Completed (4) jobs linger in the queue for a moment after they finish; while
# > they are listed they still count as "not yet gone", which errs on the safe side.
_CONDOR_STATUS: dict[int, QueueStatus] = {
    1: QueueStatus.IDLE,
    2: QueueStatus.RUNNING,
    5: QueueStatus.HELD,
}

# > the schedd this process submits to, resolved once per process
_LOCAL_SCHEDD: str | None = None


def _local_schedd() -> str:
    """Name of the schedd `condor_submit` talks to here ("" when unknown).

    Pools like CERN's attach every login node to one of several schedds, so a
    `condor_q` issued elsewhere does not see what was submitted here.  The name is
    recorded with every detached submission and the query is directed at it.
    """
    global _LOCAL_SCHEDD
    if _LOCAL_SCHEDD is None:
        try:
            out = subprocess.run(["condor_config_val", "SCHEDD_HOST"], capture_output=True, text=True)
            _LOCAL_SCHEDD = out.stdout.strip() if out.returncode == 0 else ""
        except OSError:
            _LOCAL_SCHEDD = ""
    return _LOCAL_SCHEDD


class HTCondorExec(Executor):
    """Task to execute batch jobs on HTCondor

    Attributes
    ----------
    _file_sub : str
        name of the HTCondor submisison file
    """

    _file_sub: str = "job.sub"

    # @todo consider using `concurrency_limits` instead?
    @property
    def resources(self):  # type: ignore
        return {"jobs_concurrent": self.njobs}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.htcondor_template: Path = Path(
            self.exe_data["policy_settings"]["htcondor_template"]
            if "htcondor_template" in self.exe_data["policy_settings"]
            else self.templates()[0]  # default to first template
        )
        self.file_sub: Path = Path(self.path) / self._file_sub
        self.njobs: int = len(self.exe_data["jobs"])
        self.nactive: int = self.njobs  # decrements as condor jobs complete

    @staticmethod
    def templates() -> list[GenericPath]:
        template_list: list[str] = ["htcondor.template", "lxplus.template"]
        return [Path(__file__).parent.resolve() / t for t in template_list]

    # ---------------------------------------------------------------------------
    # > queue API (detached execution)
    # ---------------------------------------------------------------------------

    def is_submitted(self, exe_data: ExeData | None = None) -> bool:
        settings: dict = (exe_data if exe_data is not None else self.exe_data).get("policy_settings", {})
        return int(settings.get("htcondor_id", 0) or 0) > 0

    @property
    def _start_seed(self) -> int:
        return min(int(job["seed"]) for job in self.exe_data["jobs"].values())

    @staticmethod
    def _condor_q(
        args: list[str], nretry: int, retry_delay: float, log: Callable[[str, LogLevel], None]
    ) -> list[tuple[str, int, int, int, str]]:
        """`(schedd, cluster, proc, status, iwd)` for every job `condor_q args` lists.

        Retries with the same backoff as the tracker; raises once the retries are
        exhausted, because a reconciliation must not mistake "could not ask" for
        "nothing there".
        """
        cmd: list[str] = ["condor_q", *args, "-nobatch", "-af:t", "GlobalJobId", "JobStatus", "Iwd"]
        last: str = ""
        for iretry in range(max(1, nretry)):
            try:
                condor_q = subprocess.run(cmd, capture_output=True, text=True)
            except OSError as exc:
                raise RuntimeError(f"cannot run condor_q: {exc}") from exc
            if condor_q.returncode == 0:
                rows: list[tuple[str, int, int, int, str]] = []
                for line in condor_q.stdout.splitlines():
                    fields = line.rstrip("\n").split("\t")
                    if len(fields) < 3:
                        continue
                    # > GlobalJobId = "<schedd>#<cluster>.<proc>#<submit time>"
                    match = re.match(r"^([^#]+)#(\d+)\.(\d+)#", fields[0])
                    if not match:
                        continue
                    try:
                        status: int = int(fields[1])
                    except ValueError:
                        continue
                    rows.append((match.group(1), int(match.group(2)), int(match.group(3)), status, fields[2]))
                return rows
            last = f"{condor_q.stdout}\n{condor_q.stderr}"
            log(
                f"HTCondorExec: condor_q {' '.join(args)} failed (attempt {iretry + 1}/{nretry}):\n{last}",
                LogLevel.INFO,
            )
            time.sleep(retry_delay * 1.5**iretry)
        raise RuntimeError(f"condor_q {' '.join(args)} failed after {nretry} attempt(s):\n{last}")

    @classmethod
    def queue_snapshot(cls, exe_datas: list[ExeData], log: Callable[[str, LogLevel], None]) -> QueueSnapshot:
        """One `condor_q` per schedd that holds any of `exe_datas`, plus the local one.

        Batches that predate the schedd bookkeeping (submitted by the live
        orchestrator) can be on any schedd of the pool; for those one `-global`
        query is added.  A failing `-global` is not fatal -- it only disables the
        reconciliation of exactly those batches (`seeds_in_queue` raises for them)
        -- whereas a named schedd that cannot be reached fails the snapshot.
        """
        nretry: int = 3
        retry_delay: float = 10.0
        schedds: set[str] = set()
        legacy: bool = False
        for exe_data in exe_datas:
            settings: dict = exe_data.get("policy_settings", {})
            if int(settings.get("htcondor_id", 0) or 0) <= 0:
                continue
            nretry = max(nretry, int(settings.get("htcondor_nretry", nretry) or nretry))
            retry_delay = float(settings.get("htcondor_retry_delay", retry_delay) or retry_delay)
            schedd: str = str(settings.get("htcondor_schedd", "") or "")
            if schedd:
                schedds.add(schedd)
            else:
                legacy = True
        if local := _local_schedd():
            schedds.add(local)

        snapshot = QueueSnapshot()
        seen: set[tuple[str, int, int]] = set()

        def ingest(rows: list[tuple[str, int, int, int, str]]) -> None:
            for schedd, cluster, proc, status, iwd in rows:
                if (schedd, cluster, proc) in seen:
                    continue
                seen.add((schedd, cluster, proc))
                snapshot.add(
                    iwd,
                    proc,
                    _CONDOR_STATUS.get(status, QueueStatus.OTHER),
                    batch_id=cluster,
                    scheduler=schedd,
                )

        for schedd in sorted(schedds):
            ingest(cls._condor_q(["-name", schedd], nretry, retry_delay, log))
            snapshot.queried.append(schedd)
        if legacy:
            try:
                ingest(cls._condor_q(["-global"], nretry, retry_delay, log))
                snapshot.queried.append("-global")
            except RuntimeError as exc:
                log(
                    "HTCondorExec: condor_q -global failed; batches without a recorded schedd"
                    f" are skipped: {exc}",
                    LogLevel.WARN,
                )
        if not schedds and not legacy:
            # > nothing recorded and no local schedd name: the plain default query
            ingest(cls._condor_q([], nretry, retry_delay, log))
            snapshot.queried.append("(default)")
        return snapshot

    def _snapshot_covers(self, snapshot: QueueSnapshot) -> bool:
        """Whether `snapshot` asked the schedd this batch was submitted to."""
        schedd: str = str(self.exe_data["policy_settings"].get("htcondor_schedd", "") or "")
        if schedd:
            return schedd in snapshot.queried
        return "-global" in snapshot.queried or "(default)" in snapshot.queried

    def seeds_in_queue(self, snapshot: QueueSnapshot) -> dict[int, QueueStatus] | None:
        key: str | None = snapshot.lookup(self.exe_data.path)
        if key is not None:
            start: int = self._start_seed
            return {start + proc: status for proc, status in snapshot.jobs[key].items()}
        if not self.is_submitted():
            return None
        if not self._snapshot_covers(snapshot):
            raise LookupError(
                f"batch {self.exe_data.path} is on a schedd the snapshot did not cover"
                f" ({self.exe_data['policy_settings'].get('htcondor_schedd') or 'unrecorded'})"
            )
        return {}

    def adopt_submission(self, snapshot: QueueSnapshot) -> bool:
        if self.is_submitted():
            return False
        key: str | None = snapshot.lookup(self.exe_data.path)
        if key is None:
            return False
        self.exe_data["policy_settings"]["htcondor_id"] = snapshot.batch_id[key]
        if schedd := snapshot.scheduler.get(key):
            self.exe_data["policy_settings"]["htcondor_schedd"] = schedd
        self.exe_data.write()
        self._logger(
            f"HTCondorExec adopted cluster {snapshot.batch_id[key]} found in the queue", LogLevel.WARN
        )
        return True

    def release_held(self, snapshot: QueueSnapshot) -> int:
        seeds = self.seeds_in_queue(snapshot)
        n_held: int = sum(1 for status in (seeds or {}).values() if status == QueueStatus.HELD)
        if n_held == 0:
            return 0
        cluster: int = int(self.exe_data["policy_settings"]["htcondor_id"])
        cmd: list[str] = ["condor_release"]
        if schedd := self.exe_data["policy_settings"].get("htcondor_schedd"):
            cmd += ["-name", str(schedd)]
        cmd.append(str(cluster))
        condor_release = subprocess.run(cmd, capture_output=True, text=True)
        self._logger(
            (
                "HTCondorExec released held jobs"
                if condor_release.returncode == 0
                else "HTCondorExec failed to release held jobs"
            )
            + f" [dim](job_id={cluster}, held={n_held})[/dim]"
            + (
                ""
                if condor_release.returncode == 0
                else f":\n{condor_release.stdout}\n{condor_release.stderr}"
            ),
            LogLevel.INFO,
        )
        return n_held

    # ---------------------------------------------------------------------------

    def exe(self):
        # > recovery mode
        if (
            "htcondor_id" in self.exe_data["policy_settings"]
            and self.exe_data["policy_settings"]["htcondor_id"] > 0
        ):
            if not self.detached:
                self._track_job()
            return

        # > populate the submission template file
        condor_settings: dict = {
            "exe": self.exe_data["exe"],
            "job_path": str(self.exe_data.path.absolute()),
            "ncores": self.exe_data["policy_settings"].get("htcondor_ncores", 1),
            "start_seed": min(job["seed"] for job in self.exe_data["jobs"].values()),
            "nseed": self.njobs,
            "input_files": ", ".join(self.exe_data["input_files"]),
            "max_runtime": int(self.exe_data["policy_settings"]["max_runtime"]),
        }
        with open(self.htcondor_template) as t, open(self.file_sub, "w") as f:
            f.write(string.Template(t.read()).substitute(condor_settings))

        job_env = os.environ.copy()
        job_env["OMP_NUM_THREADS"] = f"{condor_settings['ncores']}"
        job_env["OMP_STACKSIZE"] = "1024M"

        cluster_id: int = -1  # init failed state
        re_cluster_id = re.compile(r".*job\(s\) submitted to cluster\s+(\d+).*", re.DOTALL)

        for _ in range(self.exe_data["policy_settings"]["htcondor_nretry"]):
            condor_submit = subprocess.run(
                ["condor_submit", HTCondorExec._file_sub],
                env=job_env,
                cwd=self.exe_data.path,
                capture_output=True,
                text=True,
            )
            if condor_submit.returncode == 0 and (match_id := re.match(re_cluster_id, condor_submit.stdout)):
                cluster_id = int(match_id.group(1))
                self.exe_data["policy_settings"]["htcondor_id"] = cluster_id
                # > which schedd took it: a detached reconciliation may run on another
                # > login node, whose default schedd is a different one
                if schedd := _local_schedd():
                    self.exe_data["policy_settings"]["htcondor_schedd"] = schedd
                self.exe_data.write()
                break
            else:
                self._logger(
                    f"HTCondorExec failed to submit job {self.exe_data.path}:\n"
                    + f"{condor_submit.stdout}\n"
                    + f"{condor_submit.stderr}",
                    LogLevel.INFO,
                )
                time.sleep(self.exe_data["policy_settings"]["htcondor_retry_delay"])

        if cluster_id < 0:
            self._logger(
                f"HTCondorExec failed to submit job (exhausted max retries) {self.exe_data.path}",
                LogLevel.WARN,
            )
            return  # failed job

        if self.detached:
            self._logger(f"HTCondorExec submitted cluster {cluster_id} [dim](detached)[/dim]", LogLevel.DEBUG)
            return

        # > now we need to track the job
        self._track_job()

    def _track_job(self):
        job_id: int = self.exe_data["policy_settings"]["htcondor_id"]
        poll_time: float = self.exe_data["policy_settings"]["htcondor_poll_time"]
        nretry: int = max(1, int(self.exe_data["policy_settings"]["htcondor_nretry"]))
        retry_delay: float = self.exe_data["policy_settings"]["htcondor_retry_delay"]

        while True:
            time.sleep(poll_time)

            # > `condor_q -json` emits a JSON *array* of classads (empty stdout when
            # > the job left the queue); an empty list after the retry loop means
            # > "query failed" and is handled as give-up below.
            condor_q_json: list = []
            for iretry in range(nretry):
                condor_q = subprocess.run(["condor_q", "-json", str(job_id)], capture_output=True, text=True)
                if condor_q.returncode == 0:
                    if condor_q.stdout == "":
                        return  # job terminated: no longer in queue
                    try:
                        condor_q_json = json.loads(condor_q.stdout)
                        break
                    except json.JSONDecodeError as exc:
                        self._logger(
                            f"HTCondorExec got unparseable condor_q output [dim](job_id={job_id})[/dim]:"
                            + f" {exc}\n{condor_q.stdout}",
                            LogLevel.INFO,
                        )
                else:
                    self._logger(
                        f"HTCondorExec failed to query job [dim](job_id={job_id})[/dim]:\n"
                        + f"{condor_q.stdout}\n"
                        + f"{condor_q.stderr}",
                        LogLevel.INFO,
                    )
                time.sleep(retry_delay * 1.5**iretry)  # exponential backoff

            # > "JobStatus" codes
            # >  0 Unexpanded  U
            # >  1 Idle  I
            # >  2 Running R
            # >  3 Removed X
            # >  4 Completed C
            # >  5 Held  H
            # >  6 Submission_err  E
            count_status = [0] * 7
            for entry in condor_q_json:
                istatus = entry["JobStatus"]
                count_status[istatus] += 1
            njobs = sum(count_status)
            # print(
            #     "job[{:d}] status: R:{:d}  I:{:d}  [total:{:d}]".format(
            #         job_id, count_status[2], count_status[1], njobs
            #     )
            # )

            # > release resources for completed jobs so Luigi can schedule other tasks
            n_active = count_status[1] + count_status[2]  # Idle + Running
            n_completed = self.nactive - n_active
            if n_completed > 0:
                self.decrease_running_resources({"jobs_concurrent": n_completed})  # type: ignore[attr-defined]
                self.nactive = n_active

            if count_status[5] > 0:
                condor_release = subprocess.run(
                    ["condor_release", str(job_id)], capture_output=True, text=True
                )
                if condor_release.returncode == 0:
                    self._logger(
                        "HTCondorExec released held jobs"
                        + f" [dim](job_id={job_id}, held={count_status[5]})[/dim]",
                        LogLevel.INFO,
                    )
                    # > released jobs go back to idle/running: keep polling until they
                    # > actually terminate (stopping here would finalize ExeData while
                    # > jobs are still producing output on the cluster)
                    continue
                self._logger(
                    "HTCondorExec failed to release held jobs"
                    + f" [dim](job_id={job_id}, held={count_status[5]})[/dim]:\n"
                    + f"{condor_release.stdout}\n"
                    + f"{condor_release.stderr}",
                    LogLevel.INFO,
                )

            if njobs == 0:
                # > reached only when the query loop exhausted its retries (empty payload):
                # > give up tracking; output scanning determines the per-job outcome downstream
                self._logger(f"HTCondorExec failed to query job {job_id} with njobs = {njobs}", LogLevel.WARN)
                return
