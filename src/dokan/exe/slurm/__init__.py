import getpass
import os
import re
import string
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import ClassVar

from ..._types import GenericPath
from ...db._loglevel import LogLevel
from .._exe_data import ExeData
from .._executor import Executor
from .._queue import QueueSnapshot, QueueStatus

# > Slurm states -> what dokan cares about (terminal states are not queued at all)
_SLURM_STATUS: dict[str, QueueStatus] = {
    "PENDING": QueueStatus.IDLE,
    "CONFIGURING": QueueStatus.IDLE,
    "RUNNING": QueueStatus.RUNNING,
    "COMPLETING": QueueStatus.RUNNING,
}


class SlurmExec(Executor):
    _file_sub: str = "job.sub"
    # > Slurm states (`squeue --format=%T`) that will not produce further work;
    # > anything else is treated as still active.
    _state_terminal: ClassVar[set[str]] = {
        "BOOT_FAIL",
        "CANCELLED",
        "COMPLETED",
        "DEADLINE",
        "FAILED",
        "LAUNCH_FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "RECONFIG_FAIL",
        "REVOKED",
        "TIMEOUT",
    }

    @property
    def resources(self):  # type: ignore
        return {"jobs_concurrent": self.njobs}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.slurm_template: Path = Path(
            self.exe_data["policy_settings"]["slurm_template"]
            if "slurm_template" in self.exe_data["policy_settings"]
            else self.templates()[0]  # default to first template
        )
        self.file_sub: Path = self.exe_data.path / self._file_sub
        self.njobs: int = len(self.exe_data["jobs"])
        self.nactive: int = self.njobs  # decrements as slurm jobs complete

    @staticmethod
    def templates() -> list[GenericPath]:
        return [Path(__file__).parent.resolve() / "slurm.template"]

    def _format_slurm_time(self, seconds: int) -> str:
        """Format seconds to d-hh:mm:ss for SLURM."""
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        d, h = divmod(h, 24)
        return f"{d}-{h:02d}:{m:02d}:{s:02d}"

    @staticmethod
    def _normalize_state(state: str) -> str:
        """Normalize a Slurm state name from `squeue` output"""
        fields = state.split()
        return fields[0].rstrip("+").upper() if fields else ""

    @classmethod
    def _is_terminal_state(cls, state: str) -> bool:
        """Return True when a normalized Slurm state is terminal for tracking."""
        return state in cls._state_terminal

    @classmethod
    def _squeue_active_count(cls, stdout: str) -> int | None:
        """Count non-terminal array tasks in `squeue --format=%i|%T` output.

        Returns 0 for empty output and None for non-empty output
        without a single parseable state row.
        """
        if not stdout.strip():
            return 0
        n_active = 0
        parsed = False
        for line in stdout.splitlines():
            fields = line.split("|")
            if len(fields) < 2:
                continue
            state = cls._normalize_state(fields[1])
            if not state:
                continue
            parsed = True
            if not cls._is_terminal_state(state):
                n_active += 1
        return n_active if parsed else None

    def _decrease_active_resources(self, n_active: int) -> None:
        """Release Luigi resources for tasks that Slurm no longer reports as active."""
        n_completed = self.nactive - n_active
        if n_completed > 0:
            self.decrease_running_resources({"jobs_concurrent": n_completed})  # type: ignore[attr-defined]
            self.nactive = n_active

    # ---------------------------------------------------------------------------
    # > queue API (detached execution).  Untested against a live Slurm: it mirrors
    # > the HTCondor implementation and the tracker's own `squeue` usage.
    # ---------------------------------------------------------------------------

    def is_submitted(self, exe_data: ExeData | None = None) -> bool:
        settings: dict = (exe_data if exe_data is not None else self.exe_data).get("policy_settings", {})
        return int(settings.get("slurm_id", 0) or 0) > 0

    @classmethod
    def queue_snapshot(cls, exe_datas: list[ExeData], log: Callable[[str, LogLevel], None]) -> QueueSnapshot:
        """One `squeue` for every array task of ours: `(job id, task id, state, workdir)`."""
        nretry: int = 3
        retry_delay: float = 10.0
        for exe_data in exe_datas:
            settings: dict = exe_data.get("policy_settings", {})
            nretry = max(nretry, int(settings.get("slurm_nretry", nretry) or nretry))
            retry_delay = float(settings.get("slurm_retry_delay", retry_delay) or retry_delay)
        cmd: list[str] = [
            "squeue",
            "--noheader",
            "--array",
            f"--user={getpass.getuser()}",
            "--states=all",
            "--format=%A|%K|%T|%Z",
        ]
        last: str = ""
        for iretry in range(max(1, nretry)):
            try:
                squeue = subprocess.run(cmd, capture_output=True, text=True)
            except OSError as exc:
                raise RuntimeError(f"cannot run squeue: {exc}") from exc
            if squeue.returncode == 0:
                snapshot = QueueSnapshot()
                for line in squeue.stdout.splitlines():
                    fields = line.split("|")
                    if len(fields) < 4:
                        continue
                    state: str = cls._normalize_state(fields[2])
                    if not state or cls._is_terminal_state(state):
                        continue
                    try:
                        job_id: int = int(fields[0])
                        task_id: int = int(fields[1])
                    except ValueError:
                        continue  # > not an array task of ours
                    snapshot.add(
                        fields[3], task_id, _SLURM_STATUS.get(state, QueueStatus.OTHER), batch_id=job_id
                    )
                snapshot.queried.append("squeue")
                return snapshot
            last = f"{squeue.stdout}\n{squeue.stderr}"
            log(f"SlurmExec: squeue failed (attempt {iretry + 1}/{nretry}):\n{last}", LogLevel.INFO)
            time.sleep(retry_delay * 1.5**iretry)
        raise RuntimeError(f"squeue failed after {nretry} attempt(s):\n{last}")

    def seeds_in_queue(self, snapshot: QueueSnapshot) -> dict[int, QueueStatus] | None:
        key: str | None = snapshot.lookup(self.exe_data.path)
        if key is not None:
            # > the template maps array task `i` to the i-th seed of the (contiguous) batch
            seeds: list[int] = sorted(int(job["seed"]) for job in self.exe_data["jobs"].values())
            return {
                seeds[task]: status for task, status in snapshot.jobs[key].items() if 0 <= task < len(seeds)
            }
        return {} if self.is_submitted() else None

    def adopt_submission(self, snapshot: QueueSnapshot) -> bool:
        if self.is_submitted():
            return False
        key: str | None = snapshot.lookup(self.exe_data.path)
        if key is None:
            return False
        self.exe_data["policy_settings"]["slurm_id"] = snapshot.batch_id[key]
        self.exe_data.write()
        self._logger(f"SlurmExec adopted job {snapshot.batch_id[key]} found in the queue", LogLevel.WARN)
        return True

    # ---------------------------------------------------------------------------

    def exe(self):
        # > recovery mode
        if (
            "slurm_id" in self.exe_data["policy_settings"]
            and self.exe_data["policy_settings"]["slurm_id"] > 0
        ):
            if not self.detached:
                self._track_job()
            return

        # > populate the submission template file
        slurm_settings: dict = {
            "exe": self.exe_data["exe"],
            "job_path": str(self.exe_data.path.absolute()),
            "ncores": self.exe_data["policy_settings"].get("slurm_ncores", 1),
            "njobs_minus_1": len(self.exe_data["jobs"]) - 1,
            "all_seeds": " ".join(str(job["seed"]) for job in self.exe_data["jobs"].values()),
            "start_seed": min(job["seed"] for job in self.exe_data["jobs"].values()),
            "end_seed": max(job["seed"] for job in self.exe_data["jobs"].values()),
            "input_files": ", ".join(self.exe_data["input_files"]),
            "max_runtime": self._format_slurm_time(int(self.exe_data["policy_settings"]["max_runtime"])),
            # "max_runtime": int(self.exe_data["policy_settings"]["max_runtime"]),
        }
        with open(self.slurm_template) as t, open(self.file_sub, "w") as f:
            f.write(string.Template(t.read()).substitute(slurm_settings))

        job_env = os.environ.copy()
        job_env["OMP_NUM_THREADS"] = f"{slurm_settings['ncores']}"
        job_env["OMP_STACKSIZE"] = "1024M"

        cluster_id: int = -1  # init failed state
        re_cluster_id = re.compile(r"Submitted batch job\s+(\d+).*", re.DOTALL)

        for _ in range(self.exe_data["policy_settings"]["slurm_nretry"]):
            slurm_submit = subprocess.run(
                ["sbatch", SlurmExec._file_sub],
                env=job_env,
                cwd=self.exe_data.path,
                capture_output=True,
                text=True,
            )
            if slurm_submit.returncode == 0 and (match_id := re.match(re_cluster_id, slurm_submit.stdout)):
                cluster_id = int(match_id.group(1))
                self.exe_data["policy_settings"]["slurm_id"] = cluster_id
                self.exe_data.write()
                break
            else:
                self._logger(
                    f"SlurmExec failed to submit job {self.exe_data.path}:\n"
                    + f"{slurm_submit.stdout}\n"
                    + f"{slurm_submit.stderr}",
                    LogLevel.INFO,
                )
                time.sleep(self.exe_data["policy_settings"]["slurm_retry_delay"])

        if cluster_id < 0:
            self._logger(f"SlurmExec failed to submit job {self.exe_data.path}", LogLevel.WARN)
            return  # failed job

        if self.detached:
            self._logger(f"SlurmExec submitted job {cluster_id} [dim](detached)[/dim]", LogLevel.DEBUG)
            return

        # > now we need to track the job
        self._track_job()

    def _track_job(self):
        job_id: int = self.exe_data["policy_settings"]["slurm_id"]
        poll_time: float = self.exe_data["policy_settings"]["slurm_poll_time"]
        nretry: int = max(1, int(self.exe_data["policy_settings"]["slurm_nretry"]))
        retry_delay: float = self.exe_data["policy_settings"]["slurm_retry_delay"]

        while True:
            time.sleep(poll_time)

            for iretry in range(nretry):
                # > --array: one row per array task; --states=all also lists tasks that
                # > already reached a terminal state but still linger in the scheduler.
                squeue = subprocess.run(
                    [
                        "squeue",
                        "--noheader",
                        "--array",
                        f"--jobs={job_id}",
                        "--states=all",
                        "--format=%i|%T",
                    ],
                    capture_output=True,
                    text=True,
                )

                if squeue.returncode == 0:
                    # > empty output (no rows) yields n_active == 0: the job is finished
                    # > or has been purged from the scheduler's records. Non-empty but
                    # > unparseable output yields None and is handled as a failed query.
                    n_active = self._squeue_active_count(squeue.stdout)
                    if n_active is not None:
                        self._decrease_active_resources(n_active)
                        if n_active == 0:
                            return  # all tasks finished or no longer visible in Slurm
                        break

                # > job no longer known to the scheduler: treat as finished and let
                # > output scanning determine the per-job outcome downstream.
                if re.search("Invalid job id specified", squeue.stderr):
                    self._decrease_active_resources(0)
                    return

                detail = f"squeue stdout:\n{squeue.stdout}\nsqueue stderr:\n{squeue.stderr}"
                if iretry + 1 >= nretry:
                    # > give up tracking after exhausting retries: release resources and
                    # > let output scanning determine the per-job outcome downstream.
                    self._logger(
                        "SlurmExec failed to determine job status, giving up tracking"
                        + f" [dim](job_id={job_id}, attempts={nretry})[/dim]:\n"
                        + detail,
                        LogLevel.WARN,
                    )
                    self._decrease_active_resources(0)
                    return
                self._logger(
                    f"SlurmExec failed to query job [dim](job_id={job_id})[/dim]:\n" + detail,
                    LogLevel.INFO,
                )
                time.sleep(retry_delay * 1.5**iretry)  # exponential backoff
