"""NNLOJET execution interface.

Defines an abstraction to execute NNLOJET on different backends (policies)
and a factory design pattern to obtain tasks for the different policies.
"""

import logging
import os
import re
import subprocess
import time
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import ClassVar

import luigi

from .._types import GenericPath
from ..db._loglevel import LogLevel
from ._exe_config import ExecutionMode, ExecutionPolicy
from ._exe_data import ExeData


class Executor(luigi.Task, metaclass=ABCMeta):
    """Abstract base class for NNLOJET execution tasks.

    This class handles the setup, execution, and output collection for
    NNLOJET jobs. It delegates the actual execution mechanism to
    subclasses via the `exe` method.

    Attributes
    ----------
    path : str
        Path to the execution directory.
    log_level : LogLevel
        Logging level for the task.

    """

    _file_log: str = "exe.log"

    # Filesystem scanning parameters
    FS_MAX_RETRY: ClassVar[int] = 10
    FS_DELAY: ClassVar[float] = 1.0

    path: str = luigi.Parameter()  # type: ignore[assignment]
    log_level: LogLevel = luigi.OptionalIntParameter(default=LogLevel.INFO)  # type: ignore[assignment]
    priority_bump: int = luigi.IntParameter(default=0)  # type: ignore[assignment]

    _priority_default: ClassVar[int] = 100

    @property
    def priority(self) -> int:
        """Scheduler priority, optionally bumped by the task that spawned this executor."""
        return self._priority_default + self.priority_bump

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # ExeData is the single source of truth shared with DB-side tasks.
        self.exe_data: ExeData = ExeData(Path(self.path))
        # Per-execution log file used by backend implementations.
        self.file_log: Path = Path(self.path) / self._file_log

    @property
    def exe_logger(self) -> logging.Logger:
        """Lazy-initialized logger for the executor.

        Returns
        -------
        logging.Logger
            A logger instance configured to write to the execution log file.

        Notes
        -----
        The logger is keyed by execution path (`dokan.executor.<path>`) so
        repeated calls for the same task instance reuse the same handler.

        """
        # Create a logger specific to this executor identity to avoid handler collisions
        logger = logging.getLogger(f"dokan.executor.{self.path}")
        if not logger.handlers:
            logger.propagate = False
            logger.setLevel(logging.DEBUG)  # Filter in _logger based on self.log_level
            try:
                # Ensure directory exists before creating FileHandler
                self.file_log.parent.mkdir(parents=True, exist_ok=True)
                handler = logging.FileHandler(self.file_log, mode="a", encoding="utf-8")
                # Format matches the previous manual implementation style
                formatter = logging.Formatter(
                    "[%(asctime)s](%(levelname)s): %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
                )
                handler.setFormatter(formatter)
                logger.addHandler(handler)
            except Exception:
                # Fallback to no-op if file cannot be opened (e.g. permissions)
                pass
        return logger

    def _logger(self, message: str, level: LogLevel = LogLevel.INFO) -> None:
        """Log a message with a specific level.

        Parameters
        ----------
        message : str
            The message to log.
        level : LogLevel, optional
            The severity level of the message (default is INFO).

        """
        # > pass through log level & all signals
        if level >= 0 and level < self.log_level:
            return

        # Map IntEnum levels to logging levels
        level_val = int(level)
        if level_val < 0:
            # For signals (negative values), use INFO but include the signal name
            self.exe_logger.info(f"({level!r}): {message}")
        else:
            self.exe_logger.log(level_val, message)

    def _debug(self, message: str) -> None:
        """Log a debug message.

        Parameters
        ----------
        message : str
            The debug message.

        """
        self._logger(message, LogLevel.DEBUG)

    @staticmethod
    def get_cls(policy: ExecutionPolicy):
        """Get the Executor subclass for a given policy.

        Parameters
        ----------
        policy : ExecutionPolicy
            The execution policy (LOCAL, HTCONDOR, SLURM).

        Returns
        -------
        type
            The Executor subclass.

        Raises
        ------
        TypeError
            If `policy` is not a supported `ExecutionPolicy`.

        """
        # > local import to avoid cyclic dependence
        from .htcondor import HTCondorExec
        from .local import BatchLocalExec
        from .slurm import SlurmExec

        match policy:
            case ExecutionPolicy.LOCAL:
                return BatchLocalExec
            case ExecutionPolicy.HTCONDOR:
                return HTCondorExec
            case ExecutionPolicy.SLURM:
                return SlurmExec
            case _:
                raise TypeError(f"invalid ExecutionPolicy: {policy!r}")

    @staticmethod
    def factory(policy: ExecutionPolicy = ExecutionPolicy.LOCAL, *args, **kwargs):
        """Create an Executor for a specific policy via the factory pattern.

        Parameters
        ----------
        policy : ExecutionPolicy, optional
            The execution policy (default is LOCAL).
        *args, **kwargs
            Arguments passed to the Executor constructor.

        Returns
        -------
        Executor
            An instance of the specific Executor subclass.

        """
        exec_cls = Executor.get_cls(policy)
        return exec_cls(*args, **kwargs)

    @staticmethod
    def adapt_warmup_grids(exe_data: ExeData, log: Callable[[str, LogLevel], None]) -> None:
        """Combine the grid data of a warmup batch into its grid state file(s).

        Warmups run without grid adaption (`warmup = N[M,noadapt]`): every
        seed dumps its accumulated data to `<GRID>.s<seed>.khd` while the
        grid state `<GRID>.khs` stays fixed.  Once the batch is done, the
        data files of all seeds that produced a result are folded into the
        state file in place with `NNLOJET --adapt`, which keeps the previous
        state as `<GRID>.khs.bak`.

        Idempotent: an existing `.bak` marks an already adapted grid and the
        step is skipped (re-adapting would fail on the data-file hash check).
        A stale `<GRID>.khs.new` from an interrupted adaption would make
        NNLOJET refuse to run and is removed first.  NNLOJET ends usage errors
        with a plain `stop` (exit code 0), so success is asserted by the
        return code *and* the presence of the `.bak` file.

        Raises
        ------
        RuntimeError
            The grid adaption failed (task-level fault: the step must not be
            finalized with an un-adapted grid).
        """
        if exe_data.get("mode") != ExecutionMode.WARMUP:
            return
        path: Path = Path(exe_data.path)
        seeds_ok: set[int] = {
            int(job["seed"]) for job in exe_data.get("jobs", {}).values() if "result" in job
        }
        for khs in sorted(path.glob("*.khs")):
            bak: Path = khs.with_name(khs.name + ".bak")
            new: Path = khs.with_name(khs.name + ".new")
            if bak.exists():
                log(f"adapt: {khs.name} already adapted (found {bak.name}), skipping", LogLevel.DEBUG)
                continue
            khd: list[Path] = []
            for data_file in sorted(path.glob(f"{khs.stem}.s*.khd")):
                match = re.search(r"\.s(\d+)\.khd$", data_file.name)
                if match and int(match.group(1)) in seeds_ok:
                    khd.append(data_file)
            if not khd:
                log(f"adapt: no usable data files for {khs.name}, skipping", LogLevel.WARN)
                continue
            new.unlink(missing_ok=True)
            # > `-d` is greedy (consumes all remaining arguments): keep it last
            cmd: list[str] = [exe_data["exe"], "--adapt", "-i", khs.name, "-d", *(f.name for f in khd)]
            log("adapt: " + " ".join(cmd), LogLevel.DEBUG)
            job_env = os.environ.copy()
            job_env["OMP_NUM_THREADS"] = "1"
            job_env["OMP_STACKSIZE"] = "1024M"
            adapt_out = subprocess.run(cmd, cwd=path, env=job_env, capture_output=True, text=True)
            success: bool = adapt_out.returncode == 0 and bak.exists()
            # > the NNLOJET output is noise on success (DEBUG); keep it verbose on failure
            log(
                f"adapt: {khs.name} (rc = {adapt_out.returncode}):\n" + adapt_out.stdout + adapt_out.stderr,
                LogLevel.DEBUG if success else LogLevel.ERROR,
            )
            if not success:
                raise RuntimeError(f"grid adaption failed for {khs} (rc = {adapt_out.returncode})")
            log(f"adapt: {khs.name} <- {len(khd)} data file(s)", LogLevel.INFO)

    @staticmethod
    def templates() -> list[GenericPath]:
        """List of built-in templates for this executor.

        If the executor requires additional template files, such as submission
        files, these should be provided by overriding this method.

        Returns
        -------
        list[GenericPath]
            A list of all built-in template files for the executor.

        """
        return []

    def output(self) -> list[luigi.Target]:
        """Get the task output.

        Returns
        -------
        list[luigi.Target]
            The final status file (`job.json`) as a LocalTarget.

        """
        return [luigi.LocalTarget(self.exe_data.file_fin)]

    @abstractmethod
    def exe(self) -> None:
        """Execute the backend-specific workload.

        Subclasses are expected to:
        - submit/track work on their respective backend,
        - update `self.exe_data` as needed (for example scheduler ids),
        - return without raising for expected job failures (those are detected
          from output parsing), and raise only for task-level faults.
        """
        raise NotImplementedError("Executor::exe: abstract method must be overridden!")

    def run(self) -> None:
        """Run the execution task.

        This method handles:
        1. Scanning for existing results (recovery).
        2. Initializing/writing mutable `ExeData`.
        3. Invoking `exe()` if work is still incomplete.
        4. Re-scanning outputs, combining warmup grid data (`--adapt`), and
           finalizing to `job.json`.

        Notes
        -----
        - If recovery scanning already finds all job results, backend execution
          is skipped.
        - Finalization is always attempted so downstream tasks can rely on an
          immutable final state file.
        """
        # > more preparation for execution?

        # > scan directory and update ExeData (recovery mode)
        self.exe_data.scan_dir([self._file_log])
        if "timestamp" not in self.exe_data:
            self.exe_data["timestamp"] = time.time()
        self.exe_data.write()

        if not self.exe_data.is_complete:
            # > call the backend specific execution
            try:
                self.exe()
            except Exception as e:
                self._logger(f"exception in exe: {e}", level=LogLevel.ERROR)
                raise
        else:
            self._logger("Executor::run: skipped exe()", level=LogLevel.DEBUG)

        self.exe_data.scan_dir([self._file_log], fs_max_retry=self.FS_MAX_RETRY, fs_delay=self.FS_DELAY)
        # > warmup: fold the seeds' grid data into the grid state (no re-scan afterwards:
        # > the `.bak` left behind must not become a tracked output that propagates)
        self.adapt_warmup_grids(self.exe_data, self._logger)
        self.exe_data.finalize()
