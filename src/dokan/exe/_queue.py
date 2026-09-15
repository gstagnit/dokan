"""Batch-system queue snapshots for detached execution.

A *detached* executor submits a batch and returns without tracking it (see
`Executor.detached`).  Somebody then has to ask the batch system, later and from
possibly another process or host, what became of the batch.  This module holds the
policy-neutral answer to that question: one `QueueSnapshot` per tick, built by a
single query per scheduler, which every in-flight batch is then matched against.

Batches are matched by their *working directory* (the execution directory dokan
staged: `initialdir` on HTCondor, `--chdir`/`WorkDir` on Slurm), not by the cluster
id alone.  The directory is unique per batch by construction, it is known before
the submission happens, and it therefore also identifies a batch whose submission
succeeded but whose id was never written back (a crash between the two).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import IntEnum, unique
from pathlib import Path


@unique
class QueueStatus(IntEnum):
    """State of one queued job as far as dokan cares."""

    IDLE = 1
    RUNNING = 2
    HELD = 3
    OTHER = 4  # transferring, completing, suspended, ...: still occupies the queue


@dataclass
class QueueSnapshot:
    """Every job of ours the batch system still knows about, keyed by working directory.

    `jobs[workdir][index]` is the state of array member `index` (HTCondor `ProcId`,
    Slurm array task id); `batch_id[workdir]` is the policy-specific handle of the
    batch (`cluster id` on HTCondor, `job id` on Slurm) and `scheduler[workdir]`
    the scheduler that holds it, when the policy has several.

    Working directories are stored as the batch system reports them.  `lookup`
    normalises on the way in so that an execution directory reached through a
    symlinked prefix (`/afs/cern.ch/user/.../work` vs `/afs/cern.ch/work/...`) still
    matches: the cheap forms are tried first and a `realpath` only when the tail of
    the path says the row plausibly belongs to the batch.
    """

    jobs: dict[str, dict[int, QueueStatus]] = field(default_factory=dict)
    batch_id: dict[str, int] = field(default_factory=dict)
    scheduler: dict[str, str] = field(default_factory=dict)
    queried: list[str] = field(default_factory=list)  # human-readable: what was asked

    def add(
        self,
        workdir: str,
        index: int,
        status: QueueStatus,
        *,
        batch_id: int | None = None,
        scheduler: str | None = None,
    ) -> None:
        self.jobs.setdefault(workdir, {})[index] = status
        if batch_id is not None:
            self.batch_id[workdir] = batch_id
        if scheduler is not None:
            self.scheduler[workdir] = scheduler

    def lookup(self, exe_path: Path) -> str | None:
        """Return the snapshot key of `exe_path`, or None when the batch is not queued."""
        exe_path = Path(exe_path)
        candidates: list[str] = [
            str(exe_path),
            str(exe_path.absolute()),
            os.path.normpath(exe_path.absolute()),
        ]
        for key in candidates:
            if key in self.jobs:
                return key
        # > the batch system may report a resolved path while we hold a symlinked one
        # > (or vice versa); resolving every queue row is not free on a network
        # > filesystem, so only rows that end like the batch directory are considered
        tail: str = os.path.join(exe_path.parent.name, exe_path.name)
        real: str | None = None
        for key in self.jobs:
            if not key.endswith(tail):
                continue
            if real is None:
                real = str(exe_path.resolve())
            if key == real or os.path.realpath(key) == real:
                return key
        return None

    @property
    def n_jobs(self) -> int:
        return sum(len(procs) for procs in self.jobs.values())

    def count(self) -> dict[QueueStatus, int]:
        out: dict[QueueStatus, int] = {status: 0 for status in QueueStatus}
        for procs in self.jobs.values():
            for status in procs.values():
                out[status] += 1
        return out
