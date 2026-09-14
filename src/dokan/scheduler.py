import collections

from luigi import rpc, scheduler, worker

# > cap on `Worker._add_task_history`, which Luigi appends to on every status
# > change of every task and never drains.  Only `luigi.execution_summary`
# > reads it, and dokan reports through its own monitor and log database, so a
# > truncated tail costs nothing but bounds one of the structures that make a
# > long-running worker grow without limit.
_TASK_HISTORY_MAX: int = 10000


class WorkerSchedulerFactory:
    """Factory adapter for Luigi worker/scheduler construction.

    This mirrors Luigi's internal worker-scheduler factory behavior but allows
    runtime configuration via constructor kwargs (instead of requiring
    `luigi.cfg`). It is used with `luigi.build(..., worker_scheduler_factory=...)`.
    """

    def __init__(self, **kwargs):
        """Capture scheduler/worker options used by `luigi.build`.

        Supported kwargs
        ----------------
        resources : dict | None
            Local scheduler resource limits.
        cache_task_completion : bool
            Whether to cache `Task.complete()` results in workers.
        check_complete_on_run : bool
            Whether Luigi should run completion checks before running a task.
        check_unfulfilled_deps : bool
            Whether to validate dependencies before task execution.
        wait_interval : float
            Polling interval (seconds) between work requests.
        wait_jitter : float
            Randomized jitter added to wait interval.
        ping_interval : float
            Heartbeat interval (seconds) from worker to scheduler.
        """
        self.resources = kwargs.pop("resources", None)
        self.cache_task_completion = kwargs.pop("cache_task_completion", False)
        self.check_complete_on_run = kwargs.pop("check_complete_on_run", False)
        self.check_unfulfilled_deps = kwargs.pop("check_unfulfilled_deps", True)
        self.wait_interval = kwargs.pop("wait_interval", 0.1)  # luigi default: 1.0
        self.wait_jitter = kwargs.pop("wait_jitter", 0.5)  # luigi default: 5.0
        self.ping_interval = kwargs.pop("ping_interval", 0.1)  # luigi default: 1.0

        if kwargs:
            raise RuntimeError(f"WorkerSchedulerFactory: left-over options {kwargs}")

    def create_local_scheduler(self):
        """Create an in-process Luigi scheduler for local execution."""
        return scheduler.Scheduler(
            prune_on_get_work=True, record_task_history=False, resources=self.resources
        )

    def create_remote_scheduler(self, url):
        """Create a remote scheduler client bound to `url`."""
        return rpc.RemoteScheduler(url)

    def create_worker(self, scheduler, worker_processes, assistant=False):
        """Create a Luigi worker with configured polling/check behavior.

        The worker's task history is capped (see `_TASK_HISTORY_MAX`).  Luigi grows
        it forever otherwise: one entry per status change, each holding a reference
        to the task, drained only by the end-of-run execution summary.  A `deque`
        supports everything `execution_summary` does with it (iteration and `[0]`),
        so the only consequence is that the summary describes the recent tail rather
        than the whole run.
        """
        w = worker.Worker(
            scheduler=scheduler,
            worker_processes=worker_processes,
            assistant=assistant,
            cache_task_completion=self.cache_task_completion,
            check_complete_on_run=self.check_complete_on_run,
            check_unfulfilled_deps=self.check_unfulfilled_deps,
            wait_interval=self.wait_interval,
            wait_jitter=self.wait_jitter,
            ping_interval=self.ping_interval,
        )
        w._add_task_history = collections.deque(maxlen=_TASK_HISTORY_MAX)
        return w
