"""Task class within the dokan workflow

sub-class of a luigi Task to impose mandatory attributes to a workflow task.
"""

from pathlib import Path

import luigi
from luigi.parameter import ParameterVisibility

from ._types import GenericPath


class SharedDictParameter(luigi.DictParameter):
    """A `DictParameter` that hands every task the *same* frozen value.

    `DictParameter.normalize()` deep-freezes its input on every instantiation, so each
    task would otherwise own a private copy of the run configuration (tens of kB once
    the per-channel tables are in there).  That matters because Luigi's worker never
    prunes `_scheduled_tasks` or `_add_task_history`: every task instance it has ever
    scheduled is retained for the lifetime of the run, and a run with many parts
    creates one instance per dynamic clone -- tens of thousands of them.  The copies
    alone then reach the GB range, and since Luigi forks a process per task attempt,
    every fork pays for the whole accumulation again.

    Frozen values are immutable, so a single canonical instance can be shared safely.
    Equality is re-checked before reusing a cached entry so a hash collision degrades
    to "no sharing" rather than to the wrong value.
    """

    _canonical: dict[int, object] = {}

    def normalize(self, value):
        frozen = super().normalize(value)
        try:
            key: int = hash(frozen)
        except TypeError:
            return frozen  # > unhashable: nothing to share, keep the fresh copy
        cached = type(self)._canonical.get(key)
        if cached is not None and cached == frozen:
            return cached
        type(self)._canonical[key] = frozen
        return frozen


class Task(luigi.Task):
    """A dokan Task

    The main Task object in dokan with mandatory attributes

    Attributes
    ----------
    config : dict
        pass down the configuration for the jobs.
        Needed because once a Task is dispatched, global CONFIG is no longer
        available. Also facilitates the possibility of overrides that propagate
        down stream.
    local_path : list[str]
        path *relative* (local) to CONFIG.job_path as a list of directory names
    """

    # > insignificant: the config is identical for every task within one `luigi.build`
    # > (identity comes from the real parameters), so excluding it keeps task ids and
    # > scheduler bookkeeping small.  `to_str_params()` still carries it, so the
    # > dynamic-dependency round-trip through `load_task` is unaffected (verified).
    # > `SharedDictParameter`, not `DictParameter`: one frozen copy for the whole run
    # > instead of one per task instance (see the class docstring)
    config: dict = SharedDictParameter(visibility=ParameterVisibility.HIDDEN, significant=False)  # type: ignore[assignment]
    local_path: list[str] = luigi.ListParameter(default=[])  # type: ignore[assignment]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._path: Path = Path(self.config["run"]["path"]).joinpath(*self.local_path)

    # @todo: maybe add a _post_init_ routine that can be overwritten on a task basis?

    def _local(self, *path: GenericPath) -> Path:
        """get the "Task local" path

        take path and append to local path of the dokan task.

        Parameters
        ----------
        path : Tuple[str]
            list of paths (directories, filename) that will be concatenated.

        Returns
        -------
        Path
            resultant path relative to the task
        """
        if not self._path.exists():
            self._path.mkdir(parents=True, exist_ok=True)
        return self._path.joinpath(*path)
