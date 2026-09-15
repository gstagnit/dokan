"""configuration for the dokan workflow.

We use a custom dictionary class to store all settings that we need to exeucute
a full NNLOJET workflow.

Attributes
----------
_default_config : Path to config file that stores default values
    path to a configuration file with default values
_schema : dict
    define the structure of Config
"""

import copy
import json
import re
import warnings
from collections import UserDict
from pathlib import Path

from ._types import GenericPath
from .db._loglevel import LogLevel
from .exe import ExecutionPolicy
from .order import Order
from .runcard import RuncardTemplate
from .util import fill_missing, validate_schema

_default_config: Path = Path(__file__).parent.resolve() / "config.json"

_schema: dict = {
    "exe": {
        "path": str,  # absolute path to NNLOJET
        "policy": ExecutionPolicy,  # (local, htcondor, slurm, ...)
        "policy_settings": {
            # --- LOCAL
            "local_ncores": int,
            # --- HTCONDOR
            "htcondor_template": str,
            "htcondor_ncores": int,
            "htcondor_nretry": int,
            "htcondor_retry_delay": float,
            "htcondor_poll_time": float,
            # --- SLURM
            "slurm_template": str,
            "slurm_ncores": int,
            "slurm_nretry": int,
            "slurm_retry_delay": float,
            "slurm_poll_time": float,
            "slurm_njobs_per_node": int,
        },
    },
    "run": {
        "dokan_version": str,  # verion of the workflow
        "name": str,  # job name
        "path": str,  # absolute path to job directory
        "raw_path": str,  # (optional) absolute path to raw data directory
        "template": str,  # template file name (not path)
        "md5": str,  # hash of the template file
        "histograms": {str: {"nx": int, "cumulant": int, "grid": str}},  # list of all histograms
        "histograms_single_file": str,  # name in case we concatenate all histograms to a single file
        "order": Order,  # what order to compute (LO, NLO, NNLO)
        "opt_target": str,  # the target we wish to optimise: ["cross"|"cross_hist"|"hist"]
        # > which observables the `hist` part of the target looks at (empty: all of
        # > them); names from `run.histograms`, cumulants excluded
        "opt_observables": [str],
        # > judge each selected observable by its worst significant *bin* instead of
        # > its integral -- for a run whose goal is one distribution, not its total
        "opt_bins": bool,
        "target_rel_acc": float,  # target relative accuracy
        "job_max_runtime": float,  # integration-time budget (in sec) for a single NNLOJET run
        # > extra wall clock requested from the batch system on top of `job_max_runtime`,
        # > as a fraction of it: the enforced limit is wall time, which also covers
        # > startup and file transfer, while jobs are sized to fill the integration budget
        "job_max_runtime_margin": float,
        # > multiplier on the *predicted* runtime of a single job when asking the batch
        # > system for wall time, capped by `job_max_runtime_margin` (<=0: always ask
        # > for the cap).  Absorbs the seed-to-seed spread within one step.
        "job_runtime_safety_factor": float,
        "job_fill_max_runtime": bool,  # if we want to exhause the maximum runtime
        "jobs_max_total": int,  # max production jobs per submission (<=0: no limit)
        # > runtime the whole computation may consume, summed over every job of every
        # > submission, warmup included (<=0: derived as `jobs_max_total *
        # > job_max_runtime`, or unlimited when the job count is unlimited too)
        "jobs_max_total_runtime": float,
        # > how often, in seconds of wall time, to write the per-order results in
        # > `result/final` while production is running (<=0: only at the end of the
        # > run, as before).  Only the dynamic production dispatcher triggers this,
        # > so warmup and pre-production are never interrupted by it.
        "finalize_interval": float,
        "jobs_max_concurrent": int,  # maximum number of concurrent jobs
        "jobs_batch_size": int,  # size of runs to batch into a single submission
        "jobs_batch_unit_size": int,  # the minimum batch size of a submission
        "seed_offset": int,  # seed number offset
        "timestamps": float,  # @todo list of timestamps when `run` was called
        # > tick-time only (`nnlojet-run tick`): executors submit and return instead
        # > of tracking, the dispatcher performs one wave and returns instead of
        # > re-yielding itself.  Never persisted.
        "detached": bool,
    },
    "ui": {
        "monitor": bool,  # flag to switch on/off the live monitor
        "refresh_delay": float,  # delay (in seconds) between refreshes of the monitor
        "log_level": LogLevel,  # logging level to be displayed
    },
    "process": {
        "name": str,  # name of the process in NNLOJET
        "channels": {
            str: {
                "string": str,
                "part": str,
                "part_num": int,
                "region": str,
                "order": int,
            },
        },  # all channels for the process (auto-filled)
    },
    "warmup": {
        "ncores": int,  # #of cores to allocate to a single warmup run
        "ncall_start": int,  # initial number of events (per iteration)
        "niter": int,  # number of iterations in a single job (>=2 for chi2dof)
        "min_increment_steps": int,  # must be > 2 and < max value
        "max_increment_steps": int,  # up to how many rounds of warmups we want to run
        "fac_increment": float,  # the factor by which we increment the statistics each round
        "max_chi2dof": float,  # maximum chi2/dof from iterations of the warmup to accept
        "max_err_rel_var": float,  # maximum relative variation of the errors between iterations to accept
        "scaling_window": float,  # tolerance for 1/sqrt(N) MC error scaling
        "skip_qc": bool,  # submit-time only (`--no-warmup`): accept the current grid, skip the QC
    },
    "production": {
        "ncores": int,  # #of cores to allocate to a single production run
        "ncall_start": int,  # initial number of events (per iteration)
        "niter": int,  # number of iterations in a single job (>=2 for chi2dof)
        "penalty_wrt_warmup": float,  # factor that takes into account the slowdown from warmup -> production
        "fac_merge_trigger": float,  # triggers a merge if (#done+#merged)/(#merged+1) > fac_merge_trigger
        "min_number": int,  # minimum #of production jobs beyond pre-production (defaults to 1)
    },
    "merge": {
        # > robust-z threshold above which a dataset is *flagged* as an outlier.  0
        # > switches detection off entirely, and with it the outlier diagnostics.
        "trim_threshold": float,
        # > safety valve on removal: at most `max(1, fraction * ndat)` datasets per bin,
        # > most extreme first.  Not a budget -- the floor of one keeps the decision
        # > independent of how many datasets have accumulated.  0 reports outliers
        # > without removing any.  See doc/outlier_trimming.md.
        "trim_max_fraction": float,
        "k_scan_nsteps": int,  # number of scan steps to consider for finding the plateau
        "k_scan_maxdev_steps": float,  # maximum deviation to identify a plateau
    },
}

# > one line per option, written above it in the run's `config.json` (as `// ...`
# > comment lines, which `read_config_json` strips) so the file explains itself
_DOC: dict[str, dict[str, str]] = {
    "exe": {
        "path": "absolute path to the NNLOJET executable",
        "policy": "where jobs run: local, htcondor or slurm",
        "policy_settings": (
            "backend settings: <backend>_template (submit template in this folder), _ncores,"
            " _nretry, _retry_delay and _poll_time (seconds)"
        ),
    },
    "run": {
        "dokan_version": "dokan version that created this run",
        "name": "run name, taken from the runcard",
        "path": "this run directory (absolute)",
        "raw_path": "optional separate location for the raw job output",
        "template": "runcard template in this folder; do not edit it by hand (its checksum is verified)",
        "md5": "checksum of the template, to detect manual edits",
        "histograms": "observables of the runcard with their binning (auto-filled)",
        "histograms_single_file": "NNLOJET writes all observables of a job into one file of this name",
        "order": "perturbative order: 0 = LO, 1 = NLO, 2 = NNLO (negative: that order's contribution only)",
        "opt_target": (
            'accuracy the run optimises and reports: "cross", "hist" (worst observable)'
            ' or "cross_hist" (geometric mean of the two)'
        ),
        "opt_observables": 'observables the "hist" part looks at (empty: all; cumulants excluded)',
        "opt_bins": "judge each selected observable by its worst significant bin instead of its integral",
        "target_rel_acc": "relative accuracy at which dispatch stops, e.g. 0.01 = 1%",
        "job_max_runtime": "integration-time budget of one job, in seconds; jobs are sized to fill it",
        "job_max_runtime_margin": (
            "extra wall time requested from the batch system on top of job_max_runtime, as a fraction"
        ),
        "job_runtime_safety_factor": (
            "multiplier on a job's predicted runtime for its wall-time request (0: always request the cap)"
        ),
        "job_fill_max_runtime": "size every production job to use the whole runtime budget",
        "jobs_max_total": "maximum number of production jobs per submission (0: unlimited)",
        "jobs_max_total_runtime": (
            "runtime budget of the whole campaign in seconds, warmup included"
            " (0: jobs_max_total x job_max_runtime)"
        ),
        "finalize_interval": (
            "seconds between refreshes of result/final while production runs (0: only at the end)"
        ),
        "jobs_max_concurrent": "maximum number of jobs in the batch queue at once",
        "jobs_batch_size": (
            "seeds of one part submitted as one batch-system cluster"
            " (derived at submit/tick unless overridden there)"
        ),
        "jobs_batch_unit_size": "smallest batch a dispatch wave may submit",
        "seed_offset": "seeds are numbered from seed_offset + 1",
        "timestamps": "unused",
    },
    "ui": {
        "monitor": "show the live status board during submit",
        "refresh_delay": "board refresh interval in seconds",
        "log_level": "messages below this level are dropped: 10 debug, 20 info, 30 warn, 40 error",
    },
    "process": {
        "name": "NNLOJET process name",
        "channels": (
            "the parts of the calculation: label -> NNLOJET channel string, part, part_num, order"
            " (and region); split or regroup channels here before the first submit"
        ),
    },
    "warmup": {
        "ncores": "cores per warmup job",
        "ncall_start": "events per iteration of the first warmup step",
        "niter": "iterations per warmup job (at least 2)",
        "min_increment_steps": "warmup steps before the quality checks may end the warmup (at least 2)",
        "max_increment_steps": "warmup steps after which the warmup ends regardless",
        "fac_increment": "statistics growth factor from one warmup step to the next",
        "max_chi2dof": "largest chi2/dof accepted from the last warmup step",
        "max_err_rel_var": "largest relative spread of the per-iteration errors accepted",
        "scaling_window": "tolerance on the 1/sqrt(N) error scaling between the last two steps",
    },
    "production": {
        "ncores": "cores per production job",
        "ncall_start": "events per iteration of the pre-production job, and the floor for all production jobs",
        "niter": "iterations per production job",
        "penalty_wrt_warmup": "expected slowdown of production w.r.t. warmup, used to size the pre-production",
        "fac_merge_trigger": "re-merge a part once (#done + #merged + 1) / (#merged + 1) exceeds this (> 1)",
        "min_number": "production jobs a part needs before its error is trusted by the optimiser",
    },
    "merge": {
        "trim_threshold": "robust-z score above which a dataset is flagged as an outlier (0: off)",
        "trim_max_fraction": "at most this fraction of the datasets of a bin may be removed (0: flag only)",
        "k_scan_nsteps": "steps of the k-scan used to find the error plateau",
        "k_scan_maxdev_steps": "maximum deviation between k-scan steps still counted as a plateau",
    },
}

_COMMENT_HEADER: str = "// dokan run configuration: lines starting with // are comments and are ignored"


def read_config_json(path: GenericPath) -> dict:
    """Read a `config.json`, ignoring full-line `//` comments (see `Config.write`)."""
    with open(path) as fin:
        lines = [line for line in fin if not line.lstrip().startswith("//")]
    return json.loads("".join(lines))


def annotate_config_json(text: str) -> str:
    """Insert the `_DOC` comment above every documented option of a serialised config.

    Relies on `json.dumps(indent=2)`: sections sit at two spaces, options at four.
    Anything nested deeper (histograms, channels) is left alone.
    """
    out: list[str] = [_COMMENT_HEADER]
    section: str | None = None
    first: bool = True
    for line in text.splitlines():
        if match := re.match(r'^  "([^"]+)": ', line):
            section = match.group(1)
            first = True
        elif (match := re.match(r'^    "([^"]+)": ', line)) and section in _DOC:
            if doc := _DOC[section].get(match.group(1)):
                if not first:
                    out.append("")
                out.append(f"    // {doc}")
            first = False
        out.append(line)
    return "\n".join(out) + "\n"


# > keys dropped from the schema but possibly persisted by older versions:
# > pruned on load so validation keeps rejecting genuinely unknown keys
_deprecated: list[tuple[str, str]] = [
    ("warmup", "frozen"),  # superseded by `submit --warmup/--no-warmup` (ADR-0001)
]

# > keys that only ever live for one submission (set by the CLI, in-memory):
# > pruned on load and stripped on write so they can never become persistent
_transient: list[tuple[str, str]] = [
    ("warmup", "skip_qc"),  # `submit --no-warmup`
    ("run", "detached"),  # `tick`
]

_OPT_TARGETS: tuple[str, ...] = ("cross", "cross_hist", "hist")


def check_opt_target(config) -> None:
    """Reject an optimisation target that names unknown or unusable observables.

    Raised early (at `submit` / `tick` start) rather than at the first merge, where
    the failure would surface hours later in the log database.
    """
    run: dict = config["run"]
    target: str = run["opt_target"]
    if target not in _OPT_TARGETS:
        raise ValueError(f"run.opt_target = {target!r}: expected one of {', '.join(_OPT_TARGETS)}")
    selected: list[str] = list(run.get("opt_observables") or [])
    if not selected:
        return
    histograms: dict = run.get("histograms") or {}
    unknown: list[str] = [obs for obs in selected if obs not in histograms]
    if unknown:
        raise ValueError(
            f"run.opt_observables names observables that are not in the runcard: {unknown}"
            f" (known: {sorted(histograms)})"
        )
    cumulants: list[str] = [obs for obs in selected if "cumulant" in histograms[obs]]
    if cumulants:
        raise ValueError(f"run.opt_observables: cumulant observables cannot be optimised on: {cumulants}")
    if target == "cross":
        warnings.warn("run.opt_observables has no effect with run.opt_target = 'cross'", stacklevel=2)


# > sentinel to tell "key absent" apart from "key present with a falsy value"
# > (e.g. `frozen: false`) when pruning: presence must decide the warning, not truthiness
_MISSING = object()


class Config(UserDict):
    """configuration class of the dokan workflow

    a custom dictionary with a rigid skeleton to store workflow settings.
    """

    # > class-local variables for file name conventions
    _file_cfg: str = "config.json"

    def __init__(self, *args, **kwargs):
        path = kwargs.pop("path", None)
        default_ok: bool = kwargs.pop("default_ok", True)
        self.check_md5: bool = kwargs.pop("check_md5", True)
        super().__init__(*args, **kwargs)
        self.path: Path | None = None
        self.file_cfg: Path | None = None
        if path:
            if not default_ok:
                self.set_path(path, load=True)
            else:
                self.load(default_ok)
                self.set_path(path, load=False)
        else:
            self.load(default_ok)
        # > ensure that missing entries are always filled with defaults
        self.fill_defaults()

    def is_valid(self, convert_to_type: bool = False) -> bool:
        if not validate_schema(self.data, _schema, convert_to_type):
            return False
        # > implement boundary conditions on the configuration here
        # > that goes beyond the schema (structure and types)
        if "run" in self.data:
            if "target_rel_acc" in self.data["run"] and self.data["run"]["target_rel_acc"] <= 0.0:
                return False
            if "seed_offset" in self.data["run"] and self.data["run"]["seed_offset"] < 0:
                return False
        if (
            "warmup" in self.data
            and "min_increment_steps" in self.data["warmup"]
            and self.data["warmup"]["min_increment_steps"] < 2
        ):
            return False
        if "production" in self.data:
            if "min_number" in self.data["production"] and self.data["production"]["min_number"] < 1:
                return False
            # > the merge trigger ratio (#done+#merged+1)/(#merged+1) is exactly 1.0 once a
            # > part is fully merged, so `fac_merge_trigger <= 1.0` would make MergePart.complete()
            # > never settle (infinite re-merge loop). require it strictly above 1.0.
            if (
                "fac_merge_trigger" in self.data["production"]
                and self.data["production"]["fac_merge_trigger"] <= 1.0
            ):
                return False
        return True

    def __setitem__(self, key, item) -> None:
        super().__setitem__(key, item)
        if not self.is_valid():
            raise ValueError(f"Config: scheme forbids: {key} : {item}")

    def set_path(self, path: GenericPath, load: bool = False) -> None:
        self.path = Path(path)
        if not self.path.exists():
            self.path.mkdir(parents=True)
        if not self.path.is_dir():
            raise ValueError(f"{path} is not a folder")
        self.file_cfg = self.path / self._file_cfg
        if load:
            self.load(default_ok=False)
        self["run"]["path"] = str(self.path.absolute())

    def load_defaults(self) -> None:
        self.data = read_config_json(_default_config)
        if not self.is_valid(convert_to_type=True):
            raise RuntimeError("Config: load_defaults encountered conflict with schema")

    def load(self, default_ok: bool = True) -> None:
        if self.file_cfg and self.file_cfg.exists():
            self.data = read_config_json(self.file_cfg)
            for section, key in _deprecated:
                if self.data.get(section, {}).pop(key, _MISSING) is not _MISSING:
                    warnings.warn(f"Config: dropped deprecated setting {section}.{key}", stacklevel=2)
            for section, key in _transient:
                if self.data.get(section, {}).pop(key, _MISSING) is not _MISSING:
                    warnings.warn(
                        f"Config: {section}.{key} is submit-time only; ignored from file",
                        stacklevel=2,
                    )
        else:
            if not default_ok:
                raise FileNotFoundError(f"Config file not found: {self.file_cfg}")
            self.load_defaults()
        if not self.is_valid(convert_to_type=True):
            raise RuntimeError("Config: load encountered conflict with schema")

        # > Check whether the template file matches the md5 entry
        if self.check_md5 and self.path is not None and self.data.get("run", {}).get("template") is not None:
            template_hash = RuncardTemplate(self.path / self.data["run"]["template"]).to_md5_hash()
            # > skip the check if we don't have a md5 yet
            if (original_hash := self.data["run"].get("md5")) is not None and template_hash != original_hash:
                raise RuntimeError("Template has been manually modified, this is not allowed.")

    def fill_defaults(self):
        fill_missing(self.data, read_config_json(_default_config))

    def write(self) -> None:
        if not self.path or not self.file_cfg:
            raise RuntimeError("Config: no path set?!")
        data: dict = copy.deepcopy(self.data)
        for section, key in _transient:
            data.get(section, {}).pop(key, None)
        # > every option carries its one-line explanation as a `//` comment above it;
        # > `read_config_json` strips them, other readers must do the same
        with open(self.file_cfg, "w") as cfg:
            cfg.write(annotate_config_json(json.dumps(data, indent=2)))
