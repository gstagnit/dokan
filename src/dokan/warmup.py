"""Warmup QC assessment and pre-production sizing.

Pure decision logic for the pre-production stage: no Luigi, no SQLAlchemy,
no config dict.  The task layer (`preproduction.py`) gathers job rows and
iteration data, calls into this module, and performs the queuing that the
returned decisions prescribe.
"""

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import IntFlag, auto


class WarmupFlag(IntFlag):
    """QC criteria and termination reasons behind a warmup decision."""

    # > auto -> integers of: 2^n starting with 1
    # > QC criteria
    RELACC = auto()
    CHI2DOF = auto()
    CONST_ERR = auto()
    GRID = auto()
    SCALING = auto()
    # > increment-step bookkeeping
    MIN_INCREMENT = auto()
    MAX_INCREMENT = auto()
    # > termination reasons outside the QC
    RUNTIME = auto()
    SKIPPED = auto()

    def __str__(self) -> str:
        # > pipe-joined (no brackets: flag strings end up in rich-markup contexts)
        return "|".join(str(flag.name) for flag in WarmupFlag if flag in self)


_NO_FLAGS: WarmupFlag = WarmupFlag(0)


@dataclass(frozen=True)
class JobSize:
    """Validated statistics for one job."""

    ncall: int
    niter: int

    def __post_init__(self) -> None:
        if self.ncall <= 0:
            raise ValueError(f"ncall must be positive, got {self.ncall}")
        if self.niter <= 0:
            raise ValueError(f"niter must be positive, got {self.niter}")

    @property
    def ntot(self) -> int:
        return self.ncall * self.niter


@dataclass(frozen=True)
class WarmupJobQC:
    """The QC-relevant numbers of one successfully terminated warmup step.

    A step is `nseeds` parallel jobs of identical `size` on the same grid:
    `size` and `elapsed_time` are *per seed*, `result`/`error`/`chi2dof`
    describe the combination of all seeds, `ntot` is the step total.
    """

    size: JobSize
    elapsed_time: float
    result: float
    error: float
    chi2dof: float
    id: int = 0
    nseeds: int = 1

    @property
    def ntot(self) -> int:
        """Total statistics of the step (all seeds)."""
        return self.size.ntot * self.nseeds

    @classmethod
    def from_row(cls, row) -> "WarmupJobQC":
        """Build from any object carrying the QC fields (e.g. a `Job` DB row)."""
        return cls(
            size=JobSize(ncall=row.ncall, niter=row.niter),
            elapsed_time=row.elapsed_time,
            result=row.result,
            error=row.error,
            chi2dof=row.chi2dof,
            id=row.id,
        )


def combine_warmup_jobs(jobs: Sequence[WarmupJobQC]) -> WarmupJobQC:
    """Fold the parallel seeds of one warmup step into one `WarmupJobQC`.

    A warmup step runs `n` seeds with identical statistics on the *same* grid
    (stage-3 warmups, combined afterwards by `NNLOJET --adapt`).  `size` and
    `elapsed_time` stay *per seed* (the slowest seed gives the wall-time
    estimate of one job), `result` is the inverse-variance weighted mean,
    `error` the combined error of all seeds, `nseeds = n` records the step
    multiplicity so that `ntot` is the step total.  `chi2dof` is the
    between-seed consistency test (n-1 dof); a single seed is returned as is
    (its own between-iteration chi2dof).
    """
    if not jobs:
        raise ValueError("combine_warmup_jobs: empty warmup step")
    if len(jobs) == 1:
        return jobs[0]

    ncall: int = jobs[0].size.ncall  # uniform within a batch (enforced by DBRunner)
    niter: int = max(j.size.niter for j in jobs)  # nominal; a premature termination only lowers it
    elapsed_time: float = max(j.elapsed_time for j in jobs)
    step_id: int = min(j.id for j in jobs)
    nseeds: int = len(jobs)

    valid: list[WarmupJobQC] = [j for j in jobs if j.error > 0.0 and math.isfinite(j.error)]
    if not valid:
        # > vanishing integral (0 +- 0) in every seed
        return WarmupJobQC(
            size=JobSize(ncall=ncall, niter=niter),
            elapsed_time=elapsed_time,
            result=sum(j.result for j in jobs) / len(jobs),
            error=0.0,
            chi2dof=0.0,
            id=step_id,
            nseeds=nseeds,
        )

    n: int = len(valid)
    weight_sum: float = sum(1.0 / j.error**2 for j in valid)
    mean: float = sum(j.result / j.error**2 for j in valid) / weight_sum
    error_comb: float = math.sqrt(1.0 / weight_sum)
    chi2dof: float = (
        sum((j.result - mean) ** 2 / j.error**2 for j in valid) / float(n - 1) if n > 1 else valid[0].chi2dof
    )
    return WarmupJobQC(
        size=JobSize(ncall=ncall, niter=niter),
        elapsed_time=elapsed_time,
        result=mean,
        error=error_comb,
        chi2dof=chi2dof,
        id=step_id,
        nseeds=nseeds,
    )


@dataclass(frozen=True)
class WarmupSettingsQC:
    """The config subset that drives the warmup QC assessment."""

    start: JobSize
    min_increment_steps: int
    max_increment_steps: int
    fac_increment: float
    max_chi2dof: float
    max_err_rel_var: float
    scaling_window: float
    skip_qc: bool
    target_rel_acc: float
    job_max_runtime: float

    @classmethod
    def from_config(cls, config: dict) -> "WarmupSettingsQC":
        warmup: dict = config["warmup"]
        run: dict = config["run"]
        return cls(
            start=JobSize(ncall=warmup["ncall_start"], niter=warmup["niter"]),
            min_increment_steps=warmup["min_increment_steps"],
            max_increment_steps=warmup["max_increment_steps"],
            fac_increment=warmup["fac_increment"],
            max_chi2dof=warmup["max_chi2dof"],
            max_err_rel_var=warmup["max_err_rel_var"],
            scaling_window=warmup["scaling_window"],
            # > transient key (`submit --no-warmup`): absent unless set by the CLI
            skip_qc=warmup.get("skip_qc", False),
            target_rel_acc=run["target_rel_acc"],
            job_max_runtime=run["job_max_runtime"],
        )


@dataclass(frozen=True)
class WarmupCompleteQC:
    """Warmup QC is complete for the current grid."""

    flags: WarmupFlag = _NO_FLAGS


@dataclass(frozen=True)
class WarmupRequiredQC:
    """Another warmup step is required: `nseeds` parallel jobs of `size` each."""

    size: JobSize
    flags: WarmupFlag = _NO_FLAGS
    nseeds: int = 1


WarmupAssessmentQC = WarmupCompleteQC | WarmupRequiredQC


@dataclass(frozen=True)
class PreProductionSettings:
    """The config subset that drives the pre-production sizing."""

    start: JobSize
    penalty_wrt_warmup: float
    target_rel_acc: float
    job_max_runtime: float

    @classmethod
    def from_config(cls, config: dict) -> "PreProductionSettings":
        production: dict = config["production"]
        run: dict = config["run"]
        return cls(
            start=JobSize(ncall=production["ncall_start"], niter=production["niter"]),
            penalty_wrt_warmup=production["penalty_wrt_warmup"],
            target_rel_acc=run["target_rel_acc"],
            job_max_runtime=run["job_max_runtime"],
        )


@dataclass(frozen=True)
class SizingDecision:
    """Prescribed size of the next pre-production job (plus a log-worthy condition)."""

    size: JobSize
    warning: str | None = None


def grid_converged() -> bool:
    # @todo check grid  <->  WarmupFlag.GRID
    return True


def assess_warmup(
    past: Sequence[WarmupJobQC],
    iteration_errors: Callable[[], Sequence[float]],
    settings: WarmupSettingsQC,
    nseeds_next: int = 1,
) -> WarmupAssessmentQC:
    """Assess the warmup QC criteria and decide whether another warmup step is needed.

    `nseeds_next` is the number of parallel seeds available for the next step:
    the step's total statistics (grown by `fac_increment` w.r.t. the last
    step's total) is split evenly over them, with a per-seed floor of
    `ncall_start` (fewer seeds are used if the floor would over-shoot the
    plan), and the runtime cap is applied per seed.  The first step always
    runs a single seed (it creates the grid file).

    `past` are the successfully terminated warmup *steps*, most recent first
    (the parallel seeds of a step folded into one `WarmupJobQC` by
    `combine_warmup_jobs`), with degenerate rows (`ntot == 0`) excluded by the
    caller — `ntot` is a divisor here; `iteration_errors` supplies the
    per-iteration errors of the most recent step (all seeds concatenated: the
    relative spread is scale-free) — a callable because fetching them costs file IO: it is only
    invoked once the QC can actually conclude on data quality (CONST_ERR),
    never when the decision is already forced (no history, skip,
    MAX_INCREMENT, RUNTIME, or a mandatory increment step outstanding).  An
    empty result means "unavailable": no basis to assess error stability,
    CONST_ERR stays unset.  Assumes `min_increment_steps >= 2`: the SCALING
    criterion needs a next-to-last warmup to compare against.
    """
    wflag: WarmupFlag = WarmupFlag(0)

    # > no previous warmup? prescribe the first one
    if len(past) == 0:
        return WarmupRequiredQC(size=settings.start)

    # > QC skip requested (`submit --no-warmup`): accept the current grid
    if settings.skip_qc:
        return WarmupCompleteQC(flags=wflag | WarmupFlag.SKIPPED)

    # > check increment steps
    if len(past) >= settings.min_increment_steps:
        wflag |= WarmupFlag.MIN_INCREMENT
    if len(past) >= settings.max_increment_steps:
        wflag |= WarmupFlag.MAX_INCREMENT
        return WarmupCompleteQC(flags=wflag)

    # > last warmup (LW)
    LW: WarmupJobQC = past[0]
    # > a vanishing result with a non-zero error (large cancellations) must not divide
    if (LW.result == 0.0 and LW.error == 0.0) or (
        LW.result != 0.0 and abs(LW.error / LW.result) <= settings.target_rel_acc
    ):
        wflag |= WarmupFlag.RELACC
    if LW.chi2dof < settings.max_chi2dof:
        wflag |= WarmupFlag.CHI2DOF
    if grid_converged():
        wflag |= WarmupFlag.GRID

    # > settings for the next warmup (NW) step: grow the *step total* and split it
    # > over the seeds available now (per-seed floor: `ncall_start`)
    NW_ncall_total: int = int(LW.size.ncall * LW.nseeds * settings.fac_increment)
    NW_nseeds: int = max(1, nseeds_next)
    NW_ncall: int = max(-(-NW_ncall_total // NW_nseeds), settings.start.ncall)  # ceil division
    NW_nseeds = max(1, min(NW_nseeds, -(-NW_ncall_total // NW_ncall)))
    NW_niter: int = LW.size.niter
    NW_ntot: int = NW_ncall * NW_niter  # per seed
    # > per-seed wall-time estimate from the per-seed numbers of the last step
    NW_time_estimate: float = LW.elapsed_time * float(NW_ntot) / float(LW.size.ntot)
    # > try to accommodate runtime limit by reducing iterations
    if NW_time_estimate > settings.job_max_runtime:
        NW_niter = int(NW_niter * settings.job_max_runtime / NW_time_estimate)
        if NW_niter <= 0:
            return WarmupCompleteQC(flags=wflag | WarmupFlag.RUNTIME)

    next_size = JobSize(ncall=NW_ncall, niter=NW_niter)

    # > need to ensure that we have enough increment steps
    if WarmupFlag.MIN_INCREMENT not in wflag:
        return WarmupRequiredQC(size=next_size, flags=wflag, nseeds=NW_nseeds)

    # > only now can the QC conclude on data quality: fetch the iteration
    # > errors (the sole file-IO input) for the error-stability criterion
    if err_list := iteration_errors():
        err_mean: float = sum(err_list) / len(err_list)
        err_stdv: float = math.sqrt(sum((err - err_mean) ** 2 for err in err_list) / len(err_list))
        if err_mean == 0.0 or err_stdv / err_mean < settings.max_err_rel_var:
            wflag |= WarmupFlag.CONST_ERR

    # > next-to-last warmup (NLW): error scaling with the step-total statistics
    if len(past) >= 2:
        NLW: WarmupJobQC = past[1]
        scaling: float = 1.0
        if NLW.error != 0.0:
            scaling = (LW.error / NLW.error) * math.sqrt(float(LW.ntot) / float(NLW.ntot))
        if abs(scaling - 1.0) <= settings.scaling_window:
            wflag |= WarmupFlag.SCALING

    # > already reached accuracy and can trust it (chi2dof)
    if WarmupFlag.RELACC in wflag and WarmupFlag.CHI2DOF in wflag and WarmupFlag.CONST_ERR in wflag:
        return WarmupCompleteQC(flags=wflag)

    # > warmup has converged
    if (
        WarmupFlag.CHI2DOF in wflag
        and WarmupFlag.CONST_ERR in wflag
        and WarmupFlag.GRID in wflag
        and WarmupFlag.SCALING in wflag
    ):
        return WarmupCompleteQC(flags=wflag)

    # > need more warmup iterations
    return WarmupRequiredQC(size=next_size, flags=wflag, nseeds=NW_nseeds)


def size_preproduction(last_warmup: WarmupJobQC, settings: PreProductionSettings) -> SizingDecision:
    """Size the pre-production job from the highest-statistics warmup.

    The statistics estimate targets `penalty_wrt_warmup * job_max_runtime`
    (runtime penalty warmup -> production), capped by the statistics needed
    to reach the target accuracy, and floored at `ncall_start`.
    """
    if last_warmup.elapsed_time <= 0.0:
        # > broken/missing runtime metadata (e.g. a log without an "Elapsed time" line):
        # > no basis for a statistics estimate; fall back to the minimal pre-production
        return SizingDecision(
            size=settings.start,
            warning=(
                f"pre-production: warmup {last_warmup.id} has no usable runtime;"
                + " falling back to ncall_start"
            ),
        )

    # > per-seed statistics and runtime: what one production job can do in the time budget
    PP_ntot: int = last_warmup.size.ntot * int(
        settings.penalty_wrt_warmup * settings.job_max_runtime / last_warmup.elapsed_time
    )
    if last_warmup.result != 0.0 and last_warmup.error != 0.0:
        # > step-total statistics and combined error: what is needed for the target accuracy
        PP_ntot_acc: int = last_warmup.ntot * int(
            (last_warmup.error / last_warmup.result / settings.target_rel_acc) ** 2
        )
        PP_ntot = min(PP_ntot, PP_ntot_acc)
    PP_ncall: int = int(PP_ntot) // settings.start.niter
    if PP_ncall < settings.start.ncall:
        PP_ncall = settings.start.ncall

    return SizingDecision(size=JobSize(ncall=PP_ncall, niter=settings.start.niter))


def resize_failed_preproduction(failed: JobSize, settings: PreProductionSettings) -> SizingDecision:
    """Size the retry after a failed pre-production.

    Half the statistics of the failed attempt, floored at `ncall_start`.
    """
    PP_ntot: int = failed.ntot // 2
    PP_ncall: int = max(1, PP_ntot // settings.start.niter)
    warning: str | None = None
    if PP_ncall < settings.start.ncall:
        PP_ncall = settings.start.ncall
        warning = "pre-production failed after reaching minimum ncall"
    return SizingDecision(size=JobSize(ncall=PP_ncall, niter=settings.start.niter), warning=warning)
