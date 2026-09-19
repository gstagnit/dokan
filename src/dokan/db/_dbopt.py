"""The optimisation target: what `_distribute_time` minimises and how it is measured.

Two layers.  `optimisation_error` is the *per-part* figure `MergePart` stores in
`Part.error` (unchanged semantics: the worst significant relative error of the
selected observables, scaled to the part's cross section).  `hist_objective` is the
*run-level* figure `_distribute_time` uses for the `hist` / `cross_hist` targets: it
picks one bin of the merged (all parts summed) histograms and gives every part its
absolute error in that bin -- the per-part numbers are then contributions to a single
additive quantity, which is what the Euler-Lagrange allocation formula assumes.

Why the run-level figure is needed.  The E-L optimum `T_i ~ sigma_i sqrt(T_i)` is
exact only when the `sigma_i` are the errors of the parts on *one* observable.  The
per-part figure is not that: each part judges itself on whichever of its own bins is
worst (a different bin per part) and rescales that *relative* error by its own cross
section.  For a plain distribution this is an acceptable proxy, since the bins of
every part scale like its cross section.  For a projector-weighted histogram
(`fac=proj_A1`, bins ~ A_1 dsigma with A_1 ~ 0.01 and changing sign) the rescaling
inflates the error by ~1/A_1 with a factor that differs from part to part, and the
allocation is scrambled: observed in one campaign, the LO and V parts received 77% of
the time while carrying 3% of the target's variance.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

# > An observable (or a bin of one) must be a *measurement* before its relative error
# > may steer the optimisation: `|value| > _HIST_SIGNIFICANCE * error`.  A differential
# > distribution with large bin-to-bin cancellations can integrate to something
# > consistent with zero while keeping a perfectly finite error, and its relative error
# > is then meaningless and unboundedly large: one observed case had an integral of
# > -0.13 +/- 947.7, giving |e/r| = 7160, which set the maximum for its part and through
# > it the part's whole optimisation error (70,415 against a cross section of
# > -947.7 +/- 730.8).  Guarding only on `r != 0.0` catches exact zeros and misses
# > precisely this.  The consequences were not cosmetic: `_distribute_time` allocates
# > by `Part.error`, so such a part soaks up the entire budget chasing a relative error
# > that cannot improve -- the denominator is zero by cancellation, not by lack of
# > statistics -- and the run's reported accuracy and `T_target` are nonsense (39.3%
# > and 11200 kh, against a cross section determined to 1.1%).
_HIST_SIGNIFICANCE: float = 2.0

_OPT_TARGETS: tuple[str, ...] = ("cross", "cross_hist", "hist")


def _rel(value: float, error: float) -> float | None:
    """Relative error of a measurement; None when it is not one."""
    if value == 0.0 or abs(value) <= _HIST_SIGNIFICANCE * error:
        return None
    return abs(error / value)


@dataclass(frozen=True)
class ObsFigure:
    """One observable's contribution to a part's optimisation error.

    `result` / `error` are the integral over the distribution (the bare number for
    a total cross section); `bins` are the in-range bins' `(value, error)` for a
    differential distribution, empty otherwise.  The overflow is part of the
    integral but not of `bins`: it is rarely what a run is after, and it is often
    the worst-determined bin, which would let it steer the whole part.
    """

    name: str
    result: float
    error: float
    bins: tuple[tuple[float, float], ...] = ()

    def rel_error(self, *, per_bin: bool = False) -> float | None:
        """The worst relative error this observable asks the optimiser to bring down.

        By integral, or (`per_bin`) by its worst significant bin -- falling back
        to the integral for an observable without differential bins.  None when
        nothing about it is a measurement yet.
        """
        if per_bin and self.bins:
            rels = [rel for value, error in self.bins if (rel := _rel(value, error)) is not None]
            if rels:
                return max(rels)
        return _rel(self.result, self.error)


def read_obs_figure(file: Path, name: str, nx: int) -> ObsFigure | None:
    """The `ObsFigure` of a merged `.dat` file (`cross.dat`: nx = 0; a differential
    distribution: nx = 3), None when the file does not exist.

    The integral of a distribution is the bin-width-weighted sum of its bins plus the
    overflow; its error adds the bins in quadrature -- formally not the error of the
    integral, but a conservative figure for optimising on histograms.
    """
    if not file.exists():
        return None
    res, err = 0.0, 0.0
    bins: list[tuple[float, float]] = []
    if nx == 0:
        with open(file) as cross:
            for line in cross:
                if line.startswith("#"):
                    continue
                col: list[float] = [float(c) for c in line.split()]
                res = col[0]
                err = col[1] ** 2
                break
    elif nx == 3:
        with open(file) as diff:
            for line in diff:
                if line.startswith("#overflow"):
                    scol: list[str] = line.split()
                    res += float(scol[3])
                    err += float(scol[4]) ** 2
                if line.startswith("#"):
                    continue
                col = [float(c) for c in line.split()]
                res += (col[2] - col[0]) * col[3]
                err += ((col[2] - col[0]) * col[4]) ** 2
                bins.append((col[3], col[4]))
    else:
        raise ValueError(f"{file}: unexpected nx = {nx}")
    return ObsFigure(name, res, math.sqrt(err), tuple(bins))


def optimisation_error(
    figures: list[ObsFigure],
    rel_cross_err: float,
    opt_target: str,
    *,
    opt_observables=(),
    opt_bins: bool = False,
) -> tuple[float, float]:
    """`(relative error to optimise on, worst histogram relative error)` of one part.

    The histogram figure is the worst relative error over the observables -- all of
    them, or only those in `opt_observables` -- each judged by its integral or, with
    `opt_bins`, by its worst significant bin.  Nothing significant among them: the
    histograms say nothing yet about how well this part is determined, so the part
    is optimised on its cross section alone rather than on an invented number.  (An
    earlier version appended a synthetic `(1.0, 1e-9)` entry to keep the maximum
    non-empty; with the significance filter that would make such a part look
    perfectly determined.)

    `opt_target` then combines the two: `cross` ignores the histograms, `hist` uses
    them alone, `cross_hist` takes the geometric mean -- the histogram figure is a
    worst case, so a plain mean would over-weight it.

    This is the per-part fallback figure; see `hist_objective` for the run-level one.
    """
    selected: set[str] = set(opt_observables or ())
    candidates = [fig for fig in figures if not selected or fig.name in selected]
    rels: list[float] = [rel for fig in candidates if (rel := fig.rel_error(per_bin=opt_bins)) is not None]
    max_rel_hist_err: float = max(rels) if rels else rel_cross_err
    if opt_target == "cross":
        return rel_cross_err, max_rel_hist_err
    if opt_target == "cross_hist":
        return math.sqrt(rel_cross_err * max_rel_hist_err), max_rel_hist_err
    if opt_target == "hist":
        return max_rel_hist_err, max_rel_hist_err
    raise ValueError(f"unknown opt_target {opt_target!r}")


def opt_target_label(config: dict) -> str:
    """The target as reported next to the accuracy: `hist[ptl_1j]`, `cross_hist`, ..."""
    run: dict = config["run"]
    label: str = str(run["opt_target"])
    if label != "cross" and (selected := run.get("opt_observables")):
        label += "[" + ",".join(selected) + ("; bins" if run.get("opt_bins") else "") + "]"
    elif label != "cross" and run.get("opt_bins"):
        label += "[bins]"
    return label


@dataclass(frozen=True)
class Objective:
    """The run-level optimisation target of one dispatch.

    `errors[part_id]` is the part's absolute error on the target quantity, rescaled
    by `scale = |sum of the parts' cross sections| / |target value|` so that the
    quadrature sum over the parts divided by the total cross section -- the relative
    accuracy `_distribute_time` reports and `target_rel_acc` is compared with -- is
    the relative error of the target itself.  Parts without a merged histogram file
    are absent (the caller keeps their per-part figure).
    """

    label: str  # e.g. "ptz_A1[bin 3]" or "ptl[integral]"
    value: float  # merged value of the target quantity
    error: float  # merged error of the target quantity
    scale: float
    errors: dict[int, float]

    @property
    def rel_error(self) -> float:
        return abs(self.error / self.value)


def hist_objective(run_path: Path, config: dict, parts: dict[int, str]) -> Objective | None:
    """The run-level target for the `hist` and `cross_hist` optimisation targets.

    `parts` maps part id -> part name; each part's merged histograms are read from
    `<run_path>/result/part/<name>/<obs>.dat`.  Over the selected observables (all
    non-cumulant ones when `opt_observables` is empty), judged by their bins with
    `opt_bins` and by their integral otherwise, the *merged* quantity (values summed,
    errors in quadrature over the parts) with the worst significant relative error is
    the target; every part's error is its own absolute error on that quantity.  For
    `cross_hist` a part's error is the geometric mean of that and its cross-section
    error.  None when nothing is significant yet or no histogram has been merged:
    the caller then falls back to the per-part figures.
    """
    run: dict = config["run"]
    opt_target: str = run["opt_target"]
    if opt_target == "cross":
        return None
    if opt_target not in _OPT_TARGETS:
        raise ValueError(f"unknown opt_target {opt_target!r}")
    opt_bins: bool = bool(run.get("opt_bins", False))
    selected: set[str] = set(run.get("opt_observables") or ())
    histograms: dict = run["histograms"]

    cross: dict[int, ObsFigure] = {}
    figures: dict[str, dict[int, ObsFigure]] = {}
    for part_id, name in parts.items():
        base: Path = Path(run_path) / "result" / "part" / name
        if (fig := read_obs_figure(base / "cross.dat", "cross", 0)) is not None:
            cross[part_id] = fig
        for obs, info in histograms.items():
            if "cumulant" in info or (selected and obs not in selected):
                continue
            if (fig := read_obs_figure(base / f"{obs}.dat", obs, int(info["nx"]))) is not None:
                figures.setdefault(obs, {})[part_id] = fig

    # > candidate targets: (relative error, observable, bin index or None for the integral)
    candidates: list[tuple[float, str, int | None, float, float]] = []
    for obs, per_part in figures.items():
        with_bins = [fig for fig in per_part.values() if fig.bins]
        if opt_bins and with_bins:
            for b in range(min(len(fig.bins) for fig in with_bins)):
                value = sum(fig.bins[b][0] for fig in with_bins)
                error = math.sqrt(sum(fig.bins[b][1] ** 2 for fig in with_bins))
                if (rel := _rel(value, error)) is not None:
                    candidates.append((rel, obs, b, value, error))
        else:
            value = sum(fig.result for fig in per_part.values())
            error = math.sqrt(sum(fig.error**2 for fig in per_part.values()))
            if (rel := _rel(value, error)) is not None:
                candidates.append((rel, obs, None, value, error))
    if not candidates:
        return None
    _, obs, b, value, error = max(candidates, key=lambda c: c[0])

    sigma_tot: float = sum(fig.result for fig in cross.values())
    if sigma_tot == 0.0:  # > e.g. an order-only run whose corrections cancel: keep the scale finite
        sigma_tot = sum(abs(fig.result) for fig in cross.values())
    scale: float = abs(sigma_tot / value) if sigma_tot != 0.0 else 1.0

    errors: dict[int, float] = {}
    for part_id, fig in figures[obs].items():
        if b is not None and not fig.bins:
            continue
        err: float = (fig.bins[b][1] if b is not None else fig.error) * scale
        if opt_target == "cross_hist" and part_id in cross:
            err = math.sqrt(err * cross[part_id].error)
        errors[part_id] = err
    label: str = f"{obs}[bin {b}]" if b is not None else f"{obs}[integral]"
    return Objective(label, value, error, scale, errors)
