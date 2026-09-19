"""The per-part optimisation error: which observables it looks at, and how."""

import math
from pathlib import Path

import pytest

from dokan.config import Config, check_opt_target
from dokan.db._dbmerge import ObsFigure, opt_target_label, optimisation_error

CROSS = ObsFigure("cross", 100.0, 1.0)  # 1%
GOOD = ObsFigure("ptl", 50.0, 0.5, bins=((10.0, 0.1), (20.0, 0.2), (5.0, 1.0)))  # 1%; worst bin 20%
BAD = ObsFigure("yj", 20.0, 2.0, bins=((10.0, 0.5), (0.1, 3.0)))  # 10%; worst *significant* bin 5%
NOISE = ObsFigure("cancel", -0.13, 947.7)  # not a measurement: excluded everywhere


def test_default_is_the_worst_significant_integral():
    rel, worst = optimisation_error([CROSS, GOOD, BAD, NOISE], 0.01, "hist")
    assert worst == pytest.approx(0.10) and rel == worst


def test_cross_hist_is_the_geometric_mean():
    rel, _ = optimisation_error([CROSS, GOOD, BAD], 0.01, "cross_hist")
    assert rel == pytest.approx(math.sqrt(0.01 * 0.10))


def test_cross_ignores_the_histograms():
    rel, _ = optimisation_error([CROSS, GOOD, BAD], 0.01, "cross", opt_observables=["yj"])
    assert rel == 0.01


def test_selection_restricts_the_maximum():
    rel, _ = optimisation_error([CROSS, GOOD, BAD], 0.01, "hist", opt_observables=["ptl"])
    assert rel == pytest.approx(0.01)


def test_bins_judge_by_the_worst_significant_bin():
    rel, _ = optimisation_error([GOOD], 0.01, "hist", opt_observables=["ptl"], opt_bins=True)
    assert rel == pytest.approx(0.20)  # (5.0, 1.0) is the worst bin at 5 sigma
    rel, _ = optimisation_error([BAD], 0.01, "hist", opt_observables=["yj"], opt_bins=True)
    assert rel == pytest.approx(0.05)  # (0.1, 3.0) is consistent with zero and skipped


def test_bins_fall_back_to_the_integral_without_bins():
    rel, _ = optimisation_error([CROSS], 0.02, "hist", opt_observables=["cross"], opt_bins=True)
    assert rel == pytest.approx(0.01)


def test_nothing_significant_falls_back_to_the_cross_error():
    rel, worst = optimisation_error([NOISE], 0.03, "hist", opt_observables=["cancel"])
    assert rel == worst == 0.03


def _config(**run) -> Config:
    config = Config(default_ok=True)
    config["run"]["histograms"] = {
        "cross": {"nx": 0},
        "ptl": {"nx": 3},
        "tcut_accum": {"nx": 1, "cumulant": -1},
    }
    for key, value in run.items():
        config["run"][key] = value
    return config


def test_validation_rejects_unknown_and_cumulant_names():
    check_opt_target(_config(opt_observables=["ptl"]))
    with pytest.raises(ValueError, match="not in the runcard"):
        check_opt_target(_config(opt_observables=["nope"]))
    with pytest.raises(ValueError, match="cumulant"):
        check_opt_target(_config(opt_observables=["tcut_accum"]))
    with pytest.raises(ValueError, match="opt_target"):
        check_opt_target(_config(opt_target="bins"))


def test_label_names_the_selection():
    assert opt_target_label(_config()) == "cross_hist"
    assert (
        opt_target_label(_config(opt_target="hist", opt_observables=["ptl"], opt_bins=True))
        == "hist[ptl; bins]"
    )
    assert opt_target_label(_config(opt_target="cross", opt_observables=["ptl"])) == "cross"


# ----------------------------------------------------------------------------------------
# the run-level target of `_distribute_time`
# ----------------------------------------------------------------------------------------

from dokan.db._dbopt import hist_objective  # noqa: E402


def _write_part(run: Path, name: str, cross: tuple[float, float], hist: dict[str, list[tuple[float, float]]]):
    part = run / "result" / "part" / name
    part.mkdir(parents=True)
    (part / "cross.dat").write_text(f"#labels: tot[1] tot_Err[2]\n{cross[0]:e} {cross[1]:e}\n")
    for obs, bins in hist.items():
        lines = [f"{i:e} {i + 0.5:e} {i + 1:e} {v:e} {e:e}" for i, (v, e) in enumerate(bins)]
        header = "#labels: lo[1] c[2] hi[3] tot[4] tot_Err[5]\n"
        (part / f"{obs}.dat").write_text(header + "\n".join(lines) + "\n")


def _run_config(**run) -> dict:
    histograms = {"cross": {"nx": 0}, "ptz": {"nx": 3}, "ptz_A1": {"nx": 3}, "acc": {"nx": 1, "cumulant": -1}}
    run_defaults = {"opt_target": "hist", "opt_observables": [], "opt_bins": False}
    config = {"run": {"histograms": histograms, **run_defaults}}
    config["run"].update(run)
    return config


PARTS = {1: "LO_1", 2: "R_1"}


@pytest.fixture
def campaign(tmp_path: Path) -> Path:
    # > two parts: a big well-determined one (LO-like) and a small noisy one (R-like).  The
    # > projector histogram `ptz_A1` is small (~1% of ptz) and its error is dominated by the
    # > small part in every bin.
    _write_part(tmp_path, "LO_1", (1000.0, 1.0),
                {"ptz": [(600.0, 0.6), (400.0, 0.4)], "ptz_A1": [(6.0, 0.02), (4.0, 0.02)]})
    _write_part(tmp_path, "R_1", (-100.0, 5.0),
                {"ptz": [(-60.0, 3.0), (-40.0, 4.0)], "ptz_A1": [(-0.5, 0.5), (-0.8, 0.3)]})
    return tmp_path


def test_cross_target_has_no_run_level_objective(campaign):
    assert hist_objective(campaign, _run_config(opt_target="cross"), {1: "LO_1", 2: "R_1"}) is None


def test_worst_merged_bin_is_the_target_and_parts_get_their_absolute_error(campaign):
    obj = hist_objective(campaign, _run_config(opt_observables=["ptz_A1"], opt_bins=True), PARTS)
    # > merged bins: (5.5 +/- 0.5004) rel 9.1% and (3.2 +/- 0.3007) rel 9.4% -> bin 1
    assert obj.label == "ptz_A1[bin 1]"
    assert obj.value == pytest.approx(3.2) and obj.error == pytest.approx(math.hypot(0.02, 0.3))
    assert obj.scale == pytest.approx(900.0 / 3.2)
    # > R_1 carries the target's error: the ratio of the parts' errors is 0.3 / 0.02, not the
    # > ratio of the per-part proxies (rel. error x cross section)
    assert obj.errors[2] / obj.errors[1] == pytest.approx(15.0)
    # > quadrature sum over the parts / total cross section = the target's relative error
    assert math.sqrt(sum(e**2 for e in obj.errors.values())) / 900.0 == pytest.approx(obj.rel_error)


def test_integral_target_without_opt_bins(campaign):
    obj = hist_objective(campaign, _run_config(opt_observables=["ptz"]), {1: "LO_1", 2: "R_1"})
    assert obj.label == "ptz[integral]"
    assert obj.value == pytest.approx(900.0)  # unit bin widths, no overflow line
    assert set(obj.errors) == {1, 2}


def test_selection_defaults_to_every_non_cumulant_histogram(campaign):
    obj = hist_objective(campaign, _run_config(opt_bins=True), {1: "LO_1", 2: "R_1"})
    assert obj.label.startswith("ptz_A1")  # the projector histogram has the worse bins


def test_cross_hist_is_the_geometric_mean_per_part(campaign):
    hist = hist_objective(campaign, _run_config(opt_observables=["ptz"], opt_bins=True), PARTS)
    both = hist_objective(
        campaign, _run_config(opt_target="cross_hist", opt_observables=["ptz"], opt_bins=True), PARTS
    )
    assert both.errors[1] == pytest.approx(math.sqrt(hist.errors[1] * 1.0))
    assert both.errors[2] == pytest.approx(math.sqrt(hist.errors[2] * 5.0))


def test_parts_without_files_are_left_to_the_caller(campaign):
    obj = hist_objective(campaign, _run_config(opt_observables=["ptz"]), {1: "LO_1", 2: "R_1", 3: "V_1"})
    assert 3 not in obj.errors


def test_nothing_significant_gives_no_objective(tmp_path):
    _write_part(tmp_path, "R_1", (-100.0, 5.0), {"ptz": [(0.1, 3.0), (-0.2, 4.0)]})
    assert hist_objective(tmp_path, _run_config(opt_observables=["ptz"], opt_bins=True), {1: "R_1"}) is None
    assert hist_objective(tmp_path, _run_config(opt_observables=["ptz"]), {1: "R_1"}) is None
