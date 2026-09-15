"""The per-part optimisation error: which observables it looks at, and how."""

import math

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
