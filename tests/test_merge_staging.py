"""HDF5 staging of per-job histograms: recovery and stale-row detection (`build_obs_group`)."""

import os
from pathlib import Path

import h5py
import numpy as np

from dokan.merge._core import build_obs_group, has_stale_rows

_HIST = {"cross": {"nx": 0}}


def _write_cross(path: Path, value: float, neval: int = 100) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"#labels: tot[1] tot_Err[2]\n#neval: {neval}\n#nx: 0\n {value:.6E} {0.1 * value:.6E}\n")
    return path.name


def _stage(tmp_path: Path, names: list[str]) -> Path:
    hdf5 = tmp_path / "part.hdf5"
    build_obs_group(hdf5, "pt", {"cross": names}, _HIST, tmp_path)
    return hdf5


def test_new_group_is_born_committed(tmp_path: Path) -> None:
    names = [_write_cross(tmp_path / f"j.cross.s{i}.dat", 1.0 + i) for i in range(3)]
    hdf5 = _stage(tmp_path, names)
    with h5py.File(hdf5, "r") as h5f:
        grp = h5f["pt/cross"]
        assert int(grp.attrs["ndat_valid"]) == 3
        assert int(grp.attrs["rows_committed"]) == 1
        assert float(grp.attrs["timestamp"]) > 0
    assert not has_stale_rows(hdf5, "pt", {"cross": names})


def test_append_folds_timestamp(tmp_path: Path) -> None:
    names = [_write_cross(tmp_path / "j.cross.s1.dat", 1.0)]
    hdf5 = _stage(tmp_path, names)
    with h5py.File(hdf5, "r") as h5f:
        ts1 = float(h5f["pt/cross"].attrs["timestamp"])
    late = tmp_path / "j.cross.s2.dat"
    _write_cross(late, 2.0)

    os.utime(late, (ts1 + 100, ts1 + 100))
    names.append(late.name)
    build_obs_group(hdf5, "pt", {"cross": names}, _HIST, tmp_path)
    with h5py.File(hdf5, "r") as h5f:
        assert int(h5f["pt/cross"].attrs["ndat_valid"]) == 2
        assert float(h5f["pt/cross"].attrs["timestamp"]) == ts1 + 100


def test_interrupted_timestamp_is_recovered(tmp_path: Path) -> None:
    """`ndat_valid` committed but the crash hit before the timestamp was written."""
    names = [_write_cross(tmp_path / "j.cross.s1.dat", 1.0)]
    hdf5 = _stage(tmp_path, names)
    with h5py.File(hdf5, "a") as h5f:
        h5f["pt/cross"].attrs["timestamp"] = -1.0
    build_obs_group(hdf5, "pt", {"cross": names}, _HIST, tmp_path)
    with h5py.File(hdf5, "r") as h5f:
        assert float(h5f["pt/cross"].attrs["timestamp"]) == (tmp_path / names[0]).stat().st_mtime


def test_flagged_group_still_takes_new_files(tmp_path: Path) -> None:
    """A group left flagged mid-merge must not be skipped by a later, normal merge."""
    names = [_write_cross(tmp_path / "j.cross.s1.dat", 1.0)]
    hdf5 = _stage(tmp_path, names)
    with h5py.File(hdf5, "a") as h5f:
        h5f["pt/cross"].attrs["timestamp"] = -1.0
    names.append(_write_cross(tmp_path / "j.cross.s2.dat", 2.0))
    build_obs_group(hdf5, "pt", {"cross": names}, _HIST, tmp_path)
    with h5py.File(hdf5, "r") as h5f:
        assert int(h5f["pt/cross"].attrs["ndat_valid"]) == 2
        assert float(h5f["pt/cross"].attrs["timestamp"]) > 0


def test_stale_row_of_a_dropped_input(tmp_path: Path) -> None:
    """A failed seed ingested by an older version is not among the current inputs."""
    names = [_write_cross(tmp_path / f"j.cross.s{i}.dat", 1.0) for i in (1, 2)]
    hdf5 = _stage(tmp_path, names)
    assert has_stale_rows(hdf5, "pt", {"cross": names[:1]})


def _legacy_group(hdf5: Path) -> None:
    """Make the group look like one written before `rows_committed` existed."""
    with h5py.File(hdf5, "a") as h5f:
        del h5f["pt/cross"].attrs["rows_committed"]


def test_unwritten_legacy_rows_are_stale(tmp_path: Path) -> None:
    names = [_write_cross(tmp_path / f"j.cross.s{i}.dat", 1.0) for i in (1, 2)]
    hdf5 = _stage(tmp_path, names)
    _legacy_group(hdf5)
    with h5py.File(hdf5, "a") as h5f:
        h5f["pt/cross/neval"][1] = 0  # the fill value of a row that was never written
    assert has_stale_rows(hdf5, "pt", {"cross": names})


def test_zero_neval_input_is_not_stale_in_a_new_group(tmp_path: Path) -> None:
    """A legitimate `#neval 0` input must not trigger a rebuild on every merge."""
    names = [
        _write_cross(tmp_path / "j.cross.s1.dat", 1.0),
        _write_cross(tmp_path / "j.cross.s2.dat", 0.0, 0),
    ]
    hdf5 = _stage(tmp_path, names)
    with h5py.File(hdf5, "r") as h5f:
        assert np.any(h5f["pt/cross/neval"][:2] == 0)
    assert not has_stale_rows(hdf5, "pt", {"cross": names})


def test_missing_ndat_valid_is_pinned(tmp_path: Path) -> None:
    """An older version's failed first ingestion left no `ndat_valid`: pin it, never guess."""
    names = [_write_cross(tmp_path / "j.cross.s1.dat", 1.0)]
    hdf5 = _stage(tmp_path, names)
    with h5py.File(hdf5, "a") as h5f:
        del h5f["pt/cross"].attrs["ndat_valid"]
    build_obs_group(hdf5, "pt", {"cross": names}, _HIST, tmp_path)
    with h5py.File(hdf5, "r") as h5f:
        assert int(h5f["pt/cross"].attrs["ndat_valid"]) == 1
