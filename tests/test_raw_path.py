"""`run.raw_path`: job output and the per-part HDF5 cache live there, symlinked from the run."""

from __future__ import annotations

from pathlib import Path

import pytest
from test_tick import RUNNING, Run

from dokan.util import relocate_and_link

# ---------------------------------------------------------------------------
# > relocate_and_link
# ---------------------------------------------------------------------------


def test_relocate_moves_and_links(tmp_path: Path) -> None:
    local, dest = tmp_path / "run" / "f.dat", tmp_path / "raw" / "sub" / "f.dat"
    local.parent.mkdir()
    local.write_text("x")
    relocate_and_link(local, dest)
    assert local.is_symlink() and local.resolve() == dest.resolve()
    assert dest.read_text() == "x"
    relocate_and_link(local, dest)  # > repeat: no-op
    assert local.is_symlink() and dest.read_text() == "x"


def test_relocate_restores_a_lost_link(tmp_path: Path) -> None:
    local, dest = tmp_path / "f.dat", tmp_path / "raw" / "f.dat"
    dest.parent.mkdir()
    dest.write_text("x")
    relocate_and_link(local, dest)
    assert local.is_symlink() and local.read_text() == "x"


def test_relocate_overwrites_a_partial_copy(tmp_path: Path) -> None:
    local, dest = tmp_path / "f.dat", tmp_path / "raw" / "f.dat"
    dest.parent.mkdir()
    dest.write_text("partial")
    local.write_text("full")
    relocate_and_link(local, dest)
    assert local.read_text() == "full"


def test_relocate_nothing_to_do(tmp_path: Path) -> None:
    relocate_and_link(tmp_path / "a", tmp_path / "b")
    assert not (tmp_path / "a").exists() and not (tmp_path / "a").is_symlink()


def test_relocate_refuses_a_directory_destination(tmp_path: Path) -> None:
    local, dest = tmp_path / "f.dat", tmp_path / "raw"
    local.write_text("x")
    dest.mkdir()
    with pytest.raises(FileExistsError):
        relocate_and_link(local, dest)


# ---------------------------------------------------------------------------
# > a campaign with a raw path
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def run(tmp_path_factory) -> tuple[Run, Path]:
    base = tmp_path_factory.mktemp("raw_path")
    run = Run.create(base)
    raw_path = base / "raw_store"
    run.config(**{"run.raw_path": str(raw_path)})
    return run, raw_path


def _hdf5_links(run: Run, raw_path: Path) -> dict[str, Path]:
    links = {}
    for hdf5 in sorted((run.path / "raw").glob("*.hdf5")):
        assert hdf5.is_symlink(), hdf5
        assert hdf5.resolve() == (raw_path / "raw" / hdf5.name).resolve()
        links[hdf5.stem] = hdf5
    return links


def test_production_output_and_cache_live_in_raw_path(run):
    run, raw_path = run
    run.tick()
    for _ in range(3):
        run.condor.finish_all()
        out = run.tick()
    assert "phase=production" in out, out
    assert run.count(mode=2, status=RUNNING) == 4

    links = _hdf5_links(run, raw_path)
    assert set(links) == {"LO_1", "LO_2"}
    # > merged job output moved, symlinked from the batch directory
    dat = next((run.path / "raw" / "production" / "LO_1" / "s1").glob("*.dat"))
    assert dat.is_symlink() and dat.resolve().is_relative_to(raw_path.resolve())
    assert (run.path / "result" / "merge" / "cross.dat").is_file()


def test_unusable_linked_cache_is_rebuilt_in_raw_path(run):
    run, raw_path = run
    links = _hdf5_links(run, raw_path)
    # > every part's cache is garbage now (e.g. a writer killed mid-write)
    for link in links.values():
        link.resolve().write_bytes(b"not an hdf5 file")
    for _ in range(6):
        run.condor.finish_all()
        run.tick()
        messages = [m for (m,) in run.sql("select message from log", db="log.sqlite")]
        if any("unusable HDF5 staging cache" in m for m in messages):
            break
    assert any("unusable HDF5 staging cache" in m for m in messages)
    # > still (or again) links into the raw path, and readable
    import h5py

    for name, link in _hdf5_links(run, raw_path).items():
        if link.resolve().read_bytes().startswith(b"not an"):
            continue  # > this part had no merge since
        with h5py.File(link, "r") as h5f:
            assert name in h5f
