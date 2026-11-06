"""`parse_log_file` on a kakuhen (Basin) NNLOJET log: complete and truncated."""

from pathlib import Path

import pytest

from dokan.nnlojet import parse_log_file

LOG = Path(__file__).resolve().parent / "fixtures" / "nnlojet_LO.log"


def test_complete_basin_log() -> None:
    data = parse_log_file(LOG)
    assert [it["iteration"] for it in data["iterations"]] == [1, 2]
    assert data["result"] == pytest.approx(4.36411990e04)
    assert data["error"] == pytest.approx(5.53600423e03)
    assert data["chi2dof"] == pytest.approx(2.43783873)
    assert data["elapsed_time"] == pytest.approx(1.41725)


def _truncate_after(tmp_path: Path, marker: str, keep: int = 0) -> Path:
    """Copy the fixture up to the line containing `marker`, plus `keep` characters of it."""
    text = LOG.read_text()
    cut = text.index(marker) + keep
    out = tmp_path / "job.s1.log"
    out.write_text(text[:cut])
    return out


def test_truncated_in_second_iteration_keeps_the_first(tmp_path: Path) -> None:
    log = _truncate_after(tmp_path, "accum. integral = 4.36411990e+04")
    data = parse_log_file(log)
    assert [it["iteration"] for it in data["iterations"]] == [1]
    assert data["result"] == pytest.approx(5.82162044e04)
    assert "elapsed_time" not in data


def test_number_cut_at_eof_is_not_taken(tmp_path: Path) -> None:
    """A numeric token cut off by the end of the file must not be read as a (wrong) value."""
    log = _truncate_after(tmp_path, "chi**2/iteration = 2.43783873e+00", keep=len("chi**2/iteration = 2.43"))
    data = parse_log_file(log)
    assert [it["iteration"] for it in data["iterations"]] == [1]


def test_malformed_number_keeps_the_completed_prefix(tmp_path: Path) -> None:
    text = LOG.read_text().replace("3.85150754e+04", "3.85150754eXX4")
    log = tmp_path / "job.s1.log"
    log.write_text(text)
    data = parse_log_file(log)
    assert [it["iteration"] for it in data["iterations"]] == [1]
