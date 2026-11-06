"""A cluster policy must be fully configured, with a renderable template, before anything runs."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from test_tick import FIXTURES, Run

from dokan.config import Config, read_config_json
from dokan.exe import ExecutionPolicy
from dokan.exe.htcondor import HTCondorExec
from dokan.exe.slurm import SlurmExec

SHIPPED = Path(__file__).resolve().parent.parent / "src" / "dokan" / "exe"


def _config(tmp_path: Path, **policy_settings) -> Config:
    config = Config.__new__(Config)
    config.data = read_config_json(FIXTURES / "config.json")
    config.data["exe"]["policy"] = ExecutionPolicy(config.data["exe"]["policy"])
    assert config.data["exe"]["policy"] == ExecutionPolicy.HTCONDOR
    config.data["exe"]["policy_settings"] = policy_settings
    config.path = tmp_path
    return config


def test_complete_htcondor_settings(tmp_path: Path) -> None:
    (tmp_path / "t.sub").write_text("")
    config = _config(
        tmp_path,
        htcondor_template="t.sub",
        htcondor_nretry=2,
        htcondor_retry_delay=0.0,
        htcondor_poll_time=10.0,
    )
    assert config.missing_policy_settings() == []  # > `htcondor_ncores` is optional


def test_missing_settings_and_template_file(tmp_path: Path) -> None:
    config = _config(tmp_path, htcondor_template="t.sub", htcondor_nretry=2)
    missing = config.missing_policy_settings()
    assert "htcondor_retry_delay" in missing and "htcondor_poll_time" in missing
    assert any("template file 't.sub'" in m for m in missing)


@pytest.mark.parametrize(
    ("cls", "template"),
    [
        (HTCondorExec, SHIPPED / "htcondor" / "htcondor.template"),
        (HTCondorExec, SHIPPED / "htcondor" / "lxplus.template"),
        (SlurmExec, SHIPPED / "slurm" / "slurm.template"),
    ],
)
def test_shipped_templates_render(cls, template: Path) -> None:
    cls.check_template(template)


def test_unknown_placeholder_and_stray_dollar(tmp_path: Path) -> None:
    bad = tmp_path / "bad.template"
    bad.write_text("executable = ${exe}\nrequest_memory = ${memory}\n")
    with pytest.raises(ValueError, match=r"unknown placeholder \$\{memory\}"):
        HTCondorExec.check_template(bad)
    bad.write_text("arguments = $ 1\n")
    with pytest.raises(ValueError, match=r"write \$\$ for a literal \$"):
        HTCondorExec.check_template(bad)


def _tick_fails_untouched(run: Run, message: str) -> None:
    proc = run.cli("tick", str(run.path), check=False)
    assert proc.returncode != 0
    assert message in proc.stdout + proc.stderr, proc.stdout + proc.stderr
    assert not (run.path / "db.sqlite").exists()
    assert run.condor.calls("condor_submit") == []


def test_tick_refuses_a_broken_template(tmp_path: Path) -> None:
    run = Run.create(tmp_path)
    template = run.path / "htcondor.template"
    template.write_text(template.read_text() + "\n+MaxRuntime = ${max_wall}\n")
    _tick_fails_untouched(run, "unknown placeholder ${max_wall}")


def test_tick_refuses_missing_settings(tmp_path: Path) -> None:
    run = Run.create(tmp_path)
    config = read_config_json(run.path / "config.json")
    del config["exe"]["policy_settings"]["htcondor_poll_time"]
    (run.path / "config.json").write_text(json.dumps(config, indent=1))
    _tick_fails_untouched(run, "missing: htcondor_poll_time")
