"""`config.json` explains its options with `//` comments that the loader strips."""

from dokan.config import _DOC, Config, _schema, annotate_config_json, read_config_json


def test_every_option_of_the_schema_is_documented():
    missing = [
        f"{section}.{key}"
        for section, keys in _schema.items()
        for key in keys
        if key not in _DOC.get(section, {}) and (section, key) != ("warmup", "skip_qc") and key != "detached"
    ]
    assert missing == []


def test_written_config_round_trips_through_the_loader(tmp_path):
    config = Config(default_ok=True)
    config.set_path(tmp_path / "run")
    config["run"]["histograms"] = {"cross": {"nx": 0}}
    config.write()
    text = (tmp_path / "run" / "config.json").read_text()
    assert text.startswith("// dokan run configuration")
    assert "    // relative accuracy at which dispatch stops" in text
    assert '"target_rel_acc"' in text.split("relative accuracy at which dispatch stops")[1].splitlines()[1]
    assert read_config_json(tmp_path / "run" / "config.json") == config.data
    # > and it loads back through the normal path
    assert Config(path=tmp_path / "run", default_ok=False, check_md5=False).data == config.data


def test_annotation_leaves_nested_tables_alone():
    text = annotate_config_json(
        '{\n  "run": {\n    "histograms": {\n      "cross": {\n        "nx": 0\n      }\n    }\n  }\n}'
    )
    comments = [line for line in text.splitlines() if line.lstrip().startswith("//")]
    assert len(comments) == 2  # the header and the histograms line only
