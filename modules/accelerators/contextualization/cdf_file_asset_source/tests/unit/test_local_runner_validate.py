"""Unit tests for local_runner validation."""

from local_runner.validate import validate_default_config, validate_pipeline_configs


def test_validate_default_config_all_steps():
    out = validate_default_config()
    assert "valid" in out
    assert "results" in out
    assert len(out["results"]) == 3
    for item in out["results"]:
        assert "path" in item
        assert "default.config.yaml" in item["path"]


def test_validate_pipeline_configs_alias():
    out = validate_pipeline_configs()
    assert "valid" in out
    assert len(out["results"]) == 3


def test_validate_single_step():
    out = validate_default_config(["extract"])
    assert len(out["results"]) == 1
    assert out["results"][0]["step"] == "extract"
