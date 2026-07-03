"""Unit tests for `config.py`.

Focus is on H2 — the Optional fields (`debug_file`, `filter_property`,
`filter_values`) must default to None when omitted from the source config,
and pydantic v2 must accept that without complaint.

Run from the function directory:

    pytest -q test_config.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Same path-prepend pattern used by handler.py so flat imports work in-test.
sys.path.append(str(Path(__file__).parent))

from config import Config, Parameters, ViewPropertyConfig


def _minimal_config_dict() -> dict:
    return {
        "parameters": {
            "debug": False,
            "runAll": False,
            "cleanOldAnnotations": False,
            "rawDb": "raw-db",
            "rawTableState": "state",
            "rawTableDocTag": "doc_tag",
            "rawTableDocDoc": "doc_doc",
            "autoApprovalThreshold": 0.9,
            "autoSuggestThreshold": 0.5,
        },
        "data": {
            "annotationView": {
                "schemaSpace": "schema",
                "externalId": "AnnotationView",
                "version": "v1",
            },
            "annotationJob": {
                "fileView": {
                    "schemaSpace": "schema",
                    "instanceSpace": "instance",
                    "externalId": "FileView",
                    "version": "v1",
                    "searchProperty": "alias",
                    "type": "diagrams.FileLink",
                },
                "entityViews": [],
            },
        },
    }


class TestParametersOptionalFields:
    """H2: Parameters.debug_file is `str | None = None`."""

    def test_debug_file_omitted_defaults_to_none(self):
        cfg = Config.model_validate(_minimal_config_dict())
        assert cfg.parameters.debug_file is None

    def test_debug_file_explicit_none_accepted(self):
        d = _minimal_config_dict()
        d["parameters"]["debugFile"] = None
        cfg = Config.model_validate(d)
        assert cfg.parameters.debug_file is None

    def test_debug_file_explicit_string_accepted(self):
        d = _minimal_config_dict()
        d["parameters"]["debugFile"] = "P-001.pdf"
        cfg = Config.model_validate(d)
        assert cfg.parameters.debug_file == "P-001.pdf"


class TestViewPropertyConfigOptionalFields:
    """H2: ViewPropertyConfig.filter_property and filter_values are Optional."""

    def test_filter_fields_omitted_default_to_none(self):
        cfg = Config.model_validate(_minimal_config_dict())
        file_view = cfg.data.annotation_job.file_view
        assert file_view.filter_property is None
        assert file_view.filter_values is None

    def test_filter_fields_explicit_none_accepted(self):
        d = _minimal_config_dict()
        d["data"]["annotationJob"]["fileView"]["filterProperty"] = None
        d["data"]["annotationJob"]["fileView"]["filterValues"] = None
        cfg = Config.model_validate(d)
        file_view = cfg.data.annotation_job.file_view
        assert file_view.filter_property is None
        assert file_view.filter_values is None

    def test_filter_fields_explicit_values_accepted(self):
        d = _minimal_config_dict()
        d["data"]["annotationJob"]["fileView"]["filterProperty"] = "site"
        d["data"]["annotationJob"]["fileView"]["filterValues"] = ["a", "b"]
        cfg = Config.model_validate(d)
        file_view = cfg.data.annotation_job.file_view
        assert file_view.filter_property == "site"
        assert file_view.filter_values == ["a", "b"]


class TestRequiredFieldsStillRequired:
    """The H2 Optional fix must NOT have weakened the required fields."""

    @pytest.mark.parametrize(
        "missing_key",
        ["debug", "runAll", "cleanOldAnnotations", "rawDb", "rawTableState"],
    )
    def test_missing_required_parameter_field_raises(self, missing_key):
        d = _minimal_config_dict()
        del d["parameters"][missing_key]
        with pytest.raises(Exception):
            Config.model_validate(d)

    def test_view_property_config_requires_type_literal(self):
        with pytest.raises(Exception):
            ViewPropertyConfig.model_validate(
                {
                    "schemaSpace": "schema",
                    "instanceSpace": "instance",
                    "externalId": "FileView",
                    "version": "v1",
                    # type intentionally omitted
                }
            )

    def test_view_property_config_rejects_unknown_type_literal(self):
        with pytest.raises(Exception):
            ViewPropertyConfig.model_validate(
                {
                    "schemaSpace": "schema",
                    "instanceSpace": "instance",
                    "externalId": "FileView",
                    "version": "v1",
                    "type": "diagrams.NotARealType",
                }
            )


class TestThresholdValidation:
    """The pydantic Field constraints on the auto_*_threshold values must hold."""

    @pytest.mark.parametrize("bad_value", [0.0, -0.1, 1.1, 2.0])
    def test_approval_threshold_out_of_range_rejected(self, bad_value):
        d = _minimal_config_dict()
        d["parameters"]["autoApprovalThreshold"] = bad_value
        with pytest.raises(Exception):
            Config.model_validate(d)

    @pytest.mark.parametrize("good_value", [0.001, 0.5, 1.0])
    def test_approval_threshold_in_range_accepted(self, good_value):
        d = _minimal_config_dict()
        d["parameters"]["autoApprovalThreshold"] = good_value
        cfg = Config.model_validate(d)
        assert cfg.parameters.auto_approval_threshold == good_value


class TestParametersDirectInstantiation:
    """Smoke check that the model validates standalone, not just through Config."""

    def test_parameters_minimal_via_camel_aliases(self):
        params = Parameters.model_validate(
            {
                "debug": True,
                "runAll": False,
                "cleanOldAnnotations": True,
                "rawDb": "db",
                "rawTableState": "state",
                "rawTableDocTag": "doc_tag",
                "rawTableDocDoc": "doc_doc",
                "autoApprovalThreshold": 0.95,
                "autoSuggestThreshold": 0.6,
            }
        )
        assert params.debug is True
        assert params.debug_file is None
        assert params.auto_suggest_threshold == 0.6
