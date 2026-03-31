"""Unit tests for extraction input hashing (incremental cohort gating)."""

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.extraction_input_hash import (
    apply_preprocessing,
    build_field_map_for_hash,
    extraction_inputs_hash,
    iter_wanted_fields,
    rules_fingerprint,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.config import (
    ExtractionRuleConfig,
    SourceFieldParameter,
    SourceViewConfig,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.PassthroughMethodParameter import (
    PassthroughMethodParameter,
)


def _view():
    return SourceViewConfig(
        view_external_id="V",
        view_space="vs",
        view_version="v1",
        entity_type="asset",
        resource_property="name",
        include_properties=["fallback"],
    )


def test_apply_preprocessing_trim_and_case():
    assert apply_preprocessing("  AbC  ", ["trim", "lowercase"]) == "abc"


def test_rules_fingerprint_stable_for_same_rules():
    r1 = ExtractionRuleConfig(
        rule_id="r1",
        parameters=PassthroughMethodParameter(),
        source_fields=[
            SourceFieldParameter(
                field_name="name", required=False, preprocessing=["trim"]
            )
        ],
    )
    r2 = ExtractionRuleConfig(
        rule_id="r1",
        parameters=PassthroughMethodParameter(),
        source_fields=[
            SourceFieldParameter(
                field_name="name", required=False, preprocessing=["trim"]
            )
        ],
    )
    assert rules_fingerprint([r1]) == rules_fingerprint([r2])


def test_rules_fingerprint_changes_when_rule_changes():
    r1 = ExtractionRuleConfig(
        rule_id="r1",
        parameters=PassthroughMethodParameter(),
        source_fields=[SourceFieldParameter(field_name="name", required=False)],
    )
    r2 = ExtractionRuleConfig(
        rule_id="r2",
        parameters=PassthroughMethodParameter(),
        source_fields=[SourceFieldParameter(field_name="name", required=False)],
    )
    assert rules_fingerprint([r1]) != rules_fingerprint([r2])


def test_extraction_inputs_hash_stable():
    fp = rules_fingerprint([])
    h1 = extraction_inputs_hash("scope_a", fp, {"a": "1", "b": "2"})
    h2 = extraction_inputs_hash("scope_a", fp, {"b": "2", "a": "1"})
    assert h1 == h2
    assert len(h1) == 64


def test_extraction_inputs_hash_differs_on_value():
    fp = rules_fingerprint([])
    h1 = extraction_inputs_hash("scope_a", fp, {"x": "a"})
    h2 = extraction_inputs_hash("scope_a", fp, {"x": "b"})
    assert h1 != h2


def test_iter_wanted_fields_include_properties_fallback():
    v = _view()
    fields = iter_wanted_fields([], v)
    assert ("fallback", False, []) in fields


def test_build_field_map_matches_preprocessing():
    v = _view()
    wanted = iter_wanted_fields(
        [
            ExtractionRuleConfig(
                rule_id="r1",
                parameters=PassthroughMethodParameter(),
                source_fields=[
                    SourceFieldParameter(
                        field_name="title",
                        required=False,
                        preprocessing=["trim", "uppercase"],
                    )
                ],
            )
        ],
        v,
    )
    m = build_field_map_for_hash({"title": "  hi  "}, wanted)
    assert m["title"] == "HI"

