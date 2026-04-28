"""Unit tests for extraction input hashing (incremental cohort gating)."""

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.extraction_input_hash import (
    apply_preprocessing,
    build_field_map_for_hash,
    extraction_inputs_hash,
    iter_wanted_fields,
    resolve_key_discovery_hash_field_paths,
    rules_fingerprint,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.config import (
    EntityType,
    ExtractionRuleConfig,
    FieldExtractionSpec,
    SourceViewConfig,
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
        handler="regex_handler",
        parameters=None,
        fields=[
            FieldExtractionSpec(
                field_name="name", required=False, preprocessing=["trim"]
            )
        ],
    )
    r2 = ExtractionRuleConfig(
        rule_id="r1",
        handler="regex_handler",
        parameters=None,
        fields=[
            FieldExtractionSpec(
                field_name="name", required=False, preprocessing=["trim"]
            )
        ],
    )
    assert rules_fingerprint([r1]) == rules_fingerprint([r2])


def test_rules_fingerprint_changes_when_rule_changes():
    r1 = ExtractionRuleConfig(
        rule_id="r1",
        handler="regex_handler",
        parameters=None,
        fields=[FieldExtractionSpec(field_name="name", required=False)],
    )
    r2 = ExtractionRuleConfig(
        rule_id="r2",
        handler="regex_handler",
        parameters=None,
        fields=[FieldExtractionSpec(field_name="name", required=False)],
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


def test_extraction_inputs_hash_v2_with_workflow_scope():
    fp = rules_fingerprint([])
    h = extraction_inputs_hash(
        "scope_a",
        fp,
        {"x": "a"},
        workflow_scope="site__unit",
        source_view_fingerprint="fp1",
    )
    assert len(h) == 64
    h2 = extraction_inputs_hash(
        "scope_a",
        fp,
        {"x": "a"},
        workflow_scope="site__unit",
        source_view_fingerprint="fp1",
    )
    assert h == h2
    h3 = extraction_inputs_hash(
        "scope_a",
        fp,
        {"x": "a"},
        workflow_scope="other",
        source_view_fingerprint="fp1",
    )
    assert h3 != h


def test_resolve_key_discovery_hash_uses_explicit_paths():
    v = SourceViewConfig(
        view_external_id="v",
        view_space="s",
        view_version="1",
        entity_type=EntityType.ASSET,
        resource_property="externalId",
        include_properties=["fallback"],
        key_discovery_hash_property_paths=["title", "metadata.code"],
    )
    rules = [
        ExtractionRuleConfig(
            rule_id="r1",
            handler="regex_handler",
            parameters=None,
            fields=[
                FieldExtractionSpec(
                    field_name="title",
                    required=False,
                    preprocessing=["trim"],
                )
            ],
        )
    ]
    wanted = resolve_key_discovery_hash_field_paths(rules, v)
    names = [w[0] for w in wanted]
    assert "title" in names
    assert "metadata.code" in names
    assert "fallback" not in names


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
                handler="regex_handler",
                parameters=None,
                fields=[
                    FieldExtractionSpec(
                        field_name="title",
                        required=False,
                        preprocessing=["trim", "uppercase"],
                    )
                ],
            )
        ],
        v,
        association_pairs={(0, "r1")},
        source_view_index=0,
    )
    m = build_field_map_for_hash({"title": "  hi  "}, wanted)
    assert m["title"] == "HI"


def test_iter_wanted_fields_filters_by_associations():
    """Non-empty associations: only rules wired to the current source_view_index contribute fields."""
    rules = [
        {
            "name": "asset_only",
            "fields": [
                {
                    "field_name": "name",
                    "required": True,
                    "preprocessing": ["trim"],
                }
            ],
            "scope_filters": {},
        },
        {
            "name": "file_only",
            "fields": [
                {
                    "field_name": "name",
                    "required": True,
                    "preprocessing": ["trim"],
                }
            ],
            "scope_filters": {},
        },
    ]
    asset_view = {
        "view_external_id": "CogniteAsset",
        "entity_type": "asset",
        "include_properties": [],
    }
    file_view = {
        "view_external_id": "CogniteFile",
        "entity_type": "file",
        "include_properties": [],
    }
    assoc = {(0, "asset_only"), (1, "file_only")}
    a = iter_wanted_fields(rules, asset_view, association_pairs=assoc, source_view_index=0)
    f = iter_wanted_fields(rules, file_view, association_pairs=assoc, source_view_index=1)
    assert [x[0] for x in a] == ["name"]
    assert [x[0] for x in f] == ["name"]
    assert a[0][1] is True and f[0][1] is True


def test_iter_wanted_fields_without_associations_yields_no_rule_fields():
    rules = [
        {
            "name": "r_open",
            "fields": [{"field_name": "description", "required": False}],
            "scope_filters": {},
        },
        {
            "name": "r_assetish",
            "fields": [{"field_name": "name", "required": True}],
            "scope_filters": {"entity_type": ["asset"]},
        },
    ]
    ts_view = {"entity_type": "timeseries", "include_properties": []}
    wanted = iter_wanted_fields(rules, ts_view)
    names = [w[0] for w in wanted]
    assert names == []


def test_resolve_key_discovery_hash_respects_associations_for_rule_preprocessing():
    rules = [
        {
            "name": "r_asset",
            "fields": [
                {"field_name": "title", "required": False, "preprocessing": ["trim"]}
            ],
            "scope_filters": {},
        },
        {
            "name": "r_file",
            "fields": [
                {"field_name": "title", "required": False, "preprocessing": ["uppercase"]}
            ],
            "scope_filters": {},
        },
    ]
    asset_view = {
        "entity_type": "asset",
        "key_discovery_hash_property_paths": ["title"],
        "include_properties": [],
    }
    wanted = resolve_key_discovery_hash_field_paths(
        rules,
        asset_view,
        association_pairs={(0, "r_asset")},
        source_view_index=0,
    )
    assert len(wanted) == 1
    assert wanted[0][0] == "title"
    assert wanted[0][2] == ["trim"]

