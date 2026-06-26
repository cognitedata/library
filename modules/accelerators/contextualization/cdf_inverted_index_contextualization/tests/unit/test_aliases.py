"""Unit tests for alias self-reference guards."""

from inverted_index.aliases import (
    is_alias_term,
    is_self_reference_hit,
    normalized_instance_aliases,
    read_instance_aliases,
)


def test_read_instance_aliases_from_properties() -> None:
    instance = {"properties": {"aliases": ["P-101A", "P-102B"]}}
    assert read_instance_aliases(instance) == ["P-101A", "P-102B"]


def test_normalized_instance_aliases_case_insensitive() -> None:
    instance = {"properties": {"aliases": ["P-101A", "p101a"]}}
    assert normalized_instance_aliases(instance) == {"p101a"}


def test_is_alias_term() -> None:
    instance = {"properties": {"aliases": ["P-101A"]}}
    assert is_alias_term("P-101A", instance)
    assert is_alias_term("p-101a", instance)
    assert not is_alias_term("P-102B", instance)


def test_is_self_reference_hit() -> None:
    hit = {
        "reference_external_id": "ASSET_1",
        "reference_space": "cdf_cdm",
        "normalized_term": "p101a",
    }
    assert is_self_reference_hit(hit, "ASSET_1", "cdf_cdm")
    assert not is_self_reference_hit(hit, "ASSET_2", "cdf_cdm")
