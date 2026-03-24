"""
Unit tests for configurable alias write-back property on CogniteDescribable.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Repo root (library/) so `modules.*` imports resolve
_LIB_ROOT = Path(__file__).resolve().parents[7]
sys.path.insert(0, str(_LIB_ROOT))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_alias_persistence.pipeline import (  # noqa: E402
    _resolve_alias_writeback_property,
    persist_aliases_to_entities,
)


class TestResolveAliasWritebackProperty(unittest.TestCase):
    def test_defaults_to_aliases(self):
        self.assertEqual(_resolve_alias_writeback_property({}), "aliases")

    def test_camel_case(self):
        self.assertEqual(
            _resolve_alias_writeback_property({"aliasWritebackProperty": "tagAliases"}),
            "tagAliases",
        )

    def test_snake_case(self):
        self.assertEqual(
            _resolve_alias_writeback_property({"alias_writeback_property": "myAliases"}),
            "myAliases",
        )

    def test_camel_over_snake_when_both_set(self):
        self.assertEqual(
            _resolve_alias_writeback_property(
                {
                    "aliasWritebackProperty": "first",
                    "alias_writeback_property": "second",
                }
            ),
            "first",
        )

    def test_strips_whitespace(self):
        self.assertEqual(
            _resolve_alias_writeback_property({"alias_writeback_property": "  x  "}),
            "x",
        )

    def test_empty_string_falls_back(self):
        self.assertEqual(
            _resolve_alias_writeback_property({"alias_writeback_property": "   "}),
            "aliases",
        )


class TestPersistAliasesWritebackProperty(unittest.TestCase):
    def _minimal_aliasing_results(self):
        return [
            {
                "original_tag": "T1",
                "aliases": ["a", "b"],
                "entities": [
                    {
                        "entity_id": "E1",
                        "view_space": "cdf_cdm",
                        "view_external_id": "CogniteDescribable",
                        "view_version": "v1",
                        "instance_space": "cdf_cdm",
                        "field_name": "name",
                    }
                ],
            }
        ]

    def test_apply_uses_configured_property_name(self):
        logger = MagicMock()
        client = MagicMock()
        client.data_modeling.instances.retrieve.return_value = [MagicMock()]

        data = {
            "alias_writeback_property": "customAliases",
            "aliasing_results": self._minimal_aliasing_results(),
        }
        persist_aliases_to_entities(client, logger, data)

        client.data_modeling.instances.apply.assert_called_once()
        apply_call = client.data_modeling.instances.apply.call_args
        nodes = apply_call.kwargs.get("nodes") or apply_call[1]["nodes"]
        node_apply = nodes[0]
        sources = node_apply.sources
        props = sources[0].properties
        self.assertIn("customAliases", props)
        self.assertEqual(set(props["customAliases"]), {"a", "b"})
        self.assertEqual(data["alias_writeback_property"], "customAliases")

    def test_apply_defaults_to_aliases_property(self):
        logger = MagicMock()
        client = MagicMock()
        client.data_modeling.instances.retrieve.return_value = [MagicMock()]

        data = {"aliasing_results": self._minimal_aliasing_results()}
        persist_aliases_to_entities(client, logger, data)

        client.data_modeling.instances.apply.assert_called_once()
        apply_call = client.data_modeling.instances.apply.call_args
        nodes = apply_call.kwargs.get("nodes") or apply_call[1]["nodes"]
        props = nodes[0].sources[0].properties
        self.assertIn("aliases", props)
        self.assertEqual(data["alias_writeback_property"], "aliases")


if __name__ == "__main__":
    unittest.main()
