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
    _resolve_persistence_apply_batch_size,
    persist_aliases_to_entities,
)


class TestResolvePersistenceApplyBatchSize(unittest.TestCase):
    def test_defaults_to_1000(self):
        self.assertEqual(_resolve_persistence_apply_batch_size({}), 1000)

    def test_snake_case(self):
        self.assertEqual(
            _resolve_persistence_apply_batch_size({"persistence_apply_batch_size": 50}),
            50,
        )

    def test_camel_case(self):
        self.assertEqual(
            _resolve_persistence_apply_batch_size({"persistenceApplyBatchSize": 3}),
            3,
        )

    def test_minimum_one(self):
        self.assertEqual(
            _resolve_persistence_apply_batch_size({"persistence_apply_batch_size": 0}),
            1,
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

    def test_two_entities_single_apply(self):
        logger = MagicMock()
        client = MagicMock()
        data = {
            "aliasing_results": [
                {
                    "original_tag": "T1",
                    "aliases": ["a"],
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
                },
                {
                    "original_tag": "T2",
                    "aliases": ["b"],
                    "entities": [
                        {
                            "entity_id": "E2",
                            "view_space": "cdf_cdm",
                            "view_external_id": "CogniteDescribable",
                            "view_version": "v1",
                            "instance_space": "cdf_cdm",
                            "field_name": "name",
                        }
                    ],
                },
            ],
        }
        persist_aliases_to_entities(client, logger, data)

        self.assertEqual(client.data_modeling.instances.apply.call_count, 1)
        apply_call = client.data_modeling.instances.apply.call_args
        nodes = apply_call.kwargs.get("nodes") or apply_call[1]["nodes"]
        self.assertEqual(len(nodes), 2)
        ext_ids = {n.external_id for n in nodes}
        self.assertEqual(ext_ids, {"E1", "E2"})

    def test_batch_size_one_two_applies(self):
        logger = MagicMock()
        client = MagicMock()
        data = {
            "persistence_apply_batch_size": 1,
            "aliasing_results": [
                {
                    "original_tag": "T1",
                    "aliases": ["a"],
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
                },
                {
                    "original_tag": "T2",
                    "aliases": ["b"],
                    "entities": [
                        {
                            "entity_id": "E2",
                            "view_space": "cdf_cdm",
                            "view_external_id": "CogniteDescribable",
                            "view_version": "v1",
                            "instance_space": "cdf_cdm",
                            "field_name": "name",
                        }
                    ],
                },
            ],
        }
        persist_aliases_to_entities(client, logger, data)

        self.assertEqual(client.data_modeling.instances.apply.call_count, 2)
        self.assertEqual(data.get("persistence_apply_batch_size"), 1)


class TestForeignKeyWriteback(unittest.TestCase):
    def test_raises_when_write_fk_enabled_without_property(self):
        logger = MagicMock()
        client = MagicMock()
        data = {
            "write_foreign_key_references": True,
            "aliasing_results": [],
            "entities_keys_extracted": {
                "E1": {
                    "foreign_key_references": [{"value": "T-1", "confidence": 0.9}],
                    "instance_space": "sp",
                    "view_space": "cdf_cdm",
                    "view_external_id": "CogniteFile",
                    "view_version": "v1",
                }
            },
        }
        with self.assertRaises(ValueError) as ctx:
            persist_aliases_to_entities(client, logger, data)
        self.assertIn("foreignKeyWritebackProperty", str(ctx.exception))

    def test_apply_merges_aliases_and_fk_same_view(self):
        logger = MagicMock()
        client = MagicMock()
        data = {
            "alias_writeback_property": "customAliases",
            "write_foreign_key_references": True,
            "foreign_key_writeback_property": "references_found",
            "aliasing_results": [
                {
                    "original_tag": "T1",
                    "aliases": ["a", "b"],
                    "entities": [
                        {
                            "entity_id": "E1",
                            "view_space": "cdf_cdm",
                            "view_external_id": "CogniteFile",
                            "view_version": "v1",
                            "instance_space": "sp_x",
                            "field_name": "name",
                        }
                    ],
                }
            ],
            "entities_keys_extracted": {
                "E1": {
                    "foreign_key_references": [
                        {"value": "T-301", "confidence": 0.95},
                        {"value": "T-302", "confidence": 0.5},
                    ],
                }
            },
        }
        persist_aliases_to_entities(client, logger, data)
        client.data_modeling.instances.apply.assert_called_once()
        apply_call = client.data_modeling.instances.apply.call_args
        nodes = apply_call.kwargs.get("nodes") or apply_call[1]["nodes"]
        props = nodes[0].sources[0].properties
        self.assertEqual(set(props["customAliases"]), {"a", "b"})
        self.assertEqual(props["references_found"], ["T-301", "T-302"])
        self.assertEqual(data.get("foreign_keys_persisted"), 2)

    def test_fk_only_from_entities_keys_extracted(self):
        logger = MagicMock()
        client = MagicMock()
        data = {
            "aliasing_results": [],
            "write_foreign_key_references": True,
            "foreign_key_writeback_property": "references_found",
            "entities_keys_extracted": {
                "E1": {
                    "foreign_key_references": [{"value": "FK1", "confidence": 1.0}],
                    "instance_space": "sp_x",
                    "view_space": "cdf_cdm",
                    "view_external_id": "CogniteFile",
                    "view_version": "v1",
                }
            },
        }
        persist_aliases_to_entities(client, logger, data)
        client.data_modeling.instances.apply.assert_called_once()
        apply_call = client.data_modeling.instances.apply.call_args
        nodes = apply_call.kwargs.get("nodes") or apply_call[1]["nodes"]
        props = nodes[0].sources[0].properties
        self.assertEqual(props["references_found"], ["FK1"])
        self.assertEqual(data.get("aliases_persisted"), 0)
        self.assertEqual(data.get("foreign_keys_persisted"), 1)


if __name__ == "__main__":
    unittest.main()
