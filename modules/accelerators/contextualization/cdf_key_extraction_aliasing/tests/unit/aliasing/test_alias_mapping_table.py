"""Unit tests for alias_mapping_table (RAW loader + handler + engine hydration)."""

import re
import sys
import unittest
from pathlib import Path

import pandas as pd

project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.handlers.AliasMappingTableHandler import (  # noqa: E402
    AliasMappingTableHandler,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (  # noqa: E402
    AliasingEngine,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.alias_mapping_table_raw_loader import (  # noqa: E402
    parse_dataframe_to_rows,
)


class TestAliasMappingTableHandler(unittest.TestCase):
    def setUp(self):
        self.h = AliasMappingTableHandler()

    def test_exact_global_adds_aliases(self):
        cfg = {
            "resolved_rows": [
                {
                    "source": "PI:1",
                    "aliases": ["A", "B"],
                    "scope": "global",
                    "scope_value": None,
                    "source_match": "exact",
                    "regex_pattern": None,
                }
            ],
            "trim": True,
            "case_insensitive": False,
        }
        out = self.h.transform({"PI:1"}, cfg, {})
        self.assertIn("PI:1", out)
        self.assertIn("A", out)
        self.assertIn("B", out)

    def test_space_scope(self):
        cfg = {
            "resolved_rows": [
                {
                    "source": "X",
                    "aliases": ["SPC"],
                    "scope": "space",
                    "scope_value": "sp1",
                    "source_match": "exact",
                    "regex_pattern": None,
                }
            ],
        }
        self.assertNotIn("SPC", self.h.transform({"X"}, cfg, {"instance_space": "other"}))
        self.assertIn("SPC", self.h.transform({"X"}, cfg, {"instance_space": "sp1"}))

    def test_view_external_id_scope(self):
        cfg = {
            "resolved_rows": [
                {
                    "source": "Y",
                    "aliases": ["VW"],
                    "scope": "view_external_id",
                    "scope_value": "CogniteTimeSeries",
                    "source_match": "exact",
                    "regex_pattern": None,
                }
            ],
        }
        ctx = {"view_external_id": "CogniteAsset"}
        self.assertNotIn("VW", self.h.transform({"Y"}, cfg, ctx))
        ctx2 = {"view_external_id": "CogniteTimeSeries"}
        self.assertIn("VW", self.h.transform({"Y"}, cfg, ctx2))

    def test_instance_scope(self):
        cfg = {
            "resolved_rows": [
                {
                    "source": "Z",
                    "aliases": ["INST"],
                    "scope": "instance",
                    "scope_value": "node-a",
                    "source_match": "exact",
                    "regex_pattern": None,
                }
            ],
        }
        self.assertIn(
            "INST",
            self.h.transform(
                {"Z"}, cfg, {"entity_id": "node-a", "entity_external_id": "x"}
            ),
        )
        self.assertIn(
            "INST",
            self.h.transform(
                {"Z"}, cfg, {"entity_id": "other", "entity_external_id": "node-a"}
            ),
        )

    def test_glob_match(self):
        cfg = {
            "resolved_rows": [
                {
                    "source": "PI:*",
                    "aliases": ["GLOBHIT"],
                    "scope": "global",
                    "scope_value": None,
                    "source_match": "glob",
                    "regex_pattern": None,
                }
            ],
        }
        self.assertIn("GLOBHIT", self.h.transform({"PI:123"}, cfg, {}))

    def test_regex_fullmatch(self):
        cfg = {
            "resolved_rows": [
                {
                    "source": r"PI:\d+",
                    "aliases": ["REHIT"],
                    "scope": "global",
                    "scope_value": None,
                    "source_match": "regex",
                    "regex_pattern": re.compile(r"PI:\d+"),
                }
            ],
        }
        self.assertIn("REHIT", self.h.transform({"PI:99"}, cfg, {}))
        self.assertNotIn("REHIT", self.h.transform({"PI:xx"}, cfg, {}))


class TestParseDataframeToRows(unittest.TestCase):
    def test_basic_columns(self):
        df = pd.DataFrame(
            [
                {
                    "source_tag": "A1",
                    "alias_primary": "B1",
                    "alias_secondary": "",
                    "scope": "global",
                    "scope_value": "",
                }
            ]
        )
        raw_table = {
            "key_column": "source_tag",
            "alias_columns": ["alias_primary", "alias_secondary"],
            "scope_column": "scope",
            "scope_value_column": "scope_value",
        }
        rows, errs = parse_dataframe_to_rows(df, raw_table, "exact")
        self.assertEqual(errs, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "A1")
        self.assertEqual(rows[0]["aliases"], ["B1"])
        self.assertEqual(rows[0]["scope"], "global")

    def test_invalid_regex_row_skipped(self):
        df = pd.DataFrame(
            [
                {
                    "source_tag": "[broken",
                    "alias_primary": "X",
                    "scope": "global",
                    "scope_value": "",
                    "source_match": "regex",
                }
            ]
        )
        raw_table = {
            "key_column": "source_tag",
            "alias_columns": ["alias_primary"],
            "scope_column": "scope",
            "scope_value_column": "scope_value",
            "source_match_column": "source_match",
        }
        rows, errs = parse_dataframe_to_rows(df, raw_table, "exact")
        self.assertEqual(rows, [])
        self.assertTrue(any("Invalid regex" in e for e in errs))

    def test_single_alias_column_comma_delimited(self):
        df = pd.DataFrame(
            [
                {
                    "source_tag": "P-101",
                    "aliases": 'P101, P_101, "P 101"',
                    "scope": "global",
                    "scope_value": "",
                }
            ]
        )
        raw_table = {
            "key_column": "source_tag",
            "alias_columns": ["aliases"],
            "alias_delimiter": ",",
            "scope_column": "scope",
            "scope_value_column": "scope_value",
        }
        rows, errs = parse_dataframe_to_rows(df, raw_table, "exact")
        self.assertEqual(errs, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["aliases"], ["P101", "P_101", "P 101"])

    def test_single_alias_column_without_quote_stripping(self):
        df = pd.DataFrame(
            [
                {
                    "source_tag": "P-101",
                    "aliases": '"P101","P_101"',
                    "scope": "global",
                    "scope_value": "",
                }
            ]
        )
        raw_table = {
            "key_column": "source_tag",
            "alias_columns": ["aliases"],
            "alias_delimiter": ",",
            "alias_strip_quotes": False,
            "scope_column": "scope",
            "scope_value_column": "scope_value",
        }
        rows, errs = parse_dataframe_to_rows(df, raw_table, "exact")
        self.assertEqual(errs, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["aliases"], ['"P101"', '"P_101"'])


class TestAliasingEngineAliasMappingTable(unittest.TestCase):
    def test_resolved_rows_without_client(self):
        config = {
            "rules": [
                {
                    "name": "map",
                    "handler": "alias_mapping_table",
                    "enabled": True,
                    "priority": 5,
                    "preserve_original": True,
                    "config": {
                        "resolved_rows": [
                            {
                                "source": "T1",
                                "aliases": ["M1"],
                                "scope": "global",
                                "scope_value": None,
                                "source_match": "exact",
                                "regex_pattern": None,
                            }
                        ],
                    },
                }
            ],
            "validation": {
                "max_aliases_per_tag": 50,
                "min_confidence": 0.01,
                "confidence_match_rules": [
                    {
                        "name": "alias_shape_invalid",
                        "priority": 0,
                        "expression_match": "fullmatch",
                        "match": {
                            "expressions": [
                                {"pattern": r"^$", "description": "empty alias"},
                                {
                                    "pattern": r"^.{101,}$",
                                    "description": "exceeds max length 100",
                                },
                            ],
                        },
                        "confidence_modifier": {"mode": "explicit", "value": 0.0},
                    },
                ],
            },
        }
        engine = AliasingEngine(config, client=None)
        r = engine.generate_aliases("T1", "asset", {})
        self.assertIn("M1", r.aliases)

    def test_raw_table_without_client_disables_rule(self):
        config = {
            "rules": [
                {
                    "name": "map",
                    "handler": "alias_mapping_table",
                    "enabled": True,
                    "priority": 5,
                    "preserve_original": True,
                    "config": {
                        "raw_table": {
                            "database_name": "db",
                            "table_name": "tbl",
                            "key_column": "k",
                            "alias_columns": ["a"],
                        },
                    },
                }
            ],
            "validation": {},
        }
        engine = AliasingEngine(config, client=None)
        rule = engine.rules[0]
        self.assertFalse(rule.enabled)


if __name__ == "__main__":
    unittest.main()
