"""Pipeline entity payload: dotted source field names vs nested view properties."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

from cognite.client.data_classes.data_modeling.ids import ViewId

project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.config import (  # noqa: E402
    EntityType,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.pipeline import (  # noqa: E402
    _build_entity_payload_with_rules,
)


class TestBuildEntityPayloadPropertyPath(unittest.TestCase):
    def test_dotted_field_reads_nested_view_property(self):
        class Inst:
            external_id = "e1"
            space = "isp"

            def dump(self):
                return {
                    "properties": {
                        "sp": {
                            "V/v1": {
                                "meta": {"code": "Z-1"},
                            }
                        }
                    }
                }

        vid = ViewId(space="sp", external_id="V", version="v1")
        evc = SimpleNamespace(entity_type=EntityType.ASSET)
        log = SimpleNamespace(verbose=lambda *a, **k: None)
        out = _build_entity_payload_with_rules(
            instance=Inst(),
            merged_columns=None,
            extraction_rules=[
                {"name": "rule1", "source_fields": [{"field_name": "meta.code"}]}
            ],
            entity_view_id=vid,
            entity_view_config=evc,
            logger=log,
        )
        self.assertEqual(out.get("rule1_meta.code"), "Z-1")

    def test_json_string_property_traversal(self):
        class Inst:
            external_id = "e2"
            space = "isp"

            def dump(self):
                return {
                    "properties": {
                        "sp": {
                            "V/v1": {
                                "payload": '{"tag":"P-9"}',
                            }
                        }
                    }
                }

        vid = ViewId(space="sp", external_id="V", version="v1")
        evc = SimpleNamespace(entity_type=EntityType.ASSET)
        log = SimpleNamespace(verbose=lambda *a, **k: None)
        out = _build_entity_payload_with_rules(
            instance=Inst(),
            merged_columns=None,
            extraction_rules=[
                {"name": "r2", "source_fields": [{"field_name": "payload.tag"}]}
            ],
            entity_view_id=vid,
            entity_view_config=evc,
            logger=log,
        )
        self.assertEqual(out.get("r2_payload.tag"), "P-9")


if __name__ == "__main__":
    unittest.main()
