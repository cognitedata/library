#!/usr/bin/env python3
"""Export transform/build_index handler descriptions to UI catalog JSON and sync en.ts handlerDoc."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

MODULE_ROOT = Path(__file__).resolve().parents[1]
FUNCS = MODULE_ROOT / "functions"
if str(FUNCS) not in sys.path:
    sys.path.insert(0, str(FUNCS))

from cdf_fn_common.etl_build_index.registry import build_index_handler_catalog  # noqa: E402
from cdf_fn_common.etl_transform.registry import transform_handler_catalog  # noqa: E402


def _escape_ts_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _replace_en_handler_doc(content: str, key: str, description: str) -> str:
    """Replace ``"key": "..."`` (single- or next-line string) in en.ts."""
    escaped = _escape_ts_string(description)
    single_line = rf'("{re.escape(key)}":\s*)"[^"]*(?:\\.[^"]*)*"'
    updated, n = re.subn(single_line, rf'\1"{escaped}"', content, count=1)
    if n == 1:
        return updated
    multiline = rf'("{re.escape(key)}":\s*\n\s*)"[^"]*(?:\\.[^"]*)*"'
    updated, n = re.subn(multiline, rf'\1"{escaped}"', content, count=1)
    if n != 1:
        raise RuntimeError(f"Could not update en.ts key {key!r} (matches={n})")
    return updated


def main() -> None:
    transform = {e["handler_id"]: e["description"] for e in transform_handler_catalog()}
    build_index = {e["handler_id"]: e["description"] for e in build_index_handler_catalog()}
    for bucket in (transform, build_index):
        for handler_id, desc in bucket.items():
            if not str(desc).strip():
                raise RuntimeError(f"Empty description for handler {handler_id!r}")

    catalog = {"transform": transform, "build_index": build_index}
    out_dir = MODULE_ROOT / "ui" / "src" / "generated"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "transformHandlerCatalog.json"
    json_path.write_text(json.dumps(catalog, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    en_path = MODULE_ROOT / "ui" / "src" / "i18n" / "en.ts"
    content = en_path.read_text(encoding="utf-8")
    for handler_id, desc in transform.items():
        content = _replace_en_handler_doc(content, f"transforms.handlerDoc.{handler_id}", desc)
    for handler_id, desc in build_index.items():
        content = _replace_en_handler_doc(content, f"buildIndex.handlerDoc.{handler_id}", desc)
    en_path.write_text(content, encoding="utf-8")
    print(f"Wrote {json_path} and synced {len(transform) + len(build_index)} handlerDoc keys in en.ts")


if __name__ == "__main__":
    main()
