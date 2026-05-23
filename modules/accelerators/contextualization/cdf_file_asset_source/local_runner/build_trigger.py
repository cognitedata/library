"""Sync workflow trigger ``input.configuration`` from ``default.config.yaml``."""

from __future__ import annotations

import copy
import re
import sys
from pathlib import Path
from typing import Any

import yaml

from local_runner.paths import get_module_root

TRIGGER_REL = "workflows/create_asset_hierarchy_from_files.WorkflowTrigger.yaml"
GENERATED_HEADER = (
    "# input.configuration is synced from default.config.yaml file_asset_source by: python module.py build\n"
)
_TEMPLATE_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def _escape_templates(text: str) -> tuple[str, dict[str, str]]:
    """Replace Toolkit ``{{ var }}`` placeholders so PyYAML can parse the file."""
    tokens: dict[str, str] = {}

    def _sub(m: re.Match[str]) -> str:
        key = f"__TK_TEMPLATE_{m.group(1)}__"
        tokens[key] = m.group(0)
        return key

    return _TEMPLATE_RE.sub(_sub, text), tokens


def _restore_templates(text: str, tokens: dict[str, str]) -> str:
    for key, original in tokens.items():
        text = text.replace(key, original)
    return text


def _load_trigger(path: Path) -> tuple[dict[str, Any], dict[str, str]]:
    raw = path.read_text(encoding="utf-8")
    if raw.startswith("#"):
        raw = "\n".join(raw.splitlines()[1:]) + ("\n" if raw.endswith("\n") else "")
    escaped, tokens = _escape_templates(raw)
    doc = yaml.safe_load(escaped) or {}
    if not isinstance(doc, dict):
        raise ValueError(f"{path}: trigger root must be a mapping")
    return doc, tokens


def _expected_configuration(root: Path) -> dict[str, Any]:
    from functions.shared.utils.module_config import file_asset_source_section, load_default_config

    return copy.deepcopy(file_asset_source_section(load_default_config(root)))


def sync_workflow_trigger(*, check_only: bool = False, force: bool = False) -> dict[str, Any]:
    """
    Write ``input.configuration`` on the workflow trigger from ``file_asset_source``.

    Returns a result dict with ``ok``, ``changed``, and optional ``drift`` message.
    """
    root = get_module_root()
    trigger_path = root / TRIGGER_REL
    if not trigger_path.is_file():
        raise FileNotFoundError(f"Missing workflow trigger: {trigger_path}")

    expected = _expected_configuration(root)
    current_doc, template_tokens = _load_trigger(trigger_path)
    current_cfg = (current_doc.get("input") or {}).get("configuration")

    if check_only:
        if current_cfg == expected:
            return {"ok": True, "changed": False, "message": "Trigger configuration matches default.config.yaml."}
        return {
            "ok": False,
            "changed": False,
            "message": "Trigger input.configuration differs from default.config.yaml file_asset_source. Run: python module.py build",
        }

    if current_cfg == expected and not force:
        return {"ok": True, "changed": False, "message": "Trigger already up to date."}

    new_doc = copy.deepcopy(current_doc)
    inp = new_doc.get("input")
    if not isinstance(inp, dict):
        inp = {}
        new_doc["input"] = inp
    inp["configuration"] = expected

    body = yaml.dump(
        new_doc,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=120,
    )
    body = _restore_templates(body, template_tokens)
    trigger_path.write_text(GENERATED_HEADER + body, encoding="utf-8")
    return {
        "ok": True,
        "changed": True,
        "message": f"Updated {TRIGGER_REL} input.configuration from default.config.yaml.",
        "path": TRIGGER_REL,
    }


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    check_only = "--check" in args
    force = "--force" in args
    try:
        out = sync_workflow_trigger(check_only=check_only, force=force)
        print(out.get("message", out))
        return 0 if out.get("ok") else 1
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
