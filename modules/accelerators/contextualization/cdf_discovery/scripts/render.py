"""Jinja2 rendering for Space and Group templates."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from jinja2 import Environment, FileSystemLoader, StrictUndefined


def _env() -> Environment:
    env = Environment(undefined=StrictUndefined)

    def _tojson(value: Any) -> str:
        return json.dumps(value, sort_keys=True)

    env.filters["tojson"] = _tojson
    return env


def render_template_string(template: str, context: Mapping[str, Any]) -> str:
    return _env().from_string(template).render(**context)


def render_template_file(template_path: Path, context: Mapping[str, Any]) -> str:
    env = _env()
    env.loader = FileSystemLoader(str(template_path.parent))
    name = template_path.name
    return env.get_template(name).render(**context)
