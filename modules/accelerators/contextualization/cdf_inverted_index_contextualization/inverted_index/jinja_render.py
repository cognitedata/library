"""Minimal Jinja2 rendering for annotation identity templates."""

from __future__ import annotations

import json
from typing import Any, Mapping

from jinja2 import Environment, StrictUndefined


def _env() -> Environment:
    env = Environment(undefined=StrictUndefined)

    def _tojson(value: Any) -> str:
        return json.dumps(value, sort_keys=True)

    env.filters["tojson"] = _tojson
    return env


def render_template_string(template: str, context: Mapping[str, Any]) -> str:
    return _env().from_string(template).render(**context)
