"""Handler: substitution_variants."""

from __future__ import annotations

import re
from typing import Any, List, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class SubstitutionVariantsHandler(AbstractTransformHandler):
    handler_id = 'substitution_variants'
    multi_value = True

    @classmethod
    def apply(
        cls,
        working: str,
        block: Mapping[str, Any],
        *,
        field_values: Optional[Mapping[str, str]] = None,
        props: Optional[Mapping[str, Any]] = None,
    ) -> TransformResult:
        del field_values, props
        variants = block.get('variants') or []
        normalized = [str(v) for v in variants if str(v).strip()]
        if len(normalized) != len(set(v.strip() for v in normalized)):
            raise ValueError('substitution_variants: variants[] must not contain duplicates')

        match_literal = cls.first_nonempty(block.get('match_literal'))
        match_regex = cls.first_nonempty(block.get('match_regex'))
        prefix = ''
        suffix = working

        if match_literal and working.startswith(match_literal):
            suffix = working[len(match_literal) :]
        elif match_regex:
            m = re.search(match_regex, working)
            if not m:
                return [working]
            prefix = working[: m.start()]
            suffix = working[m.end() :]
        elif normalized:
            m = re.search(r'^[A-Za-z]+', working)
            if m:
                suffix = working[m.end() :]
            else:
                return [working]
        else:
            return [working]

        return [prefix + variant + suffix for variant in normalized]
