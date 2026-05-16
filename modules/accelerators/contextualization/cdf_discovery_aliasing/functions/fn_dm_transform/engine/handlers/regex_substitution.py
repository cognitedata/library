"""Handler: regex_substitution."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional

from .base import AbstractTransformHandler, TransformResult


class RegexSubstitutionHandler(AbstractTransformHandler):
    handler_id = 'regex_substitution'

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
        patterns = block.get('patterns')
        items: List[Dict[str, Any]] = []
        if isinstance(patterns, list):
            items = [p for p in patterns if isinstance(p, dict)]
        elif block.get('pattern'):
            items = [{'pattern': block.get('pattern'), 'replacement': block.get('replacement', '')}]
        result = working
        for item in items:
            pat = str(item.get('pattern') or '')
            if not pat:
                continue
            rep = str(item.get('replacement') or '')
            count = int(item.get('count') or 0)
            flags = 0
            flag_str = str(item.get('flags') or '').upper()
            if 'I' in flag_str:
                flags |= re.IGNORECASE
            if 'M' in flag_str:
                flags |= re.MULTILINE
            result = re.sub(pat, rep, result, count=count or 0, flags=flags)
        return result
