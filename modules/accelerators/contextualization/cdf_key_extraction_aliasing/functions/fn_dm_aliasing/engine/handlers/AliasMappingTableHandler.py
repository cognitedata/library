"""Alias mapping table: add aliases from RAW-resolved rows (scoped + exact/glob/regex)."""

from __future__ import annotations

import fnmatch
from typing import Any, Dict, Optional, Set

from .AliasTransformerHandler import AliasTransformerHandler


def _scope_applies(row: Dict[str, Any], context: Optional[Dict[str, Any]]) -> bool:
    ctx = context or {}
    scope = row.get("scope") or "global"
    if scope == "global":
        return True
    val = row.get("scope_value")
    if val is None:
        return False
    if scope == "space":
        return ctx.get("instance_space") == val
    if scope == "view_external_id":
        return ctx.get("view_external_id") == val
    if scope == "instance":
        return val == ctx.get("entity_external_id") or val == ctx.get("entity_id")
    return False


def _candidate_matches_row(
    candidate: str,
    row: Dict[str, Any],
    trim: bool,
    case_insensitive: bool,
) -> bool:
    source = row.get("source") or ""
    mode = row.get("source_match") or "exact"
    c = candidate.strip() if trim else candidate
    p = source.strip() if trim else source

    if mode == "exact":
        if case_insensitive:
            return c.lower() == p.lower()
        return c == p
    if mode == "glob":
        if case_insensitive:
            return fnmatch.fnmatch(c.lower(), p.lower())
        return fnmatch.fnmatch(c, p)
    if mode == "regex":
        pat = row.get("regex_pattern")
        if pat is None:
            return False
        return pat.fullmatch(c) is not None
    return False


class AliasMappingTableHandler(AliasTransformerHandler):
    """Apply aliases from pre-resolved alias mapping rows (loaded from RAW at engine init)."""

    def transform(
        self,
        aliases: Set[str],
        config: Dict[str, Any],
        context: Dict[str, Any] = None,
    ) -> Set[str]:
        resolved_rows: Any = config.get("resolved_rows")
        if not resolved_rows:
            return set(aliases)

        trim = bool(config.get("trim", True))
        case_insensitive = bool(config.get("case_insensitive", False))

        out: Set[str] = set(aliases)
        for row in resolved_rows:
            if not _scope_applies(row, context):
                continue
            for cand in aliases:
                if _candidate_matches_row(cand, row, trim, case_insensitive):
                    for a in row.get("aliases") or []:
                        if a is not None and str(a).strip():
                            out.add(str(a).strip())
        return out
