"""
Final-markdown assembly for Operations Summarizer v2.

Python mirror of ``src/utils/summaryMarkdown.ts``.
"""

import re
from datetime import date


def _format_period(period_start: date, period_end: date) -> str:
    return f"{period_start.isoformat()} to {period_end.isoformat()}"


def _format_truncation_block(truncation_warnings: list[str] | None) -> str:
    if not truncation_warnings:
        return ""
    lines = "\n".join(f"> - {w}" for w in truncation_warnings)
    return f"> **Data completeness:** Some data types hit the instance cap and may be incomplete:\n{lines}\n\n"


_DATATYPE_HEADING_RE = re.compile(r"^## (\w.*?) \((\d+)\)$", re.MULTILINE)
_REMAINING_H2_RE = re.compile(r"^## ", re.MULTILINE)


def _demote_h2(summary: str) -> str:
    out = _DATATYPE_HEADING_RE.sub(r"### \1 (\2)", summary)
    return _REMAINING_H2_RE.sub("### ", out)


def compile_summary_markdown(
    summary_parts: list[str],
    period_start: date,
    period_end: date,
    truncation_warnings: list[str] | None = None,
    include_work_orders: bool = True,
) -> str:
    """Assemble the standard summary markdown."""
    period_str = _format_period(period_start, period_end)
    truncation_block = _format_truncation_block(truncation_warnings)

    work_order_summary = next(
        (p for p in summary_parts if p.startswith("## Work Orders") or p.startswith("## Work orders")),
        None,
    )
    new_instances_summaries = [
        p for p in summary_parts if not p.startswith("## Work Orders") and not p.startswith("## Work orders")
    ]

    work_orders_section = ""
    if include_work_orders and work_order_summary:
        work_orders_section = work_order_summary.replace("## Work Orders", "## Work orders", 1)

    new_instances_section = ""
    if new_instances_summaries:
        demoted = [_demote_h2(part) for part in new_instances_summaries]
        new_instances_section = "## New instances\n\n" + "\n\n".join(demoted)

    sections = []
    if work_orders_section:
        sections.append(work_orders_section)
    if new_instances_section:
        sections.append(new_instances_section)

    body = "\n\n".join(sections)
    return f"# Operation Summary\nSummary period: {period_str}\n\n{truncation_block}{body}\n"
