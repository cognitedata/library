"""
Operations Summarizer v2 — Cognite Functions entry point.

Configuration is resolved from the config node, then ``data`` overrides.
Prompts and defaults come from the config node. Mirrors the app's 5-type summary generation model.

Configuration via ``data`` parameter (all fields optional):
    {
        "selected_agent":              str   - agent external_id (created automatically if omitted)
        "agent_instructions":          str   - custom agent system instructions
        "summary_period_start":        str   - "YYYY-MM-DD", "auto", or a preset:
                                               "last 24 hours" | "last 72 hours" | "last week"
                                               "last month" | "last quarter"  (default: last week)
        "summary_period_end":          str   - "YYYY-MM-DD" or "auto"   (ignored when using a preset)
        "summary_contents":            dict  - per-type bool flags (assets, files, work_orders, ...)
        "prompt_templates":            dict  - per-type prompt templates (overrides defaults)
        "summary_space":               str   - space to save results in (default: config node)
        "chunk_size":                  int   - instances per chunk      (default: config node)
        "holistic_summary_template":   str   - template for holistic roll-up
        "save_to_cdf":                 bool  - save result to CDF       (default: True)
        "generate_pdf":                bool  - produce PDF               (default: True)
        "return_pdf_bytes":            bool  - include PDF bytes in response (default: False)
        "fusion_domain":               str   - for Fusion file URLs
        "task":                        str   - "summary" (default) or "podcast"
        "persona_key":                 str   - optional preset that limits data types
                                               ("engineer" summarizes work_orders only)
    }

Podcast calls (``task`` = ``"podcast"``) also accept:
    {
        "summary_markdown":      str
        "summary_external_id":   str
        "summary_space":         str
        "podcast_agent_id":      str
        "podcast_instructions":  str
        "voice":                 str   - TTS voice (default: "en-US-AndrewMultilingualNeural")
        "speed":                 float
        "generate_audio":        bool
        "source_summary_id":     str
        "save_to_cdf":           bool
        "fusion_domain":         str
        "start_date":            str
        "end_date":              str
    }
"""

from datetime import datetime, timedelta, date, timezone
from typing import List, Optional, Tuple
from cognite.client import CogniteClient

from config import (
    DATA_TYPE_CONFIGS,
    DATA_TYPE_ORDER,
    get_config,
    runtime,
)
from cdf_helpers import CDFHelper
from agent_utils import ensure_agent_exists
from data_retrieval import fetch_instances_for_data_type
from summary_generation import process_dataset, process_work_orders
from markdown_assembly import compile_summary_markdown
from pdf_generation import save_summary_to_cdf, generate_pdf_from_markdown_in_memory
from podcast_handler import handle_podcast
from utils import strip_html_tags


# =============================================================================
# MAIN HANDLER
# =============================================================================


def handle(
    data: dict = None,
    client: CogniteClient = None,
    secrets: dict = None,
    function_call_info: dict = None,
) -> dict:
    """Cognite Functions entry-point. ``task`` ``podcast`` runs podcast generation."""
    data = data or {}
    if data.get("task") == "podcast":
        return handle_podcast(data, client, secrets, function_call_info)
    return _handle_summary(data, client, secrets, function_call_info)


def _handle_summary(
    data: dict = None,
    client: CogniteClient = None,
    secrets: dict = None,
    function_call_info: dict = None,
) -> dict:
    """Generate an operations summary from the config node."""
    try:
        data = data or {}

        print("=" * 80)
        print("OPERATIONS SUMMARIZER v2 STARTED")
        print("=" * 80)

        cfg = get_config(data, client)
        summary_contents = cfg["summary_contents"]
        print(f"Summary contents: {summary_contents}")

        start_date, end_date, period_error = _resolve_period(data)
        if period_error:
            return {"markdown": "", "success": False, "error": period_error}
        print(f"Summary period: {start_date} to {end_date}")

        tz = timezone(timedelta(hours=runtime.timezone_offset_hours))
        start_dt = datetime.combine(start_date, datetime.min.time(), tzinfo=tz)
        end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time(), tzinfo=tz)

        summary_prompt = cfg["prompt_templates"]

        print("\nEnsuring agent exists...")
        selected_agent = ensure_agent_exists(
            client,
            agent_id=data.get("selected_agent"),
            instructions=cfg["agent_instructions"],
        )
        print(f"Using agent: {selected_agent}")

        cdf_helper = CDFHelper(client)

        per_type_sections: List[str] = []
        truncation_warnings: List[str] = []
        instance_counts: dict = {}
        data_type_truncation: dict = {}

        for dt in DATA_TYPE_ORDER:
            if not summary_contents.get(dt, False):
                continue
            config = DATA_TYPE_CONFIGS.get(dt)
            if config is None:
                continue

            print(f"\nProcessing {dt}...")
            try:
                rows, truncated, had_more = fetch_instances_for_data_type(
                    client, dt, config, start_dt, end_dt,
                )
                print(f"  -> {len(rows)} {dt}")
                instance_counts[dt] = len(rows)
                data_type_truncation[dt] = {
                    "limit": runtime.max_instances_per_datatype,
                    "had_more_in_cdf": bool(truncated and had_more),
                }

                if truncated and had_more:
                    truncation_warnings.append(
                        f"{dt}: included up to {runtime.max_instances_per_datatype:,} instances; "
                        "additional data exists in CDF."
                    )

                if dt == "work_orders" and config.is_special_case:
                    if rows:
                        _, section = process_work_orders(
                            rows, start_dt, end_dt, client,
                            cdf_helper=cdf_helper,
                            selected_agent=selected_agent,
                            summary_prompt=summary_prompt,
                        )
                    else:
                        section = (
                            "## Work orders\n\n"
                            "No work orders were completed, ongoing, or scheduled "
                            "during the summary period."
                        )
                elif rows:
                    _, section = process_dataset(
                        rows, dt, start_dt, end_dt, client,
                        cdf_helper, selected_agent, summary_prompt, cfg,
                    )
                else:
                    section = (
                        f"\n### {config.display_name} (0)\n"
                        f"No new {config.display_name.lower()} added during this period."
                    )

                if section:
                    per_type_sections.append(section.lstrip("\n"))
            except Exception as e:
                print(f"Error processing {dt}: {e}")
                per_type_sections.append(
                    f"### {config.display_name}\nError retrieving {dt} data."
                )

        full_text = compile_summary_markdown(
            per_type_sections,
            start_date,
            end_date,
            truncation_warnings=truncation_warnings or None,
            include_work_orders=summary_contents.get("work_orders", False),
        )
        full_text = strip_html_tags(full_text)
        result = {"markdown": full_text, "success": True}

        if cfg.get("return_pdf_bytes", False):
            try:
                print("\nGenerating PDF bytes...")
                result["pdf_bytes"] = generate_pdf_from_markdown_in_memory(
                    full_text, f"ops_summary_{start_date}_{end_date}",
                )
            except Exception as e:
                print(f"Error generating PDF bytes: {e}")
                result["pdf_error"] = str(e)

        if cfg.get("save_to_cdf", True):
            try:
                print("\nSaving summary to CDF...")
                active_content_types = [k for k in DATA_TYPE_ORDER if summary_contents.get(k)]
                cdf_result = save_summary_to_cdf(
                    client, full_text,
                    str(start_date), str(end_date),
                    generate_pdf=cfg.get("generate_pdf", True),
                    fusion_domain=cfg.get("fusion_domain"),
                    summary_space=cfg.get("summary_space"),
                    content_types=active_content_types,
                )
                result["cdf_result"] = cdf_result
                print(f"Summary saved: {cdf_result}")
            except Exception as e:
                print(f"Error saving to CDF: {e}")
                return {
                    "markdown": full_text,
                    "success": False,
                    "error": f"Failed to save summary: {e}",
                    "instance_counts": instance_counts,
                    "data_type_truncation": data_type_truncation,
                    "truncation_warnings": truncation_warnings,
                }

        result["truncation_warnings"] = truncation_warnings
        result["instance_counts"] = instance_counts
        result["data_type_truncation"] = data_type_truncation

        print("\n" + "=" * 80)
        print("OPERATIONS SUMMARIZER v2 COMPLETED SUCCESSFULLY")
        print("=" * 80)
        return result

    except Exception as e:
        msg = f"Error generating summary: {e}"
        print(f"\n{'=' * 80}\nERROR: {msg}\n{'=' * 80}")
        return {"markdown": msg, "success": False, "error": msg}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


_RELATIVE_PERIODS = {
    "last 24 hours": 1,
    "last 72 hours": 3,
    "last week": 7,
    "last month": 30,
    "last quarter": 90,
}


def _resolve_period(data: dict) -> Tuple[Optional[date], Optional[date], Optional[str]]:
    """Return (start_date, end_date, error). Error is set instead of raising."""
    now = datetime.now(timezone(timedelta(hours=runtime.timezone_offset_hours)))
    default_days = runtime.summary_period_days

    raw_start = data.get("summary_period_start")
    raw_end = data.get("summary_period_end")
    if raw_start is not None and not isinstance(raw_start, str):
        return None, None, "summary_period_start must be a YYYY-MM-DD string"
    if raw_end is not None and not isinstance(raw_end, str):
        return None, None, "summary_period_end must be a YYYY-MM-DD string"

    contents = data.get("summary_contents")
    if contents is not None and not isinstance(contents, dict):
        return None, None, "summary_contents must be an object of data-type flags"

    preset = (raw_start or raw_end or "").strip().lower()
    if preset in _RELATIVE_PERIODS:
        end_date = now.date()
        start_date = (now - timedelta(days=_RELATIVE_PERIODS[preset])).date()
        print(f"Relative period '{preset}': {start_date} to {end_date}")
        return start_date, end_date, None

    if not raw_start and not raw_end:
        end_date = now.date()
        start_date = (now - timedelta(days=default_days)).date()
        print(f"No period specified, defaulting to last {default_days} days: {start_date} to {end_date}")
        return start_date, end_date, None

    if raw_start == "auto" or raw_end == "auto":
        end_date = now.date()
        start_date = (now - timedelta(days=default_days)).date()
        if raw_start and raw_start != "auto":
            parsed, error = _parse_date(raw_start, "summary_period_start")
            if error:
                return None, None, error
            start_date = parsed
        if raw_end and raw_end != "auto":
            parsed, error = _parse_date(raw_end, "summary_period_end")
            if error:
                return None, None, error
            end_date = parsed
        return start_date, end_date, None

    if raw_start:
        start_date, error = _parse_date(raw_start, "summary_period_start")
        if error:
            return None, None, error
    else:
        start_date = (now - timedelta(days=default_days)).date()
    if raw_end:
        end_date, error = _parse_date(raw_end, "summary_period_end")
        if error:
            return None, None, error
    else:
        end_date = now.date()
    return start_date, end_date, None


def _parse_date(value: str, field: str) -> Tuple[Optional[date], Optional[str]]:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date(), None
    except ValueError:
        return None, f"{field} must be YYYY-MM-DD, got {value!r}"
