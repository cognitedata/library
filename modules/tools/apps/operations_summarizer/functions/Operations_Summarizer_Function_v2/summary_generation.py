"""
Summary generation functions for Operations Summarizer v2.

Handles chunking, per-chunk AI summarisation, holistic roll-up, and work-order
subcategory processing (mirrors ``src/utils/workOrderSummary.ts``).
"""

import time as time_module
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import Any

from cdf_helpers import CDFHelper
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError, CogniteReadTimeout
from config import DATA_TYPE_CONFIGS, runtime
from table_generation import generate_table_for_instances

# =============================================================================
# RETRY DECORATOR
# =============================================================================


def retry_with_backoff(max_retries=3, initial_delay=1, max_delay=10):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (CogniteReadTimeout, CogniteAPIError, TimeoutError):
                    if attempt < max_retries - 1:
                        print(f"Attempt {attempt + 1} failed, retrying in {delay}s...")
                        time_module.sleep(min(delay, max_delay))
                        delay *= 2
                    else:
                        raise
                except Exception:
                    raise
            return None

        return wrapper

    return decorator


# =============================================================================
# CORE SUMMARY FUNCTIONS
# =============================================================================


@retry_with_backoff(max_retries=3, initial_delay=1, max_delay=10)
def create_summary_for_data(
    table_text: str,
    data_type: str,
    num_instances: int,
    start_time: datetime,
    end_time: datetime,
    cdf_helper: CDFHelper = None,
    selected_agent: str | None = None,
    summary_prompt: dict | None = None,
    prompt_header: str | None = None,
) -> str:
    """Send a chunk of tabular data to the AI agent and get a summary back."""
    header = prompt_header or runtime.prompt_header
    agent_prompt = header.format(
        num_instances=num_instances,
        data_type=data_type,
        start_time=start_time.strftime("%Y-%m-%d") if hasattr(start_time, "strftime") else start_time,
        # ``end_time`` is the exclusive upper bound (midnight after the last day); show the last included day.
        end_time=(end_time - timedelta(days=1)).strftime("%Y-%m-%d") if hasattr(end_time, "strftime") else end_time,
        table_text=table_text,
    )
    agent_prompt += (summary_prompt or {}).get(data_type, "")

    messages = [{"content": {"type": "text", "text": agent_prompt}, "role": "user"}]
    resp = cdf_helper.agent_chat(external_id=selected_agent, messages=messages)
    return resp.text


@retry_with_backoff(max_retries=3, initial_delay=1, max_delay=10)
def create_holistic_summary(
    chunk_summaries: list[str],
    data_type: str,
    total_instances: int,
    cdf_helper: CDFHelper = None,
    selected_agent: str | None = None,
    holistic_template: str | None = None,
) -> str:
    """Create a single holistic summary from multiple chunk summaries."""
    combined = "\n\n".join(f"Summary {i + 1}:\n{s}" for i, s in enumerate(chunk_summaries))
    template = holistic_template or runtime.holistic_summary_template
    prompt = template.format(
        num_chunks=len(chunk_summaries),
        total_instances=total_instances,
        data_type=data_type.replace("_", " "),
        combined_summaries=combined,
    )
    messages = [{"content": {"type": "text", "text": prompt}, "role": "user"}]
    resp = cdf_helper.agent_chat(external_id=selected_agent, messages=messages)
    return resp.text


def chunk_instances(instances: list[Any], chunk_size: int | None = None) -> list[list[Any]]:
    if chunk_size is None:
        chunk_size = runtime.chunk_size
    return [instances[i : i + chunk_size] for i in range(0, len(instances), chunk_size)]


def _node_prop(node, property_name: str):
    """Read a property from a work-order node across view containers."""
    if not hasattr(node, "properties"):
        return None
    for props in node.properties.values():
        if isinstance(props, dict) and property_name in props:
            return props[property_name]
    return None


def _parse_date(value) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed
    except Exception:
        return None


def _categorize_work_orders(work_orders: list[Any]):
    """Categorize work orders by status (completed, ongoing, scheduled)."""
    now = datetime.now(UTC)
    completed, ongoing, scheduled = [], [], []

    for wo in work_orders:
        start_time = _parse_date(_node_prop(wo, "startTime"))
        end_time = _parse_date(_node_prop(wo, "endTime"))

        if end_time and end_time < now:
            completed.append(wo)
        elif start_time and start_time <= now and (not end_time or end_time >= now):
            ongoing.append(wo)
        elif start_time and start_time > now:
            scheduled.append(wo)
        else:
            scheduled.append(wo)

    return [
        {"key": "completed", "items": completed, "label": "Completed Work Orders"},
        {"key": "ongoing", "items": ongoing, "label": "Ongoing Work Orders"},
        {"key": "scheduled", "items": scheduled, "label": "Scheduled Work Orders"},
    ]


@retry_with_backoff(max_retries=3, initial_delay=1, max_delay=10)
def _summarize_work_order_chunk(
    label: str,
    items: list[Any],
    cdf_helper: CDFHelper,
    selected_agent: str,
    work_order_prompt: str,
    client: CogniteClient,
) -> str:
    table = generate_table_for_instances("work_orders", items, client)
    category_prompt = (
        f"You are an Operations Summarizer. Analyze and summarize the following "
        f"{len(items)} {label.lower()}.\n\n"
        f"DATA TO SUMMARIZE:\n{table}\n\n"
        f"INSTRUCTIONS:\n{work_order_prompt}\n\n"
        "Provide your summary now. Start directly with the analysis, not with any "
        "meta-statements about what you're summarizing."
    )
    messages = [{"content": {"type": "text", "text": category_prompt}, "role": "user"}]
    resp = cdf_helper.agent_chat(external_id=selected_agent, messages=messages)
    return (resp.text or "").strip()


def _generate_work_order_category_summary(
    category: dict,
    cdf_helper: CDFHelper,
    selected_agent: str,
    work_order_prompt: str,
    client: CogniteClient,
    chunk_size: int | None = None,
) -> str:
    """Summarise one status bucket in chunks of ``chunk_size``, rolling up when there is more than one chunk."""
    items = category["items"]
    label = category["label"]

    if not items:
        return f"**{label}:** None during this period."

    chunk_summaries = [
        _summarize_work_order_chunk(label, chunk, cdf_helper, selected_agent, work_order_prompt, client)
        for chunk in chunk_instances(items, chunk_size)
    ]
    if len(chunk_summaries) == 1:
        content = chunk_summaries[0]
    else:
        content = create_holistic_summary(
            chunk_summaries,
            "work_orders",
            len(items),
            cdf_helper=cdf_helper,
            selected_agent=selected_agent,
        ).strip()

    if content:
        return f"**{label} ({len(items)}):**\n{content}"
    return f"**{label} ({len(items)}):** Details available in the data."


def process_work_orders(
    instances: list[Any],
    start_time: datetime,
    end_time: datetime,
    client: CogniteClient,
    cdf_helper: CDFHelper = None,
    selected_agent: str | None = None,
    summary_prompt: dict | None = None,
    chunk_size: int | None = None,
) -> tuple[str, str]:
    """Process work orders with subcategory summaries (mirrors generateWorkOrdersSection)."""
    total = len(instances)
    if total == 0:
        section = "## Work orders\n\nNo work orders were completed, ongoing, or scheduled during the summary period."
        return "", section

    work_order_prompt = (summary_prompt or {}).get("work_orders", "")
    categories = _categorize_work_orders(instances)
    sections = [
        _generate_work_order_category_summary(cat, cdf_helper, selected_agent, work_order_prompt, client, chunk_size)
        for cat in categories
    ]
    section = "## Work Orders\n\n" + "\n\n".join(sections)
    return "", section


# =============================================================================
# UNIFIED DATASET PROCESSOR
# =============================================================================


def process_dataset(
    instances: list[Any],
    data_type: str,
    start_time: datetime,
    end_time: datetime,
    client: CogniteClient,
    cdf_helper: CDFHelper = None,
    selected_agent: str | None = None,
    summary_prompt: dict | None = None,
    cfg: dict | None = None,
) -> tuple[str, str]:
    """Process a list of instances into (table_markdown, summary_section)."""
    cfg = cfg or {}
    chunk_size = cfg.get("chunk_size", runtime.chunk_size)
    holistic_template = cfg.get("holistic_summary_template") or runtime.holistic_summary_template
    config = DATA_TYPE_CONFIGS.get(data_type)
    display_name = config.display_name if config else data_type.replace("_", " ").title()

    total = len(instances)
    if total == 0:
        return "", ""

    if total <= chunk_size:
        table = generate_table_for_instances(data_type, instances, client)
        summary = create_summary_for_data(
            table,
            data_type,
            total,
            start_time,
            end_time,
            cdf_helper=cdf_helper,
            selected_agent=selected_agent,
            summary_prompt=summary_prompt,
        )
        section = f"\n### {display_name} ({total})\n{summary}"
        return table, section

    chunks = chunk_instances(instances, chunk_size)
    chunk_summaries = []

    for i, chunk in enumerate(chunks):
        print(f"Processing chunk {i + 1}/{len(chunks)} for {data_type}")
        chunk_table = generate_table_for_instances(data_type, chunk, client)
        chunk_summaries.append(
            create_summary_for_data(
                chunk_table,
                data_type,
                len(chunk),
                start_time,
                end_time,
                cdf_helper=cdf_helper,
                selected_agent=selected_agent,
                summary_prompt=summary_prompt,
            )
        )

    holistic = create_holistic_summary(
        chunk_summaries,
        data_type,
        total,
        cdf_helper=cdf_helper,
        selected_agent=selected_agent,
        holistic_template=holistic_template,
    )

    unified_table = generate_table_for_instances(data_type, instances[:chunk_size], client)
    unified_table = f"*Note: Showing first {chunk_size} of {total} total instances*\n\n" + unified_table

    section = f"\n### {display_name} ({total})\n{holistic}"
    return unified_table, section
