"""
Summary generation functions for Operations Summarizer v2.

Handles chunking, per-chunk AI summarisation, holistic roll-up, and work-order
subcategory processing (mirrors ``src/utils/workOrderSummary.ts``).
"""

from typing import List, Any, Tuple, Optional
from functools import wraps
import time as time_module
from datetime import datetime, date, timezone
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteReadTimeout, CogniteAPIError

from config import DATA_TYPE_CONFIGS, runtime
from cdf_helpers import CDFHelper
from table_generation import generate_table_for_instances


# =============================================================================
# RETRY DECORATOR
# =============================================================================


def retry_with_backoff(max_retries=3, initial_delay=1, max_delay=10):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (CogniteReadTimeout, CogniteAPIError, TimeoutError) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        print(f"Attempt {attempt + 1} failed, retrying in {delay}s...")
                        time_module.sleep(min(delay, max_delay))
                        delay *= 2
                    else:
                        raise last_exception
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
    selected_agent: str = None,
    summary_prompt: dict = None,
    prompt_header: str = None,
) -> str:
    """Send a chunk of tabular data to the AI agent and get a summary back."""
    header = prompt_header or runtime.prompt_header
    agent_prompt = header.format(
        num_instances=num_instances,
        data_type=data_type,
        start_time=start_time.strftime("%Y-%m-%d") if hasattr(start_time, "strftime") else start_time,
        end_time=end_time.strftime("%Y-%m-%d") if hasattr(end_time, "strftime") else end_time,
        table_text=table_text,
    )
    agent_prompt += (summary_prompt or {}).get(data_type, "")

    messages = [{"content": {"type": "text", "text": agent_prompt}, "role": "user"}]
    resp = cdf_helper.agent_chat(external_id=selected_agent, messages=messages)
    return resp.text


@retry_with_backoff(max_retries=3, initial_delay=1, max_delay=10)
def create_holistic_summary(
    chunk_summaries: List[str],
    data_type: str,
    total_instances: int,
    cdf_helper: CDFHelper = None,
    selected_agent: str = None,
    holistic_template: str = None,
) -> str:
    """Create a single holistic summary from multiple chunk summaries."""
    combined = "\n\n".join(
        f"Summary {i+1}:\n{s}" for i, s in enumerate(chunk_summaries)
    )
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


def chunk_instances(instances: List[Any], chunk_size: int | None = None) -> List[List[Any]]:
    if chunk_size is None:
        chunk_size = runtime.chunk_size
    return [instances[i:i + chunk_size] for i in range(0, len(instances), chunk_size)]


def _node_prop(node, property_name: str):
    """Read a property from a work-order node across view containers."""
    if not hasattr(node, "properties"):
        return None
    for props in node.properties.values():
        if isinstance(props, dict) and property_name in props:
            return props[property_name]
    return None


def _parse_date(value) -> Optional[datetime]:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def _categorize_work_orders(work_orders: List[Any]):
    """Categorize work orders by status (completed, ongoing, scheduled)."""
    now = datetime.now(timezone.utc)
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
def _generate_work_order_category_summary(
    category: dict,
    cdf_helper: CDFHelper,
    selected_agent: str,
    work_order_prompt: str,
    client: CogniteClient,
) -> str:
    items = category["items"]
    label = category["label"]

    if not items:
        return f"**{label}:** None during this period."

    table = generate_table_for_instances("work_orders", items, client)
    category_prompt = (
        f"You are an Operations Summarizer. Analyze and summarize the following "
        f"{len(items)} {label.lower()}.\n\n"
        f"DATA TO SUMMARIZE:\n{table}\n\n"
        f"INSTRUCTIONS:\n{work_order_prompt}\n\n"
        "Provide your summary now. Start directly with the analysis, not with any "
        "meta-statements about what you're summarizing."
    )

    try:
        messages = [{"content": {"type": "text", "text": category_prompt}, "role": "user"}]
        resp = cdf_helper.agent_chat(external_id=selected_agent, messages=messages)
        content = (resp.text or "").strip()
        if content:
            return f"**{label} ({len(items)}):**\n{content}"
        return f"**{label} ({len(items)}):** Details available in the data."
    except Exception as e:
        print(f"Error generating {category['key']} work orders summary: {e}")
        return f"**{label} ({len(items)}):** Unable to generate summary."


def process_work_orders(
    instances: List[Any],
    start_time: datetime,
    end_time: datetime,
    client: CogniteClient,
    cdf_helper: CDFHelper = None,
    selected_agent: str = None,
    summary_prompt: dict = None,
) -> Tuple[str, str]:
    """Process work orders with subcategory summaries (mirrors generateWorkOrdersSection)."""
    total = len(instances)
    if total == 0:
        section = (
            "## Work orders\n\n"
            "No work orders were completed, ongoing, or scheduled during the summary period."
        )
        return "", section

    work_order_prompt = (summary_prompt or {}).get("work_orders", "")
    categories = _categorize_work_orders(instances)
    sections = [
        _generate_work_order_category_summary(
            cat, cdf_helper, selected_agent, work_order_prompt, client
        )
        for cat in categories
    ]
    section = f"## Work Orders\n\n" + "\n\n".join(sections)
    return "", section


# =============================================================================
# UNIFIED DATASET PROCESSOR
# =============================================================================


def process_dataset(
    instances: List[Any],
    data_type: str,
    start_time: datetime,
    end_time: datetime,
    client: CogniteClient,
    cdf_helper: CDFHelper = None,
    selected_agent: str = None,
    summary_prompt: dict = None,
    cfg: dict = None,
) -> Tuple[str, str]:
    """Process a list of instances into (table_markdown, summary_section)."""
    cfg = cfg or {}
    chunk_size = cfg.get("chunk_size", runtime.chunk_size)
    holistic_template = cfg.get("holistic_summary_template") or runtime.holistic_summary_template
    config = DATA_TYPE_CONFIGS.get(data_type)
    display_name = config.display_name if config else data_type.replace("_", " ").title()

    try:
        total = len(instances)
        if total == 0:
            return "", ""

        if total <= chunk_size:
            try:
                table = generate_table_for_instances(data_type, instances, client)
            except Exception as e:
                table = f"Error generating table for {data_type}: {e}"

            try:
                summary = create_summary_for_data(
                    table, data_type, total, start_time, end_time,
                    cdf_helper=cdf_helper,
                    selected_agent=selected_agent,
                    summary_prompt=summary_prompt,
                )
            except Exception as e:
                print(f"Error creating summary for {data_type}: {e}")
                summary = (
                    f"Found {total} {data_type.replace('_', ' ')} instances. "
                    "Please review the detailed table below for more information."
                )

            section = f"\n### {display_name} ({total})\n{summary}"
            return table, section

        chunks = chunk_instances(instances, chunk_size)
        chunk_summaries = []

        for i, chunk in enumerate(chunks):
            print(f"Processing chunk {i+1}/{len(chunks)} for {data_type}")
            try:
                ctable = generate_table_for_instances(data_type, chunk, client)
            except Exception:
                ctable = f"Error generating table for chunk {i+1}"
            try:
                cs = create_summary_for_data(
                    ctable, data_type, len(chunk), start_time, end_time,
                    cdf_helper=cdf_helper,
                    selected_agent=selected_agent,
                    summary_prompt=summary_prompt,
                )
            except Exception as e:
                print(f"Error in chunk {i+1}: {e}")
                cs = f"Chunk {i+1} contains {len(chunk)} {data_type.replace('_', ' ')} instances."
            chunk_summaries.append(cs)

        try:
            holistic = create_holistic_summary(
                chunk_summaries, data_type, total,
                cdf_helper=cdf_helper,
                selected_agent=selected_agent,
                holistic_template=holistic_template,
            )
        except Exception as e:
            print(f"Error creating holistic summary: {e}")
            holistic = (
                f"Processed {total} {data_type.replace('_', ' ')} instances across "
                f"{len(chunks)} chunks."
            )

        try:
            display = instances[:chunk_size]
            unified_table = generate_table_for_instances(data_type, display, client)
        except Exception:
            unified_table = f"Error generating unified table for {data_type}"

        if total > chunk_size:
            unified_table = (
                f"*Note: Showing first {chunk_size} of {total} total instances*\n\n"
                + unified_table
            )

        section = f"\n### {display_name} ({total})\n{holistic}"
        return unified_table, section

    except Exception as e:
        print(f"Error processing dataset for {data_type}: {e}")
        return f"Error processing dataset for {data_type}: {e}", ""
