"""
Configuration for Podcast Generation Function v2.

Spaces and the file view come from the config node. Call ``data`` overrides
those values. Prompt text stays in this module.
"""

from datetime import timezone
from typing import Any

from cognite.client.data_classes.data_modeling import ViewId

from app_config import load_app_config
from tts_engine import DEFAULT_VOICE

DEFAULT_TIMEZONE = timezone.utc
# Podcast script agent identity stays in code, like the prompt text.
# The config node agent.name is the operations summarizer helper.
PODCAST_AGENT_NAME = "Podcast Script Generator"


class Runtime:
    """Values resolved from the config node, then call-data overrides."""

    summary_space: str = ""
    podcast_prefix: str = ""
    data_instance_space: str = ""
    file_view: ViewId | None = None


runtime = Runtime()


def default_podcast_instructions() -> str:
    return (
        "You are a Podcast Script Writer that transforms an operations summary into an engaging, "
        "natural-sounding audio script. The output will be read aloud by a text-to-speech engine, "
        "so it must sound like a real person speaking — conversational, clear, and easy to follow by ear.\n\n"
        "CORE RULES:\n"
        "1. You receive an operations summary (markdown or plain text). Rewrite it as a spoken podcast script.\n"
        "2. Target length: approximately 250-300 words (~2 minutes at normal speaking pace).\n"
        "3. Output ONLY the spoken script — no stage directions, no [brackets], no markdown formatting, "
        "no headers, no bullet points.\n"
        "4. Write in plain, flowing sentences and short paragraphs. Use natural transitions "
        '("Now, looking at...", "One thing worth noting...", "Moving on to...").\n'
        "5. Open with a brief, friendly greeting and context "
        '(e.g., "Hey there — here\'s your operations update for the period.").\n'
        "6. Close with a short sign-off "
        '(e.g., "That\'s the overview for this period. Thanks for listening.").\n'
        "7. Respond in English.\n\n"
        "STYLE GUIDELINES:\n"
        "- Speak as a narrator presenting the data, not as the analyst who collected it. "
        "Do not claim first-hand involvement.\n"
        "- Use conversational tone — contractions are fine.\n"
        "- Avoid jargon-heavy sentences; if a technical term is needed, briefly explain it.\n"
        '- Round counts naturally for readability ("about forty work orders" rather than '
        '"39 work orders"). NEVER round or reword identifiers, equipment tags, work order numbers, '
        "or dates — read those as written.\n"
        "- Highlight the most important 3-5 insights; don't try to cover every detail.\n"
        "- Use varied sentence lengths.\n"
        "- Avoid lists — convert list items into flowing narrative.\n"
        '- Never say "as shown in the data" or "the summary indicates."\n\n'
        "EMPTY OR MINIMAL INPUT:\n"
        "- If the input summary is empty or under ~50 words, produce a short script that opens with "
        "the greeting, states briefly that there were no significant operational events to report "
        "in this period, and signs off. Do not invent content.\n\n"
        "WHAT TO COVER (in rough order, when content exists):\n"
        "1. Big-picture overview: what period, how much activity overall.\n"
        "2. Key highlights: anything unusual, critical, or noteworthy.\n"
        "3. Operational trends: patterns in work orders, notifications, equipment, or functional locations.\n"
        "4. Brief wrap-up with the most actionable takeaway.\n\n"
        "Remember: the listener cannot see text. Everything must be understandable purely by hearing it."
    )


def _pick(data: dict, key: str, fallback: Any) -> Any:
    """Call data wins when the key is present and not None."""
    if key in data and data[key] is not None:
        return data[key]
    return fallback


def get_podcast_config(data: dict, client: Any) -> dict:
    """Return resolved config: config node overridden by call data."""
    node = load_app_config(client)
    spaces = node["spaces"]
    file_view = node["dataModel"]["views"]["summaryOutputFile"]
    runtime.file_view = ViewId(
        space=file_view["space"],
        external_id=file_view["externalId"],
        version=file_view["version"],
    )
    runtime.summary_space = _pick(data, "summary_space", spaces["summaries"])
    runtime.podcast_prefix = node["externalIdPrefixes"]["podcast"]
    runtime.data_instance_space = spaces["dataInstances"]
    return {
        "summary_space": runtime.summary_space,
        "podcast_prefix": runtime.podcast_prefix,
        "data_instance_space": runtime.data_instance_space,
        "podcast_agent_id": data.get("podcast_agent_id"),
        "podcast_instructions": data.get("podcast_instructions") or default_podcast_instructions(),
        "voice": data.get("voice", DEFAULT_VOICE),
        "speed": float(data.get("speed", 1.0)),
        "generate_audio": data.get("generate_audio", True),
        "save_to_cdf": data.get("save_to_cdf", True),
        "fusion_domain": data.get("fusion_domain"),
    }
