"""
Podcast script generation via CDF AI agent.

Sends the operations summary to the Podcast Script Generator agent and
returns the spoken-word script suitable for TTS.
"""

from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Message, TextContent
from cdf_helpers import CDFHelper
from podcast_config import PODCAST_AGENT_NAME, default_podcast_instructions


def extract_ai_content(response) -> str:
    """Extract text content from a CDF agent chat response.

    Handles AgentChatResponse SDK objects via the text_ property,
    with a fallback for legacy dict-shaped responses.
    """
    if not response:
        return ""

    if isinstance(response, str):
        return response

    # SDK AgentChatResponse — text is a convenience property returning str | None
    if hasattr(response, "text"):
        return response.text or ""

    # Legacy dict fallback
    if isinstance(response, dict):
        resp_block = response.get("response")
        if resp_block and isinstance(resp_block, dict):
            messages = resp_block.get("messages", [])
            if messages:
                content = messages[0].get("content")
                if isinstance(content, str):
                    return content
                if isinstance(content, dict) and "text" in content:
                    return content["text"]

        choices = response.get("choices", [])
        if choices:
            msg = choices[0].get("message", {})
            if isinstance(msg.get("content"), str):
                return msg["content"]

    return ""


def ensure_podcast_agent(
    client: CogniteClient,
    cdf_helper: CDFHelper,
    agent_id: str | None = None,
    instructions: str | None = None,
) -> str:
    """Ensure the Podcast Script Generator agent exists, creating if needed.

    Returns the agent's external_id.
    """
    if agent_id:
        try:
            agents, _ = cdf_helper.get_agents()
            for agent in agents:
                if agent.external_id == agent_id:
                    print(f"Using provided podcast agent: {agent_id}")
                    return agent_id
            print(f"Agent '{agent_id}' not found, will create default")
        except Exception as e:
            print(f"Error checking for agent: {e}")

    try:
        agents, _ = cdf_helper.get_agents()
        for agent in agents:
            if agent.name == PODCAST_AGENT_NAME:
                print(f"Found existing podcast agent: {agent.external_id}")
                return agent.external_id
    except Exception as e:
        print(f"Error listing agents: {e}")

    project = client.config.project
    external_id = f"{project}_podcast_script_generator_agent"

    models = cdf_helper.get_supported_model_names()
    if not models:
        raise RuntimeError("No supported language models available in CDF")
    model = models[0]

    instr = instructions or default_podcast_instructions()

    created_id = cdf_helper.create_agent(
        external_id=external_id,
        name=PODCAST_AGENT_NAME,
        description="AI agent that rewrites operations summaries into natural podcast scripts.",
        instructions=instr,
        model=model,
        tools=[],
    )
    print(f"Created podcast agent: {created_id}")
    return created_id


def generate_podcast_script(
    cdf_helper: CDFHelper,
    agent_id: str,
    summary_markdown: str,
) -> str:
    """Send an operations summary to the podcast agent and return the script."""
    prompt = (
        "Here is the operations summary. "
        "Rewrite it as a podcast script:\n\n"
        + summary_markdown
    )

    response = cdf_helper.agent_chat(agent_id, [
        Message(content=TextContent(text=prompt)),
    ])

    script = extract_ai_content(response)
    if not script.strip():
        raise RuntimeError("Podcast agent returned an empty script")

    return script
