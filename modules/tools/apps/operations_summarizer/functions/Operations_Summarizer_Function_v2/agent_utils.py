"""
Agent creation and management utilities for Operations Summarizer v2.
"""

from cdf_helpers import CDFHelper
from cognite.client import CogniteClient
from config import runtime


def ensure_agent_exists(
    client: CogniteClient,
    agent_id: str | None = None,
    instructions: str | None = None,
) -> str:
    """Ensure that an agent exists, creating the configured helper agent if necessary.

    Agents are matched by external ID only. The default is the config node
    ``agent.externalId``.

    Args:
        client: Cognite client.
        agent_id: Explicit agent external ID. Must exist.
        instructions: Agent system instructions. Falls back to the config node.

    Returns:
        External ID of the agent to use.

    Raises:
        ValueError: ``agent_id`` is given but no such agent exists.
        RuntimeError: The helper agent could not be created.
    """
    cdf_helper = CDFHelper(client)
    agents, _ = cdf_helper.get_agents()
    existing_ids = {agent.external_id for agent in agents}

    if agent_id:
        if agent_id not in existing_ids:
            raise ValueError(f"Agent '{agent_id}' not found")
        print(f"Using provided agent: {agent_id}")
        return agent_id

    external_id = runtime.helper_agent_external_id
    if external_id in existing_ids:
        print(f"Found existing helper agent: {external_id}")
        return external_id

    try:
        supported_models = cdf_helper.get_supported_model_names()
        if not supported_models:
            raise RuntimeError("No supported language models available")

        created_id = cdf_helper.create_agent(
            external_id=external_id,
            name=runtime.helper_agent_name,
            description="AI agent for summarizing operations data.",
            instructions=instructions or runtime.agent_instructions,
            model=supported_models[0],
            tools=[],
        )
        print(f"Created new helper agent: {created_id}")
        return created_id

    except Exception as e:
        print(f"Failed to create helper agent: {e}")
        raise RuntimeError(f"Could not ensure agent exists: {e}") from e
