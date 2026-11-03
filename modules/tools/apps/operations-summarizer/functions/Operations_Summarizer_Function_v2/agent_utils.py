"""
Agent creation and management utilities for Operations Summarizer v2.
"""

from cognite.client import CogniteClient
from cdf_helpers import CDFHelper
from config import runtime


def ensure_agent_exists(
    client: CogniteClient,
    agent_id: str = None,
    instructions: str = None,
) -> str:
    """Ensure that an agent exists, creating one if necessary.

    Parameters
    ----------
    client : CogniteClient
    agent_id : str, optional
        Specific agent external ID to use.
    instructions : str, optional
        Agent system instructions. Falls back to the config node.

    Returns
    -------
    str – external_id of the agent to use.
    """
    cdf_helper = CDFHelper(client)

    if agent_id:
        try:
            agents, _ = cdf_helper.get_agents()
            for agent in agents:
                if agent.external_id == agent_id:
                    print(f"Using provided agent: {agent_id}")
                    return agent_id
            print(f"Agent {agent_id} not found, will create default agent")
        except Exception as e:
            print(f"Error checking for agent: {e}")

    try:
        agents, _ = cdf_helper.get_agents()
        for agent in agents:
            if agent.name == runtime.helper_agent_name:
                print(f"Found existing helper agent: {agent.external_id}")
                return agent.external_id
    except Exception as e:
        print(f"Error getting agents: {e}")

    try:
        project_name = client.config.project
        external_id = f"{project_name}_operation_summarizer_helper_agent"

        supported_models = cdf_helper.get_supported_model_names()
        if not supported_models:
            raise RuntimeError("No supported language models available")
        model = supported_models[0]

        instr = instructions or runtime.agent_instructions

        agent_id = cdf_helper.create_agent(
            external_id=external_id,
            name=runtime.helper_agent_name,
            description="AI agent for summarizing operations data.",
            instructions=instr,
            model=model,
            tools=[],
        )
        print(f"Created new helper agent: {agent_id}")
        return agent_id

    except Exception as e:
        print(f"Failed to create helper agent: {e}")
        raise RuntimeError(f"Could not ensure agent exists: {e}") from e
