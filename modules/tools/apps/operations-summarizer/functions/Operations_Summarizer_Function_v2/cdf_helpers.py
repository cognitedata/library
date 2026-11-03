"""
CDF helper classes and functions for Operations Summarizer v2.
"""

from typing import List, Dict, Any, Tuple
from cognite.client import CogniteClient
from cognite.client.data_classes.agents import AgentList, Message


class CDFHelper:
    """Helper class for CDF agent management and AI API interactions."""

    def __init__(self, client: CogniteClient):
        self.client = client

    def get_agents(self) -> Tuple[AgentList, List[str]]:
        try:
            agents = self.client.agents.list()
            return agents, []
        except Exception as e:
            print(f"Error getting agents: {e}")
            return AgentList([]), []

    def get_supported_model_names(self) -> List[str]:
        try:
            url = f"/api/v1/projects/{self.client.config.project}/ai/services/availability"
            resp = self.client.get(url)
            resp.raise_for_status()
            data = resp.json()
            return sorted([lm["name"] for lm in data["languageModels"]])
        except Exception as e:
            print(f"Error getting supported models: {e}")
            return ["azure/gpt-4.1", "azure/gpt-4.1-mini"]

    def create_agent(
        self,
        external_id: str,
        name: str,
        description: str,
        instructions: str,
        model: str,
        tools: List[Dict[str, Any]] | None = None,
    ) -> str:
        try:
            from cognite.client.data_classes.agents import AgentUpsert

            agent = AgentUpsert(
                external_id=external_id,
                name=name,
                description=description,
                instructions=instructions,
                model=model,
                tools=tools or [],
            )
            created_agent = self.client.agents.upsert(agents=agent)
            print(f"Created agent: {created_agent.external_id}")
            return created_agent.external_id
        except Exception as e:
            print(f"Error creating agent: {e}")
            raise

    def agent_chat(self, external_id: str, messages: list):
        sdk_messages = [_to_sdk_message(m) for m in messages]
        return self.client.agents.chat(agent_external_id=external_id, messages=sdk_messages)


def _to_sdk_message(message: Message | dict) -> Message:
    """Accept either an SDK Message or the legacy {content: {text}} dict."""
    if isinstance(message, Message):
        return message
    return Message(content=message["content"]["text"])


def ensure_space_exists(client: CogniteClient, space: str) -> bool:
    """Check if a space exists and create it if it doesn't."""
    try:
        existing = client.data_modeling.spaces.retrieve(space)
        if existing:
            print(f"Space '{space}' already exists")
            return True
    except Exception:
        print(f"Space '{space}' not found, creating...")

    try:
        from cognite.client.data_classes.data_modeling import SpaceApply
        client.data_modeling.spaces.apply(SpaceApply(
            space=space,
            name=space,
            description="Space for storing operation summaries",
        ))
        print(f"Created space '{space}'")
        return True
    except Exception as e:
        print(f"Error creating space '{space}': {e}")
        raise
