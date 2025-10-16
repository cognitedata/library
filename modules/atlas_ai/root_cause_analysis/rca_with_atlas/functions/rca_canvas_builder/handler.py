import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import NewType, Optional
from collections import OrderedDict

Status = NewType("Status", str)

# Define status constants
PENDING = Status("PENDING")
FAILURE = Status("FAILURE")
SUCCESS = Status("SUCCESS")

# expected data structure: cause_map_<equipment_class_singular>.json;
# like this: /data/cause_map_pump.json
# not like this: /data/cause_map_pumps.json
DATA_DIR = Path(__file__).parent / "data"
SUPPORTED_EQUIPMENT_CLASSES = [
    file.stem.replace("cause_map_", "") for file in DATA_DIR.glob("cause_map_*.json")
]


@dataclass
class AgentInput:
    equipment_class: Optional[str] = None
    canvas_name: Optional[str] = None
    canvas_external_id: Optional[str] = None
    failure_mode: Optional[str] = None


    @classmethod
    def load(cls, data: dict):
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered_data)


@dataclass
class Response:
    status: Status = field(default=PENDING)
    _reasoning: list[str] = field(default_factory=list)

    @property
    def message(self) -> str:
        return self._reasoning[-1] if self._reasoning else "something went wrong"

    def add(self, message: str):
        self._reasoning.append(message)
        print(message)

    def failed(self, message: str | None = None) -> dict:
        if message:
            self.add(message)

        self.status = FAILURE
        return asdict(self)

    def succeeded(self, message: str | None = None) -> dict:
        if message:
            self.add(message)

        self.status = SUCCESS
        return asdict(self)


def handle(data: dict, client=None) -> dict:
    start_time = time.time()
    print("Received data:", data)  # Debug print

    response = Response()
    agent_input = AgentInput.load(data)

    # Load the cause map JSON file
    file_path = DATA_DIR / f"cause_map_{agent_input.equipment_class}.json"
    if not file_path.exists():
        return response.failed(
            f"Failed to load cause map template for Equipment class {agent_input.equipment_class}"
        )

    with file_path.open() as f:
        cause_map = json.load(f)

    # Get failure mode data
    failure_mode = agent_input.failure_mode
    if not failure_mode or failure_mode not in cause_map:
        return response.failed(
            f"Provided failure mode '{failure_mode}' not found in cause map."
        )
    
    root_cause = cause_map[failure_mode]

    # Sort failure mechanisms by Failure Rate (descending order)
    sorted_mechanisms = sorted(
        root_cause.items(),
        key=lambda item: item[1].get("Failure Rate", 0) if isinstance(item[1], dict) else 0,
        reverse=True,
    )

    # Remove "Failure Rate" from the returned dictionary and get top 3
    top_3_result = OrderedDict()
    count = 0
    for mech, details in sorted_mechanisms:
        if mech == "Failure Rate":
            continue
        if isinstance(details, dict) and "Failure Rate" in details:
            details.pop("Failure Rate")
        top_3_result[mech] = details
        count += 1
        if count == 3:
            break

    # Print execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Function execution time: {execution_time:.2f} seconds")

    # Return sorted data using OrderedDict to preserve order
    return response.succeeded(
        {
            "failure_mode": failure_mode,
            "cause_map": list(top_3_result.items())
        }
    )