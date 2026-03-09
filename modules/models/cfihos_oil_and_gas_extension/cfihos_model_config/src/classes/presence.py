from enum import Enum
from typing import ClassVar


class Presence(Enum):
    """An enum representing the presence of a property."""

    REQUIRED = "Required"
    PREFERRED = "Preferred"
    OPTIONAL = "Optional"
    NOT_APPLICABLE = "NotApplicable"

    @classmethod
    def from_string(cls, value: str) -> "Presence":
        """Convert a string to a Presence enum."""
        match value:
            case "Required":
                return cls.REQUIRED
            case "Preferred":
                return cls.PREFERRED
            case "Optional":
                return cls.OPTIONAL
            case "NotApplicable":
                return cls.NOT_APPLICABLE
            case _:
                raise ValueError(f"Invalid presence value: {value}")


class PresenceRanking:
    REQUIRED = "Required"
    PREFERRED = "Preferred"
    OPTIONAL = "Optional"
    NOT_APPLICABLE = "NotApplicable"

    rank: ClassVar = {
        NOT_APPLICABLE: 0,
        OPTIONAL: 1,
        PREFERRED: 2,
        REQUIRED: 3,
    }

    @classmethod
    def eval_highest(cls, instances: list[str]) -> str:
        """Evaluate the highest presence from a list of presence values."""
        highest = cls.NOT_APPLICABLE
        for i in instances:
            if PresenceRanking.rank[i] > PresenceRanking.rank[highest]:
                highest = i
        return highest
