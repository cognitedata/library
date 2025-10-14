from typing import Optional, List, Literal, Dict, Any
from pydantic import BaseModel, Field

from .DataStructures import SourceFieldParameter

class HeuristicStrategy(BaseModel):
    """
    Defines a single heuristic strategy and its weighting.
    """
    # Define parameters as type-annotated class attributes
    strategy_id: str = Field(..., description="Identifier for the specific strategy being applied.")
    weight: float = Field(..., description="Weight of this strategy in the final score (e.g., 0.3).")

class ScoringParameters(BaseModel):
    """
    Defines how the scores from individual strategies are combined and evaluated.
    """
    # Define parameters as type-annotated class attributes
    aggregation_method: Literal["weighted_average", "max", "consensus"] = Field(
        ..., description="How to combine strategy scores (e.g., 'weighted_average')."
    )
    min_confidence_threshold: float = Field(
        ..., description="Minimum score to accept extraction (e.g., 0.6)."
    )


class HeuristicMethodParameter(BaseModel):
    method: Literal['heuristic'] = 'heuristic'

    """
    A class defining parameters for a heuristic-based data extraction method.
    """
    # All fields are defined here, with defaults handled via = None or Field()

    heuristic_strategies: List[HeuristicStrategy] = Field(
        ..., description="List of heuristic strategies (HeuristicStrategy) to apply."
    )

    scoring: ScoringParameters = Field(
        ..., description="How to combine strategy scores and the minimum acceptance threshold."
    )

    confidence_modifiers: Optional[List[Dict[str, Any]]] = Field(
        None, description="Rules to adjust confidence scores (optional)."
    )

    validation: Optional[List[Dict[str, Any]]] = Field(
        None, description="Post-extraction validation checks (optional)."
    )

    source_fields: Optional[List[SourceFieldParameter]] = Field(
        None, description="List of source field paths to extract data from."
    )

    priority: int = Field(
        100, description="THe priority of the rule in the order of rules applied to the target field"
    )