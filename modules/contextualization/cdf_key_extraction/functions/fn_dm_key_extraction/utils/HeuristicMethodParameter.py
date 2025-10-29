from typing import Optional, List, Literal, Dict, Any, Union
from pydantic import BaseModel, Field
from cognite.client.data_classes.data_modeling.ids import NodeId

# ================================================================================
# |                            Helper classes for config                         |
# ================================================================================

class PositionalDetection(BaseModel):
    """
    Heuristic strategy for detecting key phrases based on their positional context.
    """
    method: Literal['positional_detection'] = 'positional_detection'
    position: str = Field(..., description="A qualifier for the token's position")
    pattern: str = Field(..., description="A regular expression that captures the token")
    confidence_boost: float = Field(0.0, description="Boost to apply to the confidence score if matched")

class FrequencyAnalysis(BaseModel):
    """
    Heuristic strategy for analyzing term frequency within a corpus.
    """
    analyze_corpus: bool = Field(False, description="Whether to analyze the entire corpus for frequency. If set to true, will analyze all fields present in the ingested entity.")
    min_frequency: int = Field(1, description="Minimum frequency count for a term to be considered significant.")
    pattern_stability_threshold: float = Field(..., description="Minimum stability threshold for a pattern to be considered reliable.")
    common_prefix_detection: bool = Field(True, description="Whether to enable common prefix detection.")
    common_suffix_detection: bool = Field(True, description="Whether to enable common suffix detection.")

class EquipmentTypeCorrelationParameters(BaseModel):
    enabled: bool = Field(False, description="Whether to enable equipment type correlation.")
    type_indicators: dict[str, List[str]] = Field(default_factory=dict, description="Indicators for different equipment types (i.e. 'pump': ['centrifugal', 'positive displacement', etc...]).")

class ContextInference(BaseModel):
    """
    Heuristic strategy for inferring context from surrounding text.
    """
    surrounding_keywords: dict[str, List[str]] = Field(default_factory=lambda: {'positive': [], 'negative': []}, description="Keywords indicating positive or negative context.")
    context_window: int = Field(5, description="Number of tokens to consider as context (i.e. characters before/after).")
    keyword_proximity_bonus: float = Field(0.0, description="Bonus to apply for keywords found within the context window.")
    equipment_type_correlation: EquipmentTypeCorrelationParameters = Field(None, description="Parameters for equipment type correlation.")

class ExampleBasedLearning(BaseModel):
    """
    Heuristic strategy for learning from examples and applying learned patterns.
    """
    learning_mode: Literal['similarity'] = Field('similarity', description='The learning mode to be used for example-based learning.')
    source_db: str = Field(..., description='The source database to learn from.')
    source_tbl: str = Field(..., description='The source table to learn from.')
    similarity_threshold: float = Field(..., description='The similarity threshold for matching examples.')
    feature_extraction: dict[str, Union[bool, List[int]]] = Field(..., description='Feature extraction parameters for example-based learning.')


HeuristicConfigParameter = Union[PositionalDetection, FrequencyAnalysis, ContextInference]

class HeuristicStrategyConfig(BaseModel):
    """
    Defines a single heuristic strategy and its configuration.
    """
    # Define parameters as type-annotated class attributes
    strategy_id: str = Field(..., description="Identifier for the specific strategy being applied.")
    weight: float = Field(..., description="Weight of this strategy in the final score (e.g., 0.3).")
    method: Literal['positional_detection', 'frequency_analysis', 'context_inference', 'example_based_learning'] = Field(..., description="The specific method being used.")
    rules: List[HeuristicConfigParameter]

# ================================================================================

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
    normalize_scores: bool = Field(False, description="Whether to normalize scores before aggregation.")

class HeuristicMethodParameter(BaseModel):
    method: Literal['heuristic'] = 'heuristic'

    """
    A class defining parameters for a heuristic-based data extraction method.
    """
    # All fields are defined here, with defaults handled via = None or Field()

    name: str = Field(
        "HEURISTIC", description="The name of the rule (e.g., 'Extract Project ID')."
    )

    heuristic_strategies: List[HeuristicStrategyConfig] = Field(
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

    current_node_id: NodeId