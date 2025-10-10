from typing import Optional, List, Literal, Dict, Any

class HeuristicStrategy:
    """
    Defines a single heuristic strategy and its weighting.
    """
    def __init__(
        self,
        strategy_id: str, # Assuming a strategy needs an ID or name
        weight: float
    ):
        """
        Initializes a HeuristicStrategy.

        :param strategy_id: Identifier for the specific strategy being applied.
        :param weight: Weight of this strategy in the final score (e.g., 0.3).
        """
        self.strategy_id = strategy_id
        self.weight = weight


class ScoringParameters:
    """
    Defines how the scores from individual strategies are combined and evaluated.
    """
    def __init__(
        self,
        aggregation_method: Literal["weighted_average", "max", "consensus"],
        min_confidence_threshold: float
    ):
        """
        Initializes the ScoringParameters.

        :param aggregation_method: How to combine strategy scores (e.g., "weighted_average").
        :param min_confidence_threshold: Minimum score to accept extraction (e.g., 0.6).
        """
        self.aggregation_method = aggregation_method
        self.min_confidence_threshold = min_confidence_threshold


class HeuristicMethodParameter:
    """
    A class defining parameters for a heuristic-based data extraction method.
    """
    def __init__(
        self,
        heuristic_strategies: List[HeuristicStrategy],
        scoring: ScoringParameters,
        confidence_modifiers: Optional[List[Dict[str, Any]]] = None,
        validation: Optional[List[Dict[str, Any]]] = None
    ):
        """
        Initializes the HeuristicMethodParameter configuration.

        :param heuristic_strategies: List of heuristic strategies to apply.
        :param scoring: How to combine strategy scores and the minimum acceptance threshold.
        :param confidence_modifiers: Rules to adjust confidence scores (optional).
        :param validation: Post-extraction validation checks (optional).
        """
        self.heuristic_strategies = heuristic_strategies
        self.scoring = scoring
        self.confidence_modifiers = confidence_modifiers if confidence_modifiers is not None else []
        self.validation = validation if validation is not None else []