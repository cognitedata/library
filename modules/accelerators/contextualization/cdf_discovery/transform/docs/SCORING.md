# Scoring

Scoring uses `{field}_score` parallel lists (not `{field}_confidence`).

Libraries:

- `etl_score_property.score_property_key`
- `etl_score_match_eval.apply_score_match_rules_to_float_scores`
- `etl_score_validate.score_row_properties`

Configure tasks with `scoring_rules` and required `score_fields` (or `score_field`). Legacy canvas configs may use `steps` instead of `scoring_rules` (same rule list).

On the flow canvas, **Optimize pipeline** can merge adjacent score nodes (chain or parallel siblings with matching boundaries) into one node. Right-click **Explode into separate score nodes** splits a node with multiple rules (wired as a chain) or multiple `score_fields` (wired in parallel).

Optional:

- `min_score` — floor applied to each value score after rules (default `0.0`).
- `min_threshold_filter_enabled` + `min_threshold` — when enabled, drop scored values whose final score is strictly below `min_threshold` (parallel `{field}_score` lists stay aligned).
