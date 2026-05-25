# Scoring

Scoring uses `{field}_score` parallel lists (not `{field}_confidence`).

Libraries:

- `etl_score_property.score_property_key`
- `etl_score_match_eval.apply_score_match_rules_to_float_scores`
- `etl_score_validate.score_row_properties`

Configure tasks with `scoring_rules` and optional `score_fields`.
