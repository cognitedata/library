--
-- CFIHOS: Backfill WorkOrder.assets from mainAsset when assets is empty.
--
SELECT
  wo.externalId,
  wo.mainAsset,
  CASE
    WHEN wo.assets IS NOT NULL AND size(wo.assets) > 0 THEN wo.assets
    WHEN wo.mainAsset IS NOT NULL THEN array(wo.mainAsset)
    ELSE NULL
  END AS assets
FROM
  cdf_data_models(
    "{{ space }}",
    "{{ data_model_external_id }}",
    "{{ dm_version }}",
    "WorkOrder"
  ) wo
WHERE
  wo.space = '{{ instance_space }}'
  AND wo.externalId IS NOT NULL
