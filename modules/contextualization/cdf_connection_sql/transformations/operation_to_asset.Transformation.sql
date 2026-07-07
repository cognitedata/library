--
-- CFIHOS: Backfill WorkOrderOperation assets/mainAsset from self or parent WorkOrder.
--
SELECT
  op.externalId,
  COALESCE(op.mainAsset, wo.mainAsset) AS mainAsset,
  CASE
    WHEN op.assets IS NOT NULL AND size(op.assets) > 0 THEN op.assets
    WHEN op.mainAsset IS NOT NULL THEN array(op.mainAsset)
    WHEN wo.mainAsset IS NOT NULL THEN array(wo.mainAsset)
    WHEN wo.assets IS NOT NULL AND size(wo.assets) > 0 THEN wo.assets
    ELSE NULL
  END AS assets
FROM
  cdf_data_models(
    "{{ schemaSpace }}",
    "{{ datamodelExternalId }}",
    "{{ datamodelVersion }}",
    "WorkOrderOperation"
  ) op
LEFT JOIN
  cdf_data_models(
    "{{ schemaSpace }}",
    "{{ datamodelExternalId }}",
    "{{ datamodelVersion }}",
    "WorkOrder"
  ) wo
ON
  wo.space = '{{ instanceSpace }}'
  AND op.maintenanceOrder.externalId = wo.externalId
WHERE
  op.space = '{{ instanceSpace }}'
  AND op.externalId IS NOT NULL
