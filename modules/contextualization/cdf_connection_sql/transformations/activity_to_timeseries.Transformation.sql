--
-- CFIHOS: Link WorkOrderOperation.timeSeries via shared tag (mainAsset).
--
SELECT
  op.externalId,
  CASE
    WHEN max(CASE WHEN ts.externalId IS NOT NULL AND ts.externalId != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE collect_set(node_reference('{{ instanceSpace }}', cast(ts.externalId as string)))
  END AS timeSeries
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
    "TimeSeriesData"
  ) ts
ON
  ts.space = '{{ instanceSpace }}'
  AND op.mainAsset IS NOT NULL
  AND array_contains(ts.assets, op.mainAsset)
WHERE
  op.space = '{{ instanceSpace }}'
  AND op.externalId IS NOT NULL
GROUP BY
  op.externalId
