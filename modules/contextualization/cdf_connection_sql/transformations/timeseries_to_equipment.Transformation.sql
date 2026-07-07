--
-- CFIHOS: Link TimeSeriesData.equipment via shared tag (assets -> Equipment.asset).
--
SELECT
  cast(ts.externalId as string) as externalId,
  cast(ts.isStep as boolean) as isStep,
  cast(ts.type as string) as type,
  CASE
    WHEN max(CASE WHEN eq.externalId IS NOT NULL AND eq.externalId != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE collect_set(node_reference('{{ instanceSpace }}', cast(eq.externalId as string)))
  END as equipment
FROM
  cdf_data_models(
    "{{ schemaSpace }}",
    "{{ datamodelExternalId }}",
    "{{ datamodelVersion }}",
    "TimeSeriesData"
  ) ts
LEFT JOIN
  cdf_data_models(
    "{{ schemaSpace }}",
    "{{ datamodelExternalId }}",
    "{{ datamodelVersion }}",
    "Equipment"
  ) eq
ON
  eq.space = '{{ instanceSpace }}'
  AND array_contains(ts.assets, eq.asset)
WHERE
  ts.space = '{{ instanceSpace }}'
GROUP BY
  ts.externalId, ts.isStep, ts.type
