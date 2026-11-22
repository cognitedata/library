-- Overlay only: updates Equipment.files on nodes that already exist (SAP dump must have run).
WITH equipment AS (
  SELECT
      cast(`key` AS STRING)                              AS equipment_key
    , nullif(trim(cast(asset_externalId AS STRING)), '') AS asset_key
  FROM `{{ raw_database_sap }}`.`equipment`
)

, existing_equipment AS (
  SELECT cast(externalId AS STRING) AS equipment_key
  FROM cdf_nodes('{{ enterprise_space }}', 'Equipment', '{{ dm_enterprise_version }}')
  WHERE space = '{{ instance_space }}'
)

, documents AS (
  SELECT
      concat('file_', cast(name AS STRING)) AS document_key
  FROM `{{ raw_database_sharepoint }}`.`file`
)

, annotated_tags AS (
  SELECT
      cast(startNodeExternalId AS STRING) AS document_key
    , cast(endNodeExternalId AS STRING)   AS asset_key
  FROM `{{ raw_database_sharepoint }}`.`diagram_annotation`
  WHERE lower(cast(instanceType AS STRING)) = 'edge'
    AND cast(`type` AS STRING) = 'diagrams.AssetLink'
)

SELECT
    e.equipment_key                                                    AS externalId
  , collect_set(node_reference('{{ instance_space }}', a.document_key)) AS files
FROM equipment AS e
INNER JOIN existing_equipment AS x
  ON e.equipment_key = x.equipment_key
INNER JOIN annotated_tags AS a
  ON e.asset_key = a.asset_key
INNER JOIN documents AS d
  ON a.document_key = d.document_key
WHERE e.equipment_key IS NOT NULL
GROUP BY e.equipment_key
