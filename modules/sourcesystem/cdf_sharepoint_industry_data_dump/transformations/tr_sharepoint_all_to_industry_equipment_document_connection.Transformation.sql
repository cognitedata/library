-- Overlay only: updates Equipment.files on nodes that already exist (SAP dump must have run).
-- Annotations often target child tags; walk Tag.parentTag so documents on descendants
-- still link to the equipment's asset tag (sample hierarchy is at most 4 levels deep).
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

, tags AS (
  SELECT
      cast(`key` AS STRING)                       AS tag_key
    , nullif(trim(cast(parentTag AS STRING)), '') AS parent_key
  FROM `{{ raw_database_sap }}`.`tag`
)

, annotated_tags AS (
  SELECT
      cast(startNodeExternalId AS STRING) AS document_key
    , cast(endNodeExternalId AS STRING)   AS annotated_tag
  FROM `{{ raw_database_sharepoint }}`.`diagram_annotation`
  WHERE lower(cast(instanceType AS STRING)) = 'edge'
    AND cast(`type` AS STRING) = 'diagrams.AssetLink'
)

-- Depth 0: the annotated tag itself; depths 1–4: successive parentTag ancestors.
, annotated_match_keys AS (
  SELECT document_key, annotated_tag AS match_key
  FROM annotated_tags

  UNION

  SELECT a.document_key, t0.parent_key AS match_key
  FROM annotated_tags AS a
  INNER JOIN tags AS t0
    ON a.annotated_tag = t0.tag_key
  WHERE t0.parent_key IS NOT NULL

  UNION

  SELECT a.document_key, t1.parent_key AS match_key
  FROM annotated_tags AS a
  INNER JOIN tags AS t0
    ON a.annotated_tag = t0.tag_key
  INNER JOIN tags AS t1
    ON t0.parent_key = t1.tag_key
  WHERE t1.parent_key IS NOT NULL

  UNION

  SELECT a.document_key, t2.parent_key AS match_key
  FROM annotated_tags AS a
  INNER JOIN tags AS t0
    ON a.annotated_tag = t0.tag_key
  INNER JOIN tags AS t1
    ON t0.parent_key = t1.tag_key
  INNER JOIN tags AS t2
    ON t1.parent_key = t2.tag_key
  WHERE t2.parent_key IS NOT NULL

  UNION

  SELECT a.document_key, t3.parent_key AS match_key
  FROM annotated_tags AS a
  INNER JOIN tags AS t0
    ON a.annotated_tag = t0.tag_key
  INNER JOIN tags AS t1
    ON t0.parent_key = t1.tag_key
  INNER JOIN tags AS t2
    ON t1.parent_key = t2.tag_key
  INNER JOIN tags AS t3
    ON t2.parent_key = t3.tag_key
  WHERE t3.parent_key IS NOT NULL
)

SELECT
    e.equipment_key                                                    AS externalId
  , collect_set(node_reference('{{ instance_space }}', m.document_key)) AS files
FROM equipment AS e
INNER JOIN existing_equipment AS x
  ON e.equipment_key = x.equipment_key
INNER JOIN annotated_match_keys AS m
  ON e.asset_key = m.match_key
INNER JOIN documents AS d
  ON m.document_key = d.document_key
WHERE e.equipment_key IS NOT NULL
GROUP BY e.equipment_key
