WITH source AS (
  SELECT
      concat('file_', cast(name AS STRING))                       AS document_key
    , nullif(trim(cast(name AS STRING)), '')                      AS name
    , nullif(trim(cast(description AS STRING)), '')               AS description
    , nullif(trim(cast(documentNumber AS STRING)), '')            AS document_number
    , nullif(trim(cast(disciplineCode AS STRING)), '')            AS discipline_code
    , nullif(trim(cast(currentRevision AS STRING)), '')           AS revision
    , nullif(trim(cast(documentTypeCode AS STRING)), '')          AS category_code
    , nullif(trim(cast(facility AS STRING)), '')                  AS site
    , nullif(trim(cast(area AS STRING)), '')                      AS area
    , nullif(trim(cast(system AS STRING)), '')                    AS system
    , nullif(trim(cast(mimeType AS STRING)), '')                  AS mime_type
    , nullif(trim(cast(asset_externalId AS STRING)), '')          AS asset_key
    , nullif(trim(cast(sourceId AS STRING)), '')                  AS source_id
    , nullif(trim(cast(source AS STRING)), '')                    AS source_context
    , CASE
        WHEN nullif(trim(cast(issueDate AS STRING)), '') IS NULL THEN NULL
        ELSE to_timestamp(cast(issueDate AS STRING), 'M/d/yyyy')
      END                                                         AS issue_date
  FROM `{{ raw_database_sharepoint }}`.`file`
  WHERE nullif(trim(cast(name AS STRING)), '') IS NOT NULL
)

, annotated_tags AS (
  SELECT
      cast(startNodeExternalId AS STRING) AS document_key
    , cast(endNodeExternalId AS STRING)   AS asset_key
  FROM `{{ raw_database_sharepoint }}`.`diagram_annotation`
  WHERE lower(cast(instanceType AS STRING)) = 'edge'
    AND cast(`type` AS STRING) = 'diagrams.AssetLink'
    AND nullif(trim(cast(endNodeExternalId AS STRING)), '') IS NOT NULL
)

, document_assets AS (
  SELECT
      document_key
    , collect_set(node_reference('{{ instance_space }}', asset_key)) AS assets
  FROM (
    SELECT document_key, asset_key FROM source WHERE asset_key IS NOT NULL
    UNION
    SELECT document_key, asset_key FROM annotated_tags
  ) AS links
  GROUP BY document_key
)

SELECT
    s.document_key                                       AS externalId
  , s.name                                               AS name
  , s.description                                        AS description
  , s.document_number                                    AS documentNumber
  , s.discipline_code                                    AS disciplineCode
  , s.revision                                           AS revision
  , CASE
      WHEN s.category_code IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', s.category_code)
    END                                                  AS category
  , s.site                                               AS site
  , s.area                                               AS area
  , s.system                                             AS system
  , s.mime_type                                          AS mimeType
  , array('ToAnnotate', 'DetectInDiagrams')              AS tags
  , array(regexp_replace(s.name, '(?i)\\.pdf$', ''))     AS aliases
  , true                                                 AS isUploaded
  , s.issue_date                                         AS uploadedTime
  , d.assets                                             AS assets
  , s.source_id                                          AS sourceId
  , s.source_context                                     AS sourceContext
FROM source AS s
LEFT JOIN document_assets AS d
  ON s.document_key = d.document_key
