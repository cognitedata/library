WITH source AS (
  SELECT
      cast(`key` AS STRING)                              AS tag_key
    , nullif(trim(cast(name AS STRING)), '')             AS name
    , nullif(trim(cast(description AS STRING)), '')      AS description
    , nullif(trim(cast(tagNumber AS STRING)), '')        AS tag_number
    , nullif(trim(cast(facility AS STRING)), '')         AS site
    , nullif(trim(cast(area AS STRING)), '')             AS area
    , nullif(trim(cast(system AS STRING)), '')           AS system
    , nullif(trim(cast(className AS STRING)), '')        AS tag_class
    , nullif(trim(cast(parentTag AS STRING)), '')        AS parent_key
    , nullif(trim(cast(labels AS STRING)), '')           AS source_label
  FROM `{{ raw_database_sap }}`.`tag`
)

SELECT
    tag_key                                              AS externalId
  , name                                                 AS name
  , description                                          AS description
  , site                                                 AS site
  , area                                                 AS area
  , system                                               AS system
  , tag_class                                            AS tagClass
  , CASE
      WHEN parent_key IS NULL THEN NULL
      ELSE node_reference('{{ instance_space }}', parent_key)
    END                                                  AS parent
  , array_distinct(
      filter(array(name, tag_number), x -> x IS NOT NULL)
    )                                                    AS aliases
  , CASE
      WHEN source_label IS NULL THEN array('DetectInDiagrams')
      ELSE array(source_label, 'DetectInDiagrams')
    END                                                  AS labels
  , tag_key                                              AS sourceId
FROM source
WHERE tag_key IS NOT NULL
