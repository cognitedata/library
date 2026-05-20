-- SAP Functional Locations → ISAAsset
-- Source: RAW table db_{{location}}_sap.functional_location written by the SAP OData Extractor
--         from the FunclocListSet OData entity (filtered to plant {{sapPlant}}).
-- Column names are SAP OData property names from ZGW_FUNCLOC_SRV.
-- Verify field names against your SAP NW Gateway service definition.

WITH parent_lookup AS (
  SELECT
    cast(`Functlocation` AS STRING) AS externalId,
    node_reference(
      '{{instanceSpace}}',
      cast(`Supfloc` AS STRING)
    ) AS parent
  FROM `db_{{location}}_sap`.`functional_location`
  WHERE isnotnull(`Supfloc`)
    AND cast(`Supfloc` AS STRING) != ''
)
SELECT
  cast(fl.`Functlocation` AS STRING)  AS externalId,
  cast(fl.`Functlocation` AS STRING)  AS sourceId,
  cast(fl.`Descript`      AS STRING)  AS name,
  cast(fl.`Descript`      AS STRING)  AS description,
  pl.parent
FROM `db_{{location}}_sap`.`functional_location` fl
LEFT JOIN parent_lookup pl
  ON cast(fl.`Functlocation` AS STRING) = pl.externalId
WHERE isnotnull(fl.`Functlocation`)
  AND cast(fl.`Functlocation` AS STRING) != ''
