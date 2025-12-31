--
-- Update Maintenance Orders and link to assets based on matches in asset name with sysTagsFound
--
SELECT
  dm_maintenanceorders.externalId,
  dm_maintenanceorders.name,
  dm_maintenanceorders.description,
  
  -- Asset References: Aggregate multiple asset matches into an array, limited to 1200 elements
  slice(
    array_agg(CASE
      WHEN asset.externalId IS NOT NULL AND asset.externalId != ''
      THEN node_reference('{{ instanceSpace }}', asset.externalId)
      ELSE NULL
    END), 1, 1200
  ) AS assets,
  
  -- Sort sysTagsFound array
  ARRAY_SORT(dm_maintenanceorders.sysTagsFound) AS sysTagsFound,
  
  -- Populate and sort sysTagsLinked with the asset.name value for each matching asset tag
  ARRAY_SORT(
    COLLECT_SET(
      CASE 
        WHEN asset.name IS NOT NULL AND asset.name != 'NULL' THEN asset.name
        ELSE NULL
      END
    )
  ) AS sysTagsLinked

FROM
  cdf_data_models(
    "sp_enterprise_process_industry",
    "{{ organization }}ProcessIndustries",
    "{{ datamodelVersion }}",
    "{{ organization }}MaintenanceOrder"
  ) dm_maintenanceorders

LEFT JOIN
  cdf_data_models(
    "sp_enterprise_process_industry",
    "{{ organization }}ProcessIndustries",
    "{{ datamodelVersion }}",
    "{{ organization }}Asset"
  ) asset
ON
  array_contains(dm_maintenanceorders.sysTagsFound, asset.name)

GROUP BY 
  dm_maintenanceorders.externalId,
  dm_maintenanceorders.name,
  dm_maintenanceorders.description,
  dm_maintenanceorders.sysTagsFound;