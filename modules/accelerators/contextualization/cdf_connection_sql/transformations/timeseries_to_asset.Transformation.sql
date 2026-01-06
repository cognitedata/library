--
-- Update PI data and link to assets based on matches in asset name with TS sysTagsFound
--
SELECT
  dm_timeseries.externalId,
  dm_timeseries.name,
  dm_timeseries.type,
  dm_timeseries.isStep,
  
  -- Asset References: Aggregate multiple asset matches into an array, limited to 1200 elements
  slice(
    array_agg(CASE
      WHEN asset.externalId IS NOT NULL AND asset.externalId != ''
      THEN node_reference('{{ instanceSpace }}', asset.externalId)
      ELSE NULL
    END), 1, 1200
  ) AS assets,
  
  -- Sort sysTagsFound array
  ARRAY_SORT(dm_timeseries.sysTagsFound) AS sysTagsFound,
  
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
    "{{ organization }}TimeSeries"
  ) dm_timeseries

LEFT JOIN
  cdf_data_models(
    "sp_enterprise_process_industry",
    "{{ organization }}ProcessIndustries",
    "{{ datamodelVersion }}",
    "{{ organization }}Asset"
  ) asset
ON
  array_contains(dm_timeseries.sysTagsFound, asset.name)

GROUP BY 
  dm_timeseries.externalId,
  dm_timeseries.name,
  dm_timeseries.type,
  dm_timeseries.isStep,
  dm_timeseries.sysTagsFound;
