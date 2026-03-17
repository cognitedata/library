SELECT
  -- Select the unique identifier to match existing instances (required for upsert)
  externalId,
  -- Conditional update for the 'tags' property
  CASE 
    WHEN tags IS NULL THEN ARRAY('PID')
    WHEN ARRAY_CONTAINS(tags, 'PID') THEN tags
    ELSE ARRAY_UNION(tags, ARRAY('PID'))
  END as tags
FROM
  cdf_data_models(
    "sp_enterprise_process_industry",
    "YourOrgProcessIndustries",
    "v1",
    "YourOrgFile"
  );