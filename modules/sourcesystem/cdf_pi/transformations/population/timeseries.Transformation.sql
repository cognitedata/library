select 
  externalId as externalId,
  externalId as sourceId,
  name as name,
  description as description,
  'numeric' as type,
  false as isStep,
  if(try_get_unit(`unit`) IS NOT NULL, node_reference('cdf_cdm_units', try_get_unit(`unit`)), NULL) as unit,
  `unit` as sourceUnit,
  'Time Series' as sourceContext,

  -- sysTagsFound extraction
  case 
    when name like '%-%' and name like '%:%' then 
         array(
           regexp_extract(
             regexp_replace(name, '^[A-Z]+_', ''),  -- strip prefix VAL_/AL_/etc
             '^[^:_]+',                             -- take substring before first ":" or "_"
             0
           )
         )
    else array()
  end as sysTagsFound

from `{{ rawSourceDatabase }}`.`timeseries_metadata`
