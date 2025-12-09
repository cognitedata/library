select
  key as externalId,
  key as enterprise_id,
  name as enterprise_name
from `ISA_Manufacturing`.`isa_asset`
where 
  asset_specific = 'Enterprise'

