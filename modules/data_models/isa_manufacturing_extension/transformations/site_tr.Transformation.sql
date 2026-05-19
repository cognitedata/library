select
  key as externalId,
  key as site_id,
  name as site_name,
  node_reference('{{ isaInstanceSpace }}', parent_externalId) as enterprise
from `ISA_Manufacturing`.`isa_asset`
where 
  asset_specific = 'Site'

