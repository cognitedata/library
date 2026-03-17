select
  key as externalId,
  key as unit_id,
  name as unit_name,
  node_reference('{{ isaInstanceSpace }}', parent_externalId) as process_cell
from `ISA_Manufacturing`.`isa_asset`
where 
  asset_specific = 'Unit'

