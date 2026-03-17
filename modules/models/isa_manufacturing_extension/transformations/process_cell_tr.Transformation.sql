select
  key as externalId,
  key as process_cell_id,
  name as process_cell_name,
  node_reference('{{ isaInstanceSpace }}', parent_externalId) as area
from `ISA_Manufacturing`.`isa_asset`
where 
  asset_specific = 'ProcessCell'

