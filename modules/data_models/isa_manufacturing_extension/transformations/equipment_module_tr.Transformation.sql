select
  key as externalId,
  key as equipment_module_id,
  name as equipment_module_name,
  node_reference('{{ isaInstanceSpace }}', parent_externalId) as unit
from `ISA_Manufacturing`.`isa_asset`
where 
  asset_specific = 'EquipmentModule'

