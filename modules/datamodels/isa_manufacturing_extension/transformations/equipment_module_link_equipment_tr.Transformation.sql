with equip_mod as (
  select
    key as equip_mod_ext_id,
    key as equip_mod_id,
    name as equip_mod_name
  from `ISA_Manufacturing`.`isa_asset`
  where 
    asset_specific = 'EquipmentModule'
),
equip AS (
  SELECT
  	key as equip_ext_id,
  	asset_externalId
  FROM `ISA_Manufacturing`.`isa_equipment`
)
select
  equip_mod.equip_mod_ext_id as externalId,
  equip_mod.equip_mod_id as equipment_module_id,
  equip_mod.equip_mod_name as equipment_module_name,
  collect_set(node_reference('{{ isaInstanceSpace }}', equip_ext_id)) as equipment
from equip_mod left join equip 
on equip_mod.equip_mod_ext_id=equip.asset_externalId
group by 
  equip_mod.equip_mod_ext_id,
  equip_mod.equip_mod_id,
  equip_mod.equip_mod_name