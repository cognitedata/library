select
  key as externalId,
  key as area_id,
  name as area_name,
  node_reference('{{ isaInstanceSpace }}', parent_externalId) as site
  from `ISA_Manufacturing`.`isa_asset`
where 
	asset_specific = 'Area'