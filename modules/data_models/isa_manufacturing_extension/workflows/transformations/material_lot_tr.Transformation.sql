SELECT
  CAST(key AS string) AS externalId,
  CAST(material_lot_name AS string) AS name,
  CAST(description AS string) AS description,
  CAST(material_lot_id AS string) AS material_lot_id,
  CAST(lot_number AS string) AS lot_number,
  TO_TIMESTAMP(manufacture_date, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS manufacture_date,
  TO_TIMESTAMP(expiry_date, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS expiry_date,
  CASE
    WHEN material_externalId IS NULL OR material_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', CAST(material_externalId AS string))
  END AS material
FROM `ISA_Manufacturing`.`isa_material_lot`
