SELECT
  cast(key as string) AS externalId,
  cast(personnel_id as string) AS personnel_id,
  cast(name as string) AS name,
  cast(role as string) AS role,
  CASE
    WHEN site_externalId IS NULL OR site_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', cast(site_externalId as string))
  END AS site
FROM `ISA_Manufacturing`.`isa_personnel`

