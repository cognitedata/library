SELECT
  CAST(batch_id AS string) AS externalId,
  CAST(batch_id AS string) AS batch_id,
  CAST(batch_number AS string) AS batch_number,
  CAST(batch_state AS string) AS batch_state,
  CAST(batch_size AS double) AS batch_size,
  CAST(batch_size_unit AS string) AS batch_size_unit,
  TO_TIMESTAMP(start_time, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS startTime,
  TO_TIMESTAMP(end_time, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS endTime,
  TO_TIMESTAMP(start_time, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS scheduledStartTime,
  TO_TIMESTAMP(end_time, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS scheduledEndTime,
  CASE
    WHEN recipe_externalId IS NULL OR recipe_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', CAST(recipe_externalId AS string))
  END AS recipe,
  CASE
    WHEN site_externalId IS NULL OR site_externalId = '' THEN NULL
    ELSE node_reference('{{ isaInstanceSpace }}', CAST(site_externalId AS string))
  END AS site,
  CASE
    WHEN primary_work_order_externalId IS NULL OR primary_work_order_externalId = '' THEN NULL
    ELSE ARRAY(node_reference('{{ isaInstanceSpace }}', CAST(primary_work_order_externalId AS string)))
  END AS work_orders
FROM `ISA_Manufacturing`.`isa_batch`
