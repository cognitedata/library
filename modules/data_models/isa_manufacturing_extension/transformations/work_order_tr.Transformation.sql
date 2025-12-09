WITH base AS (
  SELECT
    CAST(work_order_id AS string) AS work_order_external_id,
    CAST(work_order_id AS string) AS work_order_id,
    CAST(work_order_number AS string) AS work_order_number,
    CAST(work_order_name AS string) AS work_order_name,
    CAST(description AS string) AS description,
    CAST(work_type AS string) AS work_type,
    CAST(work_status AS string) AS work_status,
    asset_externalId,
    to_timestamp(planned_start_time, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS planned_start_time_ts,
    to_timestamp(planned_end_time, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS planned_end_time_ts,
    to_timestamp(actual_start_time, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS actual_start_time_ts,
    to_timestamp(actual_end_time, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS actual_end_time_ts,
    CASE
      WHEN equipment_externalId IS NULL OR equipment_externalId = '' THEN NULL
      ELSE node_reference('{{ isaInstanceSpace }}', CAST(equipment_externalId AS string))
    END AS assigned_equipment_ref,
    CASE
      WHEN asset_externalId IS NULL OR asset_externalId = '' THEN NULL
      ELSE node_reference('{{ isaInstanceSpace }}', CAST(asset_externalId AS string))
    END AS asset_ref,
    CASE
      WHEN assigned_personnel_externalId IS NULL OR assigned_personnel_externalId = '' THEN NULL
      ELSE node_reference('{{ isaInstanceSpace }}', CAST(assigned_personnel_externalId AS string))
    END AS assigned_personnel_ref
  FROM `ISA_Manufacturing`.`isa_work_order`
)

SELECT
  work_order_external_id AS externalId,
  FIRST(work_order_id, true) AS work_order_id,
  FIRST(work_order_number, true) AS work_order_number,
  FIRST(work_order_name, true) AS name,
  FIRST(description, true) AS description,
  FIRST(work_type, true) AS work_type,
  FIRST(work_status, true) AS work_status,
  FIRST(planned_start_time_ts, true) AS planned_start_time,
  FIRST(planned_end_time_ts, true) AS planned_end_time,
  FIRST(actual_start_time_ts, true) AS actual_start_time,
  FIRST(actual_end_time_ts, true) AS actual_end_time,
  FIRST(actual_start_time_ts, true) AS startTime,
  FIRST(actual_end_time_ts, true) AS endTime,
  FIRST(planned_start_time_ts, true) AS scheduledStartTime,
  FIRST(planned_end_time_ts, true) AS scheduledEndTime,
  FILTER(COLLECT_SET(assigned_equipment_ref), x -> x IS NOT NULL) AS equipment,
  FILTER(COLLECT_SET(asset_ref), x -> x IS NOT NULL) AS assets,
  FILTER(COLLECT_SET(assigned_personnel_ref), x -> x IS NOT NULL) AS assigned_personnel
FROM base
GROUP BY work_order_external_id
