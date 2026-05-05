SELECT
  cast(woo.externalId as string) as externalId,
  CASE
    WHEN max(trim(cast(raw_woo.`mainAsset_externalId` as string))) IS NULL
      OR max(trim(cast(raw_woo.`mainAsset_externalId` as string))) = '' THEN NULL
    ELSE node_reference('{{ instance_space }}', max(trim(cast(raw_woo.`mainAsset_externalId` as string))))
  END as mainAsset,
  CASE
    WHEN max(trim(cast(raw_woo.`mainAsset_externalId` as string))) IS NULL
      OR max(trim(cast(raw_woo.`mainAsset_externalId` as string))) = '' THEN NULL
    ELSE array(node_reference('{{ instance_space }}', max(trim(cast(raw_woo.`mainAsset_externalId` as string)))))
  END as assets,
  CASE
    WHEN max(trim(cast(raw_woo.`maintenanceOrder_externalId` as string))) IS NOT NULL
      AND max(trim(cast(raw_woo.`maintenanceOrder_externalId` as string))) != '' THEN
      node_reference('{{ instance_space }}', max(trim(cast(raw_woo.`maintenanceOrder_externalId` as string))))
    WHEN max(trim(cast(raw_wo.`key` as string))) IS NOT NULL
      AND max(trim(cast(raw_wo.`key` as string))) != '' THEN
      node_reference('{{ instance_space }}', max(trim(cast(raw_wo.`key` as string))))
    ELSE NULL
  END as maintenanceOrder,
  CASE
    WHEN max(CASE WHEN raw_eq.`key` IS NOT NULL AND trim(cast(raw_eq.`key` as string)) != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE collect_set(node_reference('{{ instance_space }}', trim(cast(raw_eq.`key` as string))))
  END as equipment,
  CASE
    WHEN max(CASE WHEN raw_ts.`key` IS NOT NULL AND trim(cast(raw_ts.`key` as string)) != '' THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE collect_set(node_reference('{{ instance_space }}', trim(cast(raw_ts.`key` as string))))
  END as timeSeries,
  CASE
    WHEN max(flm.fl_external_id) IS NULL OR trim(cast(max(flm.fl_external_id) as string)) = '' THEN NULL
    ELSE node_reference('{{ instance_space }}', trim(cast(max(flm.fl_external_id) as string)))
  END as functionalLocation
FROM cdf_nodes('{{ space }}', 'WorkOrderOperation', '{{ dm_version }}') woo
INNER JOIN `cfihos_oil_and_gas`.`work_order_operation` raw_woo
  ON woo.space = '{{ instance_space }}'
  AND trim(cast(woo.externalId as string)) = trim(cast(raw_woo.`key` as string))
LEFT JOIN `cfihos_oil_and_gas`.`work_order` raw_wo
  ON trim(cast(raw_woo.`mainAsset_externalId` as string)) = trim(cast(raw_wo.`mainAsset_externalId` as string))
LEFT JOIN `cfihos_oil_and_gas`.`equipment` raw_eq
  ON trim(cast(raw_woo.`mainAsset_externalId` as string)) = trim(cast(raw_eq.`asset_externalId` as string))
LEFT JOIN `cfihos_oil_and_gas`.`timeseries` raw_ts
  ON trim(cast(raw_woo.`mainAsset_externalId` as string)) = trim(cast(raw_ts.`asset_externalId` as string))
LEFT JOIN (
  SELECT
    trim(cast(flocMainAsset as string)) as floc_main_asset,
    min(trim(cast(fl.`key` as string))) as fl_external_id
  FROM `cfihos_oil_and_gas`.`functional_location` fl
  INNER JOIN `cfihos_oil_and_gas`.`tag` tg
    ON trim(cast(fl.`key` as string)) = trim(cast(tg.`key` as string))
  WHERE fl.flocMainAsset IS NOT NULL
    AND trim(cast(flocMainAsset as string)) != ''
  GROUP BY trim(cast(flocMainAsset as string))
) flm
  ON trim(cast(raw_woo.`mainAsset_externalId` as string)) = flm.floc_main_asset
WHERE woo.space = '{{ instance_space }}'
GROUP BY woo.externalId
