SELECT
  cast(`key` AS STRING) AS externalId,
  cast(`name` AS STRING) AS name,
  cast(`description` AS STRING) AS description,
  cast(`approvalAuthority` AS STRING) AS approvalAuthority,
  cast(`equipmentIdentifier` AS STRING) AS equipmentIdentifier,
  cast(`manufacturer` AS STRING) AS manufacturer,
  cast(`modelNumber` AS STRING) AS modelNumber,
  cast(`serialNumber` AS STRING) AS serialNumber,
  cast(`weightDry` AS FLOAT) AS weightDry,
  cast(`weightOperating` AS FLOAT) AS weightOperating,
  cast(`iOType` AS STRING) AS iOType,
  cast(`signalType` AS STRING) AS signalType
FROM
  `cfihos_oil_and_gas`.`hse_equipment`
WHERE
  is_new('hse_equipment', lastUpdatedTime)
  AND
  `key` IN (SELECT `key` FROM `cfihos_oil_and_gas`.`tag`)
