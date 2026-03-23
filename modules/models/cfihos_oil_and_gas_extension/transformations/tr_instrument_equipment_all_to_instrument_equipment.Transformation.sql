select
  cast(`key` as string) as externalId,
  cast(`name` AS STRING) AS name,
  cast(`description` AS STRING) AS description,
  cast(`approvalAuthority` AS STRING) AS approvalAuthority,
  cast(`equipmentIdentifier` as string) as equipmentIdentifier,
  cast(`manufacturer` as string) as manufacturer,
  cast(`modelNumber` as string) as modelNumber,
  cast(`serialNumber` as string) as serialNumber,
  cast(`iOType` as string) as iOType,
  cast(`signalType` as string) as signalType,
  cast(`fluidName` as string) as fluidName,
  cast(`pressureEquipmentCategory` as string) as pressureEquipmentCategory,
  cast(`safetyIntegrityLevel` as string) as safetyIntegrityLevel,
  cast(`setPoint` as float) as setPoint,
  cast(`setPointHigh` as float) as setPointHigh,
  cast(`setPointLow` as float) as setPointLow,
  cast(`efficiency` as float) as efficiency,
  cast(`weightDry` as float) as weightDry,
  cast(`weightOperating` as float) as weightOperating
from
  `cfihos_oil_and_gas`.`instrument_equipment`
where
  is_new('instrument_equipment', lastUpdatedTime)
  and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
