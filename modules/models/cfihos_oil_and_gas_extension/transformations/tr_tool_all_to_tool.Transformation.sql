select
  cast(`key` as string) as externalId,
  cast(`name` AS STRING) AS name,
  cast(`description` AS STRING) AS description,
  cast(`approvalAuthority` AS STRING) AS approvalAuthority,
  cast(`equipmentIdentifier` as string) as equipmentIdentifier,
  cast(`manufacturer` as string) as manufacturer,
  cast(`modelNumber` as string) as modelNumber,
  cast(`serialNumber` as string) as serialNumber,
  cast(`weightDry` as float) as weightDry,
  cast(`weightOperating` as float) as weightOperating,
  cast(`explosionProtectionGasGroup` as string) as explosionProtectionGasGroup,
  cast(`explosionProtectionZone` as string) as explosionProtectionZone,
  cast(`lowerLimitDesignTemperature` as float) as lowerLimitDesignTemperature,
  cast(`pressureEquipmentCategory` as string) as pressureEquipmentCategory,
  cast(`upperLimitDesignPressure` as float) as upperLimitDesignPressure,
  cast(`upperLimitDesignTemperature` as float) as upperLimitDesignTemperature
from
  `cfihos_oil_and_gas`.`tool`
where
  -- full reload: is_new('tool', lastUpdatedTime) and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
