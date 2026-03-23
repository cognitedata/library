select
  cast(`key` as string) as externalId,
  cast(`name` AS STRING) AS name,
  cast(`description` AS STRING) AS description,
  cast(`approvalAuthority` AS STRING) AS approvalAuthority,
  cast(`equipmentIdentifier` as string) as equipmentIdentifier,
  cast(`manufacturer` as string) as manufacturer,
  cast(`modelNumber` as string) as modelNumber,
  cast(`serialNumber` as string) as serialNumber,
  cast(`safetyIntegrityLevel` as string) as safetyIntegrityLevel,
  cast(`testMedium` as string) as testMedium,
  cast(`thermalInsulationClass` as string) as thermalInsulationClass,
  cast(`normalOutputPower` as float) as normalOutputPower,
  cast(`testPressure` as float) as testPressure,
  cast(`upperLimitDesignPressure` as float) as upperLimitDesignPressure,
  cast(`upperLimitDesignTemperature` as float) as upperLimitDesignTemperature,
  cast(`lowerLimitOperatingTemperature` as float) as lowerLimitOperatingTemperature,
  cast(`upperLimitOperatingTemperature` as float) as upperLimitOperatingTemperature,
  cast(`weightDry` as float) as weightDry,
  cast(`weightOperating` as float) as weightOperating
from
  `cfihos_oil_and_gas`.`turbine`
where
  is_new('turbine', lastUpdatedTime)
  and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
