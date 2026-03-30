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
  cast(`fluidName` as string) as fluidName,
  cast(`fluidPhase` as string) as fluidPhase,
  cast(`normalOperatingPressure` as float) as normalOperatingPressure,
  cast(`upperLimitDesignPressure` as float) as upperLimitDesignPressure,
  cast(`upperLimitDesignTemperature` as float) as upperLimitDesignTemperature
from
  `cfihos_oil_and_gas`.`subsea_equipment`
where
  is_new('subsea_equipment', lastUpdatedTime)
  and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
