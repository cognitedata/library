select
  cast(`key` as string) as externalId,
  cast(`name` AS STRING) AS name,
  cast(`description` AS STRING) AS description,
  cast(`approvalAuthority` AS STRING) AS approvalAuthority,
  cast(`equipmentIdentifier` as string) as equipmentIdentifier,
  cast(`manufacturer` as string) as manufacturer,
  cast(`modelNumber` as string) as modelNumber,
  cast(`serialNumber` as string) as serialNumber,
  cast(`coldSideFluidName` as string) as coldSideFluidName,
  cast(`hotSideFluidName` as string) as hotSideFluidName,
  cast(`fluidName` as string) as fluidName,
  cast(`coldSideUpperLimitDesignPressure` as float) as coldSideUpperLimitDesignPressure,
  cast(`hotSideUpperLimitDesignPressure` as float) as hotSideUpperLimitDesignPressure,
  cast(`shellSideUpperLimitOperatingTemperature` as float) as shellSideUpperLimitOperatingTemperature,
  cast(`tubeUpperLimitDesignPressure` as float) as tubeUpperLimitDesignPressure,
  cast(`weightDry` as float) as weightDry,
  cast(`weightOperating` as float) as weightOperating
from
  `cfihos_oil_and_gas`.`heat_exchanger`
where
  -- full reload: is_new('heat_exchanger', lastUpdatedTime) and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
