select
  cast(`key` as string) as externalId,
  cast(`name` AS STRING) AS name,
  cast(`description` AS STRING) AS description,
  cast(`approvalAuthority` AS STRING) AS approvalAuthority,
  cast(`equipmentIdentifier` as string) as equipmentIdentifier,
  cast(`manufacturer` as string) as manufacturer,
  cast(`modelNumber` as string) as modelNumber,
  cast(`serialNumber` as string) as serialNumber,
  cast(`pumpType` as string) as pumpType,
  CASE
    WHEN `fluidName` IS NOT NULL AND trim(`fluidName`) != '' THEN cast(`fluidName` as string)
    WHEN `actualFluid` IS NOT NULL AND trim(`actualFluid`) != '' THEN cast(`actualFluid` as string)
    ELSE NULL
  END as actualFluid,
  CASE
    WHEN `fluidName` IS NOT NULL AND trim(`fluidName`) != '' THEN cast(`fluidName` as string)
    WHEN `actualFluid` IS NOT NULL AND trim(`actualFluid`) != '' THEN cast(`actualFluid` as string)
    ELSE NULL
  END as fluidName,
  cast(`liquidName` as string) as liquidName,
  cast(`orientation` as string) as orientation,
  cast(`driverType` as string) as driverType,
  cast(`operationContinuousIntermittent` as string) as operationContinuousIntermittent,
  CASE
    WHEN `erosiveLiquid` IS NULL OR trim(`erosiveLiquid`) = '' THEN NULL
    WHEN lower(trim(`erosiveLiquid`)) IN ('yes', 'true', 'y', '1') THEN true
    WHEN lower(trim(`erosiveLiquid`)) IN ('no', 'false', 'n', '0') THEN false
    ELSE NULL
  END as erosiveLiquid,
  CASE
    WHEN `corrosiveLiquid` IS NULL OR trim(`corrosiveLiquid`) = '' THEN NULL
    WHEN lower(trim(`corrosiveLiquid`)) IN ('yes', 'true', 'y', '1') THEN true
    WHEN lower(trim(`corrosiveLiquid`)) IN ('no', 'false', 'n', '0') THEN false
    ELSE NULL
  END as corrosiveLiquid,
  cast(`density` as float) as density,
  cast(`normalOperatingVolumeFlowRate` as float) as normalOperatingVolumeFlowRate,
  cast(`normalOperatingDifferentialPressure` as float) as normalOperatingDifferentialPressure,
  cast(`normalOperatingInletTemperature` as float) as normalOperatingInletTemperature,
  cast(`ratedVolumeFlowRate` as float) as ratedVolumeFlowRate,
  cast(`weightDry` as float) as weightDry,
  cast(`weightOperating` as float) as weightOperating
from
  `cfihos_oil_and_gas`.`pump`
where
  -- full reload: is_new('pump', lastUpdatedTime) and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
