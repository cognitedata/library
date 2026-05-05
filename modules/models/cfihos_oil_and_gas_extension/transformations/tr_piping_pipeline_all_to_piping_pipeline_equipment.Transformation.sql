select
  cast(`key` as string) as externalId,
  cast(`name` AS STRING) AS name,
  cast(`description` AS STRING) AS description,
  cast(`approvalAuthority` AS STRING) AS approvalAuthority,
  cast(`equipmentIdentifier` as string) as equipmentIdentifier,
  cast(`manufacturer` as string) as manufacturer,
  cast(`modelNumber` as string) as modelNumber,
  cast(`serialNumber` as string) as serialNumber,
  CASE
    WHEN `pipingClass` IS NOT NULL AND trim(`pipingClass`) != '' THEN cast(`pipingClass` as string)
    WHEN `pipingSpecificationClass` IS NOT NULL AND trim(`pipingSpecificationClass`) != '' THEN cast(`pipingSpecificationClass` as string)
    ELSE NULL
  END as pipingSpecificationClass,
  cast(`insulationClass` as string) as insulationClass,
  cast(`fluidName` as string) as fluidName,
  cast(`phase` as string) as phase,
  CASE
    WHEN `pipingClass` IS NOT NULL AND trim(`pipingClass`) != '' THEN cast(`pipingClass` as string)
    WHEN `pipingSpecificationClass` IS NOT NULL AND trim(`pipingSpecificationClass`) != '' THEN cast(`pipingSpecificationClass` as string)
    ELSE NULL
  END as pipingClass,
  cast(`serviceCode` as string) as serviceCode,
  cast(`ndtTestClass` as string) as ndtTestClass,
  cast(`sourService` as string) as sourService,
  cast(`insulationThickness` as float) as insulationThickness,
  cast(`nominalDiameter` as float) as nominalDiameter,
  cast(`wallThickness` as float) as wallThickness,
  cast(`operatingPressureNormal` as float) as operatingPressureNormal,
  cast(`operatingTemperatureMax` as float) as operatingTemperatureMax,
  cast(`operatingTemperatureMin` as float) as operatingTemperatureMin,
  cast(`corrosionAllowance` as float) as corrosionAllowance,
  cast(`weightDry` as float) as weightDry,
  cast(`weightOperating` as float) as weightOperating
from
  `cfihos_oil_and_gas`.`piping_pipeline`
where
  -- full reload: is_new('piping_pipeline', lastUpdatedTime) and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
