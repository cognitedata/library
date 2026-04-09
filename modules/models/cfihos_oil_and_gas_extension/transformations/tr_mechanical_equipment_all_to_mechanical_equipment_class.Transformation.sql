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
  cast(`lineServiceCode` as string) as lineServiceCode,
  cast(`insulationClass` as string) as insulationClass,
  CASE
    WHEN `fireSafe` IS NULL OR trim(`fireSafe`) = '' THEN NULL
    WHEN lower(trim(`fireSafe`)) IN ('yes', 'true', 'y', '1') THEN true
    WHEN lower(trim(`fireSafe`)) IN ('no', 'false', 'n', '0') THEN false
    ELSE NULL
  END as fireSafe,
  cast(`fluidName` as string) as fluidName,
  cast(`nominalDiameter` as float) as nominalDiameter,
  cast(`ratedSpeedAt11Load` as float) as ratedSpeedAt11Load,
  cast(`operationContinuousIntermittent` as string) as operationContinuousIntermittent
from
  `cfihos_oil_and_gas`.`mechanical_equipment`
where
  is_new('mechanical_equipment', lastUpdatedTime)
  and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
