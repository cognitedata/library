select
  cast(`key` as string) as externalId,
  cast(`name` AS STRING) AS name,
  cast(`description` AS STRING) AS description,
  cast(`approvalAuthority` AS STRING) AS approvalAuthority,
  cast(`equipmentIdentifier` as string) as equipmentIdentifier,
  cast(`manufacturer` as string) as manufacturer,
  cast(`modelNumber` as string) as modelNumber,
  cast(`serialNumber` as string) as serialNumber,
  cast(`valveRating` as string) as valveRating,
  cast(`valveType` as string) as valveType,
  CASE
    WHEN `fireSafe` IS NULL OR trim(`fireSafe`) = '' THEN NULL
    WHEN lower(trim(`fireSafe`)) IN ('yes', 'true', 'y', '1') THEN true
    WHEN lower(trim(`fireSafe`)) IN ('no', 'false', 'n', '0') THEN false
    ELSE NULL
  END as fireSafe,
  cast(`fluidName` as string) as fluidName,
  cast(`fluidPhase` as string) as fluidPhase,
  cast(`pipingClass` as string) as pipingClass,
  cast(`pressureEquipmentCategory` as string) as pressureEquipmentCategory,
  cast(`safetyIntegrityLevel` as string) as safetyIntegrityLevel,
  cast(`testMedium` as string) as testMedium,
  cast(`outputSignalType` as string) as outputSignalType,
  cast(`valveSize` as string) as valveSize,
  cast(`nominalDiameter` as float) as nominalDiameter,
  cast(`testPressure` as float) as testPressure,
  cast(`coldSetPressure` as float) as coldSetPressure,
  cast(`backPressure` as float) as backPressure,
  cast(`weightDry` as float) as weightDry,
  cast(`weightOperating` as float) as weightOperating
from
  `cfihos_oil_and_gas`.`valve`
where
  -- full reload: is_new('valve', lastUpdatedTime) and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
