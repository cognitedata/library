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
  cast(`fireSafe` as boolean) as fireSafe,
  cast(`thermalInsulationClass` as string) as thermalInsulationClass
from
  `cfihos_oil_and_gas`.`infrastructure`
where
  -- full reload: is_new('infrastructure', lastUpdatedTime) and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
