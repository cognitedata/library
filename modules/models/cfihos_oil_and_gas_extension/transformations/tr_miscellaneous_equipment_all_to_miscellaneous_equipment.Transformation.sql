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
  cast(`nominalDiameter` as float) as nominalDiameter,
  cast(`nominalPower` as float) as nominalPower,
  cast(`powerConsumption` as float) as powerConsumption,
  cast(`powerFactorCos` as float) as powerFactorCos,
  cast(`ratedCurrent` as float) as ratedCurrent,
  cast(`voltageLevel` as float) as voltageLevel
from
  `cfihos_oil_and_gas`.`miscellaneous_equipment`
where
  is_new('miscellaneous_equipment', lastUpdatedTime)
  and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
