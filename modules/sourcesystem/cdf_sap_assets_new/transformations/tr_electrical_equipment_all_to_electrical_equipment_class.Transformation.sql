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
  cast(`cableType` as string) as cableType,
  cast(`crossSectionalArea` as float) as crossSectionalArea,
  cast(`iOType` as string) as iOType,
  cast(`nominalPower` as float) as nominalPower,
  cast(`powerFactorCos` as float) as powerFactorCos,
  cast(`ratedCurrent` as float) as ratedCurrent,
  cast(`setPoint` as float) as setPoint,
  cast(`signalType` as string) as signalType,
  cast(`voltageLevel` as float) as voltageLevel
from
  `cfihos_oil_and_gas`.`electrical_equipment`
where
  -- full reload: is_new('electrical_equipment', lastUpdatedTime) and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
