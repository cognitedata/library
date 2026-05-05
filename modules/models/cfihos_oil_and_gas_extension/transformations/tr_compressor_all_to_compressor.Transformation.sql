select
  cast(`key` as string) as externalId,
  cast(`name` AS STRING) AS name,
  cast(`description` AS STRING) AS description,
  cast(`approvalAuthority` AS STRING) AS approvalAuthority,
  cast(`equipmentIdentifier` as string) as equipmentIdentifier,
  cast(`manufacturer` as string) as manufacturer,
  cast(`modelNumber` as string) as modelNumber,
  cast(`serialNumber` as string) as serialNumber,
  cast(`directionOfRotation` as string) as directionOfRotation,
  cast(`fluidName` as string) as fluidName,
  cast(`atexEquipmentGroupCode` as string) as atexEquipmentGroupCode,
  cast(`designPressureMax` as float) as designPressureMax,
  cast(`designPressureMin` as float) as designPressureMin,
  cast(`compressibilityFactorZAtInlet` as float) as compressibilityFactorZAtInlet,
  cast(`normalOperatingMassFlowRate` as float) as normalOperatingMassFlowRate,
  cast(`powerOutputRated` as float) as powerOutputRated,
  cast(`upperLimitAllowableSpeed` as float) as upperLimitAllowableSpeed,
  cast(`normalOperatingDischargePressure` as float) as normalOperatingDischargePressure,
  cast(`normalOperatingDischargeTemperature` as float) as normalOperatingDischargeTemperature,
  cast(`normalOperatingSuctionPressure` as float) as normalOperatingSuctionPressure,
  cast(`normalOperatingSuctionTemperature` as float) as normalOperatingSuctionTemperature,
  cast(`weightDry` as float) as weightDry,
  cast(`weightOperating` as float) as weightOperating
from
  `cfihos_oil_and_gas`.`compressor`
where
  -- full reload: is_new('compressor', lastUpdatedTime) and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
