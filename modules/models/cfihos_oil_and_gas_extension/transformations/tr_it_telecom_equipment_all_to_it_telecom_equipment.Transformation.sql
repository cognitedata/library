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
  CASE
    WHEN `loudspeakerTappings` IS NULL OR trim(`loudspeakerTappings`) = '' OR lower(trim(`loudspeakerTappings`)) = 'n/a' THEN NULL
    WHEN regexp_extract(lower(trim(`loudspeakerTappings`)), '^([0-9]+(?:\\.[0-9]+)?)\\s*[x*]\\s*([0-9]+(?:\\.[0-9]+)?)', 1) != ''
      AND regexp_extract(lower(trim(`loudspeakerTappings`)), '^([0-9]+(?:\\.[0-9]+)?)\\s*[x*]\\s*([0-9]+(?:\\.[0-9]+)?)', 2) != ''
      THEN cast(regexp_extract(lower(trim(`loudspeakerTappings`)), '^([0-9]+(?:\\.[0-9]+)?)\\s*[x*]\\s*([0-9]+(?:\\.[0-9]+)?)', 1) as float)
         * cast(regexp_extract(lower(trim(`loudspeakerTappings`)), '^([0-9]+(?:\\.[0-9]+)?)\\s*[x*]\\s*([0-9]+(?:\\.[0-9]+)?)', 2) as float)
    WHEN regexp_extract(`loudspeakerTappings`, '([0-9]+(?:\\.[0-9]+)?)', 1) != ''
      THEN cast(regexp_extract(`loudspeakerTappings`, '([0-9]+(?:\\.[0-9]+)?)', 1) as float)
    ELSE NULL
  END as loudspeakerTappings
from
  `cfihos_oil_and_gas`.`it_telecom_equipment`
where
  -- full reload: is_new('it_telecom_equipment', lastUpdatedTime) and
  `key` in (select `key` from `cfihos_oil_and_gas`.`tag`)
