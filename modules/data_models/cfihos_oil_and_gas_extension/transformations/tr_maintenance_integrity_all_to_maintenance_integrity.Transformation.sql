SELECT
  cast(key as string) as externalId,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(flocFunctionalLocation as string) as flocFunctionalLocation,
  cast(flocCategory as string) as flocCategory,
  cast(flocCriticalityCombined as string) as flocCriticalityCombined,
  cast(flocRCMCriticality as string) as flocRCMCriticality,
  cast(flocBarrierFunction as string) as flocBarrierFunction,
  cast(flocMainWorkCenter as string) as flocMainWorkCenter,
  cast(flocDisciplineType as string) as flocDisciplineType,
  cast(flocInspCritCost as string) as flocInspCritCost,
  cast(flocInspCritConsequence as string) as flocInspCritConsequence,
  cast(flocInspCritProbability as string) as flocInspCritProbability,
  CASE WHEN lower(isDeleted) = 'true' THEN true ELSE false END AS isDeleted
FROM `cfihos_oil_and_gas`.`maintenance_integrity`
WHERE -- full reload: is_new('maintenance_integrity', lastUpdatedTime)
  `key` IN (SELECT `key` FROM `cfihos_oil_and_gas`.`tag`)
