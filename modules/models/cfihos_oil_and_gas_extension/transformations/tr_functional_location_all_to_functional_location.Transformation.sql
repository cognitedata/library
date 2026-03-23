SELECT
  cast(key as string) as externalId,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(flocFunctionalLocation as string) as flocFunctionalLocation,
  cast(flocCategory as string) as flocCategory,
  cast(flocDisciplineType as string) as flocDisciplineType,
  cast(flocObjectType as string) as flocObjectType,
  cast(flocMainAsset as string) as flocMainAsset,
  cast(flocSuperiorFunctionalLocation as string) as flocSuperiorFunctionalLocation,
  cast(flocSystemStatus as string) as flocSystemStatus,
  cast(flocMaintenancePlant as string) as flocMaintenancePlant,
  cast(flocPlannerGroup as string) as flocPlannerGroup,
  cast(flocCriticalityCombined as string) as flocCriticalityCombined,
  cast(flocBarrierArea as string) as flocBarrierArea,
  cast(flocMainWorkCenter as string) as flocMainWorkCenter,
  CASE WHEN lower(isDeleted) = 'true' THEN true ELSE false END AS isDeleted
FROM `cfihos_oil_and_gas`.`functional_location`
WHERE is_new('functional_location', lastUpdatedTime)
  AND `key` IN (SELECT `key` FROM `cfihos_oil_and_gas`.`tag`)
