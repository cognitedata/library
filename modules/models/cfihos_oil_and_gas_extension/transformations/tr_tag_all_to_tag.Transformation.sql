SELECT
  cast(key as string) as externalId,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(tagNumber as string) as tagNumber,
  cast(area as string) as area,
  cast(facility as string) as facility,
  cast(system as string) as system,
  cast(parentTag as string) as parentTag,
  cast(classId as string) as classId,
  cast(className as string) as className,
  cast(tagDiscipline as string) as tagDiscipline,
  cast(project as string) as project,
  cast(classViewExtId as string) as classViewExtId,
  CASE
    WHEN parentTag IS NULL OR parentTag = '' THEN NULL
    ELSE node_reference('{{ instance_space }}', cast(parentTag as string))
  END as parent,
  array(cast(labels as string)) as labels,
  cast(key as string) as sourceId,
  'cfihos_test' as sourceContext
FROM `cfihos_oil_and_gas`.`tag`
WHERE is_new('tag', lastUpdatedTime)
