SELECT
  concat('file_', cast(name as string)) as externalId,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(documentTypeCode as string) as documentTypeCode,
  cast(documentNumber as string) as documentNumber,
  cast(documentTitle as string) as documentTitle,
  cast(disciplineCode as string) as disciplineCode,
  cast(disciplineDesc as string) as disciplineCodeDesc,
  cast(area as string) as area,
  cast(facility as string) as facility,
  cast(system as string) as system,
  CASE WHEN issueDate IS NOT NULL AND issueDate != '' THEN to_timestamp(issueDate, 'M/d/yyyy') ELSE NULL END as issueDate,
  cast(originatingContractor as string) as originatingContractor,
  CASE WHEN currentRevision IS NOT NULL AND currentRevision != '' THEN true ELSE false END as currentRevision,
  cast(extension as string) as extension,
  cast(state as string) as state,
  cast(mainAsset as string) as mainAsset,
  cast(documentSource as string) as documentSource,
  cast(mimeType as string) as mimeType,
  CASE
    WHEN asset_externalId IS NULL OR asset_externalId = '' THEN NULL
    ELSE array(node_reference('{{ instance_space }}', cast(asset_externalId as string)))
  END as assets,
  cast(sourceId as string) as sourceId,
  cast(source as string) as sourceContext
FROM `cfihos_oil_and_gas`.`file`
WHERE 1=1 -- full reload: is_new('file', lastUpdatedTime)
