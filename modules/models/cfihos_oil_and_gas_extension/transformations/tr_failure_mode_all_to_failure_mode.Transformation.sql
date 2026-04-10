SELECT
  cast(key as string) as externalId,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(code as string) as code,
  FILTER(
    TRANSFORM(
      SPLIT(COALESCE(mainAssets, ''), ';'),
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE TRIM(x) END
    ),
    x -> x IS NOT NULL
  ) as mainAssets,
  FILTER(
    TRANSFORM(
      SPLIT(COALESCE(facilities, ''), ';'),
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE TRIM(x) END
    ),
    x -> x IS NOT NULL
  ) as facilities,
  cast(standard as string) as standard,
  FILTER(
    TRANSFORM(
      SPLIT(COALESCE(mainAssets, ''), ';'),
      x -> CASE WHEN TRIM(x) = '' THEN NULL ELSE node_reference('{{ instance_space }}', TRIM(x)) END
    ),
    x -> x IS NOT NULL
  ) as assets
FROM `cfihos_oil_and_gas`.`failure_mode`
WHERE is_new('failure_mode', lastUpdatedTime)
