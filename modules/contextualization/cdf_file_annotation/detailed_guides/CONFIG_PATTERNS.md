# File annotation configuration patterns

The public extraction-pipeline configuration is intentionally small. Fixed state/status queries and standard tags live in `functions/fn_file_annotation/fa_constants.py`.

## Group entity caches by location

Set view properties once in `default.config.yaml` (`fileSearchProperty`, `fileResourceProperty`, and the matching `targetEntity*` keys) and use scope properties only for grouping:

```yaml
parameters:
  primaryScopeProperty: site
  secondaryScopeProperty: unit
```

The named properties must exist on `data.fileView`. Empty values disable that grouping level.

## Use another matching property

Matching properties belong to each view:

```yaml
data:
  fileView:
    searchProperty: aliases
    resourceProperty: documentType
  targetEntitiesView:
    searchProperty: aliases
    resourceProperty: equipmentType
```

`resourceProperty` is optional; the view external ID is used when it is omitted.

## Normalize site-specific tag formats

Promote and auto pattern generation use separate capture-group lists for assets and files
(same model as `cdf_entity_matching` aliases_update). Set `entityNormalizationPatterns`
and `fileNormalizationPatterns` in `default.config.yaml`.
Each matching pattern yields its capture groups joined by `_`. When several patterns
match, the **longest** form is always kept. An empty list disables filtering for that
source only. Splitting the lists avoids false-positive structural samples when asset and
file alias shapes differ.

```yaml
parameters:
  patternPromote:
    textNormalization:
      entityNormalizationPatterns:
        - '([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})'
      fileNormalizationPatterns:
        - '(?<![A-Z])([A-Z]{2,4}-[A-Z0-9]+-[A-Z]-[0-9]+)'
```

Use the same patterns you configure for aliases_update so diagram text normalizes to the
same strings written on entity aliases. Prefer character classes (`[0-9]`) over `\d`
because Toolkit substitutes variables as a regex replacement.

Keep patterns specific. A broad rule can make distinct tags normalize to the same value
and produce ambiguous matches.

## Reprocess files

The default prepare query includes `ToAnnotate` and excludes `AnnotationInProcess`, `Annotated`, and `AnnotationFailed`. Resetting prior state is an operational action rather than an open-ended query block. Remove the status tag/state for the selected files, then run the workflow again.

## Run one stage locally

All debug configurations target the same handler and pass the stage first:

```text
handler.py prepare ep_file_annotation DEBUG annotation_logs/prepare.log
handler.py launch ep_file_annotation DEBUG annotation_logs/launch.log
handler.py finalize ep_file_annotation DEBUG annotation_logs/finalize.log
handler.py promote ep_file_annotation DEBUG annotation_logs/promote.log
```
