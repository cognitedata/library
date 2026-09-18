# File annotation configuration

The extraction pipeline uses the same shape as the entity-matching module: operator choices are under `parameters`, while data-model identities and property mappings are under `data`. Deployment defaults for those fields live in `default.config.yaml` and are substituted into `ep_file_annotation.config.yaml`. Fixed pipeline behavior is defined in `functions/fn_file_annotation/fa_constants.py`.

## Runtime input

Every call requires a stage and the extraction-pipeline external ID:

```json
{"stage":"prepare","ExtractionPipelineExtId":"ep_file_annotation","logLevel":"INFO"}
```

Valid stages are `prepare`, `launch`, `finalize`, and `promote`.

## Parameters

```yaml
parameters:
  patternMode: {{ patternMode }}
  cleanOldAnnotations: {{ cleanOldAnnotations }}
  autoApprovalThreshold: {{ autoApprovalThreshold }}
  autoSuggestThreshold: {{ autoSuggestThreshold }}
  primaryScopeProperty: {{ primaryScopeProperty }}
  secondaryScopeProperty: {{ secondaryScopeProperty }}
  rawDb: {{ rawDb }}
  rawTableDocTag: {{ rawTableDocTag }}
  rawTableDocDoc: {{ rawTableDocDoc }}
  rawTableDocPattern: {{ rawTableDocPattern }}
  rawTableCache: {{ rawTableCache }}
  rawManualPatternsCatalog: {{ rawManualPatternsCatalog }}
  rawTablePromoteCache: {{ rawTablePromoteCache }}
  patternPromote:
    textNormalization:
      convertToLowercase: {{ convertToLowercase }}
      normalizePattern: {{ textNormalizationPattern }}
      normalizeSelection: {{ textNormalizationSelection }}
```

- `patternMode` enables pattern-mode Diagram Detect alongside regular entity matching.
- `cleanOldAnnotations` removes prior annotations on the first finalize pass.
- `autoApprovalThreshold` and `autoSuggestThreshold` control regular annotation status.
- `primaryScopeProperty` and `secondaryScopeProperty` group files so launch can reuse a scoped entity cache.
- `rawDb` is the shared database for result and cache tables.
- The `rawTable*` keys name the function's result, cache, and catalog tables. They must match the Toolkit RAW resources and the extraction pipeline's `rawTables` list.
- `patternPromote.textNormalization.normalizePattern` is one regular expression or a list of them (same capture-group semantics as aliases_update `aliasPattern`). The normalized form is the capture groups joined by `_`.
- `normalizeSelection` is `all` or `longest` when several patterns match (same as aliases_update `aliasSelection`).
- `convertToLowercase` runs after extraction. Built-in rules then remove non-alphanumeric characters and strip leading zeros.

Example:

```yaml
textNormalization:
  convertToLowercase: false
  normalizePattern:
    - '([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})'
  normalizeSelection: all
```

## Data

Properties used to search and classify entities belong to their views:

```yaml
data:
  fileView:
    schemaSpace: {{ fileSchemaSpace }}
    instanceSpace: {{ fileInstanceSpace }}
    externalId: {{ fileExternalId }}
    version: {{ fileVersion }}
    searchProperty: {{ fileSearchProperty }}
    resourceProperty: {{ fileResourceProperty }}
  targetEntitiesView:
    schemaSpace: {{ targetEntitySchemaSpace }}
    instanceSpace: {{ targetEntityInstanceSpace }}
    externalId: {{ targetEntityExternalId }}
    version: {{ targetEntityVersion }}
    searchProperty: {{ targetEntitySearchProperty }}
    resourceProperty: {{ targetEntityResourceProperty }}
  annotationStateView:
    schemaSpace: {{ annotationStateSchemaSpace }}
    instanceSpace: {{ fileInstanceSpace }}
    externalId: {{ annotationStateExternalId }}
    version: {{ annotationStateVersion }}
  sinkNode:
    space: {{ patternModeInstanceSpace }}
    externalId: {{ patternDetectSink }}
```

`searchProperty` is sent to Diagram Detect and queried by promote. `resourceProperty` optionally classifies entities in pattern samples and reports; when omitted, the view external ID is used.

## Fixed behavior

These values are intentionally constants, not deployment configuration:

- Batch size: 50 files
- Page range: 50 pages
- Entity-cache lifetime: 0 hours (always refresh)
- Finalize retries: 3
- Global entity-search limit: 1000
- Function work budget: 7 minutes
- Local HTTP 429 wait: 900 seconds
- Promote searches file and target entities
- Rejected pattern edges are deleted from DMS after their RAW audit row is updated
- Ambiguous Suggested edges remain in DMS for review
- Annotation types, state/status filters, and standard tags

## Migration from the four-function config

Remove `dataModelViews`, `rawTables`, `prepareFunction`, `launchFunction`, `finalizeFunction`, and `promoteFunction`. Replace them with the `parameters` and `data` blocks above, and add the matching keys to `default.config.yaml` (or your `config.<env>.yaml` module variables). Query target views, fixed tags/statuses, limits, and promote cleanup flags are no longer configurable. RAW table names stay in `parameters` and `default.config.yaml`.

Function calls must use `fn_file_annotation` and include `stage`. The supplied workflow
invokes all four stages in order and finishes with the file-to-asset transformation.
