# File annotation configuration

The extraction pipeline uses the same shape as the entity-matching module: operator choices are under `parameters`, while data-model identities and property mappings are under `data`. Fixed pipeline behavior is defined in `functions/fn_file_annotation/fa_constants.py`.

## Runtime input

Every call requires a stage and the extraction-pipeline external ID:

```json
{"stage":"prepare","ExtractionPipelineExtId":"ep_file_annotation","logLevel":"INFO"}
```

Valid stages are `prepare`, `launch`, `finalize`, and `promote`.

## Parameters

```yaml
parameters:
  patternMode: true
  cleanOldAnnotations: true
  autoApprovalThreshold: 1.0
  autoSuggestThreshold: 1.0
  primaryScopeProperty:
  secondaryScopeProperty:
  rawDb: {{ rawDb }}
  patternPromote:
    textNormalization:
      convertToLowercase: false
      substitutions: []
```

- `patternMode` enables pattern-mode Diagram Detect alongside regular entity matching.
- `cleanOldAnnotations` removes prior annotations on the first finalize pass.
- `autoApprovalThreshold` and `autoSuggestThreshold` control regular annotation status.
- `primaryScopeProperty` and `secondaryScopeProperty` group files so launch can reuse a scoped entity cache.
- `rawDb` is the shared database for result and cache tables.
- `patternPromote.textNormalization.substitutions` is an ordered list of Python regular-expression replacements. Each item has `pattern` and `replacement`; capture-group replacements such as `\\1` are supported.
- `convertToLowercase` runs after project substitutions. Built-in rules then remove non-alphanumeric characters and strip leading zeros.

Example:

```yaml
textNormalization:
  convertToLowercase: false
  substitutions:
    - pattern: '^[A-Z]{2}-'
      replacement: ''
    - pattern: '[/_.]'
      replacement: '-'
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
    searchProperty: aliases
    resourceProperty:
  targetEntitiesView:
    schemaSpace: {{ targetEntitySchemaSpace }}
    instanceSpace: {{ targetEntityInstanceSpace }}
    externalId: {{ targetEntityExternalId }}
    version: {{ targetEntityVersion }}
    searchProperty: aliases
    resourceProperty:
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
- RAW table names, annotation types, state/status filters, and standard tags

## Migration from the four-function config

Remove `dataModelViews`, `rawTables`, `prepareFunction`, `launchFunction`, `finalizeFunction`, and `promoteFunction`. Replace them with the `parameters` and `data` blocks above. Query target views, fixed tags/statuses, limits, table names, and promote cleanup flags are no longer configurable.

Function calls must use `fn_file_annotation` and include `stage`. The supplied workflow
invokes all four stages in order and finishes with the file-to-asset transformation.
