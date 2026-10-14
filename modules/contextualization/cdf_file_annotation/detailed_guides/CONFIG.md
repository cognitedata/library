# File annotation configuration

The extraction pipeline uses the same shape as the entity-matching module: operator choices are under `parameters`, while data-model identities and property mappings are under `data`. Deployment defaults for those fields live in `default.config.yaml` and are substituted into `ep_file_annotation.config.yaml`. Fixed pipeline behavior is defined in `functions/fn_file_annotation/fa_constants.py`.

## Runtime input

Every call requires a stage and the extraction-pipeline external ID:

```json
{"stage":"prepare","ExtractionPipelineExtId":"ep_file_annotation","logLevel":"INFO"}
```

The input is validated before any stage runs. Valid stages are `prepare`, `launch`, `finalize`, and `promote`. `logLevel` is optional, defaults to `INFO`, and accepts `DEBUG`, `INFO`, `WARNING`, or `ERROR` (case-insensitive). A missing or invalid field fails the call with a validation error.

## Parameters

```yaml
parameters:
  cleanOldAnnotations: {{ cleanOldAnnotations }}
  assetAutoApprovalThreshold: {{ assetAutoApprovalThreshold }}
  assetAutoSuggestThreshold: {{ assetAutoSuggestThreshold }}
  fileAutoApprovalThreshold: {{ fileAutoApprovalThreshold }}
  fileAutoSuggestThreshold: {{ fileAutoSuggestThreshold }}
  primaryScopeProperty: {{ primaryScopeProperty }}
  secondaryScopeProperty: {{ secondaryScopeProperty }}
  # Pipeline tags: ToAnnotate, DetectInDiagrams, ScopeWideDetect, AnnotationInProcess,
  # Annotated, AnnotationFailed, PromoteAttempted, PromotedAuto, AmbiguousMatch.
  filesToAnnotateTags: {{ filesToAnnotateTags }}
  filesToAnnotateExcludeTags: {{ filesToAnnotateExcludeTags }}
  fileEntitiesTags: {{ fileEntitiesTags }}
  targetEntitiesTags: {{ targetEntitiesTags }}
  debugFileExternalId: {{ debugFileExternalId }}
  patternPromote:
    patternMode: {{ patternMode }}
    structuralAutoPatterns: {{ structuralAutoPatterns }}
    filterPatternPromoteByScope: {{ filterPatternPromoteByScope }}
    textNormalization:
      entityNormalizationPatterns: {{ entityNormalizationPatterns }}
      fileNormalizationPatterns: {{ fileNormalizationPatterns }}
```

- `patternPromote.patternMode` enables pattern-mode Diagram Detect alongside regular entity matching.
- `patternPromote.structuralAutoPatterns` (default `true`) makes auto patterns digit/letter **structure** templates such as `00-AA-0000` instead of enumerating letter codes like `[FE|KA|PC|VA]`. Separators from aliases (`_`, `-`, `.`, `:`, `;`, `/`) are never required constants (never `[_]`); they normalize to unbracketed `-`. Set `false` for legacy letter-enum expansion.
- `patternPromote.filterPatternPromoteByScope` (default `false`) makes Promote apply `primaryScopeProperty` / `secondaryScopeProperty` as filters. The values are read from the annotated file. Primary must match. When a secondary value is set, the entity must match it or carry `ScopeWideDetect` (still inside the primary scope). A file with no value for a configured property is not filtered on that property.
- `cleanOldAnnotations` removes this function's prior annotations (edges with `sourceCreatedUser = fn_file_annotation`, plus their RAW rows) on the first finalize pass of a file that is re-annotated. Manual and third-party annotations are kept. Files that are no longer selected for annotation are not cleaned.
- `assetAutoApprovalThreshold` and `assetAutoSuggestThreshold` control regular annotation status for asset links (`diagrams.AssetLink`). `fileAutoApprovalThreshold` and `fileAutoSuggestThreshold` do the same for file links (`diagrams.FileLink`); leave them empty to reuse the asset-link values. A detection at or above the approval threshold is `Approved`, at or above the suggest threshold `Suggested`, and below that it is dropped.
- `primaryScopeProperty` and `secondaryScopeProperty` group files so launch can reuse a scoped entity cache. Instances with no value for those properties are still matched: files in an unscoped batch against all tagged entities, and unscoped assets against all documents (warning). When both are empty, every file is matched against all targets (warning: false positives).
- `filesToAnnotateTags` is the Prepare IN filter for files to process (default `ToAnnotate`).
- `filesToAnnotateExcludeTags` is the Prepare NOT IN filter (default `AnnotationInProcess`, `Annotated`, `AnnotationFailed`). Tags also listed in `filesToAnnotateTags` are dropped from the exclude list, so adding `Annotated` reprocesses those files.
- `fileEntitiesTags` is the Launch IN filter for files used as diagram-detect match entities (default `DetectInDiagrams`).
- `targetEntitiesTags` is the Launch IN filter for assets used as diagram-detect match entities (default `DetectInDiagrams`).
- `debugFileExternalId` (default empty) restricts every stage to one file, for debugging. The file is looked up in `data.fileView.instanceSpace` (required when this is set). Prepare picks the file regardless of its `ToAnnotate`/`Annotated` tags (only `AnnotationInProcess` is skipped), Launch and Finalize only handle that file's annotation state, Promote only handles edges that start at the file, and no other files are annotated. Match entities are still read in full: Launch retrieves all `DetectInDiagrams`/`ScopeWideDetect` assets and files as usual, so the debug file is matched against the same entities as in a normal run. Each stage logs a `DEBUG MODE` line in its config header. Leave empty for normal runs.
- Possible pipeline tags: `ToAnnotate`, `DetectInDiagrams`, `ScopeWideDetect`, `AnnotationInProcess`, `Annotated`, `AnnotationFailed`, `PromoteAttempted`, `PromotedAuto`, `AmbiguousMatch`.
- `entityNormalizationPatterns` / `fileNormalizationPatterns` are separate lists (same capture-group semantics as aliases_update `aliasPattern`). Asset aliases and AssetLink promote use the entity list; file aliases and FileLink promote use the file list. Longest match wins. An **empty list** disables filtering for that source only (avoids false-positive structural samples from mixing unrelated shapes).
- Casing is preserved: DMS alias `IN` filters are case-sensitive exact matches. Built-in rules remove non-alphanumeric characters and strip leading zeros after extraction.

Example:

```yaml
textNormalization:
  entityNormalizationPatterns:
    - '([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})'
  fileNormalizationPatterns:
    - '(?<![A-Z])([A-Z]{2,4}-[A-Z0-9]+-[A-Z]-[0-9]+)'
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

`searchProperty` is sent to Diagram Detect and queried by promote with `containsAny`. When that misses, promote queries `name` and `description` with operator `AND`. `resourceProperty` optionally classifies entities in pattern samples and reports; when omitted, the view external ID is used.

## Fixed behavior

These values are intentionally constants in
`functions/fn_file_annotation/fa_constants.py`, not Toolkit / extraction-pipeline
variables. Change them by editing that file and redeploying the function.

Pipeline limits and promote cleanup:

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

### RAW database and tables

All result, cache, and catalog tables live in one RAW database. The names are fixed
constants, not configuration. The Toolkit RAW resources (`raw/`), the access group's
RAW scope (`auth/`), the extraction pipeline's `rawTables` list, the transformations, and
the Annotation Quality dashboard use the same literal names, so a rename must be made in
all of those places.

| Constant | Name | Contents |
|----------|------|----------|
| `RAW_DB` | `raw_file_annotation` | Database for all tables below |
| `RAW_TABLE_DOC_TAG` | `annotation_documents_tags` | Regular detect links to assets |
| `RAW_TABLE_DOC_DOC` | `annotation_documents_docs` | File-to-file links |
| `RAW_TABLE_DOC_PATTERN` | `annotation_documents_patterns` | Pattern-mode detections and promote outcomes |
| `RAW_TABLE_CACHE` | `annotation_entities_cache` | Entity sync state and auto pattern samples |
| `RAW_TABLE_MANUAL_PATTERNS` | `manual_patterns_catalog` | Manual pattern overrides |
| `RAW_TABLE_PROMOTE_CACHE` | `annotation_tags_cache` | Promote text-to-entity cache |
| `RAW_TABLE_ANNOTATION_STATUS_REPORT` | `annotation_file_status_report` | Output of `tr_file_annotation_status_report` |

### Diagram Detect matching (`DiagramDetectConfig`)

Launch passes these constants into Cognite Diagram Detect via
[`DiagramDetectConfig`](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/contextualization.html#cognite.client.data_classes.contextualization.DiagramDetectConfig)
(also documented on the [engineering diagrams detect API](https://api-docs.cognite.com/20230101-beta/tag/Engineering-diagrams/operation/diagramDetect/)).
`None` means the parameter is omitted and the API default applies.

| Constant | Current default | SDK / API field |
|----------|-----------------|-----------------|
| `MIN_TOKENS` | `2` | `minTokens` (annotation service, not inside `DiagramDetectConfig`) |
| `ANNOTATION_EXTRACT` | `None` | `annotationExtract` — cannot be `True` together with `READ_EMBEDDED_TEXT` |
| `CASE_SENSITIVE` | `None` | `caseSensitive` |
| `NO_TEXT_INBETWEEN` | `True` | `connectionFlags.noTextInbetween` |
| `NATURAL_READING_ORDER` | `True` | `connectionFlags.naturalReadingOrder` |
| `FUZZINESS_FUZZY_SCORE` | `None` | `customizeFuzziness.fuzzyScore` |
| `FUZZINESS_MAX_BOXES` | `None` | `customizeFuzziness.maxBoxes` |
| `FUZZINESS_MIN_CHARS` | `4` | `customizeFuzziness.minChars` |
| `DIRECTION_DELTA` | `None` | `directionDelta` |
| `DIRECTION_WEIGHTS` | `{"left": 1.0, "right": 1.0, "up": 1.0, "down": 1.0}` | `directionWeights` |
| `MIN_FUZZY_SCORE` | `0.99` | `minFuzzyScore` (`1` disables OCR character substitutions) |
| `READ_EMBEDDED_TEXT` | `True` | `readEmbeddedText` |
| `REMOVE_LEADING_ZEROS` | `None` | `removeLeadingZeros` |
| `SUBSTITUTIONS` | `None` | `substitutions` — when set, replaces the API's default look-alike map |

Confidence thresholds that decide Approved / Suggested / drop after detect
(`assetAutoApprovalThreshold`, `assetAutoSuggestThreshold`, and the optional
`fileAuto*` pair) remain Toolkit variables under `parameters` (see above). They are
not part of `DiagramDetectConfig`.

## Migration from the four-function config

Remove `dataModelViews`, `rawTables`, `prepareFunction`, `launchFunction`, `finalizeFunction`, and `promoteFunction`. Replace them with the `parameters` and `data` blocks above, and add the matching keys to `default.config.yaml` (or your `config.<env>.yaml` module variables). Query target views, fixed tags/statuses, limits, promote cleanup flags, and RAW database/table names are no longer configurable.

### Migration from configurable RAW names

The `rawDb` / `rawTable*` / `rawManualPatternsCatalog` module variables and the `parameters.rawData` block are removed. RAW names are now the constants listed under [RAW database and tables](#raw-database-and-tables). Delete those keys from your `config.<env>.yaml`. A `parameters.rawData` block left in an extraction-pipeline config is ignored. If you deployed with non-default names, move the existing rows to the fixed tables, or edit the constants and the Toolkit files that repeat them.

`patternMode`, `structuralAutoPatterns`, and `filterPatternPromoteByScope` are read from `parameters.patternPromote`. Left directly under `parameters`, they are ignored. The first two default to `true`; the scope filter defaults to `false`.

Function calls must use `fn_file_annotation` and include `stage`. The supplied workflow
invokes all four stages in order and finishes with the file-to-asset transformation.
