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
  structuralAutoPatterns: {{ structuralAutoPatterns }}
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
  rawDb: {{ rawDb }}
  rawTableDocTag: {{ rawTableDocTag }}
  rawTableDocDoc: {{ rawTableDocDoc }}
  rawTableDocPattern: {{ rawTableDocPattern }}
  rawTableCache: {{ rawTableCache }}
  rawManualPatternsCatalog: {{ rawManualPatternsCatalog }}
  rawTablePromoteCache: {{ rawTablePromoteCache }}
  patternPromote:
    textNormalization:
      entityNormalizationPatterns: {{ entityNormalizationPatterns }}
      fileNormalizationPatterns: {{ fileNormalizationPatterns }}
```

- `patternMode` enables pattern-mode Diagram Detect alongside regular entity matching.
- `structuralAutoPatterns` (default `true`) makes auto patterns digit/letter **structure** templates such as `00-AA-0000` instead of enumerating letter codes like `[FE|KA|PC|VA]`. Separators from aliases (`_`, `-`, `.`, `:`, `;`, `/`) are never required constants (never `[_]`); they normalize to unbracketed `-`. Set `false` for legacy letter-enum expansion.
- `cleanOldAnnotations` removes this function's prior annotations (edges with `sourceCreatedUser = fn_file_annotation`, plus their RAW rows) on the first finalize pass of a file that is re-annotated. Manual and third-party annotations are kept. Files that are no longer selected for annotation are not cleaned.
- `assetAutoApprovalThreshold` and `assetAutoSuggestThreshold` control regular annotation status for asset links (`diagrams.AssetLink`). `fileAutoApprovalThreshold` and `fileAutoSuggestThreshold` do the same for file links (`diagrams.FileLink`); leave them empty to reuse the asset-link values. A detection at or above the approval threshold is `Approved`, at or above the suggest threshold `Suggested`, and below that it is dropped.- `primaryScopeProperty` and `secondaryScopeProperty` group files so launch can reuse a scoped entity cache.
- `filesToAnnotateTags` is the Prepare IN filter for files to process (default `ToAnnotate`).
- `filesToAnnotateExcludeTags` is the Prepare NOT IN filter (default `AnnotationInProcess`, `Annotated`, `AnnotationFailed`). Tags also listed in `filesToAnnotateTags` are dropped from the exclude list, so adding `Annotated` reprocesses those files.
- `fileEntitiesTags` is the Launch IN filter for files used as diagram-detect match entities (default `DetectInDiagrams`).
- `targetEntitiesTags` is the Launch IN filter for assets used as diagram-detect match entities (default `DetectInDiagrams`).
- `debugFileExternalId` (default empty) restricts every stage to one file, for debugging. The file is looked up in `data.fileView.instanceSpace` (required when this is set). Prepare picks the file regardless of its `ToAnnotate`/`Annotated` tags (only `AnnotationInProcess` is skipped), Launch and Finalize only handle that file's annotation state, Promote only handles edges that start at the file, and no other files are annotated. Match entities are still read in full: Launch retrieves all `DetectInDiagrams`/`ScopeWideDetect` assets and files as usual, so the debug file is matched against the same entities as in a normal run. Each stage logs a `DEBUG MODE` line in its config header. Leave empty for normal runs.
- Possible pipeline tags: `ToAnnotate`, `DetectInDiagrams`, `ScopeWideDetect`, `AnnotationInProcess`, `Annotated`, `AnnotationFailed`, `PromoteAttempted`, `PromotedAuto`, `AmbiguousMatch`.
- `rawDb` is the shared database for result and cache tables.
- The `rawTable*` keys name the function's result, cache, and catalog tables. They must match the Toolkit RAW resources and the extraction pipeline's `rawTables` list.
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
