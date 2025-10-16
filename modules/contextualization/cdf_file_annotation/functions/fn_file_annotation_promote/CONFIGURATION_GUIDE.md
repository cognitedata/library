# Promote Function Configuration Guide

## Overview

The promote function now supports a dedicated `promoteFunction` configuration section in the extraction pipeline config, following the same pattern as `launchFunction` and `finalizeFunction`. This makes the function fully configurable and enables environment-specific tuning.

The configuration is **organized by service interface**, matching the architectural pattern used throughout the file annotation system.

## What Was Added

### 1. Configuration Classes (ConfigService.py)

The configuration follows a service-oriented structure, grouping settings by their respective service interfaces.

#### `EntitySearchServiceConfig`

Configuration for the EntitySearchService, controls entity search strategies and text normalization:

- `enableExistingAnnotationsSearch` (bool): Enable primary search via annotation edges (fast, 50-100ms)
- `enableGlobalEntitySearch` (bool): Enable fallback global entity search (slow, 500ms-2s)
- `maxEntitySearchLimit` (int): Max entities to fetch in global search (default: 1000, max: 10000)
- `textNormalization` (TextNormalizationConfig): Text normalization settings (nested)

#### `TextNormalizationConfig`

Controls text normalization and variation generation (nested within `EntitySearchServiceConfig`):

- `removeSpecialCharacters` (bool): Remove non-alphanumeric characters
- `convertToLowercase` (bool): Normalize case
- `stripLeadingZeros` (bool): Strip leading zeros (e.g., "V-0912" → "V-912")
- `generateVariations` (bool): Generate common text variations for matching

#### `PromoteCacheServiceConfig`

Configuration for the CacheService, controls caching behavior:

- `cacheTableName` (str): RAW table for text→entity cache (default: "promote_text_to_entity_cache")

#### `PromoteFunctionConfig`

Main configuration for the promote function:

- `getCandidatesQuery` (QueryConfig): Query for finding candidate edges to promote (includes `limit` field for batch size)
- `rawDb` (str): RAW database name
- `rawTableDocPattern` (str): RAW table for pattern-mode annotations
- `rawTableDocTag` (str): RAW table for tag annotations
- `rawTableDocDoc` (str): RAW table for document annotations
- `entitySearchService` (EntitySearchServiceConfig): Entity search service configuration
- `cacheService` (PromoteCacheServiceConfig): Cache service configuration

**Note**: Batch size is controlled via the `limit` field in `getCandidatesQuery`. If set to `-1` (unlimited), defaults to 500.

### 2. Updated Files

- **ConfigService.py**: Added new config classes organized by service
- **PromoteService.py**: Updated to use config values instead of hardcoded constants
- **dependencies.py**: Updated to use config values when creating services
- **ep_file_annotation.config.yaml**: Added example `promoteFunction` section

## Configuration Example

Here's the complete `promoteFunction` section added to the extraction pipeline config:

```yaml
promoteFunction:
  # Query configuration for finding candidate edges to promote
  getCandidatesQuery:
    targetView:
      schemaSpace: cdf_cdm
      externalId: CogniteDiagramAnnotation
      version: v1
    filters:
      - values: "Suggested" # Only process suggested annotations
        negate: False
        operator: Equals
        targetProperty: status
      - values: ["PromoteAttempted"] # Skip already attempted edges
        negate: True
        operator: In
        targetProperty: tags
      limit: 500 # Number of edges to process per batch

  # RAW database configuration
  rawDb: { { rawDb } }
  rawTableDocPattern: { { rawTableDocPattern } }
  rawTableDocTag: { { rawTableDocTag } }
  rawTableDocDoc: { { rawTableDocDoc } }

  # Entity search service configuration
  entitySearchService:
    enableExistingAnnotationsSearch: true # Primary: Query annotation edges (fast)
    enableGlobalEntitySearch: true # Fallback: Global entity search (slow)
    maxEntitySearchLimit: 1000 # Max entities to fetch in global search
    textNormalization:
      removeSpecialCharacters: true # Remove non-alphanumeric characters
      convertToLowercase: true # Normalize case
      stripLeadingZeros: true # Handle "V-0912" vs "V-912"
      generateVariations: true # Generate common text variations

  # Cache service configuration
  cacheService:
    cacheTableName: "promote_text_to_entity_cache"
```

## Service-Oriented Structure

The configuration mirrors the actual service architecture:

### EntitySearchService

Handles finding entities by text using multiple strategies:

- Primary: Query existing annotation edges
- Fallback: Global entity search
- Text normalization for matching

**Config Section:** `entitySearchService`

### CacheService

Manages two-tier caching (in-memory + persistent RAW):

- In-memory cache for this run
- Persistent RAW cache across all runs

**Config Section:** `cacheService`

This structure matches the patterns established in:

- `launchFunction` → `dataModelService`, `cacheService`, `annotationService`
- `finalizeFunction` → `retrieveService`, `applyService`

## Backward Compatibility

The implementation includes full backward compatibility:

1. **Optional Config Section**: The `promoteFunction` section is optional in the Config class
2. **Fallback Behavior**: If `promoteFunction` is not present, the function falls back to:
   - RAW database config from `finalizeFunction.applyService`
   - Hardcoded filter for candidate queries
   - Default values for all other settings
3. **Warning Logs**: When falling back to old behavior, a warning is logged

## Usage Examples

### Example 1: Increase Batch Size for Better Performance

```yaml
promoteFunction:
  getCandidatesQuery:
    targetView:
      schemaSpace: cdf_cdm
      externalId: CogniteDiagramAnnotation
      version: v1
    filters:
      - values: "Suggested"
        operator: Equals
        targetProperty: status
    limit: 1000 # Process more edges per batch (increased from default 500)
  # ... rest of config
```

### Example 2: Disable Global Search (Only Use Existing Annotations)

```yaml
promoteFunction:
  entitySearchService:
    enableExistingAnnotationsSearch: true
    enableGlobalEntitySearch: false # Skip slow global search
    maxEntitySearchLimit: 1000
    textNormalization:
      removeSpecialCharacters: true
      convertToLowercase: true
      stripLeadingZeros: true
      generateVariations: true
  # ... rest of config
```

### Example 3: Custom Query Filter

```yaml
promoteFunction:
  getCandidatesQuery:
    targetView:
      schemaSpace: cdf_cdm
      externalId: CogniteDiagramAnnotation
      version: v1
    filters:
      - values: "Suggested"
        operator: Equals
        targetProperty: status
      - values: ["HighPriority"] # Only promote high-priority edges
        operator: In
        targetProperty: tags
  # ... rest of config
```

### Example 4: Separate Cache Per Environment

```yaml
promoteFunction:
  cacheService:
    cacheTableName: "promote_text_to_entity_cache_prod" # Environment-specific cache
  # ... rest of config
```

### Example 5: Adjust Text Normalization

```yaml
promoteFunction:
  entitySearchService:
    enableExistingAnnotationsSearch: true
    enableGlobalEntitySearch: true
    maxEntitySearchLimit: 1000
    textNormalization:
      removeSpecialCharacters: true
      convertToLowercase: false # Preserve case sensitivity
      stripLeadingZeros: false # Keep leading zeros
      generateVariations: true
  # ... rest of config
```

## Migration Guide

### For Existing Deployments

1. **No Immediate Action Required**: The function continues to work without the new config section
2. **Recommended**: Add the `promoteFunction` section to gain benefits:
   - Flexible candidate filtering
   - Performance tuning per environment
   - Explicit configuration visibility
   - Service-oriented organization

### Adding Configuration to Existing Pipeline

1. Open your extraction pipeline config file (e.g., `ep_file_annotation.config.yaml`)
2. Add the `promoteFunction` section after `finalizeFunction`
3. Customize values as needed for your environment
4. Deploy the updated configuration

## Configuration Benefits

1. **Service-Oriented**: Configuration mirrors actual service architecture
2. **Flexibility**: Easily adjust query filters without code changes
3. **Performance Tuning**: Optimize batch sizes and search strategies per environment
4. **Visibility**: All settings are explicitly documented in config
5. **Consistency**: Follows same pattern as launch/finalize functions
6. **Environment-Specific**: Different configs for dev/test/prod

## Service Configuration Details

### EntitySearchService Configuration

Controls how the promote function finds matching entities:

| Setting                           | Type | Default | Description                     |
| --------------------------------- | ---- | ------- | ------------------------------- |
| `enableExistingAnnotationsSearch` | bool | true    | Primary search strategy (fast)  |
| `enableGlobalEntitySearch`        | bool | true    | Fallback search strategy (slow) |
| `maxEntitySearchLimit`            | int  | 1000    | Max entities in global search   |

**Text Normalization Settings:**

| Setting                   | Type | Default | Description                   |
| ------------------------- | ---- | ------- | ----------------------------- |
| `removeSpecialCharacters` | bool | true    | Remove non-alphanumeric chars |
| `convertToLowercase`      | bool | true    | Case-insensitive matching     |
| `stripLeadingZeros`       | bool | true    | Handle "V-0912" vs "V-912"    |
| `generateVariations`      | bool | true    | Generate text variations      |

### CacheService Configuration

Controls caching behavior for performance optimization:

| Setting          | Type | Default                        | Description                    |
| ---------------- | ---- | ------------------------------ | ------------------------------ |
| `cacheTableName` | str  | "promote_text_to_entity_cache" | RAW table for persistent cache |

## Future Enhancements

The config structure supports easy addition of new features:

- Retry logic configuration
- Tagging customization
- Batch processing delays
- Feature flags for A/B testing
- Additional service configurations

## Questions?

For questions or issues, refer to:

- Main README: `cdf_file_annotation/README.md`
- Code documentation in ConfigService.py
- Example configs in extraction_pipelines/
