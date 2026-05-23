# Configuration Guide for Asset Hierarchy Creation

This guide helps data engineers and partners configure the asset hierarchy creation system without deep technical knowledge.

## Quick Start

1. **Choose your template**:
   - For quick start: Use `config.simple.example.yaml`
   - For industry-specific: Choose from `config.template.*.yaml` files (manufacturing, oil_gas, utilities, pharmaceuticals)
2. **Fill in your scope**: Add your site/plant/area/system structure
3. **Add your files**: List files under each system
4. **Set your patterns**: Configure what asset tags to extract
5. **Deploy**: The system handles the rest!

## Configuration Structure

The configuration is organized into clear sections:

### 1. Business Configuration (What You Need to Change)

```yaml
# Your organization's hierarchy structure
hierarchy:
  levels: [site, plant, area, system]  # Customize these names for your organization

# Your scope and files (child nodes use "locations")
scope:
  - name: "Your Site Name"
    description: "Site Description"
    locations:
      - name: "Your Plant Name"
        description: "Plant Description"
        locations:
          # ... continue nesting
          - name: "Your System Name"
            description: "System Description"
            files:
              - "File-001"
              - "File-002"

# What asset tags to extract
patterns:
  - category: equipment
    samples:
      - "P-101"
      - "V-201"
```

### 2. Technical Configuration (Usually Don't Need to Change)

```yaml
# Where to store data (usually leave as default)
storage:
  database: db_extract_assets_by_pattern
  tables:
    results: extract_assets_by_pattern_results
    assets: extract_assets_by_pattern_assets

# How to process files
processing:
  batch_size: 5
  max_attempts: 3
  limit: -1  # -1 means process all files
```

## Common Configurations

### Example 1: Simple 3-Level Hierarchy

```yaml
hierarchy:
  levels: [facility, building, room]

scope:
  - name: "Main Facility"
    description: "Primary manufacturing facility"
    locations:
      - name: "Building A"
        description: "Production building"
        locations:
          - name: "Control Room"
            description: "Main control room"
            files:
              - "Control-Diagram-001"
```

### Example 2: Complex Multi-Site Hierarchy

```yaml
hierarchy:
  levels: [region, site, unit, system]

scope:
  - name: "North Region"
    description: "Northern operations"
    locations:
      - name: "Site Alpha"
        description: "Alpha production site"
        locations:
          - name: "Unit 1"
            description: "Primary production unit"
            locations:
              - name: "Cooling System"
                description: "Cooling water system"
                files:
                  - "CW-001"
                  - "CW-002"
```

## Configuration Tips

### ✅ DO:
- Use clear, descriptive names
- Keep hierarchy levels consistent
- List all files that belong to each system
- Test with a small subset first (use `limit: 10`)

### ❌ DON'T:
- Mix different naming conventions
- Skip hierarchy levels
- Forget to add files to systems
- Use special characters in names (stick to letters, numbers, dashes, underscores)

## Troubleshooting

**Problem**: Files not matching to systems
- **Solution**: Check that file names in your config match the actual file names in CDF (without extensions)

**Problem**: No assets extracted
- **Solution**: Verify your patterns match the asset tags in your diagrams

**Problem**: Wrong hierarchy created
- **Solution**: Double-check your `hierarchy.levels` matches your `locations` structure

## Need Help?

See the examples in:
- `config.simple.example.yaml` - Working example configuration (quick start)
- `config.template.manufacturing.yaml` - Manufacturing industry template
- `config.template.oil_gas.yaml` - Oil & Gas industry template
- `config.template.utilities.yaml` - Utilities industry template
- `config.template.pharmaceuticals.yaml` - Pharmaceuticals industry template
- `default.config.yaml` (`file_asset_source.create`) - Full production example with all options
