# Getting Started Guide

This guide helps you get started with creating asset hierarchies from diagram files.

## What This Solution Does

1. **Extracts asset tags** from your diagram files (PDFs, DWG, etc.)
2. **Creates a hierarchy** based on your organization's structure (sites, plants, areas, systems)
3. **Writes assets** to CDF data modeling

## Quick Start (5 Steps)

### Step 1: Understand Your Structure

Before configuring, identify:
- **Your hierarchy levels**: What are your organizational levels? (e.g., Site > Plant > Area > System)
- **Your locations**: What sites, plants, areas, and systems do you have?
- **Your files**: Which files belong to which systems?
- **Your asset tags**: What patterns do your asset tags follow? (e.g., P-101, V-201)

### Step 2: Choose Your Template

**Option A: Quick Start (Generic)**
```bash
cp config.simple.example.yaml my_config.yaml
```

**Option B: Industry-Specific (Recommended)**
```bash
# Choose the template that matches your industry:
cp config.template.manufacturing.yaml my_config.yaml      # Manufacturing
cp config.template.oil_gas.yaml my_config.yaml            # Oil & Gas
cp config.template.utilities.yaml my_config.yaml           # Utilities
cp config.template.pharmaceuticals.yaml my_config.yaml    # Pharmaceuticals
```

### Step 3: Fill in Your Scope

Edit `my_config.yaml` and update the `scope` section (nested child nodes use `locations`):

```yaml
scope:
  - name: "YOUR_SITE"
    description: "Your Site Description"
    locations:
      - name: "YOUR_PLANT"
        description: "Your Plant Description"
        locations:
          - name: "YOUR_AREA"
            description: "Your Area Description"
            locations:
              - name: "YOUR_SYSTEM"
                description: "Your System Description"
                files:
                  - "File-001"
                  - "File-002"
```

### Step 4: Configure Your Patterns

Update the `patterns` section to match your asset tag patterns:

```yaml
patterns:
  - category: equipment
    samples:
      - "P-101"    # Examples of tags that match
      - "V-201"
```

### Step 5: Test and Deploy

1. **Test with a small subset**: Set `limit: 10` to test with 10 files
2. **Review results**: Check the extracted assets
3. **Run full pipeline**: Set `limit: -1` to process all files

## Common Patterns

### Pattern 1: Simple Equipment Tags
```yaml
patterns:
  - category: equipment
    samples:
      - "P-101"      # Pump tags: P-101, P-102, etc.
      - "V-201"      # Valve tags: V-201, V-202, etc.
      - "T-301"      # Tank tags: T-301, T-302, etc.
```

### Pattern 2: ISA51 Standard Tags
```yaml
patterns:
  - category: equipment
    resourceType: major_equipment
    resourceSubType: Rotating_Equipment
    standard: ISA51
    samples:
      - "[C]-00"     # [C] = any letter, matches C-00, D-00, etc.
      - "[C]-000"
```

### Pattern 3: Flexible Patterns
```yaml
patterns:
  - category: general
    samples:
      - "X-00"       # X = any letters, matches P-00, V-00, etc.
      - "XX-00"      # Matches PU-00, VA-00, etc.
```

## Configuration Examples

### Quick Start
- `config.simple.example.yaml` - Working example configuration (quick start)

### Industry-Specific Templates

**Choose the template that matches your industry:**

- `config.template.manufacturing.yaml` - **Manufacturing**: Site → Plant → Area → System
  - For manufacturing facilities, production plants, assembly areas

- `config.template.oil_gas.yaml` - **Oil & Gas**: Site → Facility → Unit → System
  - For refineries, production fields, processing units (aligned with ISO 14224)

- `config.template.utilities.yaml` - **Utilities**: Region → Site → Building → Room → System
  - For power plants, substations, utility facilities

- `config.template.pharmaceuticals.yaml` - **Pharmaceuticals**: Site → Building → Suite → System
  - For pharmaceutical facilities, cleanrooms, GMP-compliant structures

### Production configuration
- `default.config.yaml` — full production example (`file_asset_source.create`, `extract`, `write`)

## Troubleshooting

### Files Not Matching
- **Problem**: Files not assigned to systems
- **Solution**: Check file names match exactly (case-sensitive, without extensions)

### No Assets Extracted
- **Problem**: No assets found in diagrams
- **Solution**: Verify patterns match your tag format, check diagram quality

### Wrong Hierarchy
- **Problem**: Assets in wrong location
- **Solution**: Verify `hierarchy_levels` matches your `locations` structure

## Next Steps

1. Read `docs/CONFIGURATION_GUIDE.md` for detailed configuration options
2. Use the **operator UI**: `docs/guides/howto_config_ui.md` (`python module.py ui` from repo root with `PYTHONPATH=.`)
3. Review `OPTIMIZATION_PLAN.md` for understanding the solution architecture
4. Edit `default.config.yaml` and run `python module.py validate` / `python module.py build`

## Need Help?

- Check the configuration guide: `docs/CONFIGURATION_GUIDE.md`
- Review example: `config.simple.example.yaml`
- See troubleshooting section in configuration guide
