# Scoped matching test resources

This folder holds the **test-only** site/unit data model and the transformations that
populate it. It is not a Toolkit resource directory, so `cdf build` / `cdf deploy`
ignore it until you copy the files into the module.

The module default is unscoped matching against `cdf_cdm` `CogniteAsset` and
`CogniteTimeSeries`. Use the helper when you want to exercise primary/secondary scope
in CDF without an enterprise model that already has `site` and `unit`.

## Enable (copy into deploy folders and patch config)

From the repository root:

```bash
python modules/contextualization/cdf_entity_matching/testing/apply_scoped_matching.py
```

That copies the payload into `data_modeling/` and `transformations/`, and sets
`default.config.yaml` to the scoped views (`Asset` / `ScopedTimeSeries`,
`primaryScopeProperty: site`, `secondaryScopeProperty: unit`). Then build and deploy
as usual.

## Disable (restore CDM defaults)

```bash
python modules/contextualization/cdf_entity_matching/testing/apply_scoped_matching.py --revert
```

Removes the copied files and points `default.config.yaml` back at `CogniteAsset` /
`CogniteTimeSeries` with empty scope properties.

## After deploy

1. In CDF → Transformations, run `tr_asset_names_to_scope` and `tr_timeseries_names_to_scope`
   (preview first). They expect a **single** instance-space string in `assetInstanceSpace`.
2. Confirm assets and time series show `site` / `unit` under `EntityMatchingScope_SOL`.
3. Run the entity matching workflow; submit logs should show one predict job per
   `(site, unit)` batch. Time series with neither `site` nor `unit` are matched against
   **all** assets (`Entities without scope … tried matched against all Targets`). A
   non-empty site/unit with no assets is skipped.

Name parsing:

| Name | `site` | `unit` |
|---|---|---|
| `VAL_23-KA-9101` | `VAL` | `23` |
| `VAL_23-KA-9101:X.Value` | `VAL` | `23` |
| `23-KA-9101` | *(null)* | `23` |
