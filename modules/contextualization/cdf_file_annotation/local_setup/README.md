# Scoped matching test resources

This folder holds the **test-only** site/unit data model and the transformations that
populate it. It is not a Toolkit resource directory, so `cdf build` / `cdf deploy`
ignore it until you copy the files into the module.

The module default is unscoped matching against `cdf_cdm` `CogniteFile` and
`CogniteAsset`. Use the helper when you want to exercise primary/secondary scope
in CDF without an enterprise model that already has `site` and `unit`.

## Enable (copy into deploy folders and patch config)

From the repository root:

```bash
python modules/contextualization/cdf_file_annotation/local_setup/apply_scoped_matching.py
```

Or from this folder:

```bash
python .\apply_scoped_matching.py
```

That copies the payload into `data_modeling/` and `transformations/`, and sets
`default.config.yaml` to the scoped views (`ScopedFile` / `Asset`,
`primaryScopeProperty: site`, `secondaryScopeProperty: unit`). Then build and deploy
as usual.

## Disable (restore CDM defaults)

```bash
python modules/contextualization/cdf_file_annotation/local_setup/apply_scoped_matching.py --revert
```

Removes the copied files and points `default.config.yaml` back at `CogniteFile` /
`CogniteAsset` with empty scope properties.

## After deploy

1. In CDF → Transformations, run `tr_asset_names_to_scope` and `tr_file_names_to_scope`
   (preview first). They expect a **single** instance-space string in
   `targetEntityInstanceSpace` / `fileInstanceSpace` (not empty multi-space mode).
2. Confirm assets and files show `site` / `unit` under `FileAnnotationScope_SOL`.
3. Run the file annotation workflow; Launch logs should show one detect batch per
   `(site, unit)` group. Files with neither `site` nor `unit` are matched against
   **all** tagged entities. Assets without scope are included in every file batch.

Name parsing:

| Name | `site` | `unit` |
|---|---|---|
| `VAL_23-KA-9101` | `VAL` | `23` |
| `VAL_23-PID-001.pdf` | `VAL` | `23` |
| `23-KA-9101` | *(null)* | `23` |
