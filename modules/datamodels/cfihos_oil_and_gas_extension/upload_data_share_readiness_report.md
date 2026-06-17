# Upload Data Share-Readiness Report

Scope: `modules/datamodels/cfihos_oil_and_gas_extension/upload_data`

Date: 2026-03-23

## Method

The dataset was checked for:

- Secret patterns (`apiKey`, `token`, `clientSecret`, private keys, bearer/JWT-like strings)
- PII indicators (email, phone, SSN, passport, card, IBAN, SWIFT keywords)
- Confidentiality markers (`confidential`, `proprietary`, `internal use only`, `restricted`)

Result: no high-risk matches found.

## File-by-File Status

### RAW CSV files

- `upload_data/RAW/file.RawRows.csv` — no risk flags
- `upload_data/RAW/tag.RawRows.csv` — no risk flags
- `upload_data/RAW/maintenance_integrity.RawRows.csv` — no risk flags
- `upload_data/RAW/piping_pipeline.RawRows.csv` — no risk flags
- `upload_data/RAW/tool.RawRows.csv` — no risk flags
- `upload_data/RAW/subsea_equipment.RawRows.csv` — no risk flags
- `upload_data/RAW/miscellaneous_equipment.RawRows.csv` — no risk flags
- `upload_data/RAW/it_telecom_equipment.RawRows.csv` — no risk flags
- `upload_data/RAW/infrastructure.RawRows.csv` — no risk flags
- `upload_data/RAW/hse_equipment.RawRows.csv` — no risk flags
- `upload_data/RAW/enclosure.RawRows.csv` — no risk flags
- `upload_data/RAW/drilling_equipment.RawRows.csv` — no risk flags
- `upload_data/RAW/mechanical_equipment.RawRows.csv` — no risk flags
- `upload_data/RAW/electrical_equipment.RawRows.csv` — no risk flags
- `upload_data/RAW/timeseries.RawRows.csv` — no risk flags
- `upload_data/RAW/instrument_equipment.RawRows.csv` — no risk flags
- `upload_data/RAW/heat_exchanger.RawRows.csv` — no risk flags
- `upload_data/RAW/turbine.RawRows.csv` — no risk flags
- `upload_data/RAW/failure_mode.RawRows.csv` — no risk flags
- `upload_data/RAW/pump.RawRows.csv` — no risk flags
- `upload_data/RAW/notification.RawRows.csv` — no risk flags
- `upload_data/RAW/valve.RawRows.csv` — no risk flags
- `upload_data/RAW/compressor.RawRows.csv` — no risk flags
- `upload_data/RAW/work_order_operation.RawRows.csv` — no risk flags
- `upload_data/RAW/work_order.RawRows.csv` — no risk flags
- `upload_data/RAW/functional_location.RawRows.csv` — no risk flags
- `upload_data/RAW/equipment.RawRows.csv` — no risk flags

### RAW manifest files

- `upload_data/RAW/compressor.Manifest.yaml` — no risk flags
- `upload_data/RAW/equipment.Manifest.yaml` — no risk flags
- `upload_data/RAW/maintenance_integrity.Manifest.yaml` — no risk flags
- `upload_data/RAW/piping_pipeline.Manifest.yaml` — no risk flags
- `upload_data/RAW/instrument_equipment.Manifest.yaml` — no risk flags
- `upload_data/RAW/timeseries.Manifest.yaml` — no risk flags
- `upload_data/RAW/electrical_equipment.Manifest.yaml` — no risk flags
- `upload_data/RAW/mechanical_equipment.Manifest.yaml` — no risk flags
- `upload_data/RAW/drilling_equipment.Manifest.yaml` — no risk flags
- `upload_data/RAW/hse_equipment.Manifest.yaml` — no risk flags
- `upload_data/RAW/infrastructure.Manifest.yaml` — no risk flags
- `upload_data/RAW/it_telecom_equipment.Manifest.yaml` — no risk flags
- `upload_data/RAW/miscellaneous_equipment.Manifest.yaml` — no risk flags
- `upload_data/RAW/subsea_equipment.Manifest.yaml` — no risk flags
- `upload_data/RAW/heat_exchanger.Manifest.yaml` — no risk flags
- `upload_data/RAW/valve.Manifest.yaml` — no risk flags
- `upload_data/RAW/enclosure.Manifest.yaml` — no risk flags
- `upload_data/RAW/work_order.Manifest.yaml` — no risk flags
- `upload_data/RAW/tool.Manifest.yaml` — no risk flags
- `upload_data/RAW/tag.Manifest.yaml` — no risk flags
- `upload_data/RAW/notification.Manifest.yaml` — no risk flags
- `upload_data/RAW/pump.Manifest.yaml` — no risk flags
- `upload_data/RAW/failure_mode.Manifest.yaml` — no risk flags
- `upload_data/RAW/file.Manifest.yaml` — no risk flags
- `upload_data/RAW/work_order_operation.Manifest.yaml` — no risk flags
- `upload_data/RAW/functional_location.Manifest.yaml` — no risk flags
- `upload_data/RAW/turbine.Manifest.yaml` — no risk flags

### Other upload files

- `upload_data/RAW/resources/file.Table.yaml` — no risk flags
- `upload_data/Files/myFileMedata.Manifest.yaml` — no risk flags

## Overall Assessment

- Technical share-readiness: safe based on pattern scanning (no detected secrets/PII/confidential markers).
- Residual caveat: legal/license provenance confirmation remains a process decision outside static content scanning.
