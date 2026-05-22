# CDF Explorer

Local read-only browser for CDF **Data** (RAW, Data Modeling, Classic, Transformations), **Orchestration** (Workflows, Pipelines), and **Governance** (spaces, groups). Document tabs cover SQL preview, data model diagrams, and transformation detail. Uses the same `.env` credentials as other library accelerators (`COGNITE_*` / `CDF_*` / `IDP_*`).

**Security:** no authentication on the operator API. Run only on a trusted workstation (`127.0.0.1`).

## Quick start

From the repository root:

```bash
export PYTHONPATH=.
pip install -r modules/accelerators/contextualization/cdf_explorer/requirements.txt
python modules/accelerators/contextualization/cdf_explorer/module.py ui
```

Default ports: API **8785**, Vite **5193** (so it can run beside cdf_discovery_aliasing and cdf_access_control).

See [docs/howto_ui.md](docs/howto_ui.md) for navigation (Object Explorer, document tabs, SQL query preview, Properties panel).
