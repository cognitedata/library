# Custom handlers (discovery pipeline)

The **`fn_dm_key_extraction`** / **`fn_dm_aliasing`** CDF functions and their standalone Python engine packages are not part of this module. Discovery workflows now run **`fn_dm_transform`**, **`fn_dm_validate`**, query/save/join stages such as **`fn_dm_view_query`** / **`fn_dm_view_save`**, and related **`fn_dm_*`** executors compiled from the UI **`canvas`** — see [`functions/README.md`](../../functions/README.md).

## Adding behaviour

- **Transforms and field logic** — implement or extend handlers under **`functions/fn_dm_transform/`** (handler registry and engine live beside `handler.py`). Keep handler IO aligned with discovery RAW row shapes produced by query stages (**`fn_dm_view_query`**, **`fn_dm_raw_query`**, **`fn_dm_classic_query`**, …).
- **Validation / confidence** — extend **`functions/fn_dm_validate/`** and wire rules through canvas **`validation`** nodes and scope **`key_extraction.config.data.validation`** where the trimmer preserves those definitions.
- **DM apply** — **`functions/fn_dm_view_save/`** applies predecessor payloads; adjust merge / property mapping there when you need new write-back semantics (or extend **`fn_dm_raw_save`** / **`fn_dm_classic_save`** when the canvas uses those kinds).

After changing handler code, **redeploy** the affected Cognite Functions (see **`functions/functions.Function.yaml`** and **`python module.py deploy-scope`**).

For YAML semantics of extraction and aliasing rule libraries still carried on the v1 scope document, see [Key extraction spec](../specifications/1.%20key_extraction.md) and [Aliasing spec](../specifications/2.%20aliasing.md).

**See also:** [Quickstart — `module.py`](howto_quickstart.md), [Scoped deployment](howto_scoped_deployment.md).
