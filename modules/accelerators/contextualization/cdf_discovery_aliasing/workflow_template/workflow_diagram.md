# Key Discovery and Aliasing Workflow

## Workflow Diagram

The diagram below is the maintained source (no committed PNG). Render it in any Mermaid-capable viewer or GitHub preview.

### Mermaid Source Code

```mermaid
graph TD
    Start([Start Workflow]) --> Query[View query<br/>fn_dm_view_query]

    Query --> RawCohort[RAW: cohort rows<br/>RUN_ID + instance payloads]

    RawCohort --> Transform[Transform<br/>fn_dm_transform]

    Transform --> RawT[RAW: transform sink]

    RawT --> Validate[Validation<br/>fn_dm_validate]

    Validate --> RawV[RAW: validation sink]

    RawV --> RefIndex[Inverted index<br/>fn_dm_inverted_index]

    RawV --> Save[View save<br/>fn_dm_view_save]

    RefIndex --> RawRefIdx[RAW: inverted index]

    Save --> DM[instances.apply<br/>CogniteDescribable]

    Save --> Cleanup[RAW cleanup<br/>fn_dm_discovery_raw_cleanup]

    DM --> End([End])
    RawRefIdx --> Cleanup
    Cleanup --> End

    style Query fill:#e1f5ff,stroke:#01579b,stroke-width:2px
    style Transform fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style Validate fill:#fce4ec,stroke:#880e4f,stroke-width:2px
    style Save fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style RefIndex fill:#e8eaf6,stroke:#283593,stroke-width:2px
    style RawCohort fill:#fff9c4,stroke:#f57f17,stroke-width:2px
    style RawT fill:#fff9c4,stroke:#f57f17,stroke-width:2px
    style RawV fill:#fff9c4,stroke:#f57f17,stroke-width:2px
    style RawRefIdx fill:#fff9c4,stroke:#f57f17,stroke-width:2px
```

**Macro execution graph (Kahn network, `dependsOn` SSOT):** [`workflow.execution.graph.yaml`](workflow.execution.graph.yaml) and [`workflow_channel_contracts.md`](workflow_channel_contracts.md).

## Detailed flow (short)

1. **Query** — `fn_dm_view_query` lists DM instances (with incremental watermarks when enabled) and writes cohort rows to discovery RAW.
2. **Transform** — `fn_dm_transform` applies YAML-driven rules from each canvas node’s `data.config`.
3. **Validate** — `fn_dm_validate` scores / filters payloads and writes validation sink RAW.
4. **Inverted index (optional)** — `fn_dm_inverted_index` consumes predecessor task snapshots (IR) and/or configured RAW columns.
5. **Save** — `fn_dm_view_save` applies predecessor payloads to `cdf_cdm:CogniteDescribable` (aliases, optional FK strings).
6. **Cleanup (optional)** — `fn_dm_discovery_raw_cleanup` truncates or deletes cohort keys post-run.

## Data flow (logical)

```
DM list → fn_dm_view_query → RAW cohort
         → fn_dm_transform → RAW transform
         → fn_dm_validate → RAW validation
         → fn_dm_inverted_index → RAW index (branch)
         → fn_dm_view_save → DM apply
         → fn_dm_discovery_raw_cleanup (optional)
```

Authoring still uses the v1 scope document (`key_extraction`, `aliasing`, `canvas`); the **compiled canvas** selects which `fn_dm_*` executors run and in what order. Besides **`fn_dm_view_query`** / **`fn_dm_view_save`**, the graph may use **`fn_dm_raw_query`**, **`fn_dm_classic_query`**, **`fn_dm_raw_save`**, **`fn_dm_classic_save`**, **`fn_dm_join`**, and **`fn_dm_discovery_raw_cleanup`** — see [`functions/README.md`](../functions/README.md).
