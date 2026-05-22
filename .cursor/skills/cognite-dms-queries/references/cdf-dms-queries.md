# CDF DMS Query Patterns (Search-First)

This reference documents recommended patterns for writing efficient, production-safe DMS queries in CDF.

The examples assume:

- `client: CogniteClient`
- `asset_vid`, `ts_vid`, `eq_vid`, `wo_vid` as `ViewId`
- `INST_SP` as the instance space

---

## Shared Helpers

```python
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter
from cognite.client.exceptions import CogniteAPIError

RETRYABLE_CODES = {408, 425, 429, 500, 502, 503, 504}

def is_retryable_exception(e: BaseException) -> bool:
    return isinstance(e, CogniteAPIError) and e.code in RETRYABLE_CODES

retry_cognite = retry(
    reraise=True,
    stop=stop_after_attempt(5),
    retry=retry_if_exception(is_retryable_exception),
    wait=wait_exponential_jitter(initial=1, max=30, jitter=2),
)

@retry_cognite
def run_query(client, query):
    return client.data_modeling.instances.query(query=query)

@retry_cognite
def run_sync(client, query):
    return client.data_modeling.instances.sync(query=query)

def summarize(label, nodes, vid, max_rows=10, extra_fields=None):
    from itertools import islice
    print(f"{label} (count={len(nodes)}):")
    for n in islice(nodes, max_rows):
        props = n.properties.get(vid, {}) or {}
        parts = [
            n.external_id,
            f"name={props.get('name')!r}",
            f"description={props.get('description')!r}",
        ]
        for key in (extra_fields or []):
            parts.append(f"{key}={props.get(key)!r}")
        print("  " + "  ".join(parts))
    if len(nodes) > max_rows:
        print(f"   ... and {len(nodes) - max_rows} more")
```

---

## 1) Search Basics (Name-only Scoring)

Use search to find anchor nodes quickly. Limit scoring fields for better relevance and lower cost.

```python
sample = client.data_modeling.instances.list(sources=asset_vid, space=INST_SP, limit=10)
sample_name = next(
    ((n.properties.get(asset_vid, {}) or {}).get("name")
     for n in sample
     if (n.properties.get(asset_vid, {}) or {}).get("name")),
    None,
)
if not sample_name:
    raise RuntimeError("No assets with name found in INST_SP")

search_term = sample_name[:-2] if len(sample_name) > 2 else sample_name
hits = client.data_modeling.instances.search(
    view=asset_vid,
    space=INST_SP,
    query=search_term,
    properties=["name"],  # score only on name
    limit=25,
)
summarize("hits", hits, asset_vid)
```

---

## 2) Constrained Search (Query + Hard Filter)

Combine relevance (`query`) with strict constraints (`filter`, `space`) for precision and lower backend load.

```python
probe = client.data_modeling.instances.list(sources=asset_vid, space=INST_SP, limit=200)
source_context = next(
    ((n.properties.get(asset_vid, {}) or {}).get("sourceContext")
     for n in probe
     if (n.properties.get(asset_vid, {}) or {}).get("sourceContext")),
    None,
)
if source_context is None:
    raise RuntimeError("No populated sourceContext found in INST_SP")

token = search_term[:4] if search_term else ""
scoped_hits = client.data_modeling.instances.search(
    view=asset_vid,
    space=INST_SP,
    query=token,
    properties=["name"],
    filter=Equals(asset_vid.as_property_ref("sourceContext"), source_context),
    limit=25,
)
summarize("scoped_hits", scoped_hits, asset_vid)
```

---

## 3) Top-K Hydrate (Search Anchors -> Related Retrieval)

Use top-ranked search results as anchors, then retrieve related entities.

```python
from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.filters import ContainsAny

top_hits = client.data_modeling.instances.search(
    view=asset_vid,
    space=INST_SP,
    query=token,
    properties=["name"],
    limit=10,
)
summarize("top_hits", top_hits, asset_vid)

asset_refs = [DirectRelationReference(n.space, n.external_id) for n in top_hits]
if asset_refs:
    ts_related = client.data_modeling.instances.search(
        view=ts_vid,
        space=INST_SP,
        query=None,
        filter=ContainsAny(ts_vid.as_property_ref("assets"), asset_refs),
        limit=100,
    )
    summarize("related_timeseries", ts_related, ts_vid, extra_fields=["unit"])
```

---

## 4) Fallback (Strict -> Broad)

Apply strict constraints first; if no hits, broaden in a controlled way.

```python
strict_hits = client.data_modeling.instances.search(
    view=asset_vid,
    space=INST_SP,
    query=token,
    properties=["name"],
    filter=Equals(asset_vid.as_property_ref("sourceContext"), source_context),
    limit=25,
)
if strict_hits:
    summarize("strict_hits", strict_hits, asset_vid)
else:
    broad_hits = client.data_modeling.instances.search(
        view=asset_vid,
        space=INST_SP,
        query=token,
        properties=["name"],
        limit=25,
    )
    summarize("broad_hits", broad_hits, asset_vid)
```

---

## 5) Subtree Retrieval with Prefix (Recommended)

For hierarchy retrieval, use `Prefix(path)` rather than broad path membership scans.

```python
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.filters import Prefix

# derive a root from real data
probe = client.data_modeling.instances.list(sources=asset_vid, space=INST_SP, limit=200)
sub_tree_root = None
for node in probe:
    parent = (node.properties.get(asset_vid, {}) or {}).get("parent")
    if parent and parent.get("space") and parent.get("externalId"):
        sub_tree_root = NodeId(parent["space"], parent["externalId"])
        break
if sub_tree_root is None:
    raise RuntimeError("No parent relation found to derive subtree root")

root = client.data_modeling.instances.retrieve_nodes(sub_tree_root, sources=asset_vid)
root_path = root.properties.data[asset_vid]["path"]

sub_tree_nodes = client.data_modeling.instances.list(
    sources=asset_vid,
    filter=Prefix(property=asset_vid.as_property_ref("path"), value=root_path),
    limit=500,
)
summarize("sub_tree_nodes_prefix", sub_tree_nodes, asset_vid)
```

---

## 6) Graph Traversals (Query API)

Use server-side traversal in one query (parent/children, nested direct-relation filtering).

```python
from cognite.client.data_classes.data_modeling.query import Query, NodeResultSetExpression, Select, SourceSelector
from cognite.client.data_classes.filters import And, Equals, Nested, ContainsAll

ASSET_PROPS = ["name", "description", "parent", "tags"]
asset_eid = top_hits[0].external_id

query = Query(
    with_={
        "asset": NodeResultSetExpression(
            filter=And(
                Equals(("node", "externalId"), value=asset_eid),
                Equals(("node", "space"), value={"parameter": "space"}),
            )
        ),
        "parent": NodeResultSetExpression(from_="asset", through=asset_vid.as_property_ref("parent"), direction="outwards"),
        "children": NodeResultSetExpression(from_="asset", through=asset_vid.as_property_ref("parent"), direction="inwards"),
    },
    select={
        "asset": Select([SourceSelector(asset_vid, ASSET_PROPS)]),
        "parent": Select([SourceSelector(asset_vid, ASSET_PROPS)]),
        "children": Select([SourceSelector(asset_vid, ASSET_PROPS)]),
    },
    parameters={"space": INST_SP},
)
res = run_query(client, query)
summarize("asset", res["asset"], asset_vid)
summarize("parent", res["parent"], asset_vid)
summarize("children", res["children"], asset_vid)

nested_query = Query(
    with_={
        "asset": NodeResultSetExpression(
            filter=Nested(
                scope=asset_vid.as_property_ref("parent"),
                filter=ContainsAll(property=asset_vid.as_property_ref("tags"), values=["functional-location"]),
            ),
            limit=100,
        )
    },
    select={"asset": Select([SourceSelector(asset_vid, ["name", "description", "tags", "parent"])])},
)
nested_res = run_query(client, nested_query)
summarize("nested_assets", nested_res["asset"], asset_vid)
```

---

## 7) Cursor Pagination (Safe Full Retrieval)

Use `/sync` in pages. Avoid one-shot full reads for large volumes.

```python
def get_data(client: CogniteClient, query: Query, max_iterations: int | None = 100):
    collected_data = defaultdict(list)
    current_iteration = 0
    if max_iterations is None or max_iterations == -1:
        max_iterations = float("inf")

    while current_iteration < max_iterations:
        res = run_sync(client, query)
        if res is None:
            return collected_data, {}
        if all(not res.data[k] for k in res.data):
            return collected_data, {}

        for key in res.data:
            collected_data[key].extend(res.data[key])
        query.cursors = res.cursors
        current_iteration += 1

    return collected_data, (res.cursors if res is not None else {})

PAGE_SIZE = 1000
all_assets_query = Query(
    with_={
        "assets": NodeResultSetExpression(
            filter=And(
                Equals(["node", "space"], value={"parameter": "space"}),
                HasData(views=[asset_vid]),
            ),
            limit=PAGE_SIZE,
        )
    },
    select={"assets": Select([SourceSelector(asset_vid, ["name", "description", "tags"])])},
    parameters={"space": INST_SP},
)

all_assets = []
page = 0
while True:
    res = run_sync(client, all_assets_query)
    batch = res.data.get("assets", [])
    if not batch:
        break
    page += 1
    all_assets.extend(batch)
    print(f"Page {page}: fetched {len(batch)} rows (running total={len(all_assets)})")
    all_assets_query.cursors = res.cursors

summarize("all_assets", all_assets, asset_vid)
```

---

## Anti-patterns (What Not To Do)

### 1) One-shot full reads

Bad:

```python
client.data_modeling.instances.list(sources=asset_vid, space=INST_SP, limit=-1)
```

Why bad:
- Large payloads.
- Timeout/throttling risk.
- Harder to retry safely.

Use:
- Cursor pagination (`run_sync`) or chunked iterator patterns.

### 2) Wildcard projection

Bad:

```python
Select([SourceSelector(asset_vid, ["*"])])
```

Why bad:
- Over-fetching fields you do not use.
- Bigger responses and slower queries.

Use:
- Explicit properties only.

### 3) Broad scans without scope

Bad:

```python
client.data_modeling.instances.list(sources=asset_vid, limit=5000)
```

Why bad:
- Cross-space scanning is expensive.

Use:
- `space=INST_SP` and relevant filter constraints.

### 4) N+1 retrieval loops

Bad:

```python
for hit in hits:
    # per-item related retrieval
    ...
```

Why bad:
- Many round-trips.

Use:
- Top-K hydrate with batched relation-aware filters or single `Query` traversal.

### 5) No transient retry handling

Bad:
- Running high-volume retrieval without retry strategy.

Use:
- Retry on 408/425/429/5xx with bounded exponential backoff + jitter.

