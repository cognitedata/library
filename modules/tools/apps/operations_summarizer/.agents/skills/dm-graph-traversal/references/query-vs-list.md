# Query vs List: Operational Guide

## Principle

`instances/list` is a flat retrieval API.
`instances/query` is a graph traversal API.

If the question is relationship-aware, default to `query`.

## Good Query Triggers

- “For each initiative, load related features and commitments.”
- “For this customer, show connected initiatives with edge attributes.”
- “Walk relation X in reverse to parent entities.”
- “Paginate multiple related result sets reliably.”

## Good List Triggers

- “Get all nodes of one type that match static filters.”
- “No edge traversal or join semantics needed.”

## Why teams regress to list

- Query payloads look verbose.
- Engineers underestimate stitching complexity.
- Subtle query errors (`limit`, `properties`, refs) can look intimidating.

## Avoiding silent regressions

- Add tests that inspect `client.instances.query` payload shape.
- Add one negative assertion that `instances.list` is not called in query-only paths.
- Keep step names explicit and stable for cursor loops and test readability.

## Common anti-patterns

1. Query intent implemented as N+1 list calls.
2. Step limit nested under `nodes` or `edges`.
3. Select sources without explicit `properties`.
4. Non-versioned traversal property refs.
5. Aggregation without dedupe/tie-break policy.

## Practical migration checklist

- Identify relation path in natural language.
- Encode path as `with` steps.
- Add explicit `select.sources.properties`.
- Add step cursor handling.
- Add payload-shape test assertions.
- Remove list fallback if query path is canonical.
