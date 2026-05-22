---
name: cognite-dms-queries
description: Search-first, production-safe query patterns for Cognite DMS (Data Modeling Service), including pagination, traversal, retries, and anti-pattern avoidance.
---

# Cognite DMS Queries Skill

Use this skill when creating or reviewing queries against CDF DMS.

## Primary Goal

Produce queries that are:

1. Correct.
2. Efficient at scale.
3. Easy to reason about and maintain.

This skill is opinionated and **search-first**.

## Core Principles

1. **Search first, traverse second**
   - Use `instances.search(...)` to find/rank anchors.
   - Then hydrate related data with `Query` traversal or relation-aware filters.

2. **Constrain early**
   - Always scope with `space` and/or hard filters (`sourceContext`, subtree `Prefix`, etc.) whenever possible.
   - Avoid broad cross-space scans unless explicitly needed for discovery.

3. **Paginate explicitly**
   - Use cursor pagination (`/sync` style) or chunked iterators.
   - Prefer fixed page sizes (for example 500-1000) over large one-shot reads.

4. **Fetch only what you need**
   - In `select`, request explicit property lists.
   - Avoid wildcard projection (`"*"`).

5. **Operational resilience**
   - Retry transient failures (408/425/429/5xx) with bounded exponential backoff + jitter.
   - Keep query payloads and per-page limits moderate to reduce timeout risk.

## Recommended Query Flow

Follow this order by default:

1. Search basics (name-only scoring).
2. Constrained search (`query` + hard filters).
3. Top-K hydrate pattern.
4. Fallback strategy (strict -> broad).
5. Prefix-based subtree retrieval.
6. Graph traversals via `Query`.
7. Cursor pagination for full retrieval.

## Anti-patterns (do not generate unless explicitly requested)

1. `limit=-1` one-shot reads for large datasets.
2. `properties=["*"]` in production retrieval paths.
3. Broad `instances.list(...)` without `space` in production paths.
4. N+1 loops (querying related entities one by one in Python).
5. Client-side filtering when server-side filters can do the job.
6. Missing retry handling for transient API failures.
7. Mixed verbose/raw dumps (`print(res)` / full object dumps) for large responses.
8. Raw HTTP payload posts when SDK APIs provide equivalent capability and retries.

## Output and Style Requirements

When providing query examples:

- Include a short purpose statement.
- Prefer compact output helpers:
  - print count
  - print first N rows (`N=10`) with `externalId`, `name`, `description` (plus optional extra fields)
  - print overflow indicator (`... and N more`)
- Use discovery-first where practical (derive real token/value before applying strict filters).

## Reference

Use the canonical examples in:

- `references/cdf-dms-queries.md`

