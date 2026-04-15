# Schema changes, view versions, and data model references

Distilled for Toolkit YAML workflows. For **index and btree rules**, use `cdf-data-model-indexes.md`. Do **not** use outdated “10 indexes per container” figures from older guides — CDF allows **20** btree indexes per container (`usedFor: node`).

## View `version` bumps

When you change a view in a **breaking** way (delete/remap properties, change types, change `implements`, change relation shape), bump **`version`** in the view YAML and update **every** reference to that view:

1. The **`*.View.yaml`** file’s top-level `version`.
2. Any other view that references this view in **`source`**, **`through`**, or **`implements`** (match `version` to the deployed view).
3. Every **`*.DataModel.yaml`** entry under `views:` for this `externalId` / `space`.

Missing one of these produces stale GraphQL / deployment errors.

## Data model `views:` entries

Each list item is a **view reference** only: `space`, `externalId`, `version`, `type: view`. Cognite Toolkit treats any other field (e.g. `name`) as invalid on those entries. Put the human-readable **`name`** on the **`*.View.yaml`** resource itself, not on the data model’s `views:` list.

## Containers (long-lived schema)

Containers are **not** versioned like views. Treat deployed container schema as **additive** where possible:

- **Avoid** deleting properties or changing property **type**, **`list`**, **`usedFor`**, or direct-relation target — these are destructive or disallowed paths that require migration (export, delete container, recreate, re-ingest).
- **Safer:** add new properties, adjust **name** / **description**, add nullable fields, add indexes/constraints per current CDF rules (see indexes reference).

Confirm current CDF rules with **`SearchCogniteDocs`** or `cdf build` before advising a specific migration.

## `requires` / index lifecycle

Adding or changing **`requires`** and **indexes** can be processed asynchronously in CDF. If ingest or queries behave oddly right after a deploy, check instance/constraint state in the API or UI before assuming misconfiguration.

## Uniqueness

Use **`constraintType: uniqueness`** on business keys when one container must enforce a unique combination of scalar properties — in addition to btree indexes used for lookup and filters.
