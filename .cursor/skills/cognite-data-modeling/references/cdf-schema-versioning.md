# Schema changes, view versions, and data model references

Distilled for Toolkit YAML workflows. For **index and btree rules**, use `cdf-data-model-indexes.md` (max **10** btree indexes per `usedFor: node` container).

## View `version` bumps

When you change a view in a **breaking** way (delete/remap properties, change types, change `implements`, change relation shape), bump **`version`** in the view YAML and update **every** reference to that view:

1. The **`*.View.yaml`** file’s top-level `version`.
2. Any other view that references this view in **`source`**, **`through`**, or **`implements`** (match `version` to the deployed view).
3. Every **`*.DataModel.yaml`** entry under `views:` for this `externalId` / `space`.

Missing one of these produces stale GraphQL / deployment errors.

## Data model `views:` entries

Each list item is a **view reference** only: `space`, `externalId`, `version`, `type: view`. Cognite Toolkit treats any other field (e.g. `name`) as invalid on those entries. Put the human-readable **`name`** on the **`*.View.yaml`** resource itself, not on the data model’s `views:` list.

## Containers (long-lived schema)

Containers are **not** versioned like views — they are the **durable contract** between enterprise and solution layers. Treat deployed container schema as **additive** where possible:

- **Avoid** deleting properties or changing property **type**, **`list`**, **`usedFor`**, or direct-relation target — these are destructive or disallowed paths that require migration (export, delete container, recreate, re-ingest).
- **Safer:** add new properties, adjust **name** / **description**, add nullable fields, add indexes/constraints per current CDF rules (see indexes reference).

Because containers are unversioned, mapping a solution view to an enterprise **container** (`container:` + `containerPropertyIdentifier:`) decouples the solution from enterprise view version churn. Mapping (or `implements:`) to an enterprise **view** re-couples you to that view's lifecycle. See `cdf-enterprise-vs-solution.md` §2–§3.

Confirm current CDF rules with **`SearchCogniteDocs`** or `cdf build` before advising a specific migration.

## `requires` / index lifecycle

Adding or changing **`requires`** and **indexes** can be processed asynchronously in CDF. If ingest or queries behave oddly right after a deploy, check instance/constraint state in the API or UI before assuming misconfiguration.

## Uniqueness

Use **`constraintType: uniqueness`** on business keys when one container must enforce a unique combination of scalar properties — in addition to btree indexes used for lookup and filters.
