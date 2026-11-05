---
name: tygen
description: Generate a typed client for one CDF data model in a Dune or Flows app, and use it to search, aggregate and query instances with the model's property names and types. Use only when the TyGen alpha flag is explicitly enabled.
allowed-tools: Read, Glob, Grep, Edit, Write, Bash
---

# TyGen

TyGen generates a typed client from one version of a CDF data model. Use it to read instances with short property names and result types that follow the request, instead of the nested `properties[space]['View/version']` objects of the native `@cognite/sdk`.

## Availability

TyGen is experimental and is not part of the normal app workflow. Before suggesting, running, or editing code that depends on generated output, verify that `COGNITE_ALPHA_ENABLE_TYGEN` is explicitly set to `true` for the command environment. If it is not enabled, explain that TyGen is an opt-in alpha capability and continue with the native `@cognite/sdk` types and APIs already used by the app.

If you introduce TyGen as an option, confirm the user wants to use it before running it. An explicit request to generate types is sufficient confirmation. Do not enable the flag, add it to a repository, or change deployment configuration without the user's explicit approval.

Without the flag the CLI reports `unknown command 'tygen'`. That means the flag is not set, not that the CLI is too old.

## Generate

From the app directory (it has an `app.json`):

```bash
npx @cognite/cli@latest tygen generate --data-model mySpace:myModel:v1
```

- In an interactive terminal, omit `--data-model` to pick a versioned model from the project. `--interactive` controls browser authentication, independently of the model picker.
- The command uses the app configuration and the saved `cognite auth login` session. Use `--base-url` and `--project` to generate outside an app directory.
- When the data-model reference is unknown, `cognite api datamodels list` lists the models. It requires the separate `COGNITE_ALPHA_ENABLE_API=true` flag; check that it is already enabled before running it.
- Output goes to `src/generated_types/<externalId>/`: `index.ts` (the entry point), `types.ts`, `views.ts`, `tygen-views.ts`, `tygen-interface.ts`, `methods/` and `internal/`. Do not write application code there, and do not edit generated files.
- Generating again into the same folder replaces it. To keep two versions of a model side by side, generate the second with `-o src/generated_types/<externalId>-<version>`.
- Commit the generated output. After a data-model change, regenerate, inspect the diff, and run the app's typecheck and tests.

Generated code needs TypeScript 5.4 or later.

## Use

Import from the generated `index.ts`, and bind the client once:

```ts
import { initTygen } from './generated_types/PlantModel';

const tygen = initTygen(client); // client from useCogniteSdk()
```

Each view of the model is a property of `tygen`, such as `tygen.equipment`, with `search`, `aggregate` and `query`. A view that only edges use has no `query`. In `filter`, `sort`, `select` and `groupBy`, a property is just its name, such as `'status'`. `filter` and `sort` also take an instance field such as `'externalId'`; `select` only takes the view's properties. A typo is a compile error.

### Search

Full-text search over one view. Items carry the view's properties directly in `item.properties`.

```ts
const found = await tygen.equipment.search({
  query: 'pump',
  properties: ['name'], // properties to search in
  filter: { equals: { property: 'status', value: 'active' } },
  limit: 10,
});
found.items.map((item) => item.properties.name);
```

Search is eventually consistent: writes made right before may not be found yet. Use `query` when that matters.

### Aggregate

```ts
const counts = await tygen.equipment.aggregate({ groupBy: ['isCritical'], aggregates: [{ count: {} }] });
for (const item of counts.items) {
  const isCritical = item.group?.isCritical; // typed from the model
  const count = item.aggregates[0]?.aggregate === 'count' ? item.aggregates[0].value : undefined;
}
```

`groupBy` only accepts text, number, boolean and direct-relation properties, lists of text or direct relations, and `externalId`, `space` and `version`. The API rejects enum, timestamp, date, JSON, file, time series and sequence properties, lists of numbers or booleans, and `createdTime`/`lastUpdatedTime`, so the types do too. To count by an enum, run one `aggregate` per value, with a `count` and an `equals` filter on that value.

### Query

`query` reads one view, with an exact, up-to-date result. It can return selected properties only, filter on a related instance, and return the instances a direct relation points to.

```ts
const page = await tygen.workOrder.query({
  filter: { nested: { scope: 'mainAsset', filter: { equals: { property: 'name', value: '43-PT-1027D' } } } },
  select: ['name', 'status'],
  include: { mainAsset: { select: ['name'] }, affectedLocations: { select: ['name'] } },
  limit: 20,
});

const rows = page.items.map((item) => ({
  ...item.properties, // Pick<WorkOrder, 'name' | 'status'>
  location: item.related.mainAsset?.properties?.name, // one relation: an instance or undefined
  affected: item.related.affectedLocations.map((l) => l.properties?.name), // a list relation: an array
}));
```

- `select` lists the properties to return. Without it, all of them are returned.
- `nested.scope` and the keys of `include` are direct relations of the view, to another view of the model. Their `filter` and `select` use the property names of the target view. A related instance may have no data in the target view, so its `properties` may be `undefined`.
- An `include` entry may be `undefined`, such as `include: { mainAsset: showAsset ? {} : undefined }`. Then `related.mainAsset` is optional.
- `include` returns at most 10,000 related instances per relation, across all items. With more, the query throws. Lower `limit`.
- `include` also reads the target view. Without read access to it or its space, the whole query is likely to fail with a 403, not return fewer related instances. A query that works without `include` and fails with it points to this, or to the 10,000 limit.
- Items that point to the same instance share one related object. Copy it before changing it.
- It returns 100 items by default and has no cursor. To page, or to follow inward relations or edges, use `tygen.query`.

### Raw typed query

`tygen.query` takes a native `instances.query` request with full property paths, `[space, 'View/version', 'property']`, and types the result from it. Use it for paging with cursors, inward relations, edges and set operations. Its results keep the native nesting under space and `View/version`.

## Patterns

- **Build a filter in steps** by spreading in the conditions that are set, each ending in `as const` so its property names stay literal:
  ```ts
  const conditions = [
    ...(type ? [{ equals: { property: 'workOrderType', value: type } } as const] : []),
    ...(location ? [{ nested: { scope: 'mainAsset', filter: { equals: { property: 'name', value: location } } } } as const] : []),
  ];
  await tygen.workOrder.query({ filter: { and: conditions }, select: ['name'] });
  ```
  The generated SDK exports no per-view filter type to annotate the array with. Without `as const`, the property names widen to `string`, and the call fails with a long `StrictKeys` error. A misspelled property is still a compile error. When no condition is set, `conditions` is empty, which is fine, see below.
- **An empty `and`** is treated as no filter, and an empty `or` returns no items without a request.
- **Requests kept in a variable** need `as const`, or `as const satisfies QueryInput` for `tygen.query`.
- **Optional parts of a request**, such as a cursor, are spread in: `...(cursor ? { cursors: { orders: cursor } } : {})`.
- **Relations to views outside the model**, such as `CogniteSourceSystem` from a model that does not include it, have no short `scope` or `include` name. Use `tygen.query` with full paths.

## Known API limits

- A `nested` filter with `prefix` on a large view can time out (408). Prefer `equals`, or filter the target view first.
- A `sort` on a property without an index, or a `query` over a very large view, can time out. Search is usually safer when you do not control the indexes.
- The query API rejects `in` on an enum property. Use `equals`, or an `or` of `equals`.
- The API cannot traverse a list of direct relations inwards.
- Reverse relations and edges are not in the per-view types; use `tygen.query`.
- `instances.list` and writes stay on the native `@cognite/sdk`. Pass generated view references to it (`sources: [{ source: EquipmentView }]`).

## Explore data from the terminal

With `COGNITE_ALPHA_ENABLE_API=true`, `cognite api instances search|aggregate|query --view space:externalId/version` runs the same requests with short property names and prints a table, or JSON with `--json`. Without `--view`, an interactive terminal lets you pick the data model and view. Use it to check property names and values before writing app code.
