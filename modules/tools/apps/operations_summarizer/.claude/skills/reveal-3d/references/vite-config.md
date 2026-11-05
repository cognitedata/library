# Vite Configuration for @cognite/reveal-widget

Unlike the old app-local copied-bundle approach, `@cognite/reveal-widget` is a regular published npm package. No `process`/`util`/`assert` polyfills, no `main.tsx` polyfill lines, and no `vite-plugin-node-polyfills` are needed.

## The only required setting: dedupe singletons

Add `three` and `@cognite/reveal` to `resolve.dedupe` alongside whatever the app already dedupes:

```typescript
export default defineConfig({
  // ...existing config
  resolve: {
    // ...existing aliases
    dedupe: [
      // ...app's existing dedupe entries (react, react-dom, etc.)
      'three',
      '@cognite/reveal',
    ],
  },
});
```

Without this, a bundler that doesn't already dedupe singletons across the dependency tree can end up with two copies of `three` or `@cognite/reveal` (one from the app, one from `@cognite/reveal-widget`'s dependency tree), producing "Multiple instances of Three.js" warnings and broken rendering.

If the app's `vite.config.ts` already dedupes `three` (e.g. from a prior Reveal integration), just confirm `@cognite/reveal` is present too.

Note that `resolve.dedupe` only collapses multiple installed copies *of the same resolved version* — it can't fix an actual version split (see the `@cognite/reveal` pin note in [SKILL.md](../SKILL.md#dependencies)).

## Nothing else to configure

Do **not** carry over any of the following from an old copied-bundle Reveal integration when migrating to `@cognite/reveal-widget`:

| Old requirement (copied-bundle approach) | Still needed for `@cognite/reveal-widget`? |
|---|---|
| `process` polyfill as the first lines of `main.tsx` | No |
| `resolve.alias` for `util`, `assert`, `process` | No |
| `process`, `util`, `assert`, `ajv` as app dependencies | No |
| `resolve.alias.three` pointing at `node_modules/three/...` | No — `resolve.dedupe` is sufficient |
| `optimizeDeps.include` listing `process`/`util`/`assert`/`three`/`@cognite/reveal` | No |
| `worker: { format: 'es' }` | Not required by the package's own build; keep it only if the app has other reasons to need ES worker output |
| `vite-plugin-node-polyfills` | No — never needed for this package |

If migrating an app that has these from a prior integration, it's safe to remove them once the copied bundle is deleted and `@cognite/reveal-widget` is the only Reveal integration left in the app — but double-check nothing else in the app still relies on them first.

## Common mistakes

| Mistake | Symptom | Fix |
|---|---|---|
| Missing `three`/`@cognite/reveal` in `resolve.dedupe` | "Multiple instances of Three.js" warning, broken/blank rendering | Add both to `resolve.dedupe` |
| Mismatched `@cognite/reveal` peer version | Type errors or runtime API mismatches | `@cognite/reveal-widget` requires an exact `@cognite/reveal` version match — pin `4.36.0` (see [SKILL.md](../SKILL.md#dependencies)) |
| `RevealWidget`'s container has no height | Canvas collapses to 0px, nothing renders | Give the parent element an explicit height (`70vh`, `100vh`, `h-full` on a sized ancestor, etc.) |
| Nesting `RevealWidget` inside another Reveal provider from this package | Duplicate/conflicting Reveal context | `RevealWidget` manages its own context — mount it directly, don't wrap it |
