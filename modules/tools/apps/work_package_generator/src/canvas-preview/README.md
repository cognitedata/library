# Canvas preview

This tree is the subset the work-package create-step preview imports:
- `canvas-preview-lib` — `CanvasPreviewProvider`, `PreviewCanvas`, and `aggregateCanvasNodes`.
- `canvas-lib` — read-only canvas (pan, zoom, fit-on-load, zoom toolbar). Drawing tools and the PDF export pipeline are not included.
- `canvas-plugins` registered in `canvas-preview-lib/src/register.ts`: text, sticky, label, comment, shape, polyline, svg, image, cause-map, pdf, timeseries, table, and video. 3D model instances render as table nodes. Charts, frames, containers, property lists, and data grids are not included.
- `canvas-app` resolution hooks and `adapters/dataSources.ts`. File and time-series data is loaded through `CogniteClient`.

`@dune-canvas/*` in `vite.config.ts` and `tsconfig.json` points at this folder. Biome and ESLint include it. Vendored canvas interaction rules (SVG titles, static click targets, non-null assertions, hook dependency lists, unused render props) are turned off in `biome.json` under `overrides` for `src/canvas-preview/**`, because this tree draws a diagram rather than app chrome. CDF reads go through `schedule` from `src/canvas-preview/cdfSchedule.ts`, which uses `cdfTaskRunner`. Vitest coverage includes this folder. Re-run `npx knip` after changing this tree to see what has become unused.

pdfjs-dist 6: `PDFRenderService.clearCache` calls `loadingTask.destroy()`, which is still the public API. It does not call `PDFDocumentProxy.destroy()`.

## Vendored, read-only

These files are the create-step preview surface copied from Industrial Canvas. Treat them as read-only except for preview-host fixes (CDF scheduling, tests, types). They are not split, even when a file mixes state, effects, and rendering:

| File                                                            | Lines |
| --------------------------------------------------------------- | ----: |
| `canvas-lib/src/react/Canvas.tsx`                               |  1719 |
| `canvas-plugins/src/pdf/PDFNode.tsx`                            |   550 |
| `canvas-plugins/src/experimental/cause-map/CauseMapAnchors.tsx` |   546 |
| `canvas-plugins/src/timeseries/TimeseriesNode.tsx`              |   525 |
| `canvas-plugins/src/table/TableNode.tsx`                        |   496 |
| `canvas-lib/src/react/NodeWrapper.tsx`                          |   453 |

Splitting them would diverge from upstream and risk the preview without changing what the user sees. Behavior is covered by the existing canvas-preview tests. The only other app component over 150 lines is `src/components/layout/TemplateCanvasInstructionsDialog.tsx` (159 lines), which is presentational and stays as one file.
