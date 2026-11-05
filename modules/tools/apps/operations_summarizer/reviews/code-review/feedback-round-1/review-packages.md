# Package audit: Operations Summarizer

Commands: `npm outdated --json`, `npm audit --json`. `npm view <pkg> deprecated` for packages that were outdated or named in the audit (`@cognite/app-sdk`, `@cognite/aura`, `@cognite/sdk`, `@tanstack/react-query`, `lucide-react`, `pdfjs-dist`). None of those are deprecated.

No direct `dependencies` entry is one or more majors behind `latest`. Packages absent from `npm outdated` match the installed range. Dev-only Vitest packages report a newer major (`vitest` / `@vitest/*` latest 5.1.0, wanted and current 4.1.12) and are not production dependencies.

Health: **Pass** (0 critical, 0 high; production deps are current or a minor/patch behind).

## Dependencies

| Package | Used version | Latest | Deprecated | CVEs | Health |
| ------- | ------------ | ------ | ---------- | ---- | ------ |
| `@cognite/app-sdk` | 0.11.47681 | 0.11.48556 | no | none | Pass |
| `@cognite/aura` | 1.48020.0 | 1.48651.0 | no | low, transitive `katex` | Pass |
| `@cognite/sdk` | 10.16.0 | 10.18.0 | no | none | Pass |
| `@tabler/icons-react` | 3.50.0 (range; not outdated) | current | not flagged | none | Pass |
| `@tanstack/react-query` | 5.105.0 | 5.105.3 | no | none | Pass |
| `clsx` | 2.1.1 (range; not outdated) | current | not flagged | none | Pass |
| `date-fns` | 4.1.0 (range; not outdated) | current | not flagged | none | Pass |
| `dompurify` | 3.4.11 (range; not outdated) | current | not flagged | none | Pass |
| `jspdf` | 4.2.1 (range; not outdated) | current | not flagged | none | Pass |
| `lucide-react` | 1.63.0 | 1.67.0 | no | none | Pass |
| `pdfjs-dist` | 6.3.289 (pinned) | 6.5.438 | no | none | Pass |
| `react` | 19.2.0 (range; not outdated) | current | not flagged | none | Pass |
| `react-dom` | 19.2.0 (range; not outdated) | current | not flagged | none | Pass |
| `react-error-boundary` | 6.1.1 (range; not outdated) | current | not flagged | none | Pass |
| `react-pdf` | 11.0.0 (range; not outdated) | current | not flagged | none | Pass |
| `rehype-raw` | 7.0.0 (range; not outdated) | current | not flagged | none | Pass |
| `rehype-sanitize` | 6.0.0 (range; not outdated) | current | not flagged | none | Pass |
| `rehype-stringify` | 10.0.1 (range; not outdated) | current | not flagged | none | Pass |
| `remark-gfm` | 4.0.1 (range; not outdated) | current | not flagged | none | Pass |
| `remark-parse` | 11.0.0 (range; not outdated) | current | not flagged | none | Pass |
| `remark-rehype` | 11.1.2 (range; not outdated) | current | not flagged | none | Pass |
| `tailwind-merge` | 3.5.0 (range; not outdated) | current | not flagged | none | Pass |
| `unified` | 11.0.5 (range; not outdated) | current | not flagged | none | Pass |

## Security audit

| Severity | Count |
| -------- | ----- |
| Critical | 0 |
| High | 0 |
| Moderate | 0 |
| Low | 6 |
| Info | 0 |

### Vulnerabilities

| Package | Severity | Title | Patched in | Advisory |
| ------- | -------- | ----- | ---------- | -------- |
| `katex` (via `@streamdown/math` / `@cognite/aura`) | low | KaTeX: Existing prototype pollution can bypass trust restrictions | `katex` >= 0.18.2 (audit’s suggested Aura downgrade to 0.3.5 is a different major line and is not a safe fix) | https://github.com/advisories/GHSA-238p-pmpm-9mq7 |
| `@cognite/aura` | low | Parent of the `katex` advisory through `@streamdown/math` | no safe semver fix in the current 1.x line | same advisory |
| `@streamdown/math` | low | Via `katex`, `rehype-katex`, `remark-math` | — | same advisory |
| `rehype-katex` | low | Via `katex` | — | same advisory |
| `remark-math` | low | Via `micromark-extension-math` | — | same advisory |
| `micromark-extension-math` | low | Via `katex` | — | same advisory |
