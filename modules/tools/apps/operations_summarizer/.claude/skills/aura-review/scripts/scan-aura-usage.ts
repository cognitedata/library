import * as fs from 'node:fs';
import * as path from 'node:path';
import ts from 'typescript';

const IGNORED_DIRS = new Set([
  'node_modules',
  'dist',
  '.git',
  'coverage',
  '.cognite-bundles',
]);

function walkSourceFiles(rootDir: string, extensions: string[]): string[] {
  const files: string[] = [];
  function walk(dir: string): void {
    if (!fs.existsSync(dir)) return;
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (entry.isDirectory()) {
        if (IGNORED_DIRS.has(entry.name)) continue;
        walk(path.join(dir, entry.name));
        continue;
      }
      if (extensions.some((ext) => entry.name.endsWith(ext))) {
        files.push(path.join(dir, entry.name));
      }
    }
  }
  walk(rootDir);
  return files;
}

function isPascalCase(name: string): boolean {
  return /^[A-Z]/.test(name);
}

function hasExportModifier(node: ts.Node): boolean {
  const modifiers = ts.canHaveModifiers(node) ? ts.getModifiers(node) : undefined;
  return !!modifiers?.some((m: ts.Modifier) => m.kind === ts.SyntaxKind.ExportKeyword);
}

function extractExportedIdentifiers(typesFilePath: string): string[] {
  if (!fs.existsSync(typesFilePath)) return [];
  const content = fs.readFileSync(typesFilePath, 'utf-8');
  const source = ts.createSourceFile(
    typesFilePath,
    content,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS
  );
  const names = new Set<string>();

  for (const statement of source.statements) {
    if (ts.isExportDeclaration(statement)) {
      if (statement.isTypeOnly) continue;
      if (statement.exportClause && ts.isNamedExports(statement.exportClause)) {
        for (const element of statement.exportClause.elements) {
          if (element.isTypeOnly) continue;
          if (isPascalCase(element.name.text)) names.add(element.name.text);
        }
      }
      continue;
    }
    if (ts.isFunctionDeclaration(statement) && statement.name && hasExportModifier(statement)) {
      if (isPascalCase(statement.name.text)) names.add(statement.name.text);
      continue;
    }
    if (ts.isClassDeclaration(statement) && statement.name && hasExportModifier(statement)) {
      if (isPascalCase(statement.name.text)) names.add(statement.name.text);
      continue;
    }
    if (ts.isVariableStatement(statement) && hasExportModifier(statement)) {
      for (const declaration of statement.declarationList.declarations) {
        if (ts.isIdentifier(declaration.name) && isPascalCase(declaration.name.text)) {
          names.add(declaration.name.text);
        }
      }
    }
  }

  return [...names];
}

function pickRootIdentifiers(identifiers: string[]): string[] {
  if (identifiers.length <= 1) return identifiers;
  const shortest = [...identifiers].sort((a, b) => a.length - b.length)[0];
  const shortestIsCommonPrefix = identifiers.every(
    (id) => id === shortest || id.startsWith(shortest)
  );
  return shortestIsCommonPrefix ? [shortest] : identifiers;
}

interface ComponentCatalogEntry {
  slug: string;
  identifiers: string[];
  rootIdentifiers: string[];
}

// Non-component subpaths Aura's package.json also exports: the barrel itself,
// docs/asset files, and utility/config entry points that don't render JSX.
//
// Everything else is a real, importable component surface and belongs in the
// catalog — including top-level exports like `./chart` and `./data-grid`, which is
// why this is a denylist rather than a `./components/` prefix match. Any component
// export missing from the catalog counts as non-Aura wherever an app uses it, so
// the coverage and could-have-been-Aura numbers understate reality.
const NON_COMPONENT_EXPORT_DENYLIST = new Set([
  './components', // barrel re-export, not a distinct component
  './DESIGN.md',
  './colors.css',
  './styles.css',
  './styles.source.css',
  './eslint',
  './utils',
]);

function loadComponentCatalog(appDir: string): ComponentCatalogEntry[] {
  const auraDir = path.join(appDir, 'node_modules/@cognite/aura');
  const packageJsonPath = path.join(auraDir, 'package.json');
  if (!fs.existsSync(packageJsonPath)) {
    throw new Error(
      `Could not find @cognite/aura package.json at ${packageJsonPath}. ` +
        'Make sure npm/pnpm install completed before running this scan.'
    );
  }
  const packageJson = JSON.parse(
    fs.readFileSync(packageJsonPath, 'utf-8')
  ) as { exports: Record<string, { types?: string } | undefined> };
  return Object.entries(packageJson.exports)
    .filter(
      ([key]) =>
        !NON_COMPONENT_EXPORT_DENYLIST.has(key) && !key.endsWith('.css') && !key.endsWith('.md')
    )
    .map(([key, value]) => {
      const slug = key.startsWith('./components/')
        ? key.slice('./components/'.length)
        : key.slice('./'.length);
      const typesPath = value?.types ? path.join(auraDir, value.types) : null;
      const identifiers = typesPath ? extractExportedIdentifiers(typesPath) : [];
      return { slug, identifiers, rootIdentifiers: pickRootIdentifiers(identifiers) };
    });
}

const STRUCTURAL_IDENTIFIER_DENYLIST = new Set([
  'Fragment',
  'StrictMode',
  'Suspense',
  'ErrorBoundary',
  'BrowserRouter',
  'Route',
  'Routes',
  'Outlet',
  'Link',
  'NavLink',
]);

interface ImportInfo {
  moduleSpecifier: string;
  // The name as actually exported by the module — differs from the map's key
  // (the local binding name) whenever the import is aliased, e.g.
  // `import { Alert as AlertBanner } from '@cognite/aura/components/alert'`.
  // Catalog lookups must use this, not the local name, or an aliased Aura
  // import gets silently misclassified as a non-Aura component.
  originalName: string;
}

function collectImports(source: ts.SourceFile): Map<string, ImportInfo> {
  const importsByIdentifier = new Map<string, ImportInfo>();
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement)) continue;
    const moduleSpecifier = (statement.moduleSpecifier as ts.StringLiteral)
      .text;
    const clause = statement.importClause;
    if (!clause?.namedBindings || !ts.isNamedImports(clause.namedBindings))
      continue;
    for (const element of clause.namedBindings.elements) {
      const localName = element.name.text;
      const originalName = element.propertyName?.text ?? localName;
      importsByIdentifier.set(localName, { moduleSpecifier, originalName });
    }
  }
  return importsByIdentifier;
}

function classifyImportSource(
  moduleSpecifier: string | undefined
): 'relative' | 'external' | 'unresolved-local' {
  if (!moduleSpecifier) return 'unresolved-local';
  if (moduleSpecifier.startsWith('.')) return 'relative';
  return 'external';
}

const SPACING_RE =
  /\b(?:p|px|py|pt|pb|pl|pr|m|mx|my|mt|mb|ml|mr|w|h|min-w|min-h|max-w|max-h|gap|space-x|space-y)-(?:\[[^\]]+\]|[\w.%]+)\b/;
const RAW_COLOR_RE =
  /\b(?:bg|text|border|ring|fill|stroke|from|via|to)-(?:red|orange|amber|yellow|lime|green|emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose|slate|gray|grey|zinc|neutral|stone|black|white)(?:-\d{2,3})?\b/;
const ARBITRARY_VALUE_RE = /([a-zA-Z-]+)-\[[^\]]+\]/g;
const VARIANT_PREFIXES =
  /^(data|aria|group|peer|has|not|in|nth|supports|open|hover|focus|active)-/;
const TOKEN_ONLY_ARBITRARY_RE = /^\[[^\]]*var\(--[^)]+\)[^\]]*\]$/;

function hasGenuineArbitraryValue(region: string): boolean {
  return [...region.matchAll(ARBITRARY_VALUE_RE)].some(
    (m) =>
      !VARIANT_PREFIXES.test(`${m[1]}-`) &&
      !TOKEN_ONLY_ARBITRARY_RE.test(m[0].slice(m[0].indexOf('[')))
  );
}

function classifyClassNameRegion(region: string): string[] {
  const categories: string[] = [];
  if (SPACING_RE.test(region)) categories.push('spacing');
  if (RAW_COLOR_RE.test(region)) categories.push('color');
  if (hasGenuineArbitraryValue(region)) categories.push('arbitrary-value');
  return categories;
}

interface AuraElementUsage {
  identifier: string;
  slug: string;
  isRootComponent: boolean;
  file: string;
  line: number;
  classNameRegion: string | null;
  escapeHatchCategories: string[];
}

interface NonAuraElementUsage {
  identifier: string;
  file: string;
  line: number;
  importSource: 'relative' | 'external' | 'unresolved-local';
  moduleSpecifier: string | null;
}

interface ExcludedElementUsage {
  identifier: string;
  file: string;
  line: number;
  reason: 'structural-denylist' | 'namespaced-tag';
}

function scan(appDir: string): {
  catalog: ComponentCatalogEntry[];
  auraElementUsages: AuraElementUsage[];
  nonAuraUsages: NonAuraElementUsage[];
  excludedUsages: ExcludedElementUsage[];
} {
  const catalog = loadComponentCatalog(appDir);
  const identifierToSlug = new Map<string, string>();
  const rootIdentifiers = new Set<string>();
  for (const entry of catalog) {
    for (const identifier of entry.identifiers) {
      if (!identifierToSlug.has(identifier)) identifierToSlug.set(identifier, entry.slug);
    }
    for (const identifier of entry.rootIdentifiers) rootIdentifiers.add(identifier);
  }
  const auraElementUsages: AuraElementUsage[] = [];
  const nonAuraUsages: NonAuraElementUsage[] = [];
  const excludedUsages: ExcludedElementUsage[] = [];
  const srcDir = path.join(appDir, 'src');

  for (const file of walkSourceFiles(srcDir, ['.tsx', '.ts'])) {
    const content = fs.readFileSync(file, 'utf-8');
    const source = ts.createSourceFile(
      file,
      content,
      ts.ScriptTarget.Latest,
      true,
      ts.ScriptKind.TSX
    );
    const importsByIdentifier = collectImports(source);

    function classNameRegionFor(attributes: ts.JsxAttributes): string | null {
      const classNameAttr = attributes.properties.find(
        (p): p is ts.JsxAttribute =>
          ts.isJsxAttribute(p) && p.name.getText(source) === 'className'
      );
      if (!classNameAttr?.initializer) return null;
      return `=${classNameAttr.initializer.getText(source)}`;
    }

    function visit(node: ts.Node): void {
      if (ts.isJsxOpeningElement(node) || ts.isJsxSelfClosingElement(node)) {
        const identifier = node.tagName.getText(source);
        if (/^[A-Z]/.test(identifier)) {
          const relativeFile = path.relative(appDir, file);
          const line =
            source.getLineAndCharacterOfPosition(node.getStart(source)).line +
            1;

          if (identifier.includes('.')) {
            excludedUsages.push({
              identifier,
              file: relativeFile,
              line,
              reason: 'namespaced-tag',
            });
          } else if (STRUCTURAL_IDENTIFIER_DENYLIST.has(identifier)) {
            excludedUsages.push({
              identifier,
              file: relativeFile,
              line,
              reason: 'structural-denylist',
            });
          } else {
            const importInfo = importsByIdentifier.get(identifier) ?? null;
            const moduleSpecifier = importInfo?.moduleSpecifier ?? null;
            const slug =
              importInfo && moduleSpecifier?.startsWith('@cognite/aura')
                ? identifierToSlug.get(importInfo.originalName) ?? null
                : null;

            if (slug && importInfo) {
              const classNameRegion = classNameRegionFor(node.attributes);
              auraElementUsages.push({
                identifier,
                slug,
                isRootComponent: rootIdentifiers.has(importInfo.originalName),
                file: relativeFile,
                line,
                classNameRegion,
                escapeHatchCategories: classNameRegion
                  ? classifyClassNameRegion(classNameRegion)
                  : [],
              });
            } else {
              nonAuraUsages.push({
                identifier,
                file: relativeFile,
                line,
                importSource: classifyImportSource(
                  moduleSpecifier ?? undefined
                ),
                moduleSpecifier,
              });
            }
          }
        }
      }
      ts.forEachChild(node, visit);
    }
    visit(source);
  }

  return { catalog, auraElementUsages, nonAuraUsages, excludedUsages };
}

function main(): void {
  const args = process.argv.slice(2);
  const appDir = args[0];
  if (!appDir) {
    console.error('Usage: scan-aura-usage.ts <app-dir> [--out <file>]');
    process.exit(1);
  }
  const outIndex = args.indexOf('--out');
  const outFile = outIndex !== -1 ? args[outIndex + 1] : null;

  const {
    catalog,
    auraElementUsages,
    nonAuraUsages: allNonAuraUsages,
    excludedUsages,
  } = scan(path.resolve(appDir));

  const availableAuraComponents = [
    ...new Set(catalog.flatMap((entry) => entry.rootIdentifiers)),
  ].sort();

  const allAuraUsages = auraElementUsages.filter((u) => u.isRootComponent);

  function groupByIdentifier<T extends { identifier: string; file: string; line: number }>(
    items: T[],
    extra: (first: T) => Record<string, unknown>
  ): Array<{ identifier: string; usageCount: number; locations: { file: string; line: number }[] }> {
    const byIdentifier = new Map<string, T[]>();
    for (const item of items) {
      const group = byIdentifier.get(item.identifier) ?? [];
      group.push(item);
      byIdentifier.set(item.identifier, group);
    }
    return [...byIdentifier.entries()].map(([identifier, group]) => ({
      identifier,
      usageCount: group.length,
      locations: group.map((g) => ({ file: g.file, line: g.line })),
      ...extra(group[0]),
    }));
  }

  const auraUsages = groupByIdentifier(allAuraUsages, (first) => ({
    slug: first.slug,
  }));
  const nonAuraUsages = groupByIdentifier(allNonAuraUsages, (first) => ({
    importSource: first.importSource,
    moduleSpecifier: first.moduleSpecifier,
  }));

  const escapeHatches = auraElementUsages
    .filter((u) => u.escapeHatchCategories.length > 0)
    .map((u) => ({
      component: u.slug,
      tag: u.identifier,
      file: u.file,
      line: u.line,
      className: u.classNameRegion,
      categories: u.escapeHatchCategories,
    }));

  const totalUsages = auraUsages.length + nonAuraUsages.length;

  const result = {
    availableAuraComponents,
    totals: {
      totalUsages,
      auraUsages: auraUsages.length,
      nonAuraUsages: nonAuraUsages.length,
      allAuraUsages: allAuraUsages.length,
      allNonAuraUsages: allNonAuraUsages.length,
      auraCoveragePct:
        totalUsages === 0
          ? null
          : Math.round((auraUsages.length / totalUsages) * 1000) / 1000,
    },
    auraUsages,
    nonAuraUsages,
    escapeHatches,
    excludedUsages,
  };

  const json = JSON.stringify(result, null, 2);
  if (outFile) {
    fs.mkdirSync(path.dirname(outFile), { recursive: true });
    fs.writeFileSync(outFile, json);
    console.log(`Wrote scan results to ${outFile}`);
  } else {
    console.log(json);
  }
}

main();
