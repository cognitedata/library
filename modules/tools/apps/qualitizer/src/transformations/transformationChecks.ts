import { SparkSQL, SparkSqlParserListener } from "dt-sql-parser";
import type { ParseError } from "dt-sql-parser";

export type ParsedInsight = {
  errors: Array<{ message?: string; startLine?: number; startCol?: number }>;
  tables: string[];
  dataModelRefs: Array<{
    space?: string;
    externalId?: string;
    version?: string;
    typeExternalId?: string;
    relationshipProperty?: string;
  }>;
  nodeReferences: Array<{
    space?: string;
    externalId?: string;
  }>;
  unitLookups: Array<{
    alias?: string;
    quantity?: string;
  }>;
  operatorUsage: {
    like: number;
    rlike: number;
    regexp: number;
  };
  nestedCalls: Array<{ outer: string; inner: string }>;
  statementCount: number;
  tokenCount: number;
  error?: string | null;
};

const EMPTY_INSIGHT: ParsedInsight = {
  errors: [],
  tables: [],
  dataModelRefs: [],
  nodeReferences: [],
  unitLookups: [],
  operatorUsage: { like: 0, rlike: 0, regexp: 0 },
  nestedCalls: [],
  statementCount: 0,
  tokenCount: 0,
  error: null,
};

function trimQuotes(part: string): string {
  return part.trim().replace(/^["'`]/, "").replace(/["'`]$/, "");
}

/** Remove lines that are comments (start with -- after leading whitespace). */
function stripSqlCommentLines(sql: string): string {
  return sql
    .split(/\r?\n/)
    .filter((line) => !line.trimStart().startsWith("--"))
    .join("\n");
}

/** Remove leading block comments and whitespace so WITH is visible. */
function stripLeadingBlockComments(sql: string): string {
  let i = 0;
  while (i < sql.length) {
    while (i < sql.length && /\s/.test(sql[i])) i += 1;
    if (i >= sql.length) break;
    if (sql[i] === "/" && sql[i + 1] === "*") {
      i += 2;
      while (i < sql.length && !(sql[i] === "*" && sql[i + 1] === "/")) i += 1;
      if (sql[i] === "*" && sql[i + 1] === "/") i += 2;
      continue;
    }
    break;
  }
  return sql.slice(i).trimStart();
}

/** Extract cdf_data_models(...) references from query text. */
export function extractDataModelRefs(query: string): ParsedInsight["dataModelRefs"] {
  return Array.from(query.matchAll(/cdf_data_models\(([\s\S]*?)\)/gi), (match) => {
    const raw = match[1] ?? "";
    const parts = raw.split(",").map(trimQuotes);
    return {
      space: parts[0],
      externalId: parts[1],
      version: parts[2],
      typeExternalId: parts[3],
      relationshipProperty: parts[4],
    };
  });
}

/** Extract node_reference(...) references from query text. */
export function extractNodeReferences(query: string): ParsedInsight["nodeReferences"] {
  return Array.from(query.matchAll(/node_reference\(([\s\S]*?)\)/gi), (match) => {
    const raw = match[1] ?? "";
    const parts = raw.split(",").map(trimQuotes);
    return parts.length >= 2
      ? { space: parts[0], externalId: parts[1] }
      : { externalId: parts[0] };
  });
}

/** Extract try_get_unit(...) lookups from query text. */
export function extractUnitLookups(query: string): ParsedInsight["unitLookups"] {
  return Array.from(query.matchAll(/try_get_unit\(([\s\S]*?)\)/gi), (match) => {
    const raw = match[1] ?? "";
    const parts = raw.split(",").map(trimQuotes);
    return { alias: parts[0], quantity: parts[1] };
  });
}

/** Count LIKE, RLIKE, REGEXP operator usage. */
export function countOperatorUsage(query: string): ParsedInsight["operatorUsage"] {
  return {
    like: (query.match(/\blike\b/gi) ?? []).length,
    rlike: (query.match(/\brlike\b/gi) ?? []).length,
    regexp: (query.match(/\bregexp\b/gi) ?? []).length,
  };
}

/** Extract nested function calls (e.g. outer(inner(...))). */
export function extractNestedCalls(query: string): ParsedInsight["nestedCalls"] {
  const nestedCalls: Array<{ outer: string; inner: string }> = [];
  const functionCallRegex = /\b([a-zA-Z_][\w]*)\s*\(([^()]*\([^()]*\)[^()]*)\)/g;
  let matchCall: RegExpExecArray | null;
  while ((matchCall = functionCallRegex.exec(query)) !== null) {
    const outer = matchCall[1];
    const innerMatch = matchCall[2].match(/\b([a-zA-Z_][\w]*)\s*\(/);
    if (innerMatch) {
      nestedCalls.push({ outer, inner: innerMatch[1] });
    }
  }
  return nestedCalls;
}

/** Get statement and token counts using SparkSQL parser. */
export function getStatementAndTokenCounts(
  parser: SparkSQL,
  query: string
): { statementCount: number; tokenCount: number } {
  const statementCount = (parser.splitSQLByStatement(query) ?? []).length;
  const tokenCount = (parser.getAllTokens(query) ?? []).length;
  return { statementCount, tokenCount };
}

/** Extract table names from parsed SQL tree. */
export function extractTablesFromParseTree(
  parser: SparkSQL,
  query: string
): string[] {
  const parseTree = parser.parse(query);
  if (parseTree == null || typeof parseTree !== "object") {
    return [];
  }
  class TableListener extends SparkSqlParserListener {
    tables: string[] = [];
    override enterTableName = (ctx: { getText: () => string }): void => {
      let name = ctx.getText();
      const match = name.match("^`(.*)`.`(.*)`$");
      if (match) {
        name = `${match[1]}.${match[2]}`;
      }
      this.tables.push(name);
    };
  }
  const listener = new TableListener();
  try {
    parser.listen(listener, parseTree);
  } catch {
    return [];
  }
  return Array.from(new Set(listener.tables));
}

/**
 * Parse a transformation SQL query and return structured insight (errors, tables,
 * data model refs, node refs, unit lookups, operator usage, nested calls, counts).
 */
export function parseTransformationQuery(query: string): ParsedInsight {
  const stripped = stripSqlCommentLines(query);
  const trimmed = stripped.trim();
  if (!trimmed) {
    return { ...EMPTY_INSIGHT };
  }

  try {
    const parser = new SparkSQL();
    const errors = parser.validate(trimmed) as ParseError[];
    const { statementCount, tokenCount } = getStatementAndTokenCounts(parser, trimmed);
    const dataModelRefs = extractDataModelRefs(trimmed);
    const nodeReferences = extractNodeReferences(trimmed);
    const unitLookups = extractUnitLookups(trimmed);
    const operatorUsage = countOperatorUsage(trimmed);
    const nestedCalls = extractNestedCalls(trimmed);

    if (errors.length > 0) {
      return {
        errors: errors.map((entry) => ({
          message: entry.message,
          startLine: entry.startLine,
          startCol: entry.startColumn,
        })),
        tables: [],
        dataModelRefs,
        nodeReferences,
        unitLookups,
        operatorUsage,
        nestedCalls,
        statementCount,
        tokenCount,
        error: null,
      };
    }

    const tables = extractTablesFromParseTree(parser, trimmed);
    return {
      errors: [],
      tables,
      dataModelRefs,
      nodeReferences,
      unitLookups,
      operatorUsage,
      nestedCalls,
      statementCount,
      tokenCount,
      error: null,
    };
  } catch (error) {
    return {
      ...EMPTY_INSIGHT,
      error: error instanceof Error ? error.message : "Failed to parse SQL.",
    };
  }
}

/** Extract CTE names from a WITH clause. Returns empty array if no WITH clause. */
export function extractCteNames(query: string): string[] {
  const stripped = stripLeadingBlockComments(stripSqlCommentLines(query));
  let index = 0;
  const trimmed = stripped.trimStart();
  const lower = trimmed.toLowerCase();
  if (!lower.startsWith("with")) return [];
  index = 4;
  const names: string[] = [];
  const skipWs = () => {
    while (index < trimmed.length && /\s/.test(trimmed[index])) index += 1;
  };
  const readName = (): string => {
    skipWs();
    if (index >= trimmed.length) return "";
    const ch = trimmed[index];
    if (ch === '"' || ch === "`") {
      index += 1;
      const start = index;
      while (index < trimmed.length && trimmed[index] !== ch) index += 1;
      const name = trimmed.slice(start, index);
      index += 1;
      return name;
    }
    const start = index;
    while (index < trimmed.length && /[a-zA-Z0-9_.]/.test(trimmed[index])) index += 1;
    return trimmed.slice(start, index);
  };
  const skipToAs = () => {
    while (index < trimmed.length) {
      skipWs();
      if (trimmed.slice(index, index + 2).toLowerCase() === "as") {
        index += 2;
        break;
      }
      index += 1;
    }
  };
  const skipBalanced = () => {
    let depth = 1;
    while (index < trimmed.length && depth > 0) {
      const ch = trimmed[index];
      if (ch === "(") depth += 1;
      if (ch === ")") depth -= 1;
      index += 1;
    }
  };
  while (index < trimmed.length) {
    const name = readName();
    if (!name) break;
    names.push(name);
    skipToAs();
    skipWs();
    if (trimmed[index] === "(") index += 1;
    skipBalanced();
    skipWs();
    if (trimmed[index] === ",") {
      index += 1;
      continue;
    }
    break;
  }
  return names;
}

export type ParsedInsightCounts = {
  errors: number;
  statements: number;
  tokens: number;
  tables: number;
  dataModelRefs: number;
  nodeReferences: number;
  unitLookups: number;
  like: number;
  rlike: number;
  regexp: number;
  nestedCalls: number;
  cteCount: number;
};

/** Return only counts from a ParsedInsight for use in tables. */
export function getParsedInsightCounts(
  insight: ParsedInsight,
  rawQuery?: string
): ParsedInsightCounts {
  const base: Omit<ParsedInsightCounts, "cteCount"> = {
    errors: insight.errors.length,
    statements: insight.statementCount,
    tokens: insight.tokenCount,
    tables: insight.tables.length,
    dataModelRefs: insight.dataModelRefs.length,
    nodeReferences: insight.nodeReferences.length,
    unitLookups: insight.unitLookups.length,
    like: insight.operatorUsage.like,
    rlike: insight.operatorUsage.rlike,
    regexp: insight.operatorUsage.regexp,
    nestedCalls: insight.nestedCalls.length,
  };
  const cteCount =
    rawQuery != null ? extractCteNames(rawQuery).length : 0;
  return { ...base, cteCount };
}
