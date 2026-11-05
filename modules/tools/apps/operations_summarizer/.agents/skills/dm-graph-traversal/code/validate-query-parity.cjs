"use strict";

const fs = require("node:fs");
const path = require("node:path");

function parseArgs(argv) {
  const args = {
    queries: [],
    check: "all",
    expect: "pass",
    json: false,
    schemaHints: null,
  };

  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === "--query" || token === "-q") {
      const value = argv[++i];
      if (!value) throw new Error("Missing value for --query");
      args.queries.push(value);
    } else if (token === "--check") {
      const value = argv[++i];
      if (!value) throw new Error("Missing value for --check");
      args.check = value;
    } else if (token === "--expect") {
      const value = argv[++i];
      if (!value) throw new Error("Missing value for --expect");
      args.expect = value;
    } else if (token === "--json") {
      args.json = true;
    } else if (token === "--schema-hints") {
      const value = argv[++i];
      if (!value) throw new Error("Missing value for --schema-hints");
      args.schemaHints = value;
    } else if (token === "--help" || token === "-h") {
      args.help = true;
    } else {
      throw new Error(`Unknown argument: ${token}`);
    }
  }

  return args;
}

function usage() {
  return [
    "Reusable CDF query payload validator",
    "",
    "Usage:",
    "  node validate-query-parity.cjs --query <path> [--query <path> ...] [--check all|sources-properties|limit-placement|start-step-hasdata|versioned-traversal-refs|cursor-shape|inwards-list-direct-relations] [--expect pass|fail] [--schema-hints <path>] [--json]",
    "",
    "Examples:",
    "  node validate-query-parity.cjs --query ./query.json",
    "  node validate-query-parity.cjs --query ./query-fail.json --expect fail",
    "  node validate-query-parity.cjs --query ./query1.json --query ./query2.json --check all",
    "  node validate-query-parity.cjs --query ./query.json --check inwards-list-direct-relations --schema-hints ./schema-hints.json",
  ].join("\n");
}

function readJson(filePath) {
  const abs = path.resolve(filePath);
  let raw = fs.readFileSync(abs, "utf8");
  if (raw.charCodeAt(0) === 0xFEFF) raw = raw.slice(1);
  try {
    return { abs, data: JSON.parse(raw) };
  } catch (err) {
    const e = new Error(`Invalid JSON in ${abs}: ${err.message}`);
    e.code = "INVALID_JSON";
    throw e;
  }
}

function validateSourcesProperties(query) {
  const issues = [];
  const select = query?.select;
  if (!select || typeof select !== "object") {
    return issues;
  }

  for (const [step, stepSelect] of Object.entries(select)) {
    const sources = stepSelect?.sources;
    if (!Array.isArray(sources)) continue;
    for (let i = 0; i < sources.length; i += 1) {
      const src = sources[i];
      if (!Array.isArray(src?.properties) || src.properties.length === 0) {
        issues.push({
          rule: "sources-properties",
          path: `select.${step}.sources[${i}].properties`,
          message: "properties must not be null",
        });
      }
    }
  }
  return issues;
}

function validateLimitPlacement(query) {
  const issues = [];
  const withBlock = query?.with;
  if (!withBlock || typeof withBlock !== "object") {
    return issues;
  }

  for (const [step, stepDef] of Object.entries(withBlock)) {
    if (stepDef?.nodes && Object.prototype.hasOwnProperty.call(stepDef.nodes, "limit")) {
      issues.push({
        rule: "limit-placement",
        path: `with.${step}.nodes.limit`,
        message: "nodes.limit is invalid; use with.<step>.limit",
      });
    }
    if (stepDef?.edges && Object.prototype.hasOwnProperty.call(stepDef.edges, "limit")) {
      issues.push({
        rule: "limit-placement",
        path: `with.${step}.edges.limit`,
        message: "edges.limit is invalid; use with.<step>.limit",
      });
    }
  }
  return issues;
}

function hasHasDataFilter(node) {
  if (!node || typeof node !== "object") return false;
  if (Object.prototype.hasOwnProperty.call(node, "hasData")) return true;
  if (Array.isArray(node)) return node.some((item) => hasHasDataFilter(item));
  return Object.values(node).some((value) => hasHasDataFilter(value));
}

function validateStartStepHasData(query) {
  const issues = [];
  const withBlock = query?.with;
  if (!withBlock || typeof withBlock !== "object") return issues;

  for (const [step, stepDef] of Object.entries(withBlock)) {
    const nodes = stepDef?.nodes;
    if (!nodes || typeof nodes !== "object") continue;
    const isStartStep = !Object.prototype.hasOwnProperty.call(nodes, "from");
    if (!isStartStep) continue;
    const filter = nodes?.filter;
    if (!hasHasDataFilter(filter)) {
      issues.push({
        rule: "start-step-hasdata",
        path: `with.${step}.nodes.filter`,
        message: "start node step should include hasData constraint",
      });
    }
  }

  return issues;
}

function collectPropertyRefs(node, refs = []) {
  if (!node || typeof node !== "object") return refs;
  if (Array.isArray(node)) {
    node.forEach((item) => collectPropertyRefs(item, refs));
    return refs;
  }
  if (Object.prototype.hasOwnProperty.call(node, "property") && Array.isArray(node.property)) {
    refs.push(node.property);
  }
  Object.values(node).forEach((value) => collectPropertyRefs(value, refs));
  return refs;
}

function validateVersionedTraversalRefs(query) {
  const issues = [];
  const withBlock = query?.with;
  if (!withBlock || typeof withBlock !== "object") return issues;

  for (const [step, stepDef] of Object.entries(withBlock)) {
    const nodes = stepDef?.nodes;
    const edges = stepDef?.edges;
    const traversalDef = nodes ?? edges;
    if (!traversalDef || typeof traversalDef !== "object") continue;
    const isTraversal = Object.prototype.hasOwnProperty.call(traversalDef, "from");
    if (!isTraversal) continue;
    const refs = collectPropertyRefs(traversalDef?.filter, []);
    for (const ref of refs) {
      if (!Array.isArray(ref) || ref.length < 3) continue;
      const scope = String(ref[0]);
      const viewRef = String(ref[1]);
      if (scope === "node" || scope === "edge") continue;
      if (!viewRef.includes("/")) {
        issues.push({
          rule: "versioned-traversal-refs",
          path: `with.${step}.${nodes ? "nodes" : "edges"}.filter.property`,
          message: "traversal property references should use View/version",
        });
      }
    }
  }

  return issues;
}

function validateCursorShape(query) {
  const issues = [];
  const withBlock = query?.with;
  const cursors = query?.cursors;

  if (Object.prototype.hasOwnProperty.call(query ?? {}, "nextCursor")) {
    issues.push({
      rule: "cursor-shape",
      path: "nextCursor",
      message: "nextCursor is a response field and should not be sent in request payloads",
    });
  }

  if (cursors === undefined) return issues;

  if (!cursors || typeof cursors !== "object" || Array.isArray(cursors)) {
    issues.push({
      rule: "cursor-shape",
      path: "cursors",
      message: "cursors must be an object keyed by step name",
    });
    return issues;
  }

  const validSteps = new Set(withBlock && typeof withBlock === "object" ? Object.keys(withBlock) : []);
  for (const [step, value] of Object.entries(cursors)) {
    if (!validSteps.has(step)) {
      issues.push({
        rule: "cursor-shape",
        path: `cursors.${step}`,
        message: "cursor key should match a declared with-step",
      });
    }
    if (typeof value !== "string" || value.trim() === "") {
      issues.push({
        rule: "cursor-shape",
        path: `cursors.${step}`,
        message: "cursor value must be a non-empty string",
      });
    }
  }
  return issues;
}

function normalizeListRelationHints(schemaHints) {
  const list = schemaHints?.directRelationLists;
  if (!Array.isArray(list)) return new Set();
  const set = new Set();
  for (const entry of list) {
    const key = [
      entry?.space ?? "",
      entry?.externalId ?? "",
      entry?.version ?? "",
      entry?.identifier ?? "",
    ].join("|");
    if (key !== "|||") set.add(key);
  }
  return set;
}

function validateInwardsListDirectRelations(query, schemaHints) {
  const issues = [];
  const withBlock = query?.with;
  if (!withBlock || typeof withBlock !== "object") return issues;
  const hintSet = normalizeListRelationHints(schemaHints);
  if (hintSet.size === 0) return issues;

  for (const [step, stepDef] of Object.entries(withBlock)) {
    const nodes = stepDef?.nodes;
    if (!nodes || typeof nodes !== "object") continue;
    if (nodes?.direction !== "inwards") continue;
    if (!Object.prototype.hasOwnProperty.call(nodes, "from")) continue;
    const through = nodes?.through;
    const view = through?.view;
    const identifier = through?.identifier;
    if (!view || typeof view !== "object" || !identifier) continue;
    const key = [view.space ?? "", view.externalId ?? "", view.version ?? "", identifier].join("|");
    if (hintSet.has(key)) {
      issues.push({
        rule: "inwards-list-direct-relations",
        path: `with.${step}.nodes`,
        message:
          "Cannot traverse lists of direct relations inwards. Use outwards traversal from the owning node or remodel as an edge.",
      });
    }
  }
  return issues;
}

function runChecks(query, checkMode, schemaHints) {
  const modes =
    checkMode === "all"
      ? [
          "sources-properties",
          "limit-placement",
          "start-step-hasdata",
          "versioned-traversal-refs",
          "cursor-shape",
          "inwards-list-direct-relations",
        ]
      : [checkMode];
  let issues = [];

  if (modes.includes("sources-properties")) {
    issues = issues.concat(validateSourcesProperties(query));
  }
  if (modes.includes("limit-placement")) {
    issues = issues.concat(validateLimitPlacement(query));
  }
  if (modes.includes("start-step-hasdata")) {
    issues = issues.concat(validateStartStepHasData(query));
  }
  if (modes.includes("versioned-traversal-refs")) {
    issues = issues.concat(validateVersionedTraversalRefs(query));
  }
  if (modes.includes("cursor-shape")) {
    issues = issues.concat(validateCursorShape(query));
  }
  if (modes.includes("inwards-list-direct-relations")) {
    issues = issues.concat(validateInwardsListDirectRelations(query, schemaHints));
  }
  return issues;
}

function formatFailureLikeApi(issues) {
  const first = issues[0];
  const tail = first?.message || "constraint violation";
  return {
    error: {
      code: 400,
      message: `Request had ${issues.length} constraint violations. Please fix the request and try again. [${tail}]`,
    },
    issues,
  };
}

function main() {
  const args = parseArgs(process.argv);
  if (args.help || args.queries.length === 0) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }

  const validChecks = new Set([
    "all",
    "sources-properties",
    "limit-placement",
    "start-step-hasdata",
    "versioned-traversal-refs",
    "cursor-shape",
    "inwards-list-direct-relations",
  ]);
  if (!validChecks.has(args.check)) {
    throw new Error(
      `Invalid --check '${args.check}'. Use all|sources-properties|limit-placement|start-step-hasdata|versioned-traversal-refs|cursor-shape|inwards-list-direct-relations`
    );
  }

  const validExpect = new Set(["pass", "fail"]);
  if (!validExpect.has(args.expect)) {
    throw new Error(`Invalid --expect '${args.expect}'. Use pass|fail`);
  }

  let schemaHints = null;
  if (args.schemaHints) {
    schemaHints = readJson(args.schemaHints).data;
  }

  const results = [];
  let hasError = false;

  for (const queryPath of args.queries) {
    const { abs, data } = readJson(queryPath);
    const issues = runChecks(data, args.check, schemaHints);
    const passed = issues.length === 0;
    const expectPass = args.expect === "pass";
    const expectationMet = expectPass ? passed : !passed;

    const result = {
      file: abs,
      check: args.check,
      expect: args.expect,
      passed,
      expectationMet,
      issues,
    };
    results.push(result);

    if (!expectationMet) {
      hasError = true;
    }
  }

  if (args.json) {
    const output = {
      ok: !hasError,
      results,
      apiLikeError: hasError ? formatFailureLikeApi(results.flatMap((r) => r.issues)) : null,
    };
    console.log(JSON.stringify(output, null, 2));
  } else {
    for (const r of results) {
      const status = r.expectationMet ? "PASS" : "FAIL";
      console.log(`${status}: ${path.basename(r.file)} (check=${r.check}, expect=${r.expect})`);
      if (!r.passed) {
        for (const issue of r.issues) {
          console.log(`  - ${issue.path}: ${issue.message}`);
        }
      }
    }

    if (hasError) {
      const allIssues = results.flatMap((r) => r.issues);
      if (allIssues.length > 0) {
        const err = formatFailureLikeApi(allIssues);
        console.log(JSON.stringify(err, null, 2));
      }
    }
  }

  process.exit(hasError ? 1 : 0);
}

try {
  main();
} catch (err) {
  console.error(`ERROR: ${err.message}`);
  process.exit(1);
}

