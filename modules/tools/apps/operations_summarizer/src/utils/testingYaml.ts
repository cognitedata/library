/**
 * Build YAML string for the Testing page export (eval-style test case).
 * Keeps YAML generation out of the page component.
 */

import type { ComparisonTestResult, DataTypeName } from "@/types/operations";
import { DATA_COUNT_KEYS } from "@/types/operations";

export function buildTestingPageYaml(
  testResult: ComparisonTestResult,
  testStartDate: string,
  testEndDate: string,
  selectedDataTypes: DataTypeName[],
): string {
  return `# Evaluation Test Case
test_name: "Summary Comparison - ${testResult.timestamp.toISOString()}"
model: "azure/gpt-4.1"
test_type: "summary_consistency"

inputs:
  original_summary: |
${testResult.originalContent
  .split("\n")
  .map((l) => `    ${l}`)
  .join("\n")}

expected:
  similarity_score_min: 80
  data_counts:
${DATA_COUNT_KEYS.map(
  (k) => `    ${k}: ${testResult.originalDataCounts[k]}`,
).join("\n")}

actual:
  similarity_score: ${testResult.similarityScore}
  data_accuracy_score: ${testResult.dataAccuracyScore}
  new_data_counts:
${DATA_COUNT_KEYS.map((k) => `    ${k}: ${testResult.newDataCounts[k]}`).join(
  "\n",
)}

metadata:
  timestamp: "${testResult.timestamp.toISOString()}"
  period_start: "${testStartDate}"
  period_end: "${testEndDate}"
  data_types:
${selectedDataTypes.map((t) => `    - ${t}`).join("\n")}
`;
}
