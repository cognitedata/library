/**
 * LLM prompt templates used by the CDF Helper Service.
 */

/**
 * Build the prompt used to compare two operation summaries.
 * The comparison focuses on data accuracy and content consistency,
 * not wording differences, and always ends with a parseable SIMILARITY_SCORE line.
 */
export function buildComparisonPrompt(
  originalSummary: string,
  newSummary: string,
): string {
  return `
Compare these two operation summaries focusing on DATA ACCURACY and CONTENT CONSISTENCY, not wording differences:

ORIGINAL SUMMARY:
${originalSummary}

NEW SUMMARY:
${newSummary}

Please analyze and provide:
1. DATA ACCURACY: Are the counts of assets, files, work orders, time series, and equipment the same or very similar?
2. CONTENT COVERAGE: Do both summaries cover the same data types and time periods?
3. KEY FINDINGS: Are the main operational insights and patterns consistent between summaries?
4. MINOR DIFFERENCES: Note any wording variations that don't affect the core meaning
5. OVERALL SIMILARITY SCORE (0-100): Focus on data accuracy and content consistency, not linguistic variations

End your reply with a line exactly in this form (required for parsing):
SIMILARITY_SCORE: <integer from 0 to 100>

IMPORTANT:
- Give high scores (80-100) if data counts and key findings are the same, even with different wording
- Give medium scores (60-79) if there are minor data discrepancies but overall content is similar
- Give low scores (0-59) only if there are significant data differences or missing content
- Ignore minor wording differences like "incomplete metadata" vs "gap in metadata documentation"
- Use 100 if the two summaries convey the same data and conclusions, including when wording is nearly identical
`;
}
