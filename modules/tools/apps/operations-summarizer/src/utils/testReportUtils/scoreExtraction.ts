/**
 * Score thresholds and extraction from test report content.
 */

export const SCORE_THRESHOLDS = {
  EXCELLENT: 90,
  GOOD: 70,
} as const;

/**
 * Extract similarity score from test report content.
 */
export function extractScoreFromContent(content: string): number {
  const scoreSlashMatch = content.match(
    /similarity\s*score\s*\**:?\**\s*(\d+(?:\.\d+)?)\s*\/\s*100/i,
  );
  if (scoreSlashMatch) return Number.parseFloat(scoreSlashMatch[1]);

  const emojiScoreMatch = content.match(
    /(?:🎉|⚠️|❌|✅)\s*(\d+(?:\.\d+)?)\s*\/\s*100/u,
  );
  if (emojiScoreMatch) return Number.parseFloat(emojiScoreMatch[1]);

  const headerScoreMatch = content.match(/Score:\s*(\d+(?:\.\d+)?)/i);
  if (headerScoreMatch) return Number.parseFloat(headerScoreMatch[1]);

  const similarityMatch = content.match(
    /similarity\s*score\s*\**:?\**\s*(\d+(?:\.\d+)?)\s*%/i,
  );
  if (similarityMatch) return Number.parseFloat(similarityMatch[1]);

  const underscoreMatch = content.match(
    /similarity_score[:\s]*(\d+(?:\.\d+)?)/i,
  );
  if (underscoreMatch) return Number.parseFloat(underscoreMatch[1]);

  const percentMatch = content.match(/(\d+(?:\.\d+)?)\s*%/);
  if (percentMatch) return Number.parseFloat(percentMatch[1]);

  return 0;
}

/**
 * Extract data accuracy score from test report content.
 */
export function extractAccuracyFromContent(content: string): number {
  const accuracySlashMatch = content.match(
    /(?:data\s*)?accuracy\s*(?:score)?\s*\**:?\**\s*(\d+(?:\.\d+)?)\s*\/\s*100/i,
  );
  if (accuracySlashMatch) return Number.parseFloat(accuracySlashMatch[1]);

  const accuracyMatch = content.match(
    /(?:data\s*)?accuracy\s*(?:score)?\s*\**:?\**\s*(\d+(?:\.\d+)?)\s*%?/i,
  );
  if (accuracyMatch) return Number.parseFloat(accuracyMatch[1]);

  return extractScoreFromContent(content);
}
