/**
 * Test Report Utilities.
 * Provides functions for extracting and analyzing test report data.
 */

export type { TestReportStats } from "./testReportUtils/index";
export {
  calculateTestReportStats,
  extractAccuracyFromContent,
  extractDataTypesFromContent,
  extractDataVolumeFromContent,
  extractPeriodFromContent,
  extractScoreFromContent,
  extractTestReportData,
  getScoreBadgeVariant,
  getScoreCategory,
  SCORE_THRESHOLDS,
  vividScoreBadgeClassName,
} from "./testReportUtils/index";
