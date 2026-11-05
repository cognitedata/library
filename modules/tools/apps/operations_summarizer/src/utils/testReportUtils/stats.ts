import type { SummaryInstance } from "@/types/operations";
import { extractSummaryContent } from "@/utils/summaryDownload";
import { parseTestReportMetrics } from "@/utils/testingSave";
import {
  extractDataTypesFromContent,
  extractDataVolumeFromContent,
  extractPeriodFromContent,
} from "./contentExtraction";
import { getScoreCategory } from "./scoreDisplay";
import {
  extractAccuracyFromContent,
  extractScoreFromContent,
} from "./scoreExtraction";

export interface TestReportStats {
  totalTests: number;
  averageScore: number;
  excellentCount: number;
  goodCount: number;
  poorCount: number;
  averageDataVolume: number;
}

interface TestReportData {
  name: string;
  content: string;
  score: number;
  accuracy: number;
  volume: number;
  date: string;
  period: string;
  dataTypes: string[];
}

/**
 * Calculate statistics from a list of test reports.
 */
export function calculateTestReportStats(
  reports: SummaryInstance[],
): TestReportStats {
  if (reports.length === 0) {
    return {
      totalTests: 0,
      averageScore: 0,
      excellentCount: 0,
      goodCount: 0,
      poorCount: 0,
      averageDataVolume: 0,
    };
  }

  let totalScore = 0;
  let totalVolume = 0;
  let excellent = 0;
  let good = 0;
  let poor = 0;

  for (const report of reports) {
    const metrics = parseTestReportMetrics(report.sourceContext);
    let score: number;
    let volume: number;
    if (metrics) {
      score = metrics.similarityScore;
      volume = Object.values(metrics.newDataCounts).reduce(
        (s, v) => s + (v ?? 0),
        0,
      );
    } else {
      const { content } = extractSummaryContent(report);
      score = extractScoreFromContent(content);
      volume = extractDataVolumeFromContent(content);
    }

    totalScore += score;
    totalVolume += volume;

    const category = getScoreCategory(score);
    if (category === "excellent") excellent++;
    else if (category === "good") good++;
    else poor++;
  }

  return {
    totalTests: reports.length,
    averageScore: totalScore / reports.length,
    excellentCount: excellent,
    goodCount: good,
    poorCount: poor,
    averageDataVolume: totalVolume / reports.length,
  };
}

/**
 * Extract all relevant data from a test report for display.
 */
export function extractTestReportData(
  report: SummaryInstance,
  displayDate: string,
): TestReportData {
  const { name, content } = extractSummaryContent(report);
  const metrics = parseTestReportMetrics(report.sourceContext);

  const score = metrics?.similarityScore ?? extractScoreFromContent(content);
  const accuracy =
    metrics?.dataAccuracyScore ?? extractAccuracyFromContent(content);
  const volume = metrics
    ? Object.values(metrics.newDataCounts).reduce((s, v) => s + (v ?? 0), 0)
    : extractDataVolumeFromContent(content);
  const period = metrics
    ? `${metrics.periodStart} — ${metrics.periodEnd}`
    : extractPeriodFromContent(content) ||
      extractPeriodFromContent(report.externalId);
  const dataTypes = extractDataTypesFromContent(content);

  return {
    name,
    content,
    score,
    accuracy,
    volume,
    date: displayDate,
    period,
    dataTypes,
  };
}
