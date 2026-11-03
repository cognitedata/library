/**
 * Hook to run comparison test: resolve original content, generate new summary,
 * compare with AI, score, and persist result. Uses useTestingState for progress/result.
 */

import { createContext, useCallback, useContext } from "react";
import { getAppConfig } from "@/config/appConfig";
import { getEnabledDataTypes, getSummarySpace } from "@/config/dataTypes";
import { useCdfClient } from "@/contexts/CdfClientContext";
import { useAgents, useEnsureDefaultAgent } from "@/hooks/useCdfData";
import { useTestingState } from "@/hooks/useTestingState";
import { CDFHelper } from "@/services/cdfHelper";
import {
  CogniteSummaryFunctionService,
  type SummaryFunctionService,
} from "@/services/summaryFunctionService";
import { DATA_COUNT_KEYS, type SummaryInstance } from "@/types/operations";
import { getErrorMessage, logError } from "@/utils/errorHandling";
import {
  extractDataCounts,
  extractSimilarityScore,
  normalizeSummaryTextForComparison,
} from "@/utils/formatting";
import { resolveSummaryContent } from "@/utils/testingContent";

type UseComparisonTestDeps = {
  useCdfClient: typeof useCdfClient;
};

const defaultComparisonTestDeps: UseComparisonTestDeps = {
  useCdfClient,
};

export const UseComparisonTestContext = createContext<UseComparisonTestDeps>(
  defaultComparisonTestDeps,
);

interface UseComparisonTestResult {
  runComparisonTest: (selectedSummary: SummaryInstance) => Promise<void>;
  isRunning: boolean;
}

export function useComparisonTest(): UseComparisonTestResult {
  const { useCdfClient: useClient } = useContext(UseComparisonTestContext);
  const { sdk } = useClient();
  const { data: agents = [] } = useAgents();
  const ensureDefaultAgent = useEnsureDefaultAgent();
  const {
    testStartDate,
    testEndDate,
    selectedDataTypes,
    isRunning,
    setIsRunning,
    setTestResultData,
    setTestError,
    setGenerationProgress,
  } = useTestingState();

  const getHelperAgentId = useCallback((): string => {
    const helperAgent = agents.find(
      (a) => a.name === getAppConfig().agent.name,
    );
    if (helperAgent) return helperAgent.externalId;
    return sdk
      ? `${sdk.project}_operation_summarizer_helper_agent`
      : "operation_summarizer_helper_agent";
  }, [agents, sdk]);

  const runComparisonTest = useCallback(
    async (selectedSummary: SummaryInstance) => {
      if (!sdk || !testStartDate || !testEndDate || isRunning) return;

      setIsRunning(true);
      setTestResultData(null);
      setTestError(null);
      setGenerationProgress({
        status: "fetching",
        message: "Starting comparison test...",
        progress: 0,
      });

      try {
        const helper = new CDFHelper(sdk);

        const helperAgent = agents.find(
          (a) => a.name === getAppConfig().agent.name,
        );
        if (!helperAgent) {
          setGenerationProgress({
            status: "fetching",
            message: "Creating helper agent...",
            progress: 2,
          });
          try {
            await ensureDefaultAgent.mutateAsync();
          } catch (agentError) {
            console.warn(
              "Could not create default agent, will try to proceed:",
              agentError,
            );
          }
        }

        setGenerationProgress({
          status: "fetching",
          message: "Resolving original summary content...",
          progress: 5,
        });

        const originalContent = await resolveSummaryContent(
          selectedSummary,
          sdk,
          getSummarySpace(),
        );
        if (!originalContent.trim()) {
          throw new Error(
            "Original summary content is empty. The selected summary may not have been saved with content, or the file could not be fetched. Check browser console for details.",
          );
        }

        setGenerationProgress({
          status: "generating",
          message: "Generating new summary with function…",
          progress: 10,
        });

        const startDate = new Date(testStartDate);
        const endDate = new Date(testEndDate);
        const dataTypes =
          selectedDataTypes.length > 0
            ? selectedDataTypes
            : getEnabledDataTypes();
        const contents = {
          assets: dataTypes.includes("assets"),
          files: dataTypes.includes("files"),
          work_orders: dataTypes.includes("work_orders"),
          time_series: dataTypes.includes("time_series"),
          equipment: dataTypes.includes("equipment"),
        };
        const service: SummaryFunctionService =
          new CogniteSummaryFunctionService(sdk);
        const generated = await service.generate({
          periodStart: startDate,
          periodEnd: endDate,
          summaryContents: contents,
          selectedAgent: getHelperAgentId(),
          saveToCdf: false,
          generatePdf: false,
        });
        const newSummaryContent = generated.markdown;

        setGenerationProgress({
          status: "comparing",
          message: "Comparing summaries with AI...",
          progress: 85,
        });

        const comparisonResponse = await helper.compareSummaries(
          originalContent,
          newSummaryContent,
        );
        const comparisonText =
          comparisonResponse?.choices?.[0]?.message?.content ||
          "Comparison analysis not available.";

        const originalCounts = extractDataCounts(originalContent);
        const newCounts = extractDataCounts(newSummaryContent);

        let accuracyScore = 100;
        let totalDifference = 0;
        let totalOriginal = 0;
        for (const key of DATA_COUNT_KEYS) {
          const origVal = originalCounts[key];
          const newVal = newCounts[key];
          totalOriginal += origVal;
          totalDifference += Math.abs(origVal - newVal);
        }
        if (totalOriginal > 0) {
          accuracyScore = Math.max(
            0,
            100 - (totalDifference / totalOriginal) * 100,
          );
        } else if (totalDifference > 0) {
          accuracyScore = 0;
        }

        const summariesTextuallyIdentical =
          normalizeSummaryTextForComparison(originalContent) ===
          normalizeSummaryTextForComparison(newSummaryContent);

        let similarityScore = extractSimilarityScore(comparisonText);
        if (similarityScore === null) {
          similarityScore = summariesTextuallyIdentical
            ? 100
            : Math.round(accuracyScore);
        }

        setGenerationProgress({
          status: "complete",
          message: "Test complete!",
          progress: 100,
        });

        setTestResultData({
          originalSummaryId: selectedSummary.externalId,
          originalSummarySpace: selectedSummary.space,
          originalContent,
          newSummary: newSummaryContent,
          similarityScore,
          dataAccuracyScore: accuracyScore,
          originalDataCounts: originalCounts,
          newDataCounts: newCounts,
          comparisonAnalysis: comparisonText,
          timestamp: new Date().toISOString(),
        });
      } catch (error) {
        logError(error, "Comparison test");
        setTestError(getErrorMessage(error, "Test comparison"));
        setGenerationProgress({
          status: "error",
          message: String(error),
          progress: 0,
        });
      } finally {
        setIsRunning(false);
      }
    },
    [
      sdk,
      testStartDate,
      testEndDate,
      selectedDataTypes,
      isRunning,
      agents,
      ensureDefaultAgent,
      getHelperAgentId,
      setIsRunning,
      setTestResultData,
      setTestError,
      setGenerationProgress,
    ],
  );

  return { runComparisonTest, isRunning };
}
