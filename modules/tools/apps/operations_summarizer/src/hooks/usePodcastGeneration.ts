/**
 * ViewModel: trigger podcast generation via the deployed Cognite Function.
 */

import { useQueryClient } from "@tanstack/react-query";
import { createContext, useContext, useState } from "react";
import { useCdfClient } from "@/contexts/CdfClientContext";
import { extractDateRangeFromSummary } from "@/services/cdfHelper";
import {
  CognitePodcastFunctionService,
  type PodcastFunctionResult,
  type PodcastFunctionService,
} from "@/services/podcastFunctionService";
import type { SummaryInstance } from "@/types/operations";
import { CDF_QUERY_KEYS } from "./cdfQueryKeys";

export type UsePodcastGenerationValue = {
  generate: (summary: SummaryInstance) => Promise<PodcastFunctionResult>;
  isGenerating: boolean;
  error: string | null;
  result: PodcastFunctionResult | null;
  clearError: () => void;
};

type UsePodcastGenerationDeps = {
  useCdfClient: typeof useCdfClient;
  createService: (
    sdk: NonNullable<ReturnType<typeof useCdfClient>["sdk"]>,
  ) => PodcastFunctionService;
};

const defaultDeps: UsePodcastGenerationDeps = {
  useCdfClient,
  createService: (sdk) => new CognitePodcastFunctionService(sdk),
};

export const UsePodcastGenerationContext =
  createContext<UsePodcastGenerationDeps>(defaultDeps);

export function usePodcastGeneration(): UsePodcastGenerationValue {
  const { useCdfClient: useClient, createService } = useContext(
    UsePodcastGenerationContext,
  );
  const { sdk } = useClient();
  const queryClient = useQueryClient();
  const [isGenerating, setIsGenerating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<PodcastFunctionResult | null>(null);

  async function generate(
    summary: SummaryInstance,
  ): Promise<PodcastFunctionResult> {
    if (!sdk) {
      const message = "CDF client is not available";
      setError(message);
      throw new Error(message);
    }

    setIsGenerating(true);
    setError(null);
    try {
      const { startDate, endDate } = extractDateRangeFromSummary(
        summary.externalId,
      );
      const service = createService(sdk);
      const next = await service.generate({
        summaryExternalId: summary.externalId,
        summarySpace: summary.space,
        sourceSummaryId: summary.externalId,
        startDate,
        endDate,
      });
      setResult(next);
      await queryClient.invalidateQueries({
        queryKey: CDF_QUERY_KEYS.podcast,
      });
      return next;
    } catch (err) {
      const message =
        err instanceof Error ? err.message : "Podcast generation failed";
      setError(message);
      throw err instanceof Error ? err : new Error(message);
    } finally {
      setIsGenerating(false);
    }
  }

  return {
    generate,
    isGenerating,
    error,
    result,
    clearError: () => setError(null),
  };
}
