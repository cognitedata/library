/**
 * Summary generation ViewModel. Calls the deployed summarizer function.
 */

import { useQueryClient } from "@tanstack/react-query";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { getDefaultPromptTemplates } from "@/config/dataTypes";
import { useCdfClient } from "@/contexts/CdfClientContext";
import { useOperations } from "@/contexts/OperationsSummarizerContext";
import { CDF_QUERY_KEYS } from "@/hooks/cdfQueryKeys";
import {
  creptGenerationProgress,
  progressFromSummaryLog,
} from "@/hooks/summaryGenerationProgress";
import { useErrorRecovery } from "@/hooks/useErrorRecovery";
import {
  CogniteSummaryFunctionService,
  type SummaryFunctionResult,
  type SummaryFunctionService,
} from "@/services/summaryFunctionService";
import type { DataTypeName } from "@/types/operations";
import type { SummaryGenerationProgressUpdate } from "@/utils/summaryGenerationTypes";

export interface SummaryResult {
  markdown: string;
  workOrderCount: number;
  newInstancesCount: Record<DataTypeName, number>;
  generatedAt: Date;
  dataTypeTruncation: SummaryFunctionResult["dataTypeTruncation"];
  cdfResult?: SummaryFunctionResult["cdfResult"];
}

interface UseSummaryGenerationResult {
  generate: () => Promise<SummaryResult | null>;
  cancel: () => void;
  progress: SummaryGenerationProgressUpdate;
  isGenerating: boolean;
  elapsedSeconds: number;
  result: SummaryResult | null;
  error: string | null;
  reset: () => void;
  recoverFromError: () => void;
  retryGeneration: () => Promise<SummaryResult | null>;
  canRetry: boolean;
}

type Deps = {
  useCdfClient: typeof useCdfClient;
  createService: (
    sdk: NonNullable<ReturnType<typeof useCdfClient>["sdk"]>,
  ) => SummaryFunctionService;
};

const defaultDeps: Deps = {
  useCdfClient,
  createService: (sdk) => new CogniteSummaryFunctionService(sdk),
};

export const UseSummaryGenerationContext = createContext<Deps>(defaultDeps);

function editedPrompts(
  current: Record<string, string>,
): Record<string, string> | undefined {
  const defaults = getDefaultPromptTemplates();
  const overrides: Record<string, string> = {};
  for (const [key, value] of Object.entries(current)) {
    if (value && value !== defaults[key]) overrides[key] = value;
  }
  return Object.keys(overrides).length > 0 ? overrides : undefined;
}

export function useSummaryGeneration(): UseSummaryGenerationResult {
  const { useCdfClient: useClient, createService } = useContext(
    UseSummaryGenerationContext,
  );
  const { sdk } = useClient();
  const { state, dispatch } = useOperations();
  const queryClient = useQueryClient();

  const progress = state.generationProgress as SummaryGenerationProgressUpdate;
  const error = state.generationError;
  const [result, setResult] = useState<SummaryResult | null>(null);
  const [elapsedSeconds, setElapsedSeconds] = useState(0);

  const { handleError, recoverFromError, retryLastOperation, canRetry } =
    useErrorRecovery({ preserveUserConfig: true });

  const progressRef = useRef(progress);
  progressRef.current = progress;
  const abortControllerRef = useRef<AbortController | null>(null);
  const runIdRef = useRef(0);

  const isGenerating = progress.status === "generating";

  useEffect(() => {
    if (!isGenerating) return;
    setElapsedSeconds(0);
    const started = Date.now();
    const timer = setInterval(() => {
      const elapsed = Math.floor((Date.now() - started) / 1000);
      setElapsedSeconds(elapsed);
      const current = progressRef.current;
      if (current.status !== "generating") return;
      const crept = creptGenerationProgress(elapsed);
      if (crept <= current.progress) return;
      dispatch({
        type: "SET_GENERATION_PROGRESS",
        payload: {
          status: "generating",
          message: current.message || "Generating in CDF Function…",
          progress: crept,
          currentStep: current.currentStep,
        },
      });
    }, 1000);
    return () => clearInterval(timer);
  }, [isGenerating, dispatch]);

  const setProgress = useCallback(
    (update: SummaryGenerationProgressUpdate) => {
      dispatch({ type: "SET_GENERATION_PROGRESS", payload: update });
    },
    [dispatch],
  );

  const setError = useCallback(
    (err: string | null) => {
      dispatch({ type: "SET_GENERATION_ERROR", payload: err });
    },
    [dispatch],
  );

  const reset = useCallback(() => {
    dispatch({
      type: "SET_GENERATION_PROGRESS",
      payload: { status: "idle", message: "Ready to generate", progress: 0 },
    });
    dispatch({ type: "SET_GENERATION_ERROR", payload: null });
    setResult(null);
    setElapsedSeconds(0);
    recoverFromError();
  }, [dispatch, recoverFromError]);

  const generate = useCallback(async (): Promise<SummaryResult | null> => {
    if (!sdk || !state.selectedAgent) {
      setError("SDK or agent not available");
      return null;
    }
    const periodStart = state.summaryPeriodStart;
    const periodEnd = state.summaryPeriodEnd;
    if (!periodStart || !periodEnd) {
      setError("Please configure date range and data types");
      return null;
    }
    if (!Object.values(state.summaryContents).some(Boolean)) {
      setError("Please configure date range and data types");
      return null;
    }

    abortControllerRef.current?.abort();
    const controller = new AbortController();
    abortControllerRef.current = controller;
    const runId = ++runIdRef.current;

    try {
      setError(null);
      setProgress({
        status: "generating",
        message: "Generating in CDF Function…",
        progress: 10,
        currentStep: "Running",
      });

      const service = createService(sdk);
      const generated = await service.generate(
        {
          periodStart,
          periodEnd,
          summaryContents: state.summaryContents,
          selectedAgent: state.selectedAgent,
          promptTemplates: editedPrompts(state.summaryPrompts),
          saveToCdf: true,
          generatePdf: true,
          fusionDomain:
            typeof window !== "undefined" ? window.location.host : undefined,
        },
        {
          signal: controller.signal,
          onLog: (line) => {
            if (runId !== runIdRef.current) return;
            const update = progressFromSummaryLog(line);
            if (!update || update.progress <= progressRef.current.progress)
              return;
            setProgress({ status: "generating", ...update });
          },
        },
      );
      if (runId !== runIdRef.current || controller.signal.aborted) return null;

      const summaryResult: SummaryResult = {
        markdown: generated.markdown,
        workOrderCount: generated.instanceCounts.work_orders,
        newInstancesCount: generated.instanceCounts,
        generatedAt: generated.generatedAt,
        dataTypeTruncation: generated.dataTypeTruncation,
        cdfResult: generated.cdfResult,
      };
      setResult(summaryResult);
      setProgress({
        status: "complete",
        message: summaryResult.cdfResult ? "Saved" : "Generated",
        progress: 100,
      });
      if (summaryResult.cdfResult) {
        void queryClient.invalidateQueries({
          queryKey: CDF_QUERY_KEYS.summaries,
        });
      }
      return summaryResult;
    } catch (err) {
      if (runId !== runIdRef.current) return null;
      if (err instanceof DOMException && err.name === "AbortError") {
        reset();
        return null;
      }
      const message = err instanceof Error ? err.message : String(err);
      if (message.toLowerCase().includes("not found")) {
        setError(
          "Summarizer function was not found. Deploy Operations_Summarizer_Function_v2 before generating.",
        );
        setProgress({
          status: "error",
          message: "Function not found",
          progress: 0,
        });
        return null;
      }
      if (message.toLowerCase().includes("timed out")) {
        setError(
          `${message}. Try a shorter date range. A large work-order period can exceed the function time limit.`,
        );
        setProgress({ status: "error", message: "Timed out", progress: 0 });
        return null;
      }
      handleError(err, "summary-generation", {
        periodStart: periodStart.toISOString(),
        periodEnd: periodEnd.toISOString(),
      });
      setProgress({ status: "error", message: "Failed", progress: 0 });
      return null;
    }
  }, [
    sdk,
    state.selectedAgent,
    state.summaryPeriodStart,
    state.summaryPeriodEnd,
    state.summaryContents,
    state.summaryPrompts,
    setProgress,
    setError,
    handleError,
    reset,
    createService,
    queryClient,
  ]);

  const cancel = useCallback(() => {
    runIdRef.current += 1;
    abortControllerRef.current?.abort();
    setProgress({
      status: "idle",
      message:
        "Stopped waiting. The function may still finish and save a summary.",
      progress: 0,
    });
  }, [setProgress]);

  const retryGeneration =
    useCallback(async (): Promise<SummaryResult | null> => {
      await retryLastOperation();
      return generate();
    }, [retryLastOperation, generate]);

  return {
    generate,
    cancel,
    progress,
    isGenerating,
    elapsedSeconds,
    result,
    error,
    reset,
    recoverFromError,
    retryGeneration,
    canRetry,
  };
}
