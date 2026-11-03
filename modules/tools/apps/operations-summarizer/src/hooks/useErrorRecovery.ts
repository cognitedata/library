/**
 * Error Recovery Hook.
 * Provides error handling with user configuration preservation and recovery options.
 */

import { useCallback, useState } from "react";
import { useOperations } from "@/contexts/OperationsSummarizerContext";
import {
  computeUserRetryDelayMs,
  getHttpStatusFromError,
  sleep,
} from "@/utils/cdfRetry";
import { getSummaryGenerationError, logError } from "@/utils/errorHandling";

interface UseErrorRecoveryOptions {
  preserveUserConfig?: boolean;
  /** Base delay (ms) for exponential backoff before user-triggered retry. */
  initialBackoffMs?: number;
  /** @deprecated Use `initialBackoffMs`. If set, overrides default base when `initialBackoffMs` is omitted. */
  retryDelay?: number;
}

interface UseErrorRecoveryResult {
  handleError: (
    error: unknown,
    operation: string,
    details?: Record<string, unknown>,
  ) => void;
  recoverFromError: () => void;
  retryLastOperation: () => Promise<void>;
  clearError: () => void;
  hasRecoverableError: boolean;
  errorMessage: string | null;
  canRetry: boolean;
  retryCount: number;
}

interface ErrorState {
  message: string;
  operation: string;
  details?: Record<string, unknown>;
  timestamp: Date;
  retryCount: number;
  isRecoverable: boolean;
  /** Original error for Retry-After / status on retry. */
  cause: unknown;
}

/**
 * Enhanced error handling hook that preserves user configuration and provides recovery options.
 */
export function useErrorRecovery({
  preserveUserConfig = true,
  initialBackoffMs,
  retryDelay,
}: UseErrorRecoveryOptions = {}): UseErrorRecoveryResult {
  const backoffBaseMs = initialBackoffMs ?? retryDelay;
  const { state, dispatch } = useOperations();
  const [errorState, setErrorState] = useState<ErrorState | null>(null);

  const handleError = useCallback(
    (error: unknown, operation: string, details?: Record<string, unknown>) => {
      const errorMessage =
        operation === "summary-generation"
          ? getSummaryGenerationError(error)
          : error instanceof Error
            ? error.message
            : String(error);

      logError(error, operation, {
        preserveUserConfig,
        userConfig: preserveUserConfig
          ? {
              summaryPeriodStart: state.summaryPeriodStart.toISOString(),
              summaryPeriodEnd: state.summaryPeriodEnd.toISOString(),
              summaryContents: state.summaryContents,
              selectedAgent: state.selectedAgent,
            }
          : undefined,
        ...details,
      });

      const isRecoverable = isErrorRecoverable(error, operation);

      setErrorState({
        message: errorMessage,
        operation,
        details,
        timestamp: new Date(),
        retryCount: 0,
        isRecoverable,
        cause: error,
      });

      // Update the relevant error state in the context
      if (operation === "summary-generation") {
        dispatch({ type: "SET_GENERATION_ERROR", payload: errorMessage });

        // Reset generation progress but preserve user configuration
        dispatch({
          type: "SET_GENERATION_PROGRESS",
          payload: {
            status: "error",
            message: errorMessage,
            progress: 0,
          },
        });
        dispatch({ type: "SET_IS_GENERATING", payload: false });
      }
    },
    [state, dispatch, preserveUserConfig],
  );

  const recoverFromError = useCallback(() => {
    if (!errorState) return;

    // Reset error states but preserve user configuration
    if (errorState.operation === "summary-generation") {
      // Only reset the generation state, not the user config
      dispatch({
        type: "SET_GENERATION_PROGRESS",
        payload: {
          status: "idle",
          message: "Ready to generate",
          progress: 0,
        },
      });
      dispatch({ type: "SET_GENERATION_ERROR", payload: null });
      dispatch({ type: "SET_SHOW_SUMMARY", payload: false });

      // Explicitly preserve user configuration by not resetting these:
      // - summaryPeriodStart
      // - summaryPeriodEnd
      // - summaryContents
      // - selectedAgent
    }

    setErrorState(null);
  }, [errorState, dispatch]);

  // User-triggered retry only. By the time an error surfaces here the SDK
  // (sdk-core) has already exhausted its own auto-retries for transient failures
  // (429, 5xx, network). This adds a manual escape hatch for the user with a
  // short backoff + Retry-After honoring. The 3-attempt cap (canRetry) reflects
  // that if SDK auto-retries already failed, unlimited manual retries won't help.
  const retryLastOperation = useCallback(async () => {
    if (!errorState?.isRecoverable) {
      throw new Error("No retryable operation available");
    }

    const attemptForDelay = errorState.retryCount;
    const delayMs = computeUserRetryDelayMs({
      attempt: attemptForDelay,
      error: errorState.cause,
      errorTimestamp: errorState.timestamp,
      ...(backoffBaseMs !== undefined ? { baseMs: backoffBaseMs } : {}),
    });

    const updatedErrorState = {
      ...errorState,
      retryCount: errorState.retryCount + 1,
    };

    setErrorState(updatedErrorState);

    if (delayMs > 0) {
      await sleep(delayMs);
    }
  }, [errorState, backoffBaseMs]);

  const clearError = useCallback(() => {
    setErrorState(null);

    // Clear context error states
    dispatch({ type: "SET_GENERATION_ERROR", payload: null });
  }, [dispatch]);

  return {
    handleError,
    recoverFromError,
    retryLastOperation,
    clearError,
    hasRecoverableError: errorState?.isRecoverable || false,
    errorMessage: errorState?.message || null,
    canRetry:
      (errorState?.isRecoverable && (errorState?.retryCount || 0) < 3) || false,
    retryCount: errorState?.retryCount || 0,
  };
}

/**
 * Determine if an error is recoverable based on its type and operation context.
 */
function isErrorRecoverable(error: unknown, operation: string): boolean {
  const status = getHttpStatusFromError(error);
  if (status === 429 || status === 502 || status === 503 || status === 504) {
    return true;
  }

  if (error instanceof Error) {
    const message = error.message.toLowerCase();

    // Network errors are often recoverable
    if (
      message.includes("network") ||
      message.includes("fetch") ||
      message.includes("timeout")
    ) {
      return true;
    }

    // Authentication errors may be recoverable if token refresh is possible
    if (message.includes("401") || message.includes("unauthorized")) {
      return true;
    }

    // Server errors might be temporary
    if (
      message.includes("500") ||
      message.includes("502") ||
      message.includes("503") ||
      message.includes("504") ||
      message.includes("429")
    ) {
      return true;
    }

    // Validation errors are usually not recoverable without user action
    if (message.includes("400") || message.includes("validation")) {
      return false;
    }
  }

  // For summary generation, most errors are recoverable by retrying
  if (operation === "summary-generation") {
    return true;
  }

  // Conservative default
  return false;
}
