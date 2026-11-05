/**
 * Enhanced Refresh Hook.
 * Provides better user feedback for refresh operations with success/error states.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { getRefreshError } from "@/utils/errorHandling";

interface UseEnhancedRefreshOptions {
  onRefresh: () => Promise<void> | void;
  successMessage?: string;
  errorMessage?: string;
  feedbackDuration?: number;
}

interface UseEnhancedRefreshResult {
  refresh: () => Promise<void>;
  isRefreshing: boolean;
  showSuccess: boolean;
  showError: boolean;
  error: string | null;
  message: string;
}

/**
 * Enhanced refresh hook that provides user feedback for refresh operations.
 * Shows success/error states with automatic dismissal.
 */
export function useEnhancedRefresh({
  onRefresh,
  successMessage = "Refreshed successfully",
  errorMessage = "Failed to refresh",
  feedbackDuration = 2000,
}: UseEnhancedRefreshOptions): UseEnhancedRefreshResult {
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [showSuccess, setShowSuccess] = useState(false);
  const [showError, setShowError] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const successTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const errorTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (!showSuccess) return;
    successTimerRef.current = setTimeout(
      () => setShowSuccess(false),
      feedbackDuration,
    );
    return () => {
      if (successTimerRef.current !== null)
        clearTimeout(successTimerRef.current);
    };
  }, [showSuccess, feedbackDuration]);

  useEffect(() => {
    if (!showError) return;
    errorTimerRef.current = setTimeout(() => {
      setShowError(false);
      setError(null);
    }, feedbackDuration);
    return () => {
      if (errorTimerRef.current !== null) clearTimeout(errorTimerRef.current);
    };
  }, [showError, feedbackDuration]);

  const clearFeedback = useCallback(() => {
    if (successTimerRef.current !== null) clearTimeout(successTimerRef.current);
    if (errorTimerRef.current !== null) clearTimeout(errorTimerRef.current);
    setShowSuccess(false);
    setShowError(false);
    setError(null);
  }, []);

  const refresh = useCallback(async () => {
    if (isRefreshing) return;

    setIsRefreshing(true);
    clearFeedback();

    try {
      await onRefresh();
      setShowSuccess(true);
    } catch (err) {
      const errorMsg = getRefreshError(err);
      setError(errorMsg);
      setShowError(true);
    } finally {
      setIsRefreshing(false);
    }
  }, [isRefreshing, onRefresh, clearFeedback]);

  const message = showSuccess
    ? successMessage
    : showError
      ? error || errorMessage
      : "Refresh";

  return {
    refresh,
    isRefreshing,
    showSuccess,
    showError,
    error,
    message,
  };
}
