import { parseError } from "./parse";

/**
 * Log an error with context for debugging.
 */
export function logError(
  error: unknown,
  context: string,
  additionalInfo?: Record<string, unknown>,
): void {
  const appError = parseError(error, context);

  console.error(`[${context}] ${appError.category}: ${appError.message}`, {
    category: appError.category,
    userMessage: appError.userMessage,
    originalError: appError.originalError,
    ...additionalInfo,
  });
}
