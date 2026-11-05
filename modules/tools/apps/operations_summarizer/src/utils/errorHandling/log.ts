import { parseError } from "./parse";

/**
 * Log an informational message with a context prefix.
 */
export function logInfo(message: string, context: string): void {
  console.info(`[${context}] ${message}`);
}

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
