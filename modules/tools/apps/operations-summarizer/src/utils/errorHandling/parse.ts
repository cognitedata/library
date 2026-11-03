import { categorizeError } from "./categorize";
import type { AppError } from "./types";
import { ERROR_MESSAGES } from "./types";

/**
 * Parse an unknown error into a structured AppError.
 */
export function parseError(error: unknown, context?: string): AppError {
  const category = categorizeError(error);
  const baseMessage = ERROR_MESSAGES[category];

  let message: string;
  let originalError: Error | undefined;

  if (error instanceof Error) {
    message = error.message;
    originalError = error;
  } else if (typeof error === "string") {
    message = error;
  } else if (error && typeof error === "object" && "message" in error) {
    message = String((error as { message: unknown }).message);
  } else {
    message = "Unknown error";
  }

  const userMessage = context ? `${baseMessage} (${context})` : baseMessage;

  return {
    category,
    message,
    userMessage,
    originalError,
  };
}

/**
 * Get a user-friendly error message from any error.
 */
export function getErrorMessage(error: unknown, context?: string): string {
  return parseError(error, context).userMessage;
}

/**
 * Get the technical error message for logging.
 */
export function getTechnicalMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === "string") {
    return error;
  }
  return "Unknown error";
}
