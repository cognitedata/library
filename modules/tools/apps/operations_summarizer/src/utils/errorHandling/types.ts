/**
 * Error types and base message templates.
 */

export type ErrorCategory =
  "network" | "authentication" | "validation" | "api" | "timeout" | "unknown";

export interface AppError {
  category: ErrorCategory;
  message: string;
  userMessage: string;
  originalError?: Error;
  details?: Record<string, unknown>;
}

export const ERROR_MESSAGES: Record<ErrorCategory, string> = {
  network:
    "Unable to connect to the server. Please check your internet connection.",
  authentication:
    "Your session has expired. Please refresh the page and try again.",
  validation:
    "The provided data is invalid. Please check your inputs and try again.",
  api: "An error occurred while communicating with the server. Please try again.",
  timeout:
    "The request took too long to complete. Please try again with a smaller data set.",
  unknown: "An unexpected error occurred. Please try again.",
};
