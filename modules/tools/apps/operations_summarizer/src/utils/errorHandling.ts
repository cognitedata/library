/**
 * Error Handling Utilities.
 * Provides consistent error handling and user-friendly error messages.
 */

export type { AppError, ErrorCategory } from "./errorHandling/index";
export {
  categorizeError,
  getAgentOperationError,
  getDataFetchError,
  getDeleteError,
  getErrorMessage,
  getRefreshError,
  getSavePdfError,
  getSummaryGenerationError,
  getTechnicalMessage,
  logError,
  parseError,
} from "./errorHandling/index";
