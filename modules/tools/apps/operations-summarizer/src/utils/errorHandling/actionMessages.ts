import { categorizeError } from "./categorize";

/**
 * Get an error message specific to summary generation failures.
 */
export function getSummaryGenerationError(error: unknown): string {
  const category = categorizeError(error);

  switch (category) {
    case "timeout":
      return "Summary generation timed out. Try selecting fewer data types or a shorter date range.";
    case "authentication":
      return "Your session has expired. Please refresh the page and log in again.";
    case "network":
      return "Lost connection to the server. Please check your internet and try again.";
    default:
      return "Failed to generate summary. Please try again or contact support if the issue persists.";
  }
}

/**
 * Get an error message specific to data fetching failures.
 */
export function getDataFetchError(error: unknown, dataType?: string): string {
  const category = categorizeError(error);
  const typeLabel = dataType ? ` for ${dataType}` : "";

  switch (category) {
    case "authentication":
      return "Your session has expired. Please refresh the page.";
    case "network":
      return `Unable to fetch data${typeLabel}. Check your connection.`;
    case "timeout":
      return `Data request${typeLabel} timed out. Try a smaller date range.`;
    default:
      return `Failed to load data${typeLabel}. Please try again.`;
  }
}

/**
 * Get an error message specific to agent operations.
 */
export function getAgentOperationError(
  error: unknown,
  operation: "create" | "delete" | "chat" | "list",
): string {
  const category = categorizeError(error);

  if (category === "authentication") {
    return "Your session has expired. Please refresh the page.";
  }

  switch (operation) {
    case "create":
      return "Failed to create the AI agent. Please check your settings and try again.";
    case "delete":
      return "Failed to delete the AI agent. Please try again.";
    case "chat":
      return "Failed to communicate with the AI agent. Please try again.";
    case "list":
      return "Failed to load available AI agents. Please refresh the page.";
    default:
      return "An error occurred with the AI agent. Please try again.";
  }
}

/**
 * Get an error message specific to data refresh operations.
 */
export function getRefreshError(error: unknown, dataType?: string): string {
  const category = categorizeError(error);
  const typeLabel = dataType ? ` for ${dataType}` : "";

  switch (category) {
    case "network":
      return `Unable to refresh data${typeLabel}. Please check your internet connection and try again.`;
    case "authentication":
      return "Your session has expired. Please refresh the page to log in again.";
    case "timeout":
      return `Data refresh${typeLabel} timed out. Please try again.`;
    default:
      return `Failed to refresh data${typeLabel}. Please try again.`;
  }
}

/**
 * Get an error message specific to PDF save operations.
 */
export function getSavePdfError(error: unknown): string {
  const category = categorizeError(error);
  const errorMessage =
    error instanceof Error ? error.message.toLowerCase() : "";

  if (
    category === "network" &&
    (errorMessage.includes("dynamically imported module") ||
      errorMessage.includes("importing a module script failed") ||
      errorMessage.includes("pdf") ||
      errorMessage.includes("module"))
  ) {
    return "The PDF generator couldn't be loaded. Please try again or download as Markdown instead.";
  }

  switch (category) {
    case "network":
      return "Unable to save PDF due to connection issues. Please check your internet connection and try again.";
    case "authentication":
      return "Your session has expired. Please refresh the page and try saving again.";
    case "validation":
      return "The summary content is invalid for PDF generation. Please regenerate the summary and try again.";
    case "timeout":
      return "PDF generation timed out. Please try again with a smaller summary.";
    default:
      return "Failed to save PDF. Please try again or download as Markdown instead.";
  }
}

/**
 * Get an error message specific to delete operations.
 */
export function getDeleteError(
  error: unknown,
  itemType: "summary" | "report" = "summary",
): string {
  const category = categorizeError(error);

  switch (category) {
    case "network":
      return `Unable to delete ${itemType} due to connection issues. Please check your internet connection and try again.`;
    case "authentication":
      return "Your session has expired. Please refresh the page and try again.";
    case "validation":
      return `The ${itemType} cannot be deleted. It may have already been removed.`;
    default:
      return `Failed to delete ${itemType}. Please try again.`;
  }
}
