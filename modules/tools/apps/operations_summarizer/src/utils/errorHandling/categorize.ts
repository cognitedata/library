import type { ErrorCategory } from "./types";

/**
 * Determine the error category based on error characteristics.
 */
export function categorizeError(error: unknown): ErrorCategory {
  if (error instanceof Error) {
    const message = error.message.toLowerCase();
    const name = error.name.toLowerCase();

    if (
      message.includes("network") ||
      message.includes("fetch") ||
      message.includes("failed to fetch") ||
      message.includes("dynamically imported module") ||
      message.includes("importing a module script failed") ||
      message.includes("chunkloaderror") ||
      message.includes("loading chunk") ||
      name.includes("chunkloaderror") ||
      name.includes("network")
    ) {
      return "network";
    }

    if (
      message.includes("401") ||
      message.includes("403") ||
      message.includes("unauthorized") ||
      message.includes("forbidden") ||
      message.includes("authentication") ||
      message.includes("token")
    ) {
      return "authentication";
    }

    if (
      message.includes("400") ||
      message.includes("validation") ||
      message.includes("invalid")
    ) {
      return "validation";
    }

    if (
      message.includes("timeout") ||
      message.includes("timed out") ||
      message.includes("504")
    ) {
      return "timeout";
    }

    if (
      message.includes("500") ||
      message.includes("502") ||
      message.includes("503") ||
      message.includes("api") ||
      message.includes("server")
    ) {
      return "api";
    }
  }

  return "unknown";
}
