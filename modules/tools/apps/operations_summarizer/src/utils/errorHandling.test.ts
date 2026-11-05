import { describe, expect, it, vi } from "vitest";
import {
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
} from "./errorHandling";

describe("categorizeError", () => {
  it("categorizes network errors", () => {
    expect(categorizeError(new Error("Failed to fetch"))).toBe("network");
    expect(categorizeError(new Error("network error"))).toBe("network");
  });

  it("treats bundler chunk load failures as network, not generic chunk wording", () => {
    expect(categorizeError(new Error("Loading chunk 12 failed: timeout"))).toBe(
      "network",
    );
    expect(categorizeError(new Error("ChunkLoadError: mock"))).toBe("network");
    const named = new Error("x");
    named.name = "ChunkLoadError";
    expect(categorizeError(named)).toBe("network");
    expect(
      categorizeError(new Error("Failed while processing chunk 3 of data")),
    ).toBe("unknown");
  });

  it("categorizes authentication errors", () => {
    expect(categorizeError(new Error("401 Unauthorized"))).toBe(
      "authentication",
    );
    expect(categorizeError(new Error("token expired"))).toBe("authentication");
    expect(categorizeError(new Error("403 Forbidden"))).toBe("authentication");
  });

  it("categorizes validation errors", () => {
    expect(categorizeError(new Error("400 Bad Request"))).toBe("validation");
    expect(categorizeError(new Error("Invalid input"))).toBe("validation");
  });

  it("categorizes timeout errors", () => {
    expect(categorizeError(new Error("Request timed out"))).toBe("timeout");
    expect(categorizeError(new Error("504 Gateway Timeout"))).toBe("timeout");
  });

  it("categorizes API errors", () => {
    expect(categorizeError(new Error("500 Internal Server Error"))).toBe("api");
    expect(categorizeError(new Error("502 Bad Gateway"))).toBe("api");
  });

  it("returns unknown for non-Error or unrecognized messages", () => {
    expect(categorizeError("string error")).toBe("unknown");
    expect(categorizeError(new Error("something else"))).toBe("unknown");
  });
});

describe("parseError", () => {
  it("parses Error into AppError", () => {
    const err = parseError(new Error("test message"), "context");
    expect(err.category).toBeDefined();
    expect(err.message).toBe("test message");
    expect(err.userMessage).toContain("context");
    expect(err.originalError).toBeInstanceOf(Error);
  });

  it("parses string into AppError", () => {
    const err = parseError("string error");
    expect(err.message).toBe("string error");
  });

  it("parses object with message property", () => {
    const err = parseError({ message: "from object" });
    expect(err.message).toBe("from object");
  });
});

describe("getErrorMessage", () => {
  it("returns user-friendly message", () => {
    const msg = getErrorMessage(new Error("raw"), "Summary");
    expect(msg).toBeTruthy();
    expect(typeof msg).toBe("string");
  });
});

describe("getTechnicalMessage", () => {
  it("returns error message for Error", () => {
    expect(getTechnicalMessage(new Error("technical"))).toBe("technical");
  });
  it("returns string as-is", () => {
    expect(getTechnicalMessage("string")).toBe("string");
  });
  it("returns 'Unknown error' for other", () => {
    expect(getTechnicalMessage(null)).toBe("Unknown error");
  });
});

describe("logError", () => {
  it("does not throw", () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    expect(() => logError(new Error("test"), "context")).not.toThrow();
    consoleSpy.mockRestore();
  });
});

describe("getSummaryGenerationError", () => {
  it("returns timeout message for timeout errors", () => {
    const msg = getSummaryGenerationError(new Error("timed out"));
    expect(msg).toContain("timed out");
  });
  it("returns auth message for auth errors", () => {
    const msg = getSummaryGenerationError(new Error("401"));
    expect(msg).toContain("session");
  });
});

describe("getDataFetchError", () => {
  it("returns message with optional data type", () => {
    const msg = getDataFetchError(new Error("fail"), "assets");
    expect(msg).toContain("assets");
  });
});

describe("getAgentOperationError", () => {
  it("returns operation-specific messages", () => {
    expect(getAgentOperationError(new Error("x"), "create")).toContain(
      "create",
    );
    expect(getAgentOperationError(new Error("x"), "list")).toContain("load");
  });
});

describe("getSavePdfError", () => {
  it("maps PDF module load failures on network category", () => {
    const msg = getSavePdfError(
      new Error("Failed dynamically imported module pdf"),
    );
    expect(msg).toContain("PDF generator");
  });

  it("maps validation and timeout categories", () => {
    expect(getSavePdfError(new Error("400 validation"))).toContain("invalid");
    expect(getSavePdfError(new Error("timeout"))).toContain("timed out");
  });

  it("returns default message for unknown errors", () => {
    expect(getSavePdfError(new Error("weird"))).toContain("Markdown");
  });
});

describe("getDeleteError", () => {
  it("uses item type in network message", () => {
    expect(getDeleteError(new Error("fetch failed"), "summary")).toContain(
      "summary",
    );
    expect(getDeleteError(new Error("fetch failed"), "report")).toContain(
      "report",
    );
  });
});

describe("getRefreshError", () => {
  it("covers network, auth, timeout, and default branches", () => {
    expect(getRefreshError(new Error("fetch failed"))).toContain("internet");
    expect(getRefreshError(new Error("401"))).toContain("session");
    expect(getRefreshError(new Error("timeout"))).toContain("timed out");
    expect(getRefreshError(new Error("other"))).toContain("refresh");
  });
});
