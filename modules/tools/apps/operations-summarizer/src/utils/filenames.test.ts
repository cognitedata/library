import { describe, expect, it, vi } from "vitest";
import { generateFilename } from "./filenames";

describe("generateFilename", () => {
  it("with extension includes _to_ segment and extension", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2024-06-15T12:34:56.789Z"));
    const name = generateFilename("ops", "2024-01-01", "2024-01-31", "pdf");
    expect(name).toBe("ops_2024-01-01_to_2024-01-31_2024-06-15T1234.pdf");
    vi.useRealTimers();
  });

  it("without extension produces CDF-safe characters", () => {
    const name = generateFilename("ops", "2024-01-01", "2024-01-31");
    expect(name).toContain("ops");
    expect(name).toMatch(/2024-01-01/);
    expect(name).not.toContain(" ");
  });
});
