import type { CogniteClient } from "@cognite/sdk";
import { QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { createTestQueryClient } from "@/test-utils/wrappers";
import { UseFileContentContext, useFileContent } from "./useFileContent";

function wrapperForSdk(sdk: object) {
  const queryClient = createTestQueryClient();
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <UseFileContentContext.Provider
          value={{
            useCdfClient: () => ({
              sdk: sdk as CogniteClient,
              isAuthLoading: false,
            }),
            withCdfConcurrency: (fn) => fn(),
          }}
        >
          {children}
        </UseFileContentContext.Provider>
      </QueryClientProvider>
    );
  }
  return Wrapper;
}

describe("useFileContent", () => {
  it("returns text when download succeeds", async () => {
    const getDownloadUrls = vi
      .fn()
      .mockResolvedValue([{ downloadUrl: "https://dl.example/c" }]);
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      text: async () => "file body",
    });

    const Wrapper = wrapperForSdk({ files: { getDownloadUrls } });

    const { result } = renderHook(
      () => useFileContent({ space: "ops_summaries", externalId: "fid" }),
      { wrapper: Wrapper },
    );

    await waitFor(() => {
      expect(result.current.data).toBe("file body");
    });
  });

  it("returns null when response not ok", async () => {
    const getDownloadUrls = vi
      .fn()
      .mockResolvedValue([{ downloadUrl: "https://dl.example/c" }]);
    global.fetch = vi.fn().mockResolvedValue({
      ok: false,
      text: async () => "",
    });

    const Wrapper = wrapperForSdk({ files: { getDownloadUrls } });

    const { result } = renderHook(() => useFileContent({ externalId: "fid" }), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.data).toBeNull();
    });
  });

  it("disabled when no externalId", () => {
    const Wrapper = wrapperForSdk({
      files: { getDownloadUrls: vi.fn() },
    });

    const { result } = renderHook(() => useFileContent(null), {
      wrapper: Wrapper,
    });

    expect(result.current.fetchStatus).toBe("idle");
  });
});
