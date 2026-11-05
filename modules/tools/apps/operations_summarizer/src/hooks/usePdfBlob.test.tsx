import type { CogniteClient } from "@cognite/sdk";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import { clearPdfBlobCache, UsePdfBlobContext, usePdfBlob } from "./usePdfBlob";

function pdfWrapper(sdk: CogniteClient | null) {
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <UsePdfBlobContext.Provider
        value={{
          useCdfClient: () => ({ sdk, isAuthLoading: !sdk }),
          withCdfConcurrency: (fn) => fn(),
        }}
      >
        {children}
      </UsePdfBlobContext.Provider>
    );
  }
  return Wrapper;
}

describe("usePdfBlob", () => {
  afterEach(() => {
    clearPdfBlobCache();
    vi.restoreAllMocks();
  });

  it("clears when disabled", () => {
    const Wrapper = pdfWrapper(
      cogniteClientMock({
        project: "p",
        files: { getDownloadUrls: vi.fn() },
      }),
    );

    const { result } = renderHook(
      () =>
        usePdfBlob({
          externalId: "f1",
          enabled: false,
        }),
      { wrapper: Wrapper },
    );

    expect(result.current.pdfFile).toBeNull();
    expect(result.current.isLoadingPdf).toBe(false);
  });

  it("fetches PDF blob when enabled", async () => {
    const getDownloadUrls = vi
      .fn()
      .mockResolvedValue([{ downloadUrl: "https://dl.example/x" }]);
    const Wrapper = pdfWrapper(
      cogniteClientMock({
        project: "p",
        files: { getDownloadUrls },
      }),
    );

    const blob = new Blob(["%PDF"], { type: "application/pdf" });
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      blob: async () => blob,
    });

    const { result } = renderHook(
      () =>
        usePdfBlob({
          externalId: "file-ext",
          enabled: true,
        }),
      { wrapper: Wrapper },
    );

    await waitFor(() => {
      expect(result.current.pdfFile).toEqual(blob);
    });
    expect(getDownloadUrls).toHaveBeenCalled();
  });

  it("sets error when download link missing", async () => {
    const getDownloadUrls = vi.fn().mockResolvedValue([{}]);
    const Wrapper = pdfWrapper(
      cogniteClientMock({
        project: "p",
        files: { getDownloadUrls },
      }),
    );

    const { result } = renderHook(
      () => usePdfBlob({ externalId: "x", enabled: true }),
      { wrapper: Wrapper },
    );

    await waitFor(() => {
      expect(result.current.pdfError).toMatch(/download link/i);
    });
  });

  it("does not request another download link when the sdk object changes", async () => {
    const getDownloadUrls = vi
      .fn()
      .mockResolvedValue([{ downloadUrl: "https://dl.example/x" }]);
    let sdk: CogniteClient | null = cogniteClientMock({
      project: "p",
      files: { getDownloadUrls },
    });
    function Wrapper({ children }: { children: ReactNode }) {
      return (
        <UsePdfBlobContext.Provider
          value={{
            useCdfClient: () => ({ sdk, isAuthLoading: !sdk }),
            withCdfConcurrency: (fn) => fn(),
          }}
        >
          {children}
        </UsePdfBlobContext.Provider>
      );
    }

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      blob: async () => new Blob(["%PDF"], { type: "application/pdf" }),
    });

    const { rerender } = renderHook(
      () => usePdfBlob({ externalId: "file-ext", enabled: true }),
      { wrapper: Wrapper },
    );

    await waitFor(() => {
      expect(getDownloadUrls).toHaveBeenCalledTimes(1);
    });

    sdk = cogniteClientMock({
      project: "p",
      files: { getDownloadUrls },
    });
    rerender();
    await new Promise((resolve) => setTimeout(resolve, 30));

    expect(getDownloadUrls).toHaveBeenCalledTimes(1);
  });

  it("reuses the cached PDF when the viewer mounts again", async () => {
    const getDownloadUrls = vi
      .fn()
      .mockResolvedValue([{ downloadUrl: "https://dl.example/x" }]);
    const Wrapper = pdfWrapper(
      cogniteClientMock({
        project: "p",
        files: { getDownloadUrls },
      }),
    );
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      blob: async () => new Blob(["%PDF"], { type: "application/pdf" }),
    });

    const first = renderHook(
      () => usePdfBlob({ externalId: "file-ext", enabled: true }),
      { wrapper: Wrapper },
    );
    await waitFor(() => {
      expect(first.result.current.pdfFile).toBeInstanceOf(Blob);
    });
    first.unmount();

    const second = renderHook(
      () => usePdfBlob({ externalId: "file-ext", enabled: true }),
      { wrapper: Wrapper },
    );
    await waitFor(() => {
      expect(second.result.current.pdfFile).toBeInstanceOf(Blob);
    });
    expect(getDownloadUrls).toHaveBeenCalledTimes(1);
  });
});
