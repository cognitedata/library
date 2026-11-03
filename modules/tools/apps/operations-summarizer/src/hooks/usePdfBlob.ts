/**
 * Hook to fetch a PDF file as a Blob for in-browser viewing.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";
import { getSummarySpace } from "@/config/dataTypes";
import { useCdfClient } from "@/contexts/CdfClientContext";
import { withCdfConcurrency } from "@/utils/cdfRequest";
import { getErrorMessage } from "@/utils/errorHandling";

type UsePdfBlobDeps = {
  useCdfClient: typeof useCdfClient;
  withCdfConcurrency: typeof withCdfConcurrency;
};

const defaultPdfBlobDeps: UsePdfBlobDeps = {
  useCdfClient,
  withCdfConcurrency,
};

export const UsePdfBlobContext =
  createContext<UsePdfBlobDeps>(defaultPdfBlobDeps);

interface UsePdfBlobParams {
  space?: string;
  externalId?: string;
  enabled: boolean;
}

/**
 * Fetches a PDF file as a Blob for viewing (e.g. react-pdf).
 * Returns a Blob instead of a blob URL or Uint8Array so production CSP does not
 * require connect-src blob:, and pdf.js can clone the file for its worker
 * without detaching the underlying ArrayBuffer on re-render.
 */
export function usePdfBlob({ space, externalId, enabled }: UsePdfBlobParams): {
  pdfFile: Blob | null;
  isLoadingPdf: boolean;
  pdfError: string | null;
  retryPdf: () => void;
} {
  const { useCdfClient: useClient, withCdfConcurrency } =
    useContext(UsePdfBlobContext);
  const { sdk } = useClient();
  const [pdfFile, setPdfFile] = useState<Blob | null>(null);
  const [isLoadingPdf, setIsLoadingPdf] = useState(false);
  const [pdfError, setPdfError] = useState<string | null>(null);
  const [retryToken, setRetryToken] = useState(0);

  const retryPdf = useCallback(() => {
    setPdfError(null);
    setRetryToken((t) => t + 1);
  }, []);

  // biome-ignore lint/correctness/useExhaustiveDependencies: retryToken is an intentional refetch trigger (see retryPdf)
  useEffect(() => {
    if (!enabled || !sdk || !externalId) {
      setPdfFile(null);
      setPdfError(null);
      return;
    }

    let cancelled = false;
    const controller = new AbortController();
    const effectiveSpace = space || getSummarySpace();

    const fetchPdf = async () => {
      setIsLoadingPdf(true);
      setPdfError(null);
      try {
        const [result] = await withCdfConcurrency(() =>
          sdk.files.getDownloadUrls([
            { instanceId: { space: effectiveSpace, externalId } },
          ]),
        );

        if (cancelled) return;

        const downloadUrl = result?.downloadUrl;
        if (!downloadUrl) {
          if (!cancelled) {
            setPdfError("Could not resolve a download link for this PDF.");
          }
          return;
        }

        const pdfResponse = await fetch(downloadUrl, {
          signal: controller.signal,
        });
        if (!pdfResponse.ok) {
          if (!cancelled) {
            setPdfError("Failed to download the PDF file.");
          }
          return;
        }

        const blob = await pdfResponse.blob();
        if (!cancelled) {
          setPdfFile(blob);
        }
      } catch (error) {
        if (
          cancelled ||
          (error instanceof Error && error.name === "AbortError")
        )
          return;
        console.error("Error fetching PDF:", error);
        setPdfError(getErrorMessage(error, "Loading PDF"));
      } finally {
        if (!cancelled) setIsLoadingPdf(false);
      }
    };

    fetchPdf();
    return () => {
      cancelled = true;
      controller.abort();
      setPdfFile(null);
    };
  }, [enabled, sdk, space, externalId, retryToken]);

  return { pdfFile, isLoadingPdf, pdfError, retryPdf };
}
