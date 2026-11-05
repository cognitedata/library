/**
 * Hook to fetch a PDF file as a Blob for in-browser viewing.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
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

const pdfBlobCache = new Map<string, Blob>();
const pdfBlobInflight = new Map<string, Promise<Blob>>();

export function clearPdfBlobCache(): void {
  pdfBlobCache.clear();
  pdfBlobInflight.clear();
}

function pdfCacheKey(space: string, externalId: string): string {
  return `${space}:${externalId}`;
}

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
  const sdkRef = useRef(sdk);
  sdkRef.current = sdk;
  const withCdfConcurrencyRef = useRef(withCdfConcurrency);
  withCdfConcurrencyRef.current = withCdfConcurrency;
  const [pdfFile, setPdfFile] = useState<Blob | null>(null);
  const [isLoadingPdf, setIsLoadingPdf] = useState(false);
  const [pdfError, setPdfError] = useState<string | null>(null);
  const [retryToken, setRetryToken] = useState(0);
  const hasSdk = sdk != null;

  const retryPdf = useCallback(() => {
    if (externalId) {
      const cacheKey = pdfCacheKey(space || getSummarySpace(), externalId);
      pdfBlobCache.delete(cacheKey);
      pdfBlobInflight.delete(cacheKey);
    }
    setPdfFile(null);
    setPdfError(null);
    setRetryToken((t) => t + 1);
  }, [externalId, space]);

  useEffect(() => {
    const client = sdkRef.current;
    if (!enabled || !client || !externalId) {
      setPdfFile(null);
      setPdfError(null);
      setIsLoadingPdf(false);
      return;
    }

    const effectiveSpace = space || getSummarySpace();
    const cacheKey = pdfCacheKey(effectiveSpace, externalId);
    const cached = pdfBlobCache.get(cacheKey);
    if (cached) {
      setPdfFile(cached);
      setPdfError(null);
      setIsLoadingPdf(false);
      return;
    }

    let cancelled = false;

    const fetchPdf = async () => {
      setIsLoadingPdf(true);
      setPdfError(null);
      try {
        let pending = pdfBlobInflight.get(cacheKey);
        if (!pending) {
          pending = (async () => {
            const [result] = await withCdfConcurrencyRef.current(() =>
              client.files.getDownloadUrls([
                { instanceId: { space: effectiveSpace, externalId } },
              ]),
            );
            const downloadUrl = result?.downloadUrl;
            if (!downloadUrl) {
              throw new Error(
                "Could not resolve a download link for this PDF.",
              );
            }
            const pdfResponse = await fetch(downloadUrl);
            if (!pdfResponse.ok) {
              throw new Error("Failed to download the PDF file.");
            }
            const blob = await pdfResponse.blob();
            pdfBlobCache.set(cacheKey, blob);
            return blob;
          })();
          pdfBlobInflight.set(cacheKey, pending);
          void pending.then(
            () => {
              pdfBlobInflight.delete(cacheKey);
            },
            () => {
              pdfBlobInflight.delete(cacheKey);
            },
          );
        }

        const blob = await pending;
        if (!cancelled) setPdfFile(blob);
      } catch (error) {
        if (cancelled) return;
        console.error("Error fetching PDF:", error);
        if (!cancelled) {
          setPdfError(
            error instanceof Error &&
              error.message.startsWith("Could not resolve")
              ? error.message
              : error instanceof Error &&
                  error.message.startsWith("Failed to download")
                ? error.message
                : getErrorMessage(error, "Loading PDF"),
          );
        }
      } finally {
        if (!cancelled) setIsLoadingPdf(false);
      }
    };

    fetchPdf();
    return () => {
      cancelled = true;
    };
  }, [enabled, hasSdk, space, externalId, retryToken]);

  return { pdfFile, isLoadingPdf, pdfError, retryPdf };
}
