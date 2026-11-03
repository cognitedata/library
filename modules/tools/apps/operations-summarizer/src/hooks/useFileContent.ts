/**
 * Hook to fetch CDF file content using the files/downloadlink endpoint.
 */

import { useQuery } from "@tanstack/react-query";
import { createContext, useContext } from "react";
import { getSummarySpace } from "@/config/dataTypes";
import { useCdfClient } from "@/contexts/CdfClientContext";
import { withCdfConcurrency } from "@/utils/cdfRequest";
import { CDF_QUERY_KEYS } from "./cdfQueryKeys";

type UseFileContentDeps = {
  useCdfClient: typeof useCdfClient;
  withCdfConcurrency: typeof withCdfConcurrency;
};

const defaultFileContentDeps: UseFileContentDeps = {
  useCdfClient,
  withCdfConcurrency,
};

export const UseFileContentContext = createContext<UseFileContentDeps>(
  defaultFileContentDeps,
);

/**
 * Hook to fetch file content using files/downloadlink endpoint with instanceId
 */
export function useFileContent(
  params: { space?: string; externalId?: string } | null,
) {
  const { useCdfClient: useClient, withCdfConcurrency } = useContext(
    UseFileContentContext,
  );
  const { sdk, isAuthLoading } = useClient();

  return useQuery({
    queryKey: CDF_QUERY_KEYS.fileContent(params?.space, params?.externalId),
    queryFn: async ({ signal }) => {
      if (!sdk || !params?.externalId)
        throw new Error("SDK or externalId not available");

      const space = params.space || getSummarySpace();

      try {
        const [result] = await withCdfConcurrency(() =>
          sdk.files.getDownloadUrls([
            { instanceId: { space, externalId: params.externalId! } },
          ]),
        );

        if (result?.downloadUrl) {
          const contentResponse = await fetch(result.downloadUrl, { signal });
          if (contentResponse.ok) {
            return await contentResponse.text();
          }
        }

        return null;
      } catch (error) {
        console.error("Error fetching file content:", error);
        throw error;
      }
    },
    enabled: !!sdk && !isAuthLoading && !!params?.externalId,
    staleTime: 5 * 60 * 1000,
    retry: false,
  });
}
