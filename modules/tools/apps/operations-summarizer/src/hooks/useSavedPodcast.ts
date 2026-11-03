/**
 * ViewModel: load an existing podcast for a summary from CDF Files.
 */

import { useQuery } from "@tanstack/react-query";
import { createContext, useContext } from "react";
import { useCdfClient } from "@/contexts/CdfClientContext";
import { fetchSavedPodcast, type SavedPodcast } from "@/services/podcastFiles";
import { CDF_QUERY_KEYS } from "./cdfQueryKeys";

export type UseSavedPodcastValue = {
  podcast: SavedPodcast | null | undefined;
  isLoading: boolean;
  isError: boolean;
  error: Error | null;
  refetch: () => void;
  hasPodcast: boolean;
};

type UseSavedPodcastDeps = {
  useCdfClient: typeof useCdfClient;
  fetchSavedPodcast: typeof fetchSavedPodcast;
};

const defaultDeps: UseSavedPodcastDeps = {
  useCdfClient,
  fetchSavedPodcast,
};

export const UseSavedPodcastContext =
  createContext<UseSavedPodcastDeps>(defaultDeps);

export function useSavedPodcast(
  sourceSummaryId: string | null | undefined,
): UseSavedPodcastValue {
  const { useCdfClient: useClient, fetchSavedPodcast: fetchPodcast } =
    useContext(UseSavedPodcastContext);
  const { sdk, isAuthLoading } = useClient();

  const query = useQuery({
    queryKey: CDF_QUERY_KEYS.savedPodcast(sourceSummaryId ?? undefined),
    queryFn: async () => {
      if (!sdk || !sourceSummaryId) {
        throw new Error("SDK or summary id not available");
      }
      return fetchPodcast(sdk, sourceSummaryId);
    },
    enabled: !!sdk && !isAuthLoading && !!sourceSummaryId,
    staleTime: 5 * 60 * 1000,
    retry: false,
  });

  const podcast = query.data;
  return {
    podcast,
    isLoading: query.isLoading || query.isFetching,
    isError: query.isError,
    error: query.error,
    refetch: () => {
      void query.refetch();
    },
    hasPodcast: !!podcast?.script,
  };
}
