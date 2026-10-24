import { useMutation, useQuery } from "@tanstack/react-query";
import {
	createContext,
	useCallback,
	useContext,
	useEffect,
	useState,
} from "react";
import {
	findWorkOrder,
	getAssetForWorkOrder,
	listRecentWorkOrders,
	searchWorkOrders,
} from "../api";
import { useAppConfig } from "../app-config";
import { useDefaultWorkPackageStore } from "../contexts/WorkPackageStoreContext";
import type { WorkOrder, WorkOrderFilters } from "../types";
import { getErrorMessage, toUserFacingErrorMessage } from "../utils/errors";
import { useCdfClientInfo } from "./useClientInfo";

const defaultWorkOrdersDeps = {
	findWorkOrder,
	getAssetForWorkOrder,
	listRecentWorkOrders,
	searchWorkOrders,
	useWorkPackageStore: useDefaultWorkPackageStore,
	useCdfClientInfo,
};

export type UseWorkOrdersContextType = typeof defaultWorkOrdersDeps;
export const UseWorkOrdersContext = createContext<UseWorkOrdersContextType>(
	defaultWorkOrdersDeps
);

export function useFindWorkOrder() {
	const {
		findWorkOrder: findWorkOrderFn,
		getAssetForWorkOrder: getAssetForWorkOrderFn,
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseWorkOrdersContext);
	const { client } = useCdfClient();
	const appConfig = useAppConfig();
	const { setWorkOrder, setAsset, setError, resetForNewWorkOrder } = useStore();

	return useMutation({
		mutationFn: async (identifier: string) => {
			if (!client) throw new Error("CDF client not available");

			const workOrder = await findWorkOrderFn(client, identifier, appConfig);
			if (!workOrder) {
				throw new Error("No work order found. Try a different identifier.");
			}

			const asset = await getAssetForWorkOrderFn(client, workOrder, appConfig);
			return { workOrder, asset };
		},
		onMutate: () => {
			resetForNewWorkOrder();
			setError(null);
		},
		onSuccess: ({ workOrder, asset }) => {
			setWorkOrder(workOrder);
			if (asset) setAsset(asset);
		},
		onError: (error) => {
			const message = getErrorMessage(error);
			if (message.includes("No work order found")) {
				setError(message);
			} else {
				setError(
					`Failed to find work order: ${toUserFacingErrorMessage(message)}`
				);
			}
		},
	});
}

export function useSearchWorkOrders() {
	const {
		searchWorkOrders: searchWorkOrdersFn,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseWorkOrdersContext);
	const { client } = useCdfClient();
	const appConfig = useAppConfig();
	const [submittedQuery, setSubmittedQuery] = useState<string | null>(null);
	const [cursorStack, setCursorStack] = useState<(string | undefined)[]>([
		undefined,
	]);
	const [pageIndex, setPageIndex] = useState(0);

	const currentCursor = cursorStack[pageIndex];

	// Reset pagination whenever the submitted query changes
	// biome-ignore lint/correctness/useExhaustiveDependencies: submittedQuery is the intentional trigger for the reset
	useEffect(() => {
		setCursorStack([undefined]);
		setPageIndex(0);
	}, [submittedQuery]);

	const query = useQuery({
		queryKey: ["search-work-orders", submittedQuery, currentCursor],
		queryFn: async () => {
			if (!client) throw new Error("CDF client not available");
			if (!submittedQuery) return { items: [], nextCursor: undefined };
			return searchWorkOrdersFn(
				client,
				submittedQuery,
				currentCursor,
				appConfig
			);
		},
		enabled: !!client && !!submittedQuery,
		staleTime: 2 * 60 * 1000,
	});

	const goToNextPage = useCallback(() => {
		const nextCursor = query.data?.nextCursor;
		if (!nextCursor) return;
		setCursorStack((prev) => {
			if (pageIndex + 1 < prev.length) return prev;
			return [...prev, nextCursor];
		});
		setPageIndex((p) => p + 1);
	}, [query.data?.nextCursor, pageIndex]);

	const goToPrevPage = useCallback(() => {
		if (pageIndex === 0) return;
		setPageIndex((p) => p - 1);
	}, [pageIndex]);

	const resetSearch = useCallback(() => {
		setSubmittedQuery(null);
		setCursorStack([undefined]);
		setPageIndex(0);
	}, []);

	return {
		searchWorkOrders: setSubmittedQuery as (query: string) => void,
		searchResults: query.data?.items ?? [],
		isSearchingWorkOrders: !!submittedQuery && query.isFetching,
		searchError: query.error,
		resetSearch,
		searchHasNextPage: !!query.data?.nextCursor,
		searchHasPrevPage: pageIndex > 0,
		goToNextSearchPage: goToNextPage,
		goToPrevSearchPage: goToPrevPage,
		searchCurrentPage: pageIndex + 1,
		isSearchFetching: query.isFetching,
	};
}

export function useRecentWorkOrders(filters?: WorkOrderFilters) {
	const {
		listRecentWorkOrders: listRecentWorkOrdersFn,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseWorkOrdersContext);
	const { client } = useCdfClient();
	const appConfig = useAppConfig();
	const [cursorStack, setCursorStack] = useState<(string | undefined)[]>([
		undefined,
	]);
	const [pageIndex, setPageIndex] = useState(0);

	const currentCursor = cursorStack[pageIndex];

	// Reset pagination whenever filters change
	// biome-ignore lint/correctness/useExhaustiveDependencies: filters is the intentional trigger for the reset
	useEffect(() => {
		setCursorStack([undefined]);
		setPageIndex(0);
	}, [filters]);

	const query = useQuery({
		queryKey: ["recent-work-orders", filters, currentCursor],
		queryFn: async () => {
			if (!client) throw new Error("CDF client not available");
			return listRecentWorkOrdersFn(client, filters, currentCursor, appConfig);
		},
		enabled: !!client,
		staleTime: 2 * 60 * 1000,
	});

	const goToNextPage = useCallback(() => {
		const nextCursor = query.data?.nextCursor;
		if (!nextCursor) return;
		setCursorStack((prev) => {
			if (pageIndex + 1 < prev.length) return prev;
			return [...prev, nextCursor];
		});
		setPageIndex((p) => p + 1);
	}, [query.data?.nextCursor, pageIndex]);

	const goToPrevPage = useCallback(() => {
		if (pageIndex === 0) return;
		setPageIndex((p) => p - 1);
	}, [pageIndex]);

	const refetchRecent = useCallback(() => {
		setCursorStack([undefined]);
		setPageIndex(0);
		return query.refetch();
	}, [query]);

	return {
		recentWorkOrders: query.data?.items ?? [],
		isLoadingRecent: query.isPending,
		/** True during initial load and manual refetch — use for refresh control feedback. */
		isFetchingRecent: query.isFetching,
		recentError: query.error,
		refetchRecent,
		recentHasNextPage: !!query.data?.nextCursor,
		recentHasPrevPage: pageIndex > 0,
		goToNextRecentPage: goToNextPage,
		goToPrevRecentPage: goToPrevPage,
		recentCurrentPage: pageIndex + 1,
	};
}

export function useSelectWorkOrder() {
	const {
		getAssetForWorkOrder: getAssetForWorkOrderFn,
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseWorkOrdersContext);
	const { client } = useCdfClient();
	const appConfig = useAppConfig();
	const { setWorkOrder, setAsset, setError, resetForNewWorkOrder } = useStore();

	return useMutation({
		mutationFn: async (workOrder: WorkOrder) => {
			if (!client) throw new Error("CDF client not available");
			const asset = await getAssetForWorkOrderFn(client, workOrder, appConfig);
			return { workOrder, asset };
		},
		onMutate: () => {
			resetForNewWorkOrder();
			setError(null);
		},
		onSuccess: ({ workOrder, asset }) => {
			setWorkOrder(workOrder);
			if (asset) setAsset(asset);
		},
		onError: (error) => {
			setError(
				`Failed to load work order: ${toUserFacingErrorMessage(getErrorMessage(error))}`
			);
		},
	});
}
