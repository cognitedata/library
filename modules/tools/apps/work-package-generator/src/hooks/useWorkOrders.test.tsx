import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createStubCdfClient } from "@/__mocks__/cdf-runtime-test-wrapper";
import { AppConfigProvider } from "@/app-config";
import {
	type CdfRuntimeContextValue,
	CdfRuntimeTestProvider,
} from "@/contexts/CdfRuntimeContext";
import { useWorkPackageStore } from "../store";
import {
	UseWorkOrdersContext,
	type UseWorkOrdersContextType,
	useFindWorkOrder,
	useRecentWorkOrders,
	useSearchWorkOrders,
	useSelectWorkOrder,
} from "./useWorkOrders";

// ─── Helpers ────────────────────────────────────────────────────────────────

const mockFindWorkOrder = vi.fn();
const mockGetAsset = vi.fn();
const mockListRecent = vi.fn();
const mockSearch = vi.fn();

const stubClient = createStubCdfClient({
	project: "proj",
	getBaseUrl: () => "https://westeurope-1.cognitedata.com",
});

const cdfRuntime: CdfRuntimeContextValue = {
	client: stubClient,
	project: "proj",
	cluster: "westeurope-1",
};

const noClientRuntime: CdfRuntimeContextValue = {
	client: undefined,
	project: "proj",
	cluster: "westeurope-1",
};

/** Fresh QueryClient + wrapper per test; prevents query/promise leaks. */
function makeWrapper(runtime: CdfRuntimeContextValue = cdfRuntime) {
	const queryClient = new QueryClient({
		defaultOptions: {
			queries: { retry: false, gcTime: 0, staleTime: 0 },
			mutations: { retry: false },
		},
	});
	const workOrdersDeps: UseWorkOrdersContextType = {
		findWorkOrder: mockFindWorkOrder,
		getAssetForWorkOrder: mockGetAsset,
		listRecentWorkOrders: mockListRecent,
		searchWorkOrders: mockSearch,
		useWorkPackageStore,
		useCdfClientInfo: () => runtime,
	};
	const Wrapper = ({ children }: { children: ReactNode }) => (
		<AppConfigProvider>
			<CdfRuntimeTestProvider value={runtime}>
				<UseWorkOrdersContext.Provider value={workOrdersDeps}>
					<QueryClientProvider client={queryClient}>
						{children}
					</QueryClientProvider>
				</UseWorkOrdersContext.Provider>
			</CdfRuntimeTestProvider>
		</AppConfigProvider>
	);
	return { wrapper: Wrapper, queryClient };
}

function resetStore() {
	useWorkPackageStore.setState({
		workOrder: null,
		asset: null,
		error: null,
	});
}

// ─── Tests ──────────────────────────────────────────────────────────────────

describe("useWorkOrders hooks", () => {
	beforeEach(() => {
		mockFindWorkOrder.mockReset();
		mockGetAsset.mockReset();
		mockListRecent.mockReset();
		mockSearch.mockReset();
		resetStore();
	});

	it("useFindWorkOrder stores work order and asset on success", async () => {
		mockFindWorkOrder.mockResolvedValue({
			externalId: "wo",
			space: "s",
			name: "W",
		});
		mockGetAsset.mockResolvedValue({ assetExternalId: "a", space: "s" });

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useFindWorkOrder(), {
			wrapper,
		});

		await act(async () => {
			await result.current.mutateAsync("wo");
		});

		await waitFor(() =>
			expect(useWorkPackageStore.getState().workOrder?.externalId).toBe("wo")
		);
		expect(useWorkPackageStore.getState().asset?.assetExternalId).toBe("a");

		unmount();
		queryClient.clear();
	});

	it("useFindWorkOrder stores work order without asset when none is linked", async () => {
		mockFindWorkOrder.mockResolvedValue({
			externalId: "wo",
			space: "s",
			name: "W",
		});
		mockGetAsset.mockResolvedValue(null);

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useFindWorkOrder(), {
			wrapper,
		});

		await act(async () => {
			await result.current.mutateAsync("wo");
		});

		await waitFor(() =>
			expect(useWorkPackageStore.getState().workOrder?.externalId).toBe("wo")
		);
		expect(useWorkPackageStore.getState().asset).toBeNull();

		unmount();
		queryClient.clear();
	});

	it("useFindWorkOrder sets error when no work order found", async () => {
		mockFindWorkOrder.mockResolvedValue(null);

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useFindWorkOrder(), {
			wrapper,
		});

		await act(async () => {
			await expect(result.current.mutateAsync("x")).rejects.toBeDefined();
		});

		await waitFor(() =>
			expect(useWorkPackageStore.getState().error).toContain(
				"No work order found"
			)
		);

		unmount();
		queryClient.clear();
	});

	it("useFindWorkOrder prefixes unexpected errors with a user-facing message", async () => {
		mockFindWorkOrder.mockRejectedValue(new Error("timeout"));

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useFindWorkOrder(), {
			wrapper,
		});

		await act(async () => {
			await expect(result.current.mutateAsync("wo")).rejects.toBeDefined();
		});

		await waitFor(() =>
			expect(useWorkPackageStore.getState().error).toBe(
				"Failed to find work order: timeout"
			)
		);

		unmount();
		queryClient.clear();
	});

	it("useFindWorkOrder fails when the CDF client is unavailable", async () => {
		const { wrapper, queryClient } = makeWrapper(noClientRuntime);
		const { result, unmount } = renderHook(() => useFindWorkOrder(), {
			wrapper,
		});

		await act(async () => {
			await expect(result.current.mutateAsync("wo")).rejects.toThrow(
				"CDF client not available"
			);
		});

		unmount();
		queryClient.clear();
	});

	it("useSearchWorkOrders starts with no results before submit", () => {
		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useSearchWorkOrders(), {
			wrapper,
		});

		expect(result.current.searchResults).toEqual([]);
		expect(result.current.searchHasNextPage).toBe(false);
		expect(mockSearch).not.toHaveBeenCalled();

		unmount();
		queryClient.clear();
	});

	it("useSearchWorkOrders fetches results and paginates without duplicating cursors", async () => {
		const page1 = [{ externalId: "a", space: "s" }];
		const page2 = [{ externalId: "b", space: "s" }];
		mockSearch.mockImplementation(async (_client, _query, cursor) => {
			if (cursor === "c1") {
				return { items: page2, nextCursor: undefined };
			}
			return { items: page1, nextCursor: "c1" };
		});

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useSearchWorkOrders(), {
			wrapper,
		});

		act(() => {
			result.current.searchWorkOrders("pump");
		});

		await waitFor(() => expect(result.current.searchResults).toEqual(page1));
		expect(result.current.searchHasNextPage).toBe(true);
		expect(result.current.searchHasPrevPage).toBe(false);
		expect(result.current.searchCurrentPage).toBe(1);
		expect(result.current.isSearchingWorkOrders).toBe(false);

		act(() => {
			result.current.goToNextSearchPage();
		});

		await waitFor(() => expect(result.current.searchResults).toEqual(page2));
		expect(result.current.searchCurrentPage).toBe(2);
		expect(result.current.searchHasPrevPage).toBe(true);
		expect(result.current.searchHasNextPage).toBe(false);

		act(() => {
			result.current.goToNextSearchPage();
		});
		expect(result.current.searchCurrentPage).toBe(2);

		act(() => {
			result.current.goToPrevSearchPage();
		});

		await waitFor(() => expect(result.current.searchResults).toEqual(page1));
		expect(result.current.searchCurrentPage).toBe(1);

		act(() => {
			result.current.goToNextSearchPage();
		});
		await waitFor(() => {
			expect(result.current.searchCurrentPage).toBe(2);
			expect(result.current.searchResults).toEqual(page2);
		});

		unmount();
		queryClient.clear();
	});

	it("useSearchWorkOrders ignores previous-page when already on the first page", () => {
		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useSearchWorkOrders(), {
			wrapper,
		});

		act(() => {
			result.current.goToPrevSearchPage();
		});

		expect(result.current.searchCurrentPage).toBe(1);

		unmount();
		queryClient.clear();
	});

	it("useSearchWorkOrders resets submitted query and pagination", async () => {
		mockSearch.mockResolvedValue({
			items: [{ externalId: "a", space: "s" }],
			nextCursor: "c1",
		});

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useSearchWorkOrders(), {
			wrapper,
		});

		act(() => {
			result.current.searchWorkOrders("pump");
		});
		await waitFor(() => expect(result.current.searchResults).toHaveLength(1));

		act(() => {
			result.current.goToNextSearchPage();
		});
		await waitFor(() => expect(result.current.searchCurrentPage).toBe(2));

		act(() => {
			result.current.resetSearch();
		});

		expect(result.current.searchResults).toEqual([]);
		expect(result.current.searchCurrentPage).toBe(1);
		expect(result.current.searchHasPrevPage).toBe(false);
		expect(result.current.isSearchingWorkOrders).toBe(false);

		unmount();
		queryClient.clear();
	});

	it("useSearchWorkOrders resets pagination when the submitted query changes", async () => {
		mockSearch.mockImplementation(async (_client, query, cursor) => {
			if (query === "valve") {
				return {
					items: [{ externalId: "z", space: "s" }],
					nextCursor: undefined,
				};
			}
			if (cursor === "c1") {
				return {
					items: [{ externalId: "b", space: "s" }],
					nextCursor: undefined,
				};
			}
			return {
				items: [{ externalId: "a", space: "s" }],
				nextCursor: "c1",
			};
		});

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useSearchWorkOrders(), {
			wrapper,
		});

		act(() => {
			result.current.searchWorkOrders("pump");
		});
		await waitFor(() =>
			expect(result.current.searchResults[0]?.externalId).toBe("a")
		);

		act(() => {
			result.current.goToNextSearchPage();
		});
		await waitFor(() => expect(result.current.searchCurrentPage).toBe(2));

		act(() => {
			result.current.searchWorkOrders("valve");
		});

		await waitFor(() => {
			expect(result.current.searchCurrentPage).toBe(1);
			expect(result.current.searchResults[0]?.externalId).toBe("z");
		});

		unmount();
		queryClient.clear();
	});

	it("useRecentWorkOrders fetches on mount", async () => {
		mockListRecent.mockResolvedValue({ items: [], nextCursor: undefined });

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useRecentWorkOrders(), {
			wrapper,
		});

		await waitFor(() => expect(result.current.isLoadingRecent).toBe(false));
		expect(mockListRecent).toHaveBeenCalled();

		unmount();
		queryClient.clear();
	});

	it("useRecentWorkOrders paginates, refetch resets to the first page, and filters reset cursors", async () => {
		const page1 = [{ externalId: "r1", space: "s" }];
		const page2 = [{ externalId: "r2", space: "s" }];
		mockListRecent.mockImplementation(async (_client, filters, cursor) => {
			if (filters?.status === "closed") {
				return {
					items: [{ externalId: "r3", space: "s" }],
					nextCursor: undefined,
				};
			}
			if (cursor === "c1") {
				return { items: page2, nextCursor: undefined };
			}
			return { items: page1, nextCursor: "c1" };
		});

		const { wrapper, queryClient } = makeWrapper();
		const { result, rerender, unmount } = renderHook(
			({ filters }) => useRecentWorkOrders(filters),
			{ wrapper, initialProps: { filters: { status: "open" } } }
		);

		await waitFor(() => expect(result.current.recentWorkOrders).toEqual(page1));
		expect(result.current.recentHasNextPage).toBe(true);
		expect(result.current.recentCurrentPage).toBe(1);

		act(() => {
			result.current.goToNextRecentPage();
		});
		await waitFor(() => expect(result.current.recentWorkOrders).toEqual(page2));
		expect(result.current.recentCurrentPage).toBe(2);
		expect(result.current.recentHasPrevPage).toBe(true);

		act(() => {
			result.current.goToNextRecentPage();
		});
		expect(result.current.recentCurrentPage).toBe(2);

		act(() => {
			result.current.goToPrevRecentPage();
		});
		await waitFor(() => expect(result.current.recentWorkOrders).toEqual(page1));

		act(() => {
			result.current.goToNextRecentPage();
		});
		await waitFor(() => expect(result.current.recentCurrentPage).toBe(2));

		await act(async () => {
			await result.current.refetchRecent();
		});
		await waitFor(() => expect(result.current.recentCurrentPage).toBe(1));

		rerender({ filters: { status: "closed" } });
		await waitFor(() =>
			expect(result.current.recentWorkOrders[0]?.externalId).toBe("r3")
		);
		expect(result.current.recentCurrentPage).toBe(1);

		unmount();
		queryClient.clear();
	});

	it("useRecentWorkOrders ignores previous-page when already on the first page", async () => {
		mockListRecent.mockResolvedValue({ items: [], nextCursor: undefined });

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useRecentWorkOrders(), {
			wrapper,
		});

		await waitFor(() => expect(result.current.isLoadingRecent).toBe(false));

		act(() => {
			result.current.goToPrevRecentPage();
		});
		expect(result.current.recentCurrentPage).toBe(1);

		unmount();
		queryClient.clear();
	});

	it("useSelectWorkOrder stores selected work order and asset", async () => {
		const wo = { externalId: "w", space: "s" };
		mockGetAsset.mockResolvedValue({ assetExternalId: "a", space: "s" });

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useSelectWorkOrder(), {
			wrapper,
		});

		await act(async () => {
			await result.current.mutateAsync(wo);
		});

		expect(useWorkPackageStore.getState().workOrder).toEqual(wo);
		expect(useWorkPackageStore.getState().asset?.assetExternalId).toBe("a");

		unmount();
		queryClient.clear();
	});

	it("useSelectWorkOrder stores the work order when no asset is linked", async () => {
		const wo = { externalId: "w", space: "s" };
		mockGetAsset.mockResolvedValue(null);

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useSelectWorkOrder(), {
			wrapper,
		});

		await act(async () => {
			await result.current.mutateAsync(wo);
		});

		expect(useWorkPackageStore.getState().workOrder).toEqual(wo);
		expect(useWorkPackageStore.getState().asset).toBeNull();

		unmount();
		queryClient.clear();
	});

	it("useSelectWorkOrder sets a user-facing error on failure", async () => {
		const wo = { externalId: "w", space: "s" };
		mockGetAsset.mockRejectedValue(new Error("lookup failed"));

		const { wrapper, queryClient } = makeWrapper();
		const { result, unmount } = renderHook(() => useSelectWorkOrder(), {
			wrapper,
		});

		await act(async () => {
			await expect(result.current.mutateAsync(wo)).rejects.toBeDefined();
		});

		await waitFor(() =>
			expect(useWorkPackageStore.getState().error).toBe(
				"Failed to load work order: lookup failed"
			)
		);

		unmount();
		queryClient.clear();
	});

	it("useSelectWorkOrder fails when the CDF client is unavailable", async () => {
		const wo = { externalId: "w", space: "s" };
		const { wrapper, queryClient } = makeWrapper(noClientRuntime);
		const { result, unmount } = renderHook(() => useSelectWorkOrder(), {
			wrapper,
		});

		await act(async () => {
			await expect(result.current.mutateAsync(wo)).rejects.toThrow(
				"CDF client not available"
			);
		});

		unmount();
		queryClient.clear();
	});
});
