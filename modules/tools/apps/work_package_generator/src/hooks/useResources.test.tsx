import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_CANVAS_CONFIG } from "@/__mocks__/app-config";
import { createStubCdfClient } from "@/__mocks__/cdf-runtime-test-wrapper";
import { AppConfigProvider } from "@/app-config";
import {
	type CdfRuntimeContextValue,
	CdfRuntimeTestProvider,
} from "@/contexts/CdfRuntimeContext";
import { useWorkPackageStore } from "../store";
import {
	mergeFetchedCandidates,
	UseAssetResourcesContext,
	type UseAssetResourcesContextType,
	useAssetResources,
} from "./useResources";

const mockAssetGeneric = vi.fn();
const mockActivityGeneric = vi.fn();
const mockInstancesByRefs = vi.fn();

const cdfRuntime: CdfRuntimeContextValue = {
	client: createStubCdfClient({
		project: "p",
		getBaseUrl: () => "https://x.cognitedata.com",
	}),
	project: "p",
	cluster: "x",
};

function makeWrapper() {
	const queryClient = new QueryClient({
		defaultOptions: { queries: { retry: false } },
	});
	const deps: UseAssetResourcesContextType = {
		getAssetResourcesGeneric: mockAssetGeneric,
		getActivityResourcesGeneric: mockActivityGeneric,
		getResourceInstancesByRefs: mockInstancesByRefs,
		useWorkPackageStore,
		useCdfClientInfo: () => cdfRuntime,
	};
	const Wrapper = ({ children }: { children: ReactNode }) => (
		<AppConfigProvider>
			<CdfRuntimeTestProvider value={cdfRuntime}>
				<UseAssetResourcesContext.Provider value={deps}>
					<QueryClientProvider client={queryClient}>
						{children}
					</QueryClientProvider>
				</UseAssetResourcesContext.Provider>
			</CdfRuntimeTestProvider>
		</AppConfigProvider>
	);
	return Wrapper;
}

describe("useAssetResources", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockAssetGeneric.mockResolvedValue([]);
		mockActivityGeneric.mockResolvedValue([]);
		mockInstancesByRefs.mockResolvedValue([]);
		useWorkPackageStore.setState({
			asset: { assetExternalId: "a1", space: "s" },
			workOrder: {
				externalId: "wo",
				space: "s",
				assets: [{ externalId: "a2", space: "s" }],
			},
			config: DEFAULT_CANVAS_CONFIG,
			candidatesByType: {},
			removedCandidateIdsByType: {},
		});
	});

	it("fetches and merges resources per type", async () => {
		const { result } = renderHook(() => useAssetResources(), {
			wrapper: makeWrapper(),
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(mockAssetGeneric).toHaveBeenCalled();
		expect(mockActivityGeneric).toHaveBeenCalled();
		expect(mockInstancesByRefs).toHaveBeenCalled();
	});

	it("C29: keeps the same externalId from two spaces", async () => {
		const inS1 = { externalId: "same", space: "s1", properties: {} };
		const inS2 = { externalId: "same", space: "s2", properties: {} };
		mockAssetGeneric.mockImplementation(
			async (_client, _id, _space, typeKey) =>
				typeKey === "file" ? [inS1] : []
		);
		mockActivityGeneric.mockImplementation(
			async (_client, _id, _space, typeKey) =>
				typeKey === "file" ? [inS2] : []
		);

		const { result } = renderHook(() => useAssetResources(), {
			wrapper: makeWrapper(),
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(
			result.current.resources.file?.map((r) => `${r.space}:${r.externalId}`)
		).toEqual(["s1:same", "s2:same"]);
	});

	it("C06: fetches for the first asset only when several exist", async () => {
		useWorkPackageStore.setState({
			...useWorkPackageStore.getState(),
			asset: { assetExternalId: "first", space: "s" },
			workOrder: {
				externalId: "wo",
				space: "s",
				assets: [
					{ externalId: "second", space: "s" },
					{ externalId: "third", space: "other" },
				],
			},
		});

		const { result } = renderHook(() => useAssetResources(), {
			wrapper: makeWrapper(),
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		const fetchedAssetIds = mockAssetGeneric.mock.calls.map((call) => call[1]);
		expect(fetchedAssetIds.length).toBeGreaterThan(0);
		expect(new Set(fetchedAssetIds)).toEqual(new Set(["first"]));
	});

	it("N14: shows a fetched resource again after removals are cleared", async () => {
		const kept = { externalId: "keep-me", space: "s", properties: {} };
		const removed = { externalId: "drop-me", space: "s", properties: {} };
		mockAssetGeneric.mockImplementation(
			async (_client, _id, _space, typeKey) =>
				typeKey === "file" ? [kept, removed] : []
		);
		useWorkPackageStore.setState({
			...useWorkPackageStore.getState(),
			removedCandidateIdsByType: { file: ["drop-me"] },
		});

		const { result } = renderHook(() => useAssetResources(), {
			wrapper: makeWrapper(),
		});
		await waitFor(() =>
			expect(result.current.resources.file?.map((r) => r.externalId)).toEqual([
				"keep-me",
			])
		);

		act(() => {
			useWorkPackageStore.getState().restoreRemovedCandidates();
		});

		await waitFor(() =>
			expect(result.current.resources.file?.map((r) => r.externalId)).toEqual([
				"keep-me",
				"drop-me",
			])
		);
	});

	it("omits user-removed ids when merging fetch results into the store", async () => {
		const fetchedFile = {
			externalId: "keep-me",
			space: "s",
			properties: {},
		};
		const removedFile = {
			externalId: "drop-me",
			space: "s",
			properties: {},
		};
		mockAssetGeneric.mockImplementation(
			async (_client, _id, _space, typeKey) => {
				if (typeKey === "file") return [fetchedFile, removedFile];
				return [];
			}
		);
		useWorkPackageStore.setState({
			...useWorkPackageStore.getState(),
			removedCandidateIdsByType: { file: ["drop-me"] },
		});

		const { result } = renderHook(() => useAssetResources(), {
			wrapper: makeWrapper(),
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.resources.file?.map((r) => r.externalId)).toEqual([
			"keep-me",
		]);
	});
});

describe("mergeFetchedCandidates", () => {
	const fetchedA = { externalId: "a", space: "s", properties: {} };
	const fetchedB = { externalId: "b", space: "s", properties: {} };
	const manualC = { externalId: "c", space: "s", properties: {} };

	it("keeps fetched items, manual-only items, and drops removed ids", () => {
		expect(
			mergeFetchedCandidates(
				[fetchedA, fetchedB],
				[fetchedA, manualC],
				["b"]
			).map((r) => r.externalId)
		).toEqual(["a", "c"]);
	});

	it("C29: does not collapse the same externalId in two spaces", () => {
		const inS1 = { externalId: "a", space: "s1", properties: {} };
		const inS2 = { externalId: "a", space: "s2", properties: {} };
		expect(
			mergeFetchedCandidates([inS1, inS2], [], []).map(
				(r) => `${r.space}:${r.externalId}`
			)
		).toEqual(["s1:a", "s2:a"]);
		expect(
			mergeFetchedCandidates([inS1, inS2], [], ["a:s2"]).map(
				(r) => `${r.space}:${r.externalId}`
			)
		).toEqual(["s1:a"]);
	});
});
