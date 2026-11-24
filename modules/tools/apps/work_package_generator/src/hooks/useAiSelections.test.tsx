import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	BASE_TEMPLATE_CANVAS,
	BASE_TEMPLATE_ID,
	DEFAULT_CANVAS_CONFIG,
} from "@/__mocks__/app-config";
import { createStubCdfClient } from "@/__mocks__/cdf-runtime-test-wrapper";
import { AppConfigProvider } from "@/app-config";
import {
	type CdfRuntimeContextValue,
	CdfRuntimeTestProvider,
} from "@/contexts/CdfRuntimeContext";
import { useWorkPackageStore } from "../store";
import {
	UseAiSelectionsContext,
	type UseAiSelectionsContextType,
	useAiCanvasRecommendation,
	useAiSelection,
} from "./useAiSelections";

const mockAgent = vi.fn();
const mockBest = vi.fn();
const mockSelectRes = vi.fn();

const stubClient = createStubCdfClient({
	project: "proj",
	getBaseUrl: () => "https://x.cognitedata.com",
});

const cdfRuntime: CdfRuntimeContextValue = {
	client: stubClient,
	project: "proj",
	cluster: "x",
};

function makeWrapper() {
	const queryClient = new QueryClient({
		defaultOptions: {
			queries: { retry: false },
			mutations: { retry: false },
		},
	});
	const deps: UseAiSelectionsContextType = {
		aiSelectBestCanvasTemplate: mockBest,
		aiSelectResource: mockSelectRes,
		getOrCreateAgent: mockAgent,
		useWorkPackageStore,
		useCdfClientInfo: () => cdfRuntime,
	};
	const Wrapper = ({ children }: { children: ReactNode }) => (
		<AppConfigProvider>
			<CdfRuntimeTestProvider value={cdfRuntime}>
				<UseAiSelectionsContext.Provider value={deps}>
					<QueryClientProvider client={queryClient}>
						{children}
					</QueryClientProvider>
				</UseAiSelectionsContext.Provider>
			</CdfRuntimeTestProvider>
		</AppConfigProvider>
	);
	return Wrapper;
}

describe("useAiCanvasRecommendation", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockAgent.mockResolvedValue("agent-id");
		mockBest.mockResolvedValue("canvas-1");
		useWorkPackageStore.setState({
			workOrder: { externalId: "w", space: "s" },
			asset: { assetExternalId: "a", space: "s" },
			aiSelectedCanvasId: null,
			candidateTemplates: [
				{
					externalId: "c1",
					space: "s",
					name: "T",
					visibility: "public",
				},
			],
			config: DEFAULT_CANVAS_CONFIG,
		});
	});

	it("sets template from AI result", async () => {
		const { result } = renderHook(() => useAiCanvasRecommendation(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await result.current.mutateAsync(undefined);
		});
		await waitFor(() =>
			expect(useWorkPackageStore.getState().templateCanvasId).toBe("canvas-1")
		);
	});

	it("C02: discards a late template recommendation after the template changes", async () => {
		let resolveBest: (value: string) => void = () => {};
		mockBest.mockReturnValue(
			new Promise((resolve) => {
				resolveBest = resolve;
			})
		);
		const { result } = renderHook(() => useAiCanvasRecommendation(), {
			wrapper: makeWrapper(),
		});
		act(() => {
			useWorkPackageStore.setState({
				templateCanvasId: "initial",
				aiSelectedCanvasId: null,
			});
			result.current.mutate(undefined);
		});
		await waitFor(() => expect(mockBest).toHaveBeenCalled());
		act(() => {
			useWorkPackageStore.setState({ templateCanvasId: "user-pick" });
		});
		await act(async () => {
			resolveBest("canvas-1");
		});
		await waitFor(() => expect(result.current.isPending).toBe(false));
		expect(useWorkPackageStore.getState().templateCanvasId).toBe("user-pick");
		expect(useWorkPackageStore.getState().aiSelectedCanvasId).toBeNull();
	});

	it("handles error", async () => {
		mockAgent.mockRejectedValueOnce(new Error("e"));
		const { result } = renderHook(() => useAiCanvasRecommendation(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			try {
				await result.current.mutateAsync(undefined);
			} catch {
				/* */
			}
		});
		await waitFor(() =>
			expect(useWorkPackageStore.getState().error).toContain(
				"AI canvas recommendation failed"
			)
		);
	});

	it("falls back to All Resources when AI returns no selection", async () => {
		mockBest.mockResolvedValueOnce(null);
		useWorkPackageStore.setState({
			templateCanvasId: BASE_TEMPLATE_ID,
			aiSelectedCanvasId: null,
		});
		const { result } = renderHook(() => useAiCanvasRecommendation(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await result.current.mutateAsync(undefined);
		});
		await waitFor(() =>
			expect(useWorkPackageStore.getState().templateCanvasId).toBe(
				BASE_TEMPLATE_ID
			)
		);
		expect(useWorkPackageStore.getState().aiSelectedCanvasId).toBeNull();
	});

	it("skips the AI call when only All Resources is available", async () => {
		useWorkPackageStore.setState({
			candidateTemplates: [BASE_TEMPLATE_CANVAS],
			templateCanvasId: BASE_TEMPLATE_ID,
		});
		const { result } = renderHook(() => useAiCanvasRecommendation(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await result.current.mutateAsync(undefined);
		});
		expect(mockAgent).not.toHaveBeenCalled();
		expect(mockBest).not.toHaveBeenCalled();
		expect(useWorkPackageStore.getState().templateCanvasId).toBe(
			BASE_TEMPLATE_ID
		);
	});
});

describe("useAiSelection", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockAgent.mockResolvedValue("agent-id");
		mockSelectRes.mockResolvedValue({
			externalId: "r1",
			space: "s",
			name: "R",
		});
		useWorkPackageStore.setState({
			workOrder: { externalId: "w", space: "s" },
			asset: { assetExternalId: "a", space: "s" },
			templateCanvasId: "tpl-1",
			templateFields: [
				{ type: "file", label: "Doc", viewExternalId: "CogniteFile" },
			],
			candidatesByType: {
				file: [
					{
						externalId: "r1",
						space: "s",
						properties: {},
					},
				],
			},
			config: DEFAULT_CANVAS_CONFIG,
		});
	});

	it("runs selection loop and stores selections", async () => {
		const { result } = renderHook(() => useAiSelection(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await result.current.mutateAsync(undefined);
		});
		await waitFor(() =>
			expect(useWorkPackageStore.getState().aiSelections.Doc).toBeDefined()
		);
	});

	it("C01: discards a late selection after the work order changes", async () => {
		let resolveSelection: (value: {
			externalId: string;
			space: string;
			name: string;
		}) => void = () => {};
		mockSelectRes.mockReturnValue(
			new Promise((resolve) => {
				resolveSelection = resolve;
			})
		);
		const { result } = renderHook(() => useAiSelection(), {
			wrapper: makeWrapper(),
		});
		act(() => {
			useWorkPackageStore.setState({ aiSelections: {} });
			result.current.mutate(undefined);
		});
		await waitFor(() => expect(mockSelectRes).toHaveBeenCalled());
		act(() => {
			useWorkPackageStore.setState({
				workOrder: { externalId: "other", space: "s" },
			});
		});
		await act(async () => {
			resolveSelection({ externalId: "r1", space: "s", name: "R" });
		});
		await waitFor(() => expect(result.current.isPending).toBe(false));
		expect(useWorkPackageStore.getState().aiSelections.Doc).toBeUndefined();
	});

	it("C03: keeps the other slots when one slot fails", async () => {
		mockSelectRes
			.mockResolvedValueOnce({ externalId: "r1", space: "s", name: "One" })
			.mockRejectedValueOnce(new Error("slot failed"))
			.mockResolvedValueOnce({ externalId: "r3", space: "s", name: "Three" });
		useWorkPackageStore.setState({
			templateFields: [
				{ type: "file", label: "One", viewExternalId: "CogniteFile" },
				{ type: "file", label: "Two", viewExternalId: "CogniteFile" },
				{ type: "file", label: "Three", viewExternalId: "CogniteFile" },
			],
		});
		const { result } = renderHook(() => useAiSelection(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await result.current.mutateAsync(undefined);
		});
		const selections = useWorkPackageStore.getState().aiSelections;
		expect(selections.One?.externalId).toBe("r1");
		expect(selections.Three?.externalId).toBe("r3");
		expect(selections.Two).toBeNull();
		expect(result.current.isError).toBe(false);
	});

	it("C16: records a per-slot error result without failing the run", async () => {
		mockSelectRes.mockResolvedValueOnce({
			kind: "error",
			status: 503,
			retryable: true,
		});
		const { result } = renderHook(() => useAiSelection(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await result.current.mutateAsync(undefined);
		});
		expect(useWorkPackageStore.getState().aiSelections.Doc).toBeNull();
		expect(result.current.isError).toBe(false);
	});

	it("C23: re-runs one slot without clearing the others", async () => {
		useWorkPackageStore.setState({
			templateFields: [
				{ type: "file", label: "One", viewExternalId: "CogniteFile" },
				{ type: "file", label: "Two", viewExternalId: "CogniteFile" },
			],
			aiSelections: {
				One: { externalId: "keep", space: "s", name: "Keep" },
				Two: null,
			},
		});
		mockSelectRes.mockResolvedValueOnce({
			externalId: "fresh",
			space: "s",
			name: "Fresh",
		});
		const { result } = renderHook(() => useAiSelection(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await result.current.mutateAsync({ slotKey: "Two" });
		});
		const selections = useWorkPackageStore.getState().aiSelections;
		expect(selections.One?.externalId).toBe("keep");
		expect(selections.Two?.externalId).toBe("fresh");
	});

	it("C28: keeps a manual pick made while AI selection is running", async () => {
		let resolveSelection: (value: {
			externalId: string;
			space: string;
			name: string;
		}) => void = () => {};
		mockSelectRes.mockReturnValue(
			new Promise((resolve) => {
				resolveSelection = resolve;
			})
		);
		const { result } = renderHook(() => useAiSelection(), {
			wrapper: makeWrapper(),
		});
		act(() => {
			result.current.mutate(undefined);
		});
		await waitFor(() => expect(mockSelectRes).toHaveBeenCalled());
		act(() => {
			useWorkPackageStore.getState().setSlotSelection("Doc", {
				externalId: "manual",
				space: "s",
				name: "Manual",
			});
		});
		await act(async () => {
			resolveSelection({ externalId: "r1", space: "s", name: "AI" });
		});
		await waitFor(() => expect(result.current.isPending).toBe(false));
		expect(useWorkPackageStore.getState().aiSelections.Doc?.externalId).toBe(
			"manual"
		);
	});

	it("skips fields with no resource config and records null when AI returns none", async () => {
		mockSelectRes.mockResolvedValueOnce(null);
		useWorkPackageStore.setState({
			templateFields: [
				{ type: "file", label: "Doc", viewExternalId: "CogniteFile" },
				{ type: "unknown-type", label: "Skip", viewExternalId: "X" },
			],
			candidatesByType: {
				file: [{ externalId: "r1", space: "s", properties: {} }],
			},
		});
		const progress = vi.fn();
		const { result } = renderHook(() => useAiSelection(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await result.current.mutateAsync(progress);
		});
		await waitFor(() =>
			expect(useWorkPackageStore.getState().aiSelections.Doc).toBeNull()
		);
		expect(progress).toHaveBeenCalled();
	});
});
