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
