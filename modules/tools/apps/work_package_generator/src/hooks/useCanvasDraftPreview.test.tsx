import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	BASE_TEMPLATE_ID,
	DEFAULT_CANVAS_CONFIG,
	DEFAULT_CANVAS_DIMENSIONS,
} from "@/__mocks__/app-config";
import { createStubCdfClient } from "@/__mocks__/cdf-runtime-test-wrapper";
import { AppConfigProvider } from "@/app-config";
import {
	type CdfRuntimeContextValue,
	CdfRuntimeTestProvider,
} from "@/contexts/CdfRuntimeContext";
import { useWorkPackageStore } from "../store";
import {
	UseCanvasDraftPreviewContext,
	type UseCanvasDraftPreviewContextType,
	useCanvasDraftPreview,
} from "./useCanvasDraftPreview";

const tmpl = {
	...DEFAULT_CANVAS_DIMENSIONS,
	properties: {} as Record<string, unknown>,
};

const mockAggregate = vi.fn().mockResolvedValue({ nodes: [] });
const mockBuildFromTemplate = vi.fn().mockResolvedValue([]);
const mockBuildBase = vi.fn().mockReturnValue([]);
const mockInstances = vi.fn();
const mockAnnotations = vi.fn();

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
	const deps: UseCanvasDraftPreviewContextType = {
		aggregateCanvasNodes: mockAggregate,
		buildCanvasItemsFromTemplate: mockBuildFromTemplate,
		buildBaseTemplateItems: mockBuildBase,
		getTemplateCanvasInstances: mockInstances,
		getTemplateCanvasAnnotations: mockAnnotations,
		useWorkPackageStore,
		useCdfClientInfo: () => cdfRuntime,
	};
	const Wrapper = ({ children }: { children: ReactNode }) => (
		<AppConfigProvider>
			<CdfRuntimeTestProvider value={cdfRuntime}>
				<UseCanvasDraftPreviewContext.Provider value={deps}>
					<QueryClientProvider client={queryClient}>
						{children}
					</QueryClientProvider>
				</UseCanvasDraftPreviewContext.Provider>
			</CdfRuntimeTestProvider>
		</AppConfigProvider>
	);
	return Wrapper;
}

describe("useCanvasDraftPreview", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockAggregate.mockResolvedValue({ nodes: [] });
		mockBuildFromTemplate.mockResolvedValue([]);
		mockInstances.mockResolvedValue([
			{
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		mockAnnotations.mockResolvedValue([]);
		useWorkPackageStore.setState({
			templateCanvasId: "tpl",
			aiSelections: { x: { externalId: "r", space: "s" } },
			asset: { assetExternalId: "a", space: "s" },
			workOrder: { externalId: "w", space: "s" },
			config: DEFAULT_CANVAS_CONFIG,
		});
	});

	it("aggregates preview when ready", async () => {
		const { result } = renderHook(() => useCanvasDraftPreview(true), {
			wrapper: makeWrapper(),
		});
		await waitFor(() => expect(result.current.isSuccess).toBe(true));
		expect(mockInstances).toHaveBeenCalled();
		expect(mockBuildFromTemplate).toHaveBeenCalled();
		expect(mockAggregate).toHaveBeenCalled();
	});

	it("stays idle until the package is ready", () => {
		const { result } = renderHook(() => useCanvasDraftPreview(false), {
			wrapper: makeWrapper(),
		});
		expect(result.current.fetchStatus).toBe("idle");
		expect(mockInstances).not.toHaveBeenCalled();
	});

	it("builds a base-template preview without loading stored instances", async () => {
		useWorkPackageStore.setState({
			templateCanvasId: BASE_TEMPLATE_ID,
			aiSelections: {},
			candidatesByType: { file: [] },
		});
		const { result } = renderHook(() => useCanvasDraftPreview(true), {
			wrapper: makeWrapper(),
		});
		await waitFor(() => expect(result.current.isSuccess).toBe(true));
		expect(mockBuildBase).toHaveBeenCalled();
		expect(mockInstances).not.toHaveBeenCalled();
	});

	it("fails when the stored template has no instances", async () => {
		mockInstances.mockResolvedValue([]);
		const { result } = renderHook(() => useCanvasDraftPreview(true), {
			wrapper: makeWrapper(),
		});
		await waitFor(() => expect(result.current.isError).toBe(true));
		expect(result.current.error).toEqual(
			expect.objectContaining({
				message: "No template instances found for preview.",
			})
		);
	});

	it("includes a cleared selection in the preview query", async () => {
		useWorkPackageStore.setState({
			aiSelections: {
				b: null,
				a: { externalId: "r", space: "s" },
			},
		});
		const { result } = renderHook(() => useCanvasDraftPreview(true), {
			wrapper: makeWrapper(),
		});
		await waitFor(() => expect(result.current.isSuccess).toBe(true));
		expect(mockBuildFromTemplate).toHaveBeenCalled();
	});
});
