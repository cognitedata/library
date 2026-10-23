import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	ACTIVITY_VIEW_ID,
	BASE_TEMPLATE_CANVAS,
	BASE_TEMPLATE_ID,
	DEFAULT_CANVAS_CONFIG,
	DEFAULT_CANVAS_DIMENSIONS,
	FILE_VIEW_ID,
	TIME_SERIES_VIEW_ID,
} from "@/__mocks__/app-config";
import { createStubCdfClient } from "@/__mocks__/cdf-runtime-test-wrapper";
import {
	type CdfRuntimeContextValue,
	CdfRuntimeTestProvider,
} from "@/contexts/CdfRuntimeContext";
import { AppConfigProvider } from "../app-config";
import { useWorkPackageStore } from "../store";
import {
	UseTemplatesContext,
	type UseTemplatesContextType,
	useAllTemplateSlots,
	useFindCanvasesByName,
	useTemplateCanvases,
} from "./useTemplates";

const tmpl = {
	...DEFAULT_CANVAS_DIMENSIONS,
	properties: {} as Record<string, unknown>,
};

const mockListTemplates = vi.fn();
const mockFindByName = vi.fn();
const mockGetInstances = vi.fn();

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
	const deps: UseTemplatesContextType = {
		listTemplateCanvases: mockListTemplates,
		findCanvasesByName: mockFindByName,
		getTemplateCanvasInstances: mockGetInstances,
		useWorkPackageStore,
		useCdfClientInfo: () => cdfRuntime,
	};
	const Wrapper = ({ children }: { children: ReactNode }) => (
		<AppConfigProvider>
			<CdfRuntimeTestProvider value={cdfRuntime}>
				<UseTemplatesContext.Provider value={deps}>
					<QueryClientProvider client={queryClient}>
						{children}
					</QueryClientProvider>
				</UseTemplatesContext.Provider>
			</CdfRuntimeTestProvider>
		</AppConfigProvider>
	);
	return Wrapper;
}

const wrapper = ({ children }: { children: ReactNode }) => {
	const Inner = makeWrapper();
	return <Inner>{children}</Inner>;
};

describe("useTemplateCanvases", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockListTemplates.mockResolvedValue([
			{
				externalId: "t1",
				space: "s",
				name: "template_x",
				visibility: "public",
			},
		]);
		useWorkPackageStore.setState({
			candidateTemplates: [],
			templateCanvasId: "",
			config: DEFAULT_CANVAS_CONFIG,
		});
	});

	it("loads templates into store", async () => {
		const { result } = renderHook(() => useTemplateCanvases(), { wrapper });
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(mockListTemplates).toHaveBeenCalled();
	});

	it("defaults to All Resources even when API returns one template", async () => {
		mockListTemplates.mockResolvedValue([
			{
				externalId: "sole",
				space: "s",
				name: "Only",
				visibility: "public",
			},
		]);
		useWorkPackageStore.setState({
			candidateTemplates: [],
			templateCanvasId: "",
			config: DEFAULT_CANVAS_CONFIG,
		});
		renderHook(() => useTemplateCanvases(), { wrapper });
		await waitFor(() =>
			expect(useWorkPackageStore.getState().templateCanvasId).toBe(
				BASE_TEMPLATE_ID
			)
		);
		expect(useWorkPackageStore.getState().candidateTemplates[0]).toEqual(
			BASE_TEMPLATE_CANVAS
		);
	});

	it("writes candidate templates once after the list resolves", async () => {
		mockListTemplates.mockResolvedValue([]);
		useWorkPackageStore.setState({
			candidateTemplates: [],
			templateCanvasId: "",
			config: DEFAULT_CANVAS_CONFIG,
		});
		renderHook(() => useTemplateCanvases(), { wrapper });
		await waitFor(() =>
			expect(useWorkPackageStore.getState().candidateTemplates).toEqual([
				BASE_TEMPLATE_CANVAS,
			])
		);
		const written = useWorkPackageStore.getState().candidateTemplates;
		await new Promise((resolve) => setTimeout(resolve, 50));
		expect(useWorkPackageStore.getState().candidateTemplates).toBe(written);
	});

	it("defaults to All Resources when the API returns no templates", async () => {
		mockListTemplates.mockResolvedValue([]);
		useWorkPackageStore.setState({
			candidateTemplates: [],
			templateCanvasId: "",
			config: DEFAULT_CANVAS_CONFIG,
		});
		renderHook(() => useTemplateCanvases(), { wrapper });
		await waitFor(() =>
			expect(useWorkPackageStore.getState().candidateTemplates).toEqual([
				BASE_TEMPLATE_CANVAS,
			])
		);
		expect(useWorkPackageStore.getState().templateCanvasId).toBe(
			BASE_TEMPLATE_ID
		);
	});

	it("does not overwrite templateCanvasId when API returns one template but id already set", async () => {
		mockListTemplates.mockResolvedValue([
			{
				externalId: "sole",
				space: "s",
				name: "Only",
				visibility: "public",
			},
		]);
		useWorkPackageStore.setState({
			candidateTemplates: [],
			templateCanvasId: "preset-id",
			config: DEFAULT_CANVAS_CONFIG,
		});
		renderHook(() => useTemplateCanvases(), { wrapper });
		await waitFor(() => expect(mockListTemplates).toHaveBeenCalled());
		await waitFor(() =>
			expect(useWorkPackageStore.getState().templateCanvasId).toBe("preset-id")
		);
	});

	it("prefers store candidateTemplates while list query is still pending", () => {
		mockListTemplates.mockImplementation(
			() =>
				new Promise(() => {
					/* never resolves */
				})
		);
		const seeded = [
			{
				externalId: "seed",
				space: "s",
				name: "Seeded",
				visibility: "public" as const,
			},
		];
		useWorkPackageStore.setState({
			candidateTemplates: seeded,
			templateCanvasId: "",
			config: DEFAULT_CANVAS_CONFIG,
		});
		const { result } = renderHook(() => useTemplateCanvases(), { wrapper });
		expect(result.current.templates).toEqual(seeded);
	});
});

describe("useFindCanvasesByName", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockFindByName.mockResolvedValue([]);
	});

	it("queries when name non-empty", async () => {
		const { result } = renderHook(() => useFindCanvasesByName("MyCanvas"), {
			wrapper,
		});
		await waitFor(() => expect(result.current.isSuccess).toBe(true));
		expect(mockFindByName).toHaveBeenCalled();
	});

	it("does not call API when canvas name is only whitespace", async () => {
		mockFindByName.mockClear();
		const { result } = renderHook(() => useFindCanvasesByName("   \t"), {
			wrapper,
		});
		await waitFor(() => expect(result.current.isFetching).toBe(false));
		expect(mockFindByName).not.toHaveBeenCalled();
	});
});

describe("useAllTemplateSlots", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockGetInstances.mockResolvedValue([
			{
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
	});

	it("resolves slots per template", async () => {
		const templates = [
			{
				externalId: "tx",
				space: "s",
				name: "T",
				visibility: "public" as const,
			},
		];
		const { result } = renderHook(() => useAllTemplateSlots(templates), {
			wrapper,
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.slotsByTemplate.tx?.length).toBeGreaterThan(0);
	});

	it("sets slotsError when instance fetch fails", async () => {
		mockGetInstances.mockRejectedValueOnce(new Error("slot fetch failed"));
		const templates = [
			{
				externalId: "tx",
				space: "s",
				name: "T",
				visibility: "public" as const,
			},
		];
		const { result } = renderHook(() => useAllTemplateSlots(templates), {
			wrapper,
		});
		await waitFor(() => expect(result.current.slotsError).toBe(true));
		expect(result.current.firstSlotsError).toEqual(
			expect.objectContaining({ message: "slot fetch failed" })
		);
	});

	it("categorizes main work order, file resource, and other view slots", async () => {
		mockGetInstances.mockResolvedValue([
			{
				label: "main schedule",
				viewExternalId: ACTIVITY_VIEW_ID.externalId,
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
			{
				label: "Drawing",
				viewExternalId: FILE_VIEW_ID.externalId,
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
			{
				label: "Extra",
				viewExternalId: "CustomView",
				viewSpace: "custom",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		const templates = [
			{
				externalId: "tm",
				space: "s",
				name: "T",
				visibility: "public" as const,
			},
		];
		const { result } = renderHook(() => useAllTemplateSlots(templates), {
			wrapper,
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		const slots = result.current.slotsByTemplate.tm ?? [];
		expect(slots.some((s) => s.category === "workorder")).toBe(true);
		expect(
			slots.some((s) => s.category === "file" && s.populatedBy === "ai")
		).toBe(true);
		expect(slots.some((s) => s.category === "other")).toBe(true);
	});

	it("categorizes time series resource slots as timeseries with ai population", async () => {
		mockGetInstances.mockResolvedValue([
			{
				label: "TS slot",
				viewExternalId: TIME_SERIES_VIEW_ID.externalId,
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		const templates = [
			{
				externalId: "tm",
				space: "s",
				name: "T",
				visibility: "public" as const,
			},
		];
		const { result } = renderHook(() => useAllTemplateSlots(templates), {
			wrapper,
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		const slots = result.current.slotsByTemplate.tm ?? [];
		expect(
			slots.some(
				(s) =>
					s.category === "timeseries" &&
					s.populatedBy === "ai" &&
					s.viewExternalId === TIME_SERIES_VIEW_ID.externalId
			)
		).toBe(true);
	});

	it("returns empty slots when templates array is empty", () => {
		const { result } = renderHook(() => useAllTemplateSlots([]), { wrapper });
		expect(result.current.slotsByTemplate).toEqual({});
		expect(result.current.isLoading).toBe(false);
		expect(result.current.slotsError).toBe(false);
		expect(mockGetInstances).not.toHaveBeenCalled();
	});

	it("does not stay loading when only the base template is present", () => {
		const { result } = renderHook(
			() => useAllTemplateSlots([BASE_TEMPLATE_CANVAS]),
			{ wrapper }
		);
		expect(result.current.isLoading).toBe(false);
		expect(result.current.isFetchingSlots).toBe(false);
		expect(result.current.slotsByTemplate).toEqual({});
		expect(mockGetInstances).not.toHaveBeenCalled();
	});

	it("fetches slots for real templates and skips the base template", async () => {
		const templates = [
			{
				externalId: "tx",
				space: "s",
				name: "T",
				visibility: "public" as const,
			},
			BASE_TEMPLATE_CANVAS,
		];
		const { result } = renderHook(() => useAllTemplateSlots(templates), {
			wrapper,
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.slotsByTemplate.tx?.length).toBeGreaterThan(0);
		expect(
			result.current.slotsByTemplate[BASE_TEMPLATE_CANVAS.externalId]
		).toBeUndefined();
		expect(mockGetInstances).toHaveBeenCalledTimes(1);
		expect(mockGetInstances).toHaveBeenCalledWith(
			expect.anything(),
			"tx",
			expect.anything()
		);
	});

	it("refetchSlots triggers another instance fetch", async () => {
		const templates = [
			{
				externalId: "tm",
				space: "s",
				name: "T",
				visibility: "public" as const,
			},
		];
		const { result } = renderHook(() => useAllTemplateSlots(templates), {
			wrapper,
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(mockGetInstances).toHaveBeenCalledTimes(1);
		await result.current.refetchSlots();
		expect(mockGetInstances).toHaveBeenCalledTimes(2);
	});
});
