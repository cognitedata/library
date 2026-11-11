import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { type ReactNode, useContext } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
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
	UseCanvasCreationContext,
	type UseCanvasCreationContextType,
	useCanvasCreation,
} from "./useCanvasCreation";

const mockCreate = vi.fn();
const mockUpdate = vi.fn();
const mockLink = vi.fn();
const mockBuildBase = vi.fn();
const mockGetInstances = vi.fn();
const mockGetAnnotations = vi.fn();

const cdfRuntime: CdfRuntimeContextValue = {
	client: createStubCdfClient({
		project: "proj",
		getBaseUrl: () => "https://x.cognitedata.com",
	}),
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
	const deps: UseCanvasCreationContextType = {
		createCanvasFromTemplateAndSelections: mockCreate,
		updateCanvasFromTemplateAndSelections: mockUpdate,
		generateCanvasLink: mockLink,
		buildBaseTemplateItems: mockBuildBase,
		getTemplateCanvasInstances: mockGetInstances,
		getTemplateCanvasAnnotations: mockGetAnnotations,
		useWorkPackageStore,
		useCdfClientInfo: () => cdfRuntime,
		getOrg: () => "acme",
	};
	const Wrapper = ({ children }: { children: ReactNode }) => (
		<AppConfigProvider>
			<CdfRuntimeTestProvider value={cdfRuntime}>
				<UseCanvasCreationContext.Provider value={deps}>
					<QueryClientProvider client={queryClient}>
						{children}
					</QueryClientProvider>
				</UseCanvasCreationContext.Provider>
			</CdfRuntimeTestProvider>
		</AppConfigProvider>
	);
	return Wrapper;
}

describe("useCanvasCreation", () => {
	it("uses the Fusion hostname as the canvas org", () => {
		vi.stubGlobal("location", {
			...window.location,
			hostname: "apps.cognite.com",
			ancestorOrigins: ["https://cog-demo.fusion.cognite.com"],
		});
		const { result } = renderHook(
			() => useContext(UseCanvasCreationContext).getOrg(),
			{
				wrapper: ({ children }: { children: ReactNode }) => (
					<AppConfigProvider>{children}</AppConfigProvider>
				),
			}
		);
		expect(result.current).toBe("cog-demo");
		vi.unstubAllGlobals();
	});

	beforeEach(() => {
		vi.clearAllMocks();
		mockCreate.mockResolvedValue("new-canvas");
		mockUpdate.mockResolvedValue("existing");
		mockLink.mockReturnValue("https://link");
		mockGetInstances.mockResolvedValue([]);
		mockGetAnnotations.mockResolvedValue([]);
		useWorkPackageStore.setState({
			workOrder: { externalId: "w", space: "s" },
			asset: { assetExternalId: "a", space: "s" },
			templateCanvasId: "tpl",
			aiSelections: { f: null },
			config: DEFAULT_CANVAS_CONFIG,
		});
	});

	it("creates new canvas and returns link", async () => {
		const { result } = renderHook(() => useCanvasCreation(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await result.current.mutateAsync({ canvasName: "N" });
		});
		await waitFor(() => expect(mockCreate).toHaveBeenCalled());
		expect(mockLink).toHaveBeenCalled();
	});

	it("updates when existingCanvasId set", async () => {
		const { result } = renderHook(() => useCanvasCreation(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await result.current.mutateAsync({
				canvasName: "N",
				existingCanvasId: "ex",
			});
		});
		expect(mockUpdate).toHaveBeenCalled();
	});

	it("surfaces error", async () => {
		mockCreate.mockRejectedValueOnce(new Error("fail"));
		const { result } = renderHook(() => useCanvasCreation(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			try {
				await result.current.mutateAsync({ canvasName: "N" });
			} catch {
				/* */
			}
		});
		await waitFor(() =>
			expect(useWorkPackageStore.getState().error).toContain(
				"Canvas creation failed"
			)
		);
	});

	it("requires a work order and asset", async () => {
		useWorkPackageStore.setState({ workOrder: null, asset: null });
		const { result } = renderHook(() => useCanvasCreation(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await expect(
				result.current.mutateAsync({ canvasName: "N" })
			).rejects.toThrow(/required/i);
		});
	});

	it("requires a template canvas", async () => {
		useWorkPackageStore.setState({ templateCanvasId: "" });
		const { result } = renderHook(() => useCanvasCreation(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			await expect(
				result.current.mutateAsync({ canvasName: "N" })
			).rejects.toThrow(/template/i);
		});
	});

	it("C11: does not validate the canvas after a base-template save", async () => {
		useWorkPackageStore.setState({ templateCanvasId: BASE_TEMPLATE_ID });
		mockBuildBase.mockReturnValue([{ id: "a" }, { id: "b" }]);
		const { result } = renderHook(() => useCanvasCreation(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			const created = await result.current.mutateAsync({ canvasName: "N" });
			expect(created.validation.success).toBe(true);
			expect(created.isUpdate).toBe(false);
		});
		expect(mockBuildBase).not.toHaveBeenCalled();
		expect(mockGetInstances).not.toHaveBeenCalled();
	});

	it("requires a CDF client", async () => {
		const queryClient = new QueryClient({
			defaultOptions: { mutations: { retry: false } },
		});
		const deps: UseCanvasCreationContextType = {
			createCanvasFromTemplateAndSelections: mockCreate,
			updateCanvasFromTemplateAndSelections: mockUpdate,
			generateCanvasLink: mockLink,
			buildBaseTemplateItems: mockBuildBase,
			getTemplateCanvasInstances: mockGetInstances,
			getTemplateCanvasAnnotations: mockGetAnnotations,
			useWorkPackageStore,
			useCdfClientInfo: () => ({
				client: undefined,
				project: "proj",
				cluster: "x",
			}),
			getOrg: () => "acme",
		};
		const { result } = renderHook(() => useCanvasCreation(), {
			wrapper: ({ children }: { children: ReactNode }) => (
				<AppConfigProvider>
					<UseCanvasCreationContext.Provider value={deps}>
						<QueryClientProvider client={queryClient}>
							{children}
						</QueryClientProvider>
					</UseCanvasCreationContext.Provider>
				</AppConfigProvider>
			),
		});
		await act(async () => {
			await expect(
				result.current.mutateAsync({ canvasName: "N" })
			).rejects.toThrow(/client/i);
		});
	});

	it("C11: does not re-read the template to verify an update", async () => {
		mockGetInstances.mockResolvedValue([{ id: "i1" }, { id: "i2" }]);
		mockGetAnnotations.mockResolvedValue([{ id: "a1" }]);
		const { result } = renderHook(() => useCanvasCreation(), {
			wrapper: makeWrapper(),
		});
		await act(async () => {
			const created = await result.current.mutateAsync({
				canvasName: "N",
				existingCanvasId: "ex",
			});
			expect(created.isUpdate).toBe(true);
			expect(created.validation.success).toBe(true);
		});
		expect(mockGetInstances).not.toHaveBeenCalled();
		expect(mockGetAnnotations).not.toHaveBeenCalled();
		expect(useWorkPackageStore.getState().isLoading).toBe(false);
	});
});
