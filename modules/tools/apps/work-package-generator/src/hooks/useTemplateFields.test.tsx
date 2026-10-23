import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
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
	UseTemplateFieldsContext,
	type UseTemplateFieldsContextType,
	useTemplateFields,
} from "./useTemplateFields";

const tmpl = {
	...DEFAULT_CANVAS_DIMENSIONS,
	properties: {} as Record<string, unknown>,
};

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
	const deps: UseTemplateFieldsContextType = {
		getTemplateCanvasInstances: mockGetInstances,
		useWorkPackageStore,
		useCdfClientInfo: () => cdfRuntime,
	};
	const Wrapper = ({ children }: { children: ReactNode }) => (
		<AppConfigProvider>
			<CdfRuntimeTestProvider value={cdfRuntime}>
				<UseTemplateFieldsContext.Provider value={deps}>
					<QueryClientProvider client={queryClient}>
						{children}
					</QueryClientProvider>
				</UseTemplateFieldsContext.Provider>
			</CdfRuntimeTestProvider>
		</AppConfigProvider>
	);
	return Wrapper;
}

describe("useTemplateFields", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockGetInstances.mockResolvedValue([
			{
				label: "Pick file",
				viewExternalId: "CogniteFile",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		useWorkPackageStore.setState({
			templateCanvasId: "tpl1",
			templateFieldsLoadedFrom: null,
			templateFields: [],
			config: DEFAULT_CANVAS_CONFIG,
		});
	});

	it("loads fields from template instances", async () => {
		const { result } = renderHook(() => useTemplateFields(), {
			wrapper: makeWrapper(),
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(mockGetInstances).toHaveBeenCalledWith(
			expect.anything(),
			"tpl1",
			expect.anything()
		);
		expect(result.current.fields.length).toBeGreaterThan(0);
	});

	it("does not fetch when queryEnabled is false", () => {
		const { result } = renderHook(
			() => useTemplateFields({ queryEnabled: false }),
			{ wrapper: makeWrapper() }
		);
		expect(mockGetInstances).not.toHaveBeenCalled();
		expect(result.current.fields).toEqual([]);
		expect(result.current.isLoading).toBe(false);
	});
});
