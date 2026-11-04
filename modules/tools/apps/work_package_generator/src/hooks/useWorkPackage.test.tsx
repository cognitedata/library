import { renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_CANVAS_CONFIG } from "@/__mocks__/app-config";
import {
	cdfRuntimeQueryWrapper,
	createStubCdfClient,
} from "@/__mocks__/cdf-runtime-test-wrapper";
import * as api from "@/api";
import { useWorkPackageStore } from "@/store";
import { useWorkPackageGenerator } from "./useWorkPackage";

const wrapper = cdfRuntimeQueryWrapper(
	{
		client: createStubCdfClient({
			project: "p",
			getBaseUrl: () => "https://x.cognitedata.com",
		}),
		project: "p",
		cluster: "x",
	},
	{
		defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
	}
);

const mockListTemplates = vi.spyOn(api, "listTemplateCanvases");

describe("useWorkPackageGenerator", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockListTemplates.mockResolvedValue([]);
		useWorkPackageStore.setState({
			...useWorkPackageStore.getState(),
			candidateTemplates: [],
			templateCanvasId: "",
			config: DEFAULT_CANVAS_CONFIG,
			workOrder: null,
			asset: null,
		});
	});

	it("exposes query refetch helpers and search reset", async () => {
		const { result } = renderHook(() => useWorkPackageGenerator(), {
			wrapper,
		});
		await waitFor(() => expect(result.current.isLoadingTemplates).toBe(false));
		expect(typeof result.current.refetchTemplates).toBe("function");
		expect(typeof result.current.refetchResources).toBe("function");
		expect(typeof result.current.refetchTemplateFields).toBe("function");
		expect(typeof result.current.resetSearch).toBe("function");
	});

	it("surfaces templatesError when template list query fails", async () => {
		mockListTemplates.mockRejectedValueOnce(new Error("network down"));
		const { result } = renderHook(() => useWorkPackageGenerator(), {
			wrapper,
		});
		await waitFor(() =>
			expect(result.current.templatesError).toEqual(
				expect.objectContaining({ message: "network down" })
			)
		);
	});
});
