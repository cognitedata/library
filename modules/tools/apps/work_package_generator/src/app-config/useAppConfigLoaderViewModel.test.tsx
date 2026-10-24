import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createMockAppConfig } from "@/__mocks__/app-config";
import { AppConfigLoadError, type AppConfigService } from "./AppConfigService";
import { getAppConfig, installAppConfig } from "./load";
import {
	AppConfigLoaderContext,
	type AppConfigLoaderContextType,
	useAppConfigLoaderViewModel,
} from "./useAppConfigLoaderViewModel";

describe(useAppConfigLoaderViewModel.name, () => {
	let service: AppConfigService;
	let wrapper: ({ children }: { children: ReactNode }) => ReactNode;

	beforeEach(() => {
		service = { load: vi.fn() };
		const queryClient = new QueryClient({
			defaultOptions: { queries: { retry: false } },
		});
		const context: AppConfigLoaderContextType = {
			useAppConfigService: () => service,
		};
		wrapper = ({ children }) => (
			<QueryClientProvider client={queryClient}>
				<AppConfigLoaderContext.Provider value={context}>
					{children}
				</AppConfigLoaderContext.Provider>
			</QueryClientProvider>
		);
	});

	afterEach(() => {
		installAppConfig(createMockAppConfig());
	});

	it("is loading until the service resolves", () => {
		vi.mocked(service.load).mockReturnValue(new Promise(() => undefined));
		const { result } = renderHook(() => useAppConfigLoaderViewModel(), {
			wrapper,
		});
		expect(result.current.isLoading).toBe(true);
		expect(result.current.error).toBeNull();
	});

	it("installs the loaded config", async () => {
		const config = createMockAppConfig();
		vi.mocked(service.load).mockResolvedValue(config);
		const { result } = renderHook(() => useAppConfigLoaderViewModel(), {
			wrapper,
		});

		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.config).toBe(config);
		expect(getAppConfig()).toBe(config);
		expect(result.current.error).toBeNull();
	});

	it("surfaces each load error kind", async () => {
		const error = new AppConfigLoadError("notFound", "missing node");
		vi.mocked(service.load).mockRejectedValue(error);
		const { result } = renderHook(() => useAppConfigLoaderViewModel(), {
			wrapper,
		});

		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.error?.kind).toBe("notFound");
		expect(result.current.config).toBeUndefined();
	});

	it("retries the load", async () => {
		const config = createMockAppConfig();
		vi.mocked(service.load)
			.mockRejectedValueOnce(new AppConfigLoadError("network", "offline"))
			.mockResolvedValueOnce(config);
		const { result } = renderHook(() => useAppConfigLoaderViewModel(), {
			wrapper,
		});

		await waitFor(() => expect(result.current.error?.kind).toBe("network"));
		result.current.retry();
		await waitFor(() => expect(result.current.config).toBe(config));
		expect(service.load).toHaveBeenCalledTimes(2);
	});
});
