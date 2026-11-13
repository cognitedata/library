import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createMockAppConfig } from "@/__mocks__/app-config";
import { PLACEHOLDER_CANVAS_CONFIG, useWorkPackageStore } from "@/store";
import { AppConfigGate } from "./AppConfigGate";
import { AppConfigLoadError, type AppConfigService } from "./AppConfigService";
import { installAppConfig } from "./load";
import { toCanvasConfig } from "./runtime";
import {
	AppConfigLoaderContext,
	type AppConfigLoaderContextType,
} from "./useAppConfigLoaderViewModel";

describe(AppConfigGate.name, () => {
	let service: AppConfigService;
	let queryClient: QueryClient;

	beforeEach(() => {
		service = { load: vi.fn() };
		queryClient = new QueryClient({
			defaultOptions: { queries: { retry: false } },
		});
	});

	afterEach(() => {
		installAppConfig(createMockAppConfig());
	});

	it("renders children after the config loads", async () => {
		const mockConfig = createMockAppConfig();
		useWorkPackageStore.setState({ config: PLACEHOLDER_CANVAS_CONFIG });
		vi.mocked(service.load).mockResolvedValue(mockConfig);
		renderGate(<p>Ready</p>);
		expect(await screen.findByText("Ready")).toBeInTheDocument();
		expect(useWorkPackageStore.getState().config).toEqual(
			toCanvasConfig(mockConfig)
		);
	});

	it.each([
		["schemaMissing", "not set up"],
		["notFound", "was not found"],
		["forbidden", "dataModels:read"],
		["invalid", "invalid"],
		["unsupportedVersion", "not supported"],
		["network", "could not be loaded"],
	] as const)("shows the %s setup screen", async (kind, text) => {
		vi.mocked(service.load).mockRejectedValue(
			new AppConfigLoadError(kind, "detail", ["WPGConfig.limits: required"])
		);
		renderGate(<p>Ready</p>);
		expect(await screen.findByRole("alert")).toHaveTextContent(
			new RegExp(text, "i")
		);
		expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
		expect(screen.queryByText("Ready")).not.toBeInTheDocument();
	});

	function renderGate(children: ReactNode) {
		const context: AppConfigLoaderContextType = {
			useAppConfigService: () => service,
		};
		return render(
			<QueryClientProvider client={queryClient}>
				<AppConfigLoaderContext.Provider value={context}>
					<AppConfigGate>{children}</AppConfigGate>
				</AppConfigLoaderContext.Provider>
			</QueryClientProvider>
		);
	}
});
