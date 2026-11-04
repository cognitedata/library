import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createMockAppConfig } from "@/__mocks__/app-config";
import rawConfig from "@/__mocks__/app-config.fixture.json";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { WPG_CONFIG_LOCATION } from "@/app-config/config-location";
import { installAppConfig } from "@/app-config/load";

import App from "./App";

const gate = vi.hoisted(() => ({
	retrieve: (): Promise<{ items: unknown[] }> =>
		Promise.resolve({ items: [configNode()] }),
}));

// vi.mock: CogniteSdkProvider/useCogniteSdk come from the host SDK; stub avoids Fusion runtime.
vi.mock("@cognite/app-sdk/react", () => ({
	CogniteSdkProvider: ({ children }: { children: ReactNode }) => children,
	useCogniteSdk: () =>
		cdfClientTestDouble({
			project: "test-project",
			getBaseUrl: () => "https://test.cognitedata.com",
			dataModels: {
				list: () => ({
					autoPagingToArray: async () => [
						{
							externalId: WPG_CONFIG_LOCATION.dataModelExternalId,
							version: "v1",
							views: [
								{
									type: "view",
									space: WPG_CONFIG_LOCATION.schemaSpace,
									externalId: WPG_CONFIG_LOCATION.viewExternalId,
									version: "v1",
								},
							],
						},
					],
				}),
			},
			instances: {
				retrieve: () => gate.retrieve(),
			},
		}),
}));

// vi.mock: WorkPackageGenerator needs CdfRuntimeProvider from the Fusion host; stub keeps App shell tests isolated.
vi.mock("@/pages/WorkPackageGenerator", () => ({
	WorkPackageGenerator: () => (
		<div data-testid="work-package-generator">Work Package Generator</div>
	),
}));

// vi.mock: Navigation is covered elsewhere; stub verifies App shell composition only.
vi.mock("@/components/layout", () => ({
	Navigation: () => <nav data-testid="navigation">Navigation</nav>,
}));

describe("App", () => {
	afterEach(() => {
		gate.retrieve = () => Promise.resolve({ items: [configNode()] });
		installAppConfig(createMockAppConfig());
	});

	it("shows loading, then the app shell", async () => {
		let finish: (value: { items: unknown[] }) => void = () => undefined;
		const pending = new Promise<{ items: unknown[] }>((resolve) => {
			finish = resolve;
		});
		gate.retrieve = () => pending;

		renderApp();
		expect(
			screen.getByRole("status", { name: "Loading configuration" })
		).toBeInTheDocument();

		finish({ items: [configNode()] });
		expect(await screen.findByTestId("navigation")).toBeInTheDocument();
		expect(screen.getByTestId("work-package-generator")).toBeInTheDocument();
	});

	it("shows the setup screen when the config node is missing", async () => {
		gate.retrieve = async () => ({ items: [] });
		renderApp();
		expect(await screen.findByRole("alert")).toHaveTextContent(/not found/i);
		expect(screen.queryByTestId("navigation")).not.toBeInTheDocument();
	});

	it("wraps content in an error boundary", async () => {
		renderApp();
		expect(
			await screen.findByRole("link", { name: /skip to main content/i })
		).toBeInTheDocument();
	});

	it("renders main landmark with correct id for skip-link target", async () => {
		renderApp();
		const main = await screen.findByRole("main");
		expect(main).toHaveAttribute("id", "main-content");
	});
});

function renderApp() {
	const queryClient = new QueryClient({
		defaultOptions: { queries: { retry: false } },
	});
	return render(
		<QueryClientProvider client={queryClient}>
			<App />
		</QueryClientProvider>
	);
}

function configNode() {
	return {
		instanceType: "node",
		space: WPG_CONFIG_LOCATION.instanceSpace,
		externalId: WPG_CONFIG_LOCATION.nodeExternalId,
		properties: {
			[WPG_CONFIG_LOCATION.schemaSpace]: {
				[`${WPG_CONFIG_LOCATION.viewExternalId}/v1`]: rawConfig,
			},
		},
	};
}
