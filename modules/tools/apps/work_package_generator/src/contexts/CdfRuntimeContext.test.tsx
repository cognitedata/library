import { useCogniteSdk } from "@cognite/app-sdk/react";
import type { CogniteClient } from "@cognite/sdk";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { CdfRuntimeProvider, useCdfRuntime } from "./CdfRuntimeContext";

// vi.mock: useCogniteSdk comes from the host SDK (not DI); mock supplies a stub client.
vi.mock("@cognite/app-sdk/react", () => ({
	useCogniteSdk: vi.fn(),
}));

function Consumer() {
	const { project, cluster, client } = useCdfRuntime();
	return (
		<div>
			<span data-testid="project">{project}</span>
			<span data-testid="cluster">{cluster}</span>
			<span data-testid="has-client">{client ? "yes" : "no"}</span>
		</div>
	);
}

describe("CdfRuntimeProvider", () => {
	it("derives project and cluster from the SDK session", () => {
		vi.mocked(useCogniteSdk).mockReturnValue(
			cdfClientTestDouble({
				project: "live",
				getBaseUrl: () => "https://my.cognitedata.com",
			})
		);

		render(
			<CdfRuntimeProvider>
				<Consumer />
			</CdfRuntimeProvider>
		);

		expect(screen.getByTestId("project")).toHaveTextContent("live");
		expect(screen.getByTestId("cluster")).toHaveTextContent("my");
		expect(screen.getByTestId("has-client")).toHaveTextContent("yes");
	});

	it("throws when the SDK session has no project", () => {
		vi.mocked(useCogniteSdk).mockReturnValue(asClient(undefined));

		expect(() =>
			render(
				<CdfRuntimeProvider>
					<Consumer />
				</CdfRuntimeProvider>
			)
		).toThrow(/CDF session is missing a project/);
	});

	it("throws when the session URL cannot be read", () => {
		vi.mocked(useCogniteSdk).mockReturnValue(
			cdfClientTestDouble({
				project: "live",
				getBaseUrl: () => {
					throw new Error("no url");
				},
			})
		);

		expect(() =>
			render(
				<CdfRuntimeProvider>
					<Consumer />
				</CdfRuntimeProvider>
			)
		).toThrow(/no url/);
	});
});

describe("useCdfRuntime", () => {
	it("throws outside provider", () => {
		function Bad() {
			useCdfRuntime();
			return null;
		}
		expect(() => render(<Bad />)).toThrow(
			/useCdfRuntime must be used within a CdfRuntimeProvider/
		);
	});
});

function asClient(sdk: CogniteClient | undefined): CogniteClient {
	return sdk as CogniteClient;
}
