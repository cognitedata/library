import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { createMockAppConfig } from "@/__mocks__/app-config";
import { AppConfigProvider, useAppConfig } from "./context";

function OrgLabel() {
	const config = useAppConfig();
	return <span>{config.ui.terms.workPackage}</span>;
}

describe("AppConfigProvider", () => {
	it("serves the shipped config by default", () => {
		render(
			<AppConfigProvider>
				<OrgLabel />
			</AppConfigProvider>
		);
		expect(
			screen.getByText(createMockAppConfig().ui.terms.workPackage)
		).toBeInTheDocument();
	});

	it("serves an override when one is provided", () => {
		const value = createMockAppConfig();
		value.ui.terms.workPackage = "override-package";
		render(
			<AppConfigProvider value={value}>
				<OrgLabel />
			</AppConfigProvider>
		);
		expect(screen.getByText("override-package")).toBeInTheDocument();
	});
});
