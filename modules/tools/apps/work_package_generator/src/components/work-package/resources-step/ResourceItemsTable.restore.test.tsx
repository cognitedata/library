import {
	fireEvent,
	render as renderWithoutConfig,
	screen,
} from "@testing-library/react";
import type { ReactElement } from "react";
import { beforeEach, describe, expect, it } from "vitest";
import { AppConfigProvider } from "@/app-config";
import { useWorkPackageStore } from "@/store";
import type { ResourceInstance } from "@/types";
import { ResourceItemsTable } from "./ResourceItemsTable";

function render(ui: ReactElement) {
	return renderWithoutConfig(ui, { wrapper: AppConfigProvider });
}

const sharedId = "same";

const items: ResourceInstance[] = [
	{
		externalId: sharedId,
		space: "s1",
		name: "In space one",
		properties: { name: "In space one" },
	},
	{
		externalId: sharedId,
		space: "s2",
		name: "In space two",
		properties: { name: "In space two" },
	},
];

describe("ResourceItemsTable", () => {
	beforeEach(() => {
		useWorkPackageStore.setState({ removedCandidateIdsByType: {} });
	});

	it("C29: renders both rows when externalId is shared across spaces", () => {
		render(
			<ResourceItemsTable
				items={items}
				extraProperties={[]}
				displayName="Files"
			/>
		);
		expect(screen.getByText("In space one")).toBeInTheDocument();
		expect(screen.getByText("In space two")).toBeInTheDocument();
		expect(screen.getAllByText(sharedId)).toHaveLength(2);
	});

	it("N14: offers Restore removed (n) and clears stored removals", () => {
		useWorkPackageStore.setState({
			removedCandidateIdsByType: { file: ["a", "b"] },
		});
		render(
			<ResourceItemsTable
				items={items.slice(0, 1)}
				extraProperties={[]}
				displayName="Files"
			/>
		);
		fireEvent.click(
			screen.getByRole("button", { name: "Restore removed (2)" })
		);
		expect(useWorkPackageStore.getState().removedCandidateIdsByType).toEqual(
			{}
		);
		expect(
			screen.queryByRole("button", { name: /Restore removed/ })
		).not.toBeInTheDocument();
	});
});
