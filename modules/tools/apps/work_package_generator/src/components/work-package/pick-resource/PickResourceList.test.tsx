import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ResourceInstance } from "@/types";
import { PickResourceList } from "./PickResourceList";

const items: ResourceInstance[] = [
	{
		externalId: "same",
		space: "s1",
		name: "Pump s1",
		properties: {},
	},
	{
		externalId: "same",
		space: "s2",
		name: "Pump s2",
		properties: {},
	},
];

describe("PickResourceList", () => {
	it("C29: lists both resources when externalId is shared across spaces", () => {
		render(
			<PickResourceList
				candidates={items}
				filterText=""
				onFilterTextChange={vi.fn()}
				currentSelection={null}
				onSelect={vi.fn()}
			/>
		);
		expect(screen.getByText("Pump s1")).toBeInTheDocument();
		expect(screen.getByText("Pump s2")).toBeInTheDocument();
		expect(screen.getAllByRole("button", { name: "Select" })).toHaveLength(2);
	});
});
