import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { WorkOrderFilterToolbar } from "./WorkOrderFilterToolbar";

const defaults = {
	showFilters: false,
	setShowFilters: vi.fn(),
	hasActiveFilters: false,
	activeFiltersCount: 0,
	hasSearched: false,
	isFetchingRecent: false,
	onRefetchRecent: vi.fn(),
	onClearFilters: vi.fn(),
};

describe("WorkOrderFilterToolbar", () => {
	it("N05: keeps Refresh visible after a search", () => {
		const onRefetchRecent = vi.fn();
		render(
			<WorkOrderFilterToolbar
				{...defaults}
				hasSearched
				onRefetchRecent={onRefetchRecent}
			/>
		);

		fireEvent.click(
			screen.getByRole("button", { name: "Refresh work orders list" })
		);
		expect(onRefetchRecent).toHaveBeenCalledTimes(1);
	});
});
