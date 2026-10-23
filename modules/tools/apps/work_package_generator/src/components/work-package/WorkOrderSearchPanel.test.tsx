import {
	fireEvent,
	render as renderWithoutConfig,
	screen,
} from "@testing-library/react";
import type { ReactElement } from "react";
import { describe, expect, it, vi } from "vitest";
import { AppConfigProvider } from "@/app-config";
import type { WorkOrderFilters } from "@/types";
import { WorkOrderSearchPanel } from "./WorkOrderSearchPanel";

function render(ui: ReactElement) {
	return renderWithoutConfig(ui, { wrapper: AppConfigProvider });
}

const refetchRecent = vi.fn();
const setFilters = vi.fn();

const defaultRecentProps = {
	searchInput: "",
	onSearchInputChange: vi.fn(),
	onSearch: vi.fn(),
	isSearching: false,
	isClientAvailable: true,
	searchResults: [],
	hasSearched: false,
	isConfirming: false,
	onSelect: vi.fn(),
	recentWorkOrders: [] as {
		externalId: string;
		space: string;
		name?: string;
	}[],
	isLoadingRecent: false,
	isFetchingRecent: false,
	recentError: null as Error | null,
	onRefetchRecent: refetchRecent,
	recentHasNextPage: false,
	recentHasPrevPage: false,
	onRecentNextPage: vi.fn(),
	onRecentPrevPage: vi.fn(),
	recentCurrentPage: 1,
	workOrderFilters: {} as WorkOrderFilters,
	onWorkOrderFiltersChange: setFilters,
};

describe("WorkOrderSearchPanel", () => {
	it("exposes date filter inputs with accessible names and filter disclosure", () => {
		render(<WorkOrderSearchPanel {...defaultRecentProps} />);

		const filtersButton = screen.getByRole("button", { name: /^Filters$/ });
		expect(filtersButton).toHaveAttribute("aria-expanded", "false");
		expect(filtersButton).toHaveAttribute(
			"aria-controls",
			"work-order-filters"
		);

		fireEvent.click(filtersButton);

		expect(filtersButton).toHaveAttribute("aria-expanded", "true");
		expect(
			screen.getByRole("region", { name: /^Filters$/ })
		).toBeInTheDocument();

		expect(
			screen.getByLabelText("Scheduled from (work order filter)")
		).toBeInTheDocument();
		expect(
			screen.getByLabelText("Scheduled to (work order filter)")
		).toBeInTheDocument();
	});

	it("shows search error and calls onRetrySearch", () => {
		const onRetrySearch = vi.fn();
		render(
			<WorkOrderSearchPanel
				{...defaultRecentProps}
				searchInput="q"
				hasSearched
				searchErrorMessage="Search failed"
				onRetrySearch={onRetrySearch}
			/>
		);
		expect(screen.getByText("Search failed")).toBeInTheDocument();
		fireEvent.click(screen.getByRole("button", { name: "Retry search" }));
		expect(onRetrySearch).toHaveBeenCalledTimes(1);
	});

	it("shows empty search message when search returns no rows", () => {
		render(
			<WorkOrderSearchPanel
				{...defaultRecentProps}
				hasSearched
				searchResults={[]}
			/>
		);
		expect(
			screen.getByText("No work orders matched your search.")
		).toBeInTheDocument();
	});

	it("shows filtered-empty message when filters active and no recent rows", () => {
		render(
			<WorkOrderSearchPanel
				{...defaultRecentProps}
				workOrderFilters={{ dateFrom: "2024-01-01" }}
			/>
		);
		expect(
			screen.getByText("No work orders match the applied filters.")
		).toBeInTheDocument();
	});

	it("shows recent load error and refetches on retry icon click", () => {
		render(
			<WorkOrderSearchPanel
				{...defaultRecentProps}
				recentError={new Error("network")}
			/>
		);
		expect(
			screen.getByText(/Failed to load recent work orders/i)
		).toBeInTheDocument();
		fireEvent.click(screen.getByLabelText("Retry"));
		expect(refetchRecent).toHaveBeenCalled();
	});

	it("refresh button triggers another recent fetch", () => {
		render(<WorkOrderSearchPanel {...defaultRecentProps} />);
		fireEvent.click(
			screen.getByRole("button", { name: "Refresh work orders list" })
		);
		expect(refetchRecent).toHaveBeenCalled();
	});

	it("selects a work order row when not confirming", () => {
		const onSelect = vi.fn();
		render(
			<WorkOrderSearchPanel
				{...defaultRecentProps}
				onSelect={onSelect}
				recentWorkOrders={[
					{ externalId: "wo-x", space: "s", name: "Row", status: "Active" },
				]}
			/>
		);
		fireEvent.click(screen.getByText("wo-x"));
		expect(onSelect).toHaveBeenCalledWith(
			expect.objectContaining({ externalId: "wo-x" })
		);
	});

	it("does not select while confirming", () => {
		const onSelect = vi.fn();
		render(
			<WorkOrderSearchPanel
				{...defaultRecentProps}
				onSelect={onSelect}
				isConfirming
				recentWorkOrders={[
					{ externalId: "wo-y", space: "s", name: "Row", status: "Active" },
				]}
			/>
		);
		fireEvent.click(screen.getByText("wo-y"));
		expect(onSelect).not.toHaveBeenCalled();
	});

	it("notifies parent when a date filter changes", () => {
		const onWorkOrderFiltersChange = vi.fn();
		render(
			<WorkOrderSearchPanel
				{...defaultRecentProps}
				onWorkOrderFiltersChange={onWorkOrderFiltersChange}
			/>
		);
		fireEvent.click(screen.getByRole("button", { name: /^Filters$/ }));
		fireEvent.change(
			screen.getByLabelText("Scheduled from (work order filter)"),
			{ target: { value: "2024-01-01" } }
		);
		expect(onWorkOrderFiltersChange).toHaveBeenCalledWith(
			expect.objectContaining({ dateFrom: "2024-01-01" })
		);
	});

	it("renders dash for missing status and description", () => {
		render(
			<WorkOrderSearchPanel
				{...defaultRecentProps}
				recentWorkOrders={[{ externalId: "wo-z", space: "s", name: "Minimal" }]}
			/>
		);
		expect(screen.getByText("wo-z")).toBeInTheDocument();
		const dashes = screen.getAllByText("—");
		expect(dashes.length).toBeGreaterThanOrEqual(2);
	});
});
