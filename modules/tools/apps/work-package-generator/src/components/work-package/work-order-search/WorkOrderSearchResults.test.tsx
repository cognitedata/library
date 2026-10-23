import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { WorkOrder } from "@/types";
import {
	buildWorkOrderPagination,
	WorkOrderSearchResults,
} from "./WorkOrderSearchResults";

const row: WorkOrder = {
	externalId: "wo-1",
	space: "s",
	name: "Pump",
};

const pagination = {
	hasNextPage: false,
	hasPrevPage: false,
	onNextPage: vi.fn(),
	onPrevPage: vi.fn(),
	currentPage: 1,
	pageSize: 20,
	isLoading: false,
};

describe("buildWorkOrderPagination", () => {
	const input = {
		hasSearched: false,
		searchResultLimit: 25,
		searchHasNextPage: true,
		searchHasPrevPage: true,
		onSearchNextPage: vi.fn(),
		onSearchPrevPage: vi.fn(),
		searchCurrentPage: 2,
		isSearchFetching: true,
		recentHasNextPage: true,
		recentHasPrevPage: false,
		onRecentNextPage: vi.fn(),
		onRecentPrevPage: vi.fn(),
		recentCurrentPage: 3,
		isFetchingRecent: true,
	};

	it("uses search paging after a search", () => {
		const paging = buildWorkOrderPagination({ ...input, hasSearched: true });
		expect(paging.currentPage).toBe(2);
		expect(paging.pageSize).toBe(25);
		expect(paging.isLoading).toBe(true);
		expect(paging.onNextPage).toBe(input.onSearchNextPage);
	});

	it("falls back to no-op handlers when search paging is omitted", () => {
		const paging = buildWorkOrderPagination({
			...input,
			hasSearched: true,
			onSearchNextPage: undefined,
			onSearchPrevPage: undefined,
		});
		expect(() => paging.onNextPage()).not.toThrow();
		expect(() => paging.onPrevPage()).not.toThrow();
	});

	it("uses recent paging and only marks later pages as loading", () => {
		const paging = buildWorkOrderPagination(input);
		expect(paging.currentPage).toBe(3);
		expect(paging.isLoading).toBe(true);
		expect(paging.onNextPage).toBe(input.onRecentNextPage);

		const firstPage = buildWorkOrderPagination({
			...input,
			recentCurrentPage: 1,
		});
		expect(firstPage.isLoading).toBe(false);
	});
});

describe("WorkOrderSearchResults", () => {
	it("shows a searching label", () => {
		render(
			<WorkOrderSearchResults
				hasSearched
				isLoadingDisplay
				searchErrorMessage={null}
				recentError={null}
				onRefetchRecent={vi.fn()}
				displayRows={[]}
				isConfirming={false}
				onSelect={vi.fn()}
				hasActiveFilters={false}
				pagination={pagination}
			/>
		);
		expect(screen.getByText("Searching...")).toBeInTheDocument();
	});

	it("shows a recent-orders loading label", () => {
		render(
			<WorkOrderSearchResults
				hasSearched={false}
				isLoadingDisplay
				searchErrorMessage={null}
				recentError={null}
				onRefetchRecent={vi.fn()}
				displayRows={[]}
				isConfirming={false}
				onSelect={vi.fn()}
				hasActiveFilters={false}
				pagination={pagination}
			/>
		);
		expect(
			screen.getByText("Loading recent work orders...")
		).toBeInTheDocument();
	});

	it("retries a failed search when a handler is provided", () => {
		const onRetrySearch = vi.fn();
		render(
			<WorkOrderSearchResults
				hasSearched
				isLoadingDisplay={false}
				searchErrorMessage="Search failed"
				onRetrySearch={onRetrySearch}
				recentError={null}
				onRefetchRecent={vi.fn()}
				displayRows={[]}
				isConfirming={false}
				onSelect={vi.fn()}
				hasActiveFilters={false}
				pagination={pagination}
			/>
		);
		fireEvent.click(screen.getByRole("button", { name: "Retry search" }));
		expect(onRetrySearch).toHaveBeenCalled();
	});

	it("shows a search error without a retry button when retry is omitted", () => {
		render(
			<WorkOrderSearchResults
				hasSearched
				isLoadingDisplay={false}
				searchErrorMessage="Search failed"
				recentError={null}
				onRefetchRecent={vi.fn()}
				displayRows={[]}
				isConfirming={false}
				onSelect={vi.fn()}
				hasActiveFilters={false}
				pagination={pagination}
			/>
		);
		expect(screen.getByText("Search failed")).toBeInTheDocument();
		expect(
			screen.queryByRole("button", { name: "Retry search" })
		).not.toBeInTheDocument();
	});

	it("retries loading recent work orders", () => {
		const onRefetchRecent = vi.fn();
		render(
			<WorkOrderSearchResults
				hasSearched={false}
				isLoadingDisplay={false}
				searchErrorMessage={null}
				recentError={new Error("down")}
				onRefetchRecent={onRefetchRecent}
				displayRows={[]}
				isConfirming={false}
				onSelect={vi.fn()}
				hasActiveFilters={false}
				pagination={pagination}
			/>
		);
		fireEvent.click(screen.getByLabelText("Retry"));
		expect(onRefetchRecent).toHaveBeenCalled();
	});

	it("shows the empty search message", () => {
		render(
			<WorkOrderSearchResults
				hasSearched
				isLoadingDisplay={false}
				searchErrorMessage={null}
				recentError={null}
				onRefetchRecent={vi.fn()}
				displayRows={[]}
				isConfirming={false}
				onSelect={vi.fn()}
				hasActiveFilters={false}
				pagination={pagination}
			/>
		);
		expect(
			screen.getByText("No work orders matched your search.")
		).toBeInTheDocument();
	});

	it("shows the filtered empty message", () => {
		render(
			<WorkOrderSearchResults
				hasSearched={false}
				isLoadingDisplay={false}
				searchErrorMessage={null}
				recentError={null}
				onRefetchRecent={vi.fn()}
				displayRows={[]}
				isConfirming={false}
				onSelect={vi.fn()}
				hasActiveFilters
				pagination={pagination}
			/>
		);
		expect(
			screen.getByText("No work orders match the applied filters.")
		).toBeInTheDocument();
	});

	it("shows recent rows and selects one", () => {
		const onSelect = vi.fn();
		render(
			<WorkOrderSearchResults
				hasSearched={false}
				isLoadingDisplay={false}
				searchErrorMessage={null}
				recentError={null}
				onRefetchRecent={vi.fn()}
				displayRows={[row]}
				isConfirming={false}
				onSelect={onSelect}
				hasActiveFilters={false}
				pagination={{ ...pagination, hasNextPage: true }}
			/>
		);
		fireEvent.click(screen.getByText("Pump"));
		expect(onSelect).toHaveBeenCalledWith(row);
	});
});
