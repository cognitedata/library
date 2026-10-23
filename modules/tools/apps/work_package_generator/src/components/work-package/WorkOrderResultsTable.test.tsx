import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { WorkOrder } from "@/types";
import { WorkOrderResultsTable } from "./WorkOrderResultsTable";

const rows: WorkOrder[] = [
	{
		externalId: "wo-1",
		space: "s",
		name: "Pump WO",
		status: "open",
		scheduledStartTime: "2024-01-15T00:00:00Z",
		description: "Inspect pump",
	},
	{
		externalId: "wo-2",
		space: "s",
	},
];

describe("WorkOrderResultsTable", () => {
	it("shows the empty message when there are no rows", () => {
		render(
			<WorkOrderResultsTable
				rows={[]}
				isConfirming={false}
				onSelect={vi.fn()}
				emptyMessage="Nothing here"
			/>
		);
		expect(screen.getByText("Nothing here")).toBeInTheDocument();
	});

	it("renders rows and selects on click", () => {
		const onSelect = vi.fn();
		render(
			<WorkOrderResultsTable
				rows={rows}
				isConfirming={false}
				onSelect={onSelect}
			/>
		);
		expect(screen.getByText("wo-1")).toBeInTheDocument();
		expect(screen.getByText("Pump WO")).toBeInTheDocument();
		expect(screen.getByText("open")).toBeInTheDocument();
		expect(screen.getByText("Inspect pump")).toBeInTheDocument();
		expect(screen.getAllByText("—").length).toBeGreaterThan(0);

		fireEvent.click(screen.getByText("wo-1"));
		expect(onSelect).toHaveBeenCalledWith(rows[0]);
	});

	it("does not select while confirming", () => {
		const onSelect = vi.fn();
		render(
			<WorkOrderResultsTable rows={rows} isConfirming onSelect={onSelect} />
		);
		fireEvent.click(screen.getByText("wo-1"));
		expect(onSelect).not.toHaveBeenCalled();
	});

	it("paginates and reports the visible range", () => {
		const onNextPage = vi.fn();
		const onPrevPage = vi.fn();
		render(
			<WorkOrderResultsTable
				rows={rows}
				isConfirming={false}
				onSelect={vi.fn()}
				pagination={{
					hasNextPage: true,
					hasPrevPage: true,
					onNextPage,
					onPrevPage,
					currentPage: 2,
					pageSize: 10,
				}}
			/>
		);
		expect(screen.getByText("Showing 11–12")).toBeInTheDocument();
		expect(screen.getByText("Page 2")).toBeInTheDocument();
		fireEvent.click(screen.getByRole("button", { name: "Next page" }));
		fireEvent.click(screen.getByRole("button", { name: "Previous page" }));
		expect(onNextPage).toHaveBeenCalledTimes(1);
		expect(onPrevPage).toHaveBeenCalledTimes(1);
	});

	it("shows a loading footer and disables page buttons", () => {
		render(
			<WorkOrderResultsTable
				rows={[]}
				isConfirming={false}
				onSelect={vi.fn()}
				pagination={{
					hasNextPage: true,
					hasPrevPage: false,
					onNextPage: vi.fn(),
					onPrevPage: vi.fn(),
					currentPage: 1,
					pageSize: 10,
					isLoading: true,
				}}
			/>
		);
		expect(screen.getByText("Loading...")).toBeInTheDocument();
		expect(screen.getByRole("button", { name: "Next page" })).toBeDisabled();
		expect(
			screen.getByRole("button", { name: "Previous page" })
		).toBeDisabled();
	});
});
