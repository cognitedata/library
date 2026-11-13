import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import type { WorkOrderFilters } from "@/types";
import {
	WorkOrderFiltersSection,
	type WorkOrderFiltersSectionProps,
} from "./WorkOrderFiltersSection";

function FiltersHarness(overrides: Partial<WorkOrderFiltersSectionProps> = {}) {
	const [filters, setFilters] = useState<WorkOrderFilters>(
		overrides.filters ?? {}
	);
	const [showFilters, setShowFilters] = useState(
		overrides.showFilters ?? false
	);

	return (
		<WorkOrderFiltersSection
			filters={filters}
			setFilters={overrides.setFilters ?? setFilters}
			showFilters={showFilters}
			setShowFilters={overrides.setShowFilters ?? setShowFilters}
			hasActiveFilters={
				overrides.hasActiveFilters ??
				Boolean(
					filters.dateFrom ||
						filters.dateTo ||
						filters.status ||
						filters.priority
				)
			}
			hasSearched={overrides.hasSearched ?? false}
			isFetchingRecent={overrides.isFetchingRecent ?? false}
			onRefetchRecent={overrides.onRefetchRecent ?? vi.fn()}
		/>
	);
}

describe("WorkOrderFiltersSection", () => {
	it("toggles the filters panel via the Filters button", () => {
		render(<FiltersHarness />);
		const trigger = screen.getByRole("button", { name: /Filters/i });
		expect(trigger).toHaveAttribute("aria-expanded", "false");
		expect(
			screen.getByLabelText(/Scheduled from/i).closest("section")
		).toHaveAttribute("hidden");

		fireEvent.click(trigger);
		expect(trigger).toHaveAttribute("aria-expanded", "true");
		expect(
			screen.getByLabelText(/Scheduled from/i).closest("section")
		).not.toHaveAttribute("hidden");
	});

	it("shows active filter count and clears filters", () => {
		const setFilters = vi.fn();
		render(
			<WorkOrderFiltersSection
				filters={{
					dateFrom: "2024-01-01",
					status: "Active",
					priority: "High",
				}}
				setFilters={setFilters}
				showFilters
				setShowFilters={vi.fn()}
				hasActiveFilters
				hasSearched={false}
				isFetchingRecent={false}
				onRefetchRecent={vi.fn()}
			/>
		);
		expect(screen.getByText("3")).toBeInTheDocument();
		fireEvent.click(screen.getByRole("button", { name: /Clear/i }));
		expect(setFilters).toHaveBeenCalledWith({});
	});

	it("hides Clear and count when no filters are active", () => {
		render(
			<WorkOrderFiltersSection
				filters={{}}
				setFilters={vi.fn()}
				showFilters={false}
				setShowFilters={vi.fn()}
				hasActiveFilters={false}
				hasSearched={false}
				isFetchingRecent={false}
				onRefetchRecent={vi.fn()}
			/>
		);
		expect(
			screen.queryByRole("button", { name: /Clear/i })
		).not.toBeInTheDocument();
	});

	it("updates date filters through controlled inputs", () => {
		render(<FiltersHarness showFilters />);

		const from = screen.getByLabelText(
			"Scheduled from (work order filter)"
		) as HTMLInputElement;
		const to = screen.getByLabelText(
			"Scheduled to (work order filter)"
		) as HTMLInputElement;

		fireEvent.input(from, { target: { value: "2024-02-01" } });
		expect(from.value).toBe("2024-02-01");

		fireEvent.input(to, { target: { value: "2024-02-28" } });
		expect(to.value).toBe("2024-02-28");
	});

	it("clears a date filter when the input is emptied", () => {
		render(<FiltersHarness showFilters filters={{ dateFrom: "2024-01-01" }} />);
		const from = screen.getByLabelText(
			"Scheduled from (work order filter)"
		) as HTMLInputElement;
		expect(from.value).toBe("2024-01-01");

		fireEvent.input(from, { target: { value: "" } });
		expect(from.value).toBe("");
	});

	it("shows Refresh when not searched and calls onRefetchRecent", () => {
		const onRefetchRecent = vi.fn();
		render(
			<WorkOrderFiltersSection
				filters={{}}
				setFilters={vi.fn()}
				showFilters={false}
				setShowFilters={vi.fn()}
				hasActiveFilters={false}
				hasSearched={false}
				isFetchingRecent={false}
				onRefetchRecent={onRefetchRecent}
			/>
		);
		fireEvent.click(
			screen.getByRole("button", { name: /Refresh work orders list/i })
		);
		expect(onRefetchRecent).toHaveBeenCalledTimes(1);
	});

	it("disables Refresh and shows busy label while fetching", () => {
		render(
			<WorkOrderFiltersSection
				filters={{}}
				setFilters={vi.fn()}
				showFilters={false}
				setShowFilters={vi.fn()}
				hasActiveFilters={false}
				hasSearched={false}
				isFetchingRecent
				onRefetchRecent={vi.fn()}
			/>
		);
		const refresh = screen.getByRole("button", {
			name: /Refreshing work orders/i,
		});
		expect(refresh).toBeDisabled();
		expect(refresh).toHaveAttribute("aria-busy", "true");
		expect(screen.getByText(/Refreshing…/i)).toBeInTheDocument();
	});

	it("hides Refresh after a search has been performed", () => {
		render(
			<WorkOrderFiltersSection
				filters={{}}
				setFilters={vi.fn()}
				showFilters={false}
				setShowFilters={vi.fn()}
				hasActiveFilters={false}
				hasSearched
				isFetchingRecent={false}
				onRefetchRecent={vi.fn()}
			/>
		);
		expect(
			screen.queryByRole("button", { name: /Refresh work orders list/i })
		).not.toBeInTheDocument();
	});
});
