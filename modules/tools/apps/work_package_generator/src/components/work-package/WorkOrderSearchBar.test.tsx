import { fireEvent, render, screen } from "@testing-library/react";
import type { FormEvent } from "react";
import { describe, expect, it, vi } from "vitest";
import { WorkOrderSearchBar } from "./WorkOrderSearchBar";

describe("WorkOrderSearchBar", () => {
	it("submits search and wires input onChange", () => {
		const onSearch = vi.fn((e: FormEvent) => e.preventDefault());
		const onChange = vi.fn();
		render(
			<WorkOrderSearchBar
				searchInput="abc"
				onSearchInputChange={onChange}
				onSearch={onSearch}
				isSearching={false}
				isClientAvailable
			/>
		);
		fireEvent.change(
			screen.getByRole("textbox", { name: "Search work orders" }),
			{
				target: { value: "x" },
			}
		);
		expect(onChange).toHaveBeenCalledWith("x");
		const form = screen.getByRole("textbox").closest("form");
		expect(form).not.toBeNull();
		fireEvent.submit(form as HTMLElement);
		expect(onSearch).toHaveBeenCalled();
	});

	it("shows searching state on narrow submit button", () => {
		render(
			<WorkOrderSearchBar
				searchInput="q"
				onSearchInputChange={vi.fn()}
				onSearch={vi.fn()}
				isSearching
				isClientAvailable
			/>
		);
		expect(screen.getAllByText(/Searching/i).length).toBeGreaterThanOrEqual(1);
	});
});
