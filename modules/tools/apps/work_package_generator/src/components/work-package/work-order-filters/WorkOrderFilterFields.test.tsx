import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import type { WorkOrderFilters } from "@/types";
import { WorkOrderFilterFields } from "./WorkOrderFilterFields";

function Harness({ initial = {} }: { initial?: WorkOrderFilters }) {
	const [filters, setFilters] = useState<WorkOrderFilters>(initial);
	return (
		<WorkOrderFilterFields
			filters={filters}
			setFilters={setFilters}
			showFilters
		/>
	);
}

describe("WorkOrderFilterFields", () => {
	it("sets and clears scheduled dates", () => {
		render(<Harness />);
		const from = screen.getByLabelText(
			"Scheduled from (work order filter)"
		) as HTMLInputElement;
		const to = screen.getByLabelText(
			"Scheduled to (work order filter)"
		) as HTMLInputElement;

		fireEvent.change(from, { target: { value: "2024-03-01" } });
		fireEvent.change(to, { target: { value: "2024-03-31" } });
		expect(from.value).toBe("2024-03-01");
		expect(to.value).toBe("2024-03-31");
		expect(from).toHaveAttribute("max", "2024-03-31");
		expect(to).toHaveAttribute("min", "2024-03-01");

		fireEvent.change(from, { target: { value: "" } });
		fireEvent.change(to, { target: { value: "" } });
		expect(from.value).toBe("");
		expect(to.value).toBe("");
	});

	it("sets and clears status and priority", async () => {
		const user = userEvent.setup();
		render(<Harness />);

		const [status, priority] = screen.getAllByRole("combobox");
		await user.click(status);
		await user.click(screen.getByRole("option", { name: "Active" }));
		expect(status).toHaveTextContent("Active");

		await user.click(status);
		await user.click(screen.getByRole("option", { name: "Any status" }));
		expect(status).toHaveTextContent("Any status");

		await user.click(priority);
		await user.click(screen.getByRole("option", { name: "High" }));
		expect(priority).toHaveTextContent("High");

		await user.click(priority);
		await user.click(screen.getByRole("option", { name: "Any priority" }));
		expect(priority).toHaveTextContent("Any priority");
	});

	it("hides the panel when filters are collapsed", () => {
		render(
			<WorkOrderFilterFields
				filters={{}}
				setFilters={() => undefined}
				showFilters={false}
			/>
		);
		expect(screen.getByRole("region", { hidden: true })).toHaveAttribute(
			"hidden"
		);
	});
});
