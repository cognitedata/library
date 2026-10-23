import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { WorkOrderFields } from "./WorkOrderFields";

describe("WorkOrderFields", () => {
	it("renders the work order name and external id", () => {
		render(
			<WorkOrderFields
				workOrder={{
					externalId: "wo-1",
					space: "s",
					name: "Inspect pump",
					status: "Open",
				}}
			/>
		);
		expect(screen.getByText("Inspect pump")).toBeInTheDocument();
		expect(screen.getByText("wo-1")).toBeInTheDocument();
		expect(screen.getByText("Open")).toBeInTheDocument();
	});
});
