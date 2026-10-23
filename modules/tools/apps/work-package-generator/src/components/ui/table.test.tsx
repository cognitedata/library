import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
	Table,
	TableBody,
	TableCaption,
	TableCell,
	TableFooter,
	TableHead,
	TableHeader,
	TableRow,
} from "./table";

describe("Table", () => {
	it("renders header, body, footer, and caption", () => {
		render(
			<Table className="extra">
				<TableCaption>Caption</TableCaption>
				<TableHeader>
					<TableRow>
						<TableHead>Name</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					<TableRow data-state="selected">
						<TableCell>Pump</TableCell>
					</TableRow>
				</TableBody>
				<TableFooter>
					<TableRow>
						<TableCell>1</TableCell>
					</TableRow>
				</TableFooter>
			</Table>
		);
		expect(screen.getByText("Name")).toBeInTheDocument();
		expect(screen.getByText("Pump")).toBeInTheDocument();
		expect(screen.getByText("Caption")).toBeInTheDocument();
		expect(screen.getByText("1")).toBeInTheDocument();
		expect(document.querySelector("[data-slot=table]")).toHaveClass("extra");
	});
});
