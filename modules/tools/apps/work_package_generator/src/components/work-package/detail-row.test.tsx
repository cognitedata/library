import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { BadgeRow, Row, statusVariant } from "./detail-row";

describe("statusVariant", () => {
	it("maps known status groups", () => {
		expect(statusVariant("Active")).toBe("inProgress");
		expect(statusVariant("Completed")).toBe("secondary");
		expect(statusVariant("Critical")).toBe("error");
		expect(statusVariant("Other")).toBe("gray");
	});

	it("treats undefined as gray", () => {
		expect(statusVariant(undefined)).toBe("gray");
	});
});

describe("Row", () => {
	it("renders when value present", () => {
		render(
			<dl>
				<Row label="L" value="v" />
			</dl>
		);
		expect(screen.getByText("L")).toBeInTheDocument();
		expect(screen.getByText("v")).toBeInTheDocument();
	});

	it("returns null for empty value", () => {
		const { container } = render(
			<dl>
				<Row label="L" value="" />
			</dl>
		);
		expect(container.querySelector("dt")).toBeNull();
	});
});

describe("BadgeRow", () => {
	it("renders badge when value set", () => {
		render(
			<dl>
				<BadgeRow label="Status" value="open" />
			</dl>
		);
		expect(screen.getByText("open")).toBeInTheDocument();
	});

	it("returns null without value", () => {
		const { container } = render(
			<dl>
				<BadgeRow label="Status" />
			</dl>
		);
		expect(container.querySelector("dt")).toBeNull();
	});
});
