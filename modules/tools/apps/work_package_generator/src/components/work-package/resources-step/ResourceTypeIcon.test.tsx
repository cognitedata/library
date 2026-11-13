import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ResourceTypeIcon } from "./ResourceTypeIcon";

describe("ResourceTypeIcon", () => {
	it("renders a distinct icon for known resource types and a fallback", () => {
		const { container, rerender } = render(<ResourceTypeIcon typeKey="file" />);
		expect(container.querySelector("svg")).toBeTruthy();

		for (const typeKey of [
			"timeseries",
			"activity",
			"asset",
			"equipment",
			"unknown",
		]) {
			rerender(<ResourceTypeIcon typeKey={typeKey} />);
			expect(container.querySelector("svg")).toBeTruthy();
		}
	});
});
