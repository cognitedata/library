import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
	getCandidatesForSlotType,
	SlotIcon,
	slotCategoryLabel,
} from "./slot-helpers";

describe("getCandidatesForSlotType", () => {
	it("returns candidates for the slot type key", () => {
		const pool = {
			file: [{ externalId: "f1", space: "s", properties: {} }],
		};
		expect(getCandidatesForSlotType("file", pool)).toHaveLength(1);
		expect(getCandidatesForSlotType("timeseries", pool)).toEqual([]);
	});
});

describe("slotCategoryLabel", () => {
	it("maps categories", () => {
		expect(slotCategoryLabel("asset")).toBe("Asset");
		expect(slotCategoryLabel("workorder")).toBe("Work Order");
		expect(slotCategoryLabel("activity")).toBe("Activity");
		expect(slotCategoryLabel("equipment")).toBe("Equipment");
		expect(slotCategoryLabel("file")).toBe("File");
		expect(slotCategoryLabel("timeseries")).toBe("Time Series");
		expect(slotCategoryLabel("other")).toBe("Other");
	});
});

describe("SlotIcon", () => {
	it("renders per category", () => {
		const { container, rerender } = render(<SlotIcon category="asset" />);
		expect(container.querySelector("svg")).toBeTruthy();
		rerender(<SlotIcon category="workorder" />);
		rerender(<SlotIcon category="activity" />);
		rerender(<SlotIcon category="equipment" />);
		rerender(<SlotIcon category="file" />);
		rerender(<SlotIcon category="timeseries" />);
		rerender(<SlotIcon category="other" />);
	});
});
