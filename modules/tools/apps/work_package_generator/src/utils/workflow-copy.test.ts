import { describe, expect, it } from "vitest";
import { SEARCH_WORK_ORDER_TO_BEGIN } from "./workflow-copy";

describe("workflow copy", () => {
	it("exports the shared search prompt", () => {
		expect(SEARCH_WORK_ORDER_TO_BEGIN).toMatch(/work order/i);
	});
});
