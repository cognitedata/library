import { describe, expect, it } from "vitest";
import { renderPromptTemplate } from "./prompt-template";

describe("renderPromptTemplate", () => {
	it("replaces every named placeholder", () => {
		expect(renderPromptTemplate("Hello {{name}}", { name: "Ada" })).toBe(
			"Hello Ada"
		);
	});

	it("leaves unknown tokens in place", () => {
		expect(renderPromptTemplate("{{missing}}", {})).toBe("{{missing}}");
	});
});
