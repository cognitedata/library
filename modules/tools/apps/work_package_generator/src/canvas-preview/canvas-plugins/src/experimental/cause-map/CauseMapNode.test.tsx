import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { CauseMapNode } from "./CauseMapNode";

const props = {
	size: { width: 160, height: 80 },
	onDataChange: vi.fn(),
	isSelected: true,
	isFocused: true,
	canvasScale: 1,
	isCanvasPanning: false,
	readOnly: false,
};

describe("CauseMapNode", () => {
	it("shows each status and edits the text", () => {
		const onDataChange = vi.fn();
		const { rerender } = render(
			<CauseMapNode
				{...props}
				onDataChange={onDataChange}
				data={{ text: "", status: "pending" }}
			/>
		);
		const field = screen.getByRole("textbox");
		fireEvent.change(field, { target: { value: "Leak" } });
		expect(onDataChange).toHaveBeenCalledWith(
			expect.objectContaining({ text: "Leak" })
		);
		fireEvent.keyDown(field, { key: "Escape" });
		fireEvent.blur(field);

		for (const status of ["accepted", "rejected", "possible"] as const) {
			rerender(
				<CauseMapNode
					{...props}
					readOnly
					data={{ text: status, status, backgroundColor: "#fff" }}
				/>
			);
			expect(screen.getByText(status)).toBeInTheDocument();
		}
	});
});
