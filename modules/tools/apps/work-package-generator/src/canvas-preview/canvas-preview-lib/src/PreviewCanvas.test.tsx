import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { PreviewCanvas } from "./PreviewCanvas";
import type { CanvasNodeDef } from "./types";

function node(
	id: string,
	type: string,
	data: unknown,
	position = { x: 20, y: 30 }
): CanvasNodeDef {
	return {
		id,
		type,
		position,
		size: { width: 160, height: 80 },
		zIndex: 1,
		data,
	} as CanvasNodeDef;
}

describe("PreviewCanvas", () => {
	beforeEach(() => {
		vi.stubGlobal(
			"ResizeObserver",
			class {
				observe(): void {}
				unobserve(): void {}
				disconnect(): void {}
			}
		);
		vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockReturnValue({
			x: 0,
			y: 0,
			width: 1200,
			height: 800,
			top: 0,
			left: 0,
			bottom: 800,
			right: 1200,
			toJSON: () => ({}),
		});
	});

	it("places preview nodes and zooms from the toolbar", () => {
		const { container } = render(
			<PreviewCanvas
				showZoomToolbar
				fitOnLoad
				nodes={[
					node("text", "text", { content: "Hello" }),
					node("sticky", "sticky", { content: "Note" }, { x: 200, y: 40 }),
					node("line", "polyline", {
						points: [
							{ x: 0, y: 0 },
							{ x: 40, y: 20 },
						],
					}),
					node("empty-line", "polyline", { points: [{ x: 0, y: 0 }] }),
					node("table", "table", {
						title: "Asset",
						columns: [{ key: "name", label: "Name" }],
						rows: [{ name: "Pump" }],
					}),
					node("image", "image", {
						url: "https://files.example.test/missing.png",
					}),
					node("label", "label", { text: "Label" }),
					node("comment", "comment", { content: "Comment", author: "Sam" }),
					node("shape", "shape", { shape: "rectangle" }),
				]}
				connections={[{ id: "c1", from: "text", to: "sticky" }]}
			/>
		);

		expect(screen.getByText("Hello")).toBeInTheDocument();
		expect(screen.getByText("Note")).toBeInTheDocument();
		expect(screen.getByText("Insufficient points")).toBeInTheDocument();

		fireEvent.click(screen.getByTitle("Zoom in"));
		fireEvent.click(screen.getByTitle("Zoom out"));
		fireEvent.click(screen.getByTitle("Zoom to fit (fit all content)"));

		const surface = container.querySelector("div");
		expect(surface).toBeTruthy();
		if (surface) {
			fireEvent.wheel(surface, { deltaY: -100, clientX: 40, clientY: 40 });
			fireEvent.pointerDown(surface, { button: 1, clientX: 10, clientY: 10 });
			fireEvent.pointerMove(surface, { clientX: 30, clientY: 20 });
			fireEvent.pointerUp(surface, { clientX: 30, clientY: 20 });
		}
	});
});
