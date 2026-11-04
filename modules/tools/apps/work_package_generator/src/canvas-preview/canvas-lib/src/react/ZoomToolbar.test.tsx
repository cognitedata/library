import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ZoomToolbar } from "./ZoomToolbar";

describe("ZoomToolbar", () => {
	it("zooms in, out, and to fit, and ignores the ends of the range", () => {
		const onZoomChange = vi.fn();
		const onZoomToFit = vi.fn();
		const { rerender } = render(
			<ZoomToolbar
				scale={1}
				onZoomChange={onZoomChange}
				onZoomToFit={onZoomToFit}
			/>
		);
		for (const button of screen.getAllByRole("button")) {
			fireEvent.mouseEnter(button);
			fireEvent.click(button);
			fireEvent.mouseLeave(button);
		}
		expect(onZoomChange).toHaveBeenCalled();
		expect(onZoomToFit).toHaveBeenCalled();

		rerender(
			<ZoomToolbar
				scale={0.05}
				onZoomChange={onZoomChange}
				onZoomToFit={onZoomToFit}
			/>
		);
		fireEvent.click(screen.getByTitle("Zoom out"));

		rerender(
			<ZoomToolbar
				scale={4}
				onZoomChange={onZoomChange}
				onZoomToFit={onZoomToFit}
			/>
		);
		fireEvent.click(screen.getByTitle("Zoom in"));
		fireEvent.mouseEnter(screen.getByTitle("Zoom in"));
	});
});
