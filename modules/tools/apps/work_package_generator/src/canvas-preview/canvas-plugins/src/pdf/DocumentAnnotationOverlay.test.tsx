import type { DocumentAnnotation } from "@dune-canvas/canvas-lib";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { DocumentAnnotationOverlay } from "./DocumentAnnotationOverlay";

function annotation(
	overrides: Partial<DocumentAnnotation> = {}
): DocumentAnnotation {
	return {
		id: "a1",
		resourceType: "asset",
		x: 0.1,
		y: 0.2,
		width: 0.3,
		height: 0.1,
		page: 1,
		text: "P-100",
		...overrides,
	};
}

const size = {
	documentWidth: 200,
	documentHeight: 100,
	containerWidth: 100,
	containerHeight: 200,
};

describe("DocumentAnnotationOverlay", () => {
	it("draws a titled box and reports a click", () => {
		const onAnnotationClick = vi.fn();
		render(
			<DocumentAnnotationOverlay
				annotations={[annotation()]}
				{...size}
				rotation={0}
				onAnnotationClick={onAnnotationClick}
			/>
		);
		expect(screen.getByText("P-100")).toBeInTheDocument();
		const box = document.querySelector("rect");
		if (!(box instanceof SVGRectElement)) throw new Error("rect missing");
		fireEvent.mouseEnter(box);
		fireEvent.click(box);
		fireEvent.mouseLeave(box);
		expect(onAnnotationClick).toHaveBeenCalledWith(
			expect.objectContaining({ id: "a1" })
		);
	});

	it("places boxes for each rotation and a wide page", () => {
		for (const rotation of [90, 180, 270, 0]) {
			const { unmount } = render(
				<DocumentAnnotationOverlay
					annotations={[
						annotation({ id: `r${rotation}`, resourceType: "file" }),
					]}
					documentWidth={100}
					documentHeight={200}
					containerWidth={200}
					containerHeight={100}
					rotation={rotation}
				/>
			);
			expect(document.querySelector("rect")).toBeTruthy();
			unmount();
		}
	});

	it("renders nothing when there are no annotations", () => {
		const { container } = render(
			<DocumentAnnotationOverlay annotations={[]} {...size} />
		);
		expect(container.querySelector("svg")).toBeNull();
	});
});
