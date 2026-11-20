import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";

// uplot touches canvas APIs that jsdom does not implement.
vi.mock("uplot", () => {
	class FakeUPlot {
		setData(): void {}
		setSize(): void {}
		destroy(): void {}
	}
	return { default: FakeUPlot };
});

import { CommentNode } from "./comment/CommentNode";
import { CauseMapNode } from "./experimental/cause-map/CauseMapNode";
import { ImageNode } from "./image/ImageNode";
import { ImageAnnotationNode } from "./image-annotation/ImageAnnotationNode";
import { LabelNode } from "./label/LabelNode";
import { PolylineNode } from "./polyline/PolylineNode";
import { ShapeNode } from "./shape/ShapeNode";
import { StickyNode } from "./sticky/StickyNode";
import { SvgNode } from "./svg/SvgNode";
import { TableNode } from "./table/TableNode";
import { TextNode } from "./text/TextNode";
import { TimeseriesNode } from "./timeseries/TimeseriesNode";
import { TimeseriesPlaceholder } from "./timeseries/TimeseriesPlaceholder";

const props = {
	size: { width: 200, height: 120 },
	onDataChange: vi.fn(),
	isSelected: true,
	isFocused: true,
	canvasScale: 1,
	isCanvasPanning: false,
	readOnly: false,
};

function wrap(children: ReactNode) {
	return (
		<QueryClientProvider client={new QueryClient()}>
			{children}
		</QueryClientProvider>
	);
}

describe("node state coverage", () => {
	it("renders text, sticky, label, comment, and shape variants", () => {
		const onDataChange = vi.fn();
		render(
			wrap(
				<>
					<TextNode
						{...props}
						onDataChange={onDataChange}
						data={{ content: "Hello", fontWeight: "bold", textAlign: "center" }}
					/>
					<StickyNode
						{...props}
						data={{ content: "Note", backgroundColor: "#fff59d" }}
					/>
					<LabelNode {...props} data={{ text: "Tag", fontWeight: "bold" }} />
					<CommentNode
						{...props}
						data={{ text: "Look here", author: "Sam", resolved: true }}
					/>
					<ShapeNode {...props} data={{ shape: "rectangle", fill: "#eee" }} />
					<ShapeNode {...props} data={{ shape: "ellipse" }} />
					<ShapeNode
						{...props}
						data={{
							shape: "line",
							points: [
								{ x: 0, y: 0 },
								{ x: 10, y: 10 },
							],
						}}
					/>
					<SvgNode
						{...props}
						data={{ paths: [{ d: "M0 0 L10 10", stroke: "#000" }] }}
					/>
					<PolylineNode
						{...props}
						data={{
							points: [
								{ x: 0, y: 0 },
								{ x: 40, y: 10 },
								{ x: 80, y: 0 },
							],
							startEndType: "arrow",
							endEndType: "arrow",
						}}
					/>
					<PolylineNode {...props} data={{ points: [{ x: 0, y: 0 }] }} />
				</>
			)
		);
		expect(screen.getByText("Hello")).toBeInTheDocument();
		expect(screen.getByText("Insufficient points")).toBeInTheDocument();
		fireEvent.doubleClick(screen.getByText("Hello"));
	});

	it("renders an image, an annotation stamp, and a timeseries placeholder", () => {
		vi.stubGlobal(
			"Image",
			class {
				onload: (() => void) | null = null;
				onerror: (() => void) | null = null;
				naturalWidth = 20;
				naturalHeight = 10;
				set src(_value: string) {
					this.onload?.();
				}
			}
		);
		render(
			wrap(
				<>
					<ImageNode
						{...props}
						data={{ url: "https://files.example.test/ok.png" }}
					/>
					<ImageAnnotationNode
						{...props}
						data={{ url: "https://files.example.test/stamp.png", opacity: 0.5 }}
					/>
					<ImageAnnotationNode {...props} data={{ url: "" }} />
					<TimeseriesPlaceholder
						size={{ width: 200, height: 80 }}
						data={{
							series: [
								{ x: 1, y: 2 },
								{ x: 2, y: 4 },
							],
							label: "Pressure",
						}}
					/>
					<CauseMapNode
						{...props}
						readOnly
						data={{ text: "Cause", status: "pending" }}
					/>
				</>
			)
		);
		expect(screen.getByText("No image URL provided")).toBeInTheDocument();
	});

	it("activates a timeseries chart and renders table property rows", () => {
		render(
			wrap(
				<>
					<TimeseriesNode
						{...props}
						data={{
							series: [
								{ x: 1, y: 2, min: 1, max: 3, count: 4 },
								{ x: 2, y: 5 },
							],
							label: "Flow",
						}}
					/>
					<TableNode
						{...props}
						data={{
							title: "Pump",
							columns: [
								{ key: "name", label: "Name" },
								{ key: "tags", label: "Tags" },
							],
							rows: [{ name: "P-100", tags: "hot" }],
						}}
					/>
				</>
			)
		);
		expect(screen.getByText("Flow")).toBeInTheDocument();
		expect(screen.getByText("P-100")).toBeInTheDocument();
		fireEvent.mouseDown(screen.getByText("Flow"));
		fireEvent.keyDown(window, { key: "Escape" });
	});
});
