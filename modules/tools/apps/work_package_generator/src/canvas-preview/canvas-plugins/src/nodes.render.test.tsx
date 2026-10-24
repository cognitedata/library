import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
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

import { ImageNode } from "./image/ImageNode";
import { TableNode } from "./table/TableNode";
import { TimeseriesNode } from "./timeseries/TimeseriesNode";

const props = {
	size: { width: 200, height: 120 },
	onDataChange: vi.fn(),
	isSelected: true,
	isFocused: false,
	canvasScale: 1,
	isCanvasPanning: false,
	readOnly: true,
};

function wrap(children: ReactNode) {
	return (
		<QueryClientProvider client={new QueryClient()}>
			{children}
		</QueryClientProvider>
	);
}

describe("preview node states", () => {
	it("shows an empty timeseries and a chart when points exist", () => {
		const { rerender } = render(
			wrap(
				<TimeseriesNode {...props} data={{ series: [], label: "Pressure" }} />
			)
		);
		expect(screen.getByText("No data available")).toBeInTheDocument();

		rerender(
			wrap(
				<TimeseriesNode
					{...props}
					data={{
						series: [
							{ x: 1, y: 2 },
							{ x: 2, y: 4 },
						],
						label: "Pressure",
					}}
				/>
			)
		);
		expect(screen.getByText("Pressure")).toBeInTheDocument();
	});

	it("renders table rows", () => {
		render(
			wrap(
				<TableNode
					{...props}
					data={{
						title: "Asset",
						columns: [{ key: "name", label: "Name" }],
						rows: [{ name: "Pump" }],
					}}
				/>
			)
		);
		expect(screen.getByText("Asset")).toBeInTheDocument();
		expect(screen.getByText("Pump")).toBeInTheDocument();
	});

	it("shows an image load failure", () => {
		vi.stubGlobal(
			"Image",
			class {
				onload: (() => void) | null = null;
				onerror: (() => void) | null = null;
				naturalWidth = 10;
				naturalHeight = 10;
				set src(_value: string) {
					this.onerror?.();
				}
			}
		);
		render(
			wrap(
				<ImageNode
					{...props}
					data={{ url: "https://files.example.test/missing.png" }}
				/>
			)
		);
		expect(screen.getByText("Failed to load image")).toBeInTheDocument();
	});
});
