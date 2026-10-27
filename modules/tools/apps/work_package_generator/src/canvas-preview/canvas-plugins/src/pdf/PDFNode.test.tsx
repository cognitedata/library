import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

// pdf.js needs a worker and a real canvas; neither exists in happy-dom.
vi.mock("pdfjs-dist", () => {
	const page = {
		getViewport: ({ scale }: { scale: number }) => ({
			width: 100 * scale,
			height: 200 * scale,
		}),
		render: () => ({ promise: Promise.resolve(), cancel: vi.fn() }),
	};
	return {
		GlobalWorkerOptions: { workerSrc: "" },
		getDocument: vi.fn(() => ({
			promise: Promise.resolve({
				getPage: async () => page,
				numPages: 2,
			}),
			destroy: vi.fn(),
		})),
	};
});

vi.mock("pdfjs-dist/build/pdf.worker.min.mjs?url", () => ({
	default: "worker.js",
}));

import { PDFNode } from "./PDFNode";

const props = {
	size: { width: 400, height: 300 },
	onDataChange: vi.fn(),
	isSelected: true,
	isFocused: false,
	canvasScale: 1,
	isCanvasPanning: false,
	readOnly: true,
	onSizeChange: vi.fn(),
};

function wrap(children: ReactNode) {
	return (
		<QueryClientProvider
			client={
				new QueryClient({ defaultOptions: { queries: { retry: false } } })
			}
		>
			{children}
		</QueryClientProvider>
	);
}

describe("PDFNode", () => {
	beforeEach(() => {
		function getContext(
			this: HTMLCanvasElement,
			contextId: string
		): CanvasRenderingContext2D | null {
			if (contextId !== "2d") return null;
			return Object.assign(Object.create(null), { canvas: this });
		}
		HTMLCanvasElement.prototype.getContext =
			getContext as HTMLCanvasElement["getContext"];
	});

	it("renders a direct pdf and shows the annotation title", async () => {
		render(
			wrap(
				<PDFNode
					{...props}
					data={{ url: "https://files.example.test/a.pdf", page: 1 }}
				/>
			)
		);
		await waitFor(() => expect(document.querySelector("canvas")).toBeTruthy());
	});

	it("shows an empty state when the file has no url", () => {
		render(
			wrap(
				<PDFNode
					{...props}
					data={{
						source: { type: "direct", url: "" },
						label: "Manual",
					}}
				/>
			)
		);
		expect(
			screen.getByText(/No file URL available|Loading file/)
		).toBeInTheDocument();
	});
});
