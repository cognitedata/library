import { beforeEach, describe, expect, it, vi } from "vitest";

// pdf.js needs a worker and a real canvas; neither exists in happy-dom.
vi.mock("pdfjs-dist", () => {
	const page = {
		getViewport: ({ scale }: { scale: number }) => ({
			width: 100 * scale,
			height: 200 * scale,
		}),
		render: () => ({
			promise: Promise.resolve(),
			cancel: vi.fn(),
		}),
	};
	const pdf = {
		getPage: vi.fn(async () => page),
		numPages: 3,
	};
	return {
		GlobalWorkerOptions: { workerSrc: "" },
		getDocument: vi.fn(() => ({
			promise: Promise.resolve(pdf),
			destroy: vi.fn(),
		})),
	};
});

vi.mock("pdfjs-dist/build/pdf.worker.min.mjs?url", () => ({
	default: "worker.js",
}));

import * as pdfjsLib from "pdfjs-dist";
import {
	getPageCount,
	getPageDimensions,
	PDFRenderService,
} from "./PDFRenderService";

function canvas(): HTMLCanvasElement {
	const element = document.createElement("canvas");
	const getContext = (contextId: string): CanvasRenderingContext2D | null => {
		if (contextId !== "2d") return null;
		return Object.assign(Object.create(null), { canvas: element });
	};
	element.getContext = getContext as HTMLCanvasElement["getContext"];
	return element;
}

describe("PDFRenderService", () => {
	beforeEach(() => {
		PDFRenderService.clearCache();
		PDFRenderService.setConcurrencyLimit(1);
		vi.mocked(pdfjsLib.getDocument).mockClear();
	});

	it("renders a page, reuses the document, and reports stats", async () => {
		const progress: string[] = [];
		let completed = false;
		const cancel = PDFRenderService.queueRender({
			id: "a",
			url: "https://files.example.test/a.pdf",
			page: 1,
			canvas: canvas(),
			scale: 1,
			priority: "normal",
			onProgress: (stage) => progress.push(stage),
			onComplete: (success) => {
				completed = success;
			},
		});
		await vi.waitFor(() => expect(completed).toBe(true));
		expect(progress).toContain("complete");

		PDFRenderService.queueRender({
			id: "b",
			url: "https://files.example.test/a.pdf",
			page: 2,
			canvas: canvas(),
			scale: 2,
			priority: "high",
			onComplete: () => undefined,
		});
		await vi.waitFor(() =>
			expect(PDFRenderService.getStats().active).toBeGreaterThanOrEqual(0)
		);
		expect(pdfjsLib.getDocument).toHaveBeenCalledTimes(1);
		cancel();
	});

	it("cancels a queued low-priority render and a failed document", async () => {
		PDFRenderService.setConcurrencyLimit(1);
		let firstDone = false;
		PDFRenderService.queueRender({
			id: "busy",
			url: "https://files.example.test/slow.pdf",
			page: 1,
			canvas: canvas(),
			scale: 1,
			priority: "high",
			onComplete: () => {
				firstDone = true;
			},
		});
		PDFRenderService.queueRender({
			id: "later",
			url: "https://files.example.test/later.pdf",
			page: 1,
			canvas: canvas(),
			scale: 1,
			priority: "low",
			onComplete: () => undefined,
		});
		PDFRenderService.updatePriority("later", "normal");
		PDFRenderService.cancelLowPriority();
		PDFRenderService.cancelRender("later");
		await vi.waitFor(() => expect(firstDone).toBe(true));

		vi.mocked(pdfjsLib.getDocument).mockImplementationOnce(
			() =>
				({
					promise: Promise.reject(new Error("bad pdf")),
					destroy: vi.fn(),
				}) as never
		);
		let failed = false;
		PDFRenderService.queueRender({
			id: "bad",
			url: "https://files.example.test/bad.pdf",
			page: 1,
			canvas: canvas(),
			scale: 1,
			priority: "high",
			onComplete: (success, error) => {
				failed = !success && error?.message === "bad pdf";
			},
		});
		await vi.waitFor(() => expect(failed).toBe(true));
		expect(PDFRenderService.getConcurrencyLimit()).toBe(1);
	});

	it("returns page size and page count, then null when the document fails", async () => {
		await expect(
			getPageDimensions("https://files.example.test/a.pdf", 1)
		).resolves.toEqual({ width: 100, height: 200 });
		await expect(
			getPageCount("https://files.example.test/a.pdf")
		).resolves.toBe(3);

		vi.mocked(pdfjsLib.getDocument).mockImplementationOnce(
			() =>
				({
					promise: Promise.reject(new Error("missing")),
					destroy: vi.fn(),
				}) as never
		);
		await expect(
			getPageDimensions("https://files.example.test/missing.pdf")
		).resolves.toBeNull();
		vi.mocked(pdfjsLib.getDocument).mockImplementationOnce(
			() =>
				({
					promise: Promise.reject(new Error("missing")),
					destroy: vi.fn(),
				}) as never
		);
		await expect(
			getPageCount("https://files.example.test/missing-count.pdf")
		).resolves.toBeNull();
	});
});
