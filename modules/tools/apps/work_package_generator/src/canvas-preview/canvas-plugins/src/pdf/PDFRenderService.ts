/**
 * PDF Render Service
 *
 * Manages PDF rendering with:
 * - Core-based concurrency limits (half of available cores)
 * - Priority queue favoring visible PDFs
 * - Automatic cancellation for off-screen requests
 * - Document caching (for re-renders of same page)
 */

import type {
	PDFDocumentLoadingTask,
	PDFDocumentProxy,
	PDFPageProxy,
	RenderTask,
} from "pdfjs-dist";
import * as pdfjsLib from "pdfjs-dist";
// Set up PDF.js worker
import pdfjsWorker from "pdfjs-dist/build/pdf.worker.min.mjs?url";

pdfjsLib.GlobalWorkerOptions.workerSrc = pdfjsWorker;

// ============================================================================
// Types
// ============================================================================

export type RenderPriority = "high" | "normal" | "low";

export interface RenderRequest {
	id: string;
	url: string;
	page: number;
	canvas: HTMLCanvasElement;
	scale: number;
	priority: RenderPriority;
	onProgress?: (stage: "loading" | "rendering" | "complete") => void;
	onComplete?: (success: boolean, error?: Error) => void;
}

interface QueuedRequest extends RenderRequest {
	abortController: AbortController;
	addedAt: number;
}

interface ActiveRender {
	request: QueuedRequest;
	renderTask: RenderTask | null;
	documentPromise: Promise<PDFDocumentProxy> | null;
}

// ============================================================================
// Service Configuration
// ============================================================================

/**
 * Calculate optimal concurrency based on available CPU cores.
 * Uses half of logical cores, with min of 2 and max of 8.
 */
function getOptimalConcurrency(): number {
	const cores =
		typeof navigator !== "undefined" ? navigator.hardwareConcurrency || 4 : 4;
	return Math.min(8, Math.max(2, Math.floor(cores / 2)));
}

// PDF.js configuration
const PDFJS_CONFIG = {
	isEvalSupported: false,
};

// ============================================================================
// PDF Render Service
// ============================================================================

class PDFRenderServiceImpl {
	private queue: QueuedRequest[] = [];
	private activeRenders: Map<string, ActiveRender> = new Map();
	private documentCache: Map<string, PDFDocumentProxy> = new Map();
	private loadingTaskCache: Map<string, PDFDocumentLoadingTask> = new Map();
	private maxConcurrent: number;

	constructor() {
		this.maxConcurrent = getOptimalConcurrency();
	}

	/**
	 * Get current concurrency limit.
	 */
	getConcurrencyLimit(): number {
		return this.maxConcurrent;
	}

	/**
	 * Update concurrency limit dynamically.
	 */
	setConcurrencyLimit(limit: number): void {
		this.maxConcurrent = Math.max(1, Math.min(16, limit));
		this.processQueue();
	}

	/**
	 * Queue a PDF render request.
	 * Returns an abort function to cancel the request.
	 */
	queueRender(request: RenderRequest): () => void {
		const abortController = new AbortController();

		const queuedRequest: QueuedRequest = {
			...request,
			abortController,
			addedAt: Date.now(),
		};

		// Check if this request is already active - if so, cancel and re-queue
		const existing = this.activeRenders.get(request.id);
		if (existing) {
			this.cancelRender(request.id);
		}

		// Remove any existing queued request with same ID
		this.queue = this.queue.filter((r) => r.id !== request.id);

		// Add to queue with priority sorting
		this.queue.push(queuedRequest);
		this.sortQueue();

		// Try to process
		this.processQueue();

		// Return abort function
		return () => this.cancelRender(request.id);
	}

	/**
	 * Cancel a pending or active render.
	 */
	cancelRender(id: string): void {
		// Remove from queue
		const queueIndex = this.queue.findIndex((r) => r.id === id);
		if (queueIndex !== -1) {
			this.queue[queueIndex].abortController.abort();
			this.queue.splice(queueIndex, 1);
		}

		// Cancel active render
		const active = this.activeRenders.get(id);
		if (active) {
			active.request.abortController.abort();
			if (active.renderTask) {
				active.renderTask.cancel();
			}
			this.activeRenders.delete(id);
			// Process next in queue
			this.processQueue();
		}
	}

	/**
	 * Update priority of a queued request (e.g., when visibility changes).
	 */
	updatePriority(id: string, priority: RenderPriority): void {
		const request = this.queue.find((r) => r.id === id);
		if (request) {
			request.priority = priority;
			this.sortQueue();
		}
	}

	/**
	 * Cancel all low-priority requests (useful when many items go off-screen).
	 */
	cancelLowPriority(): void {
		const lowPriorityIds = this.queue
			.filter((r) => r.priority === "low")
			.map((r) => r.id);

		for (const id of lowPriorityIds) {
			this.cancelRender(id);
		}
	}

	/**
	 * Get queue statistics for debugging.
	 */
	getStats(): {
		queued: number;
		active: number;
		cached: number;
		maxConcurrent: number;
	} {
		return {
			queued: this.queue.length,
			active: this.activeRenders.size,
			cached: this.documentCache.size,
			maxConcurrent: this.maxConcurrent,
		};
	}

	/**
	 * Clear document cache (e.g., on memory pressure).
	 */
	clearCache(): void {
		for (const loadingTask of this.loadingTaskCache.values()) {
			loadingTask.destroy();
		}
		this.documentCache.clear();
		this.loadingTaskCache.clear();
	}

	// ============================================================================
	// Private Methods
	// ============================================================================

	private sortQueue(): void {
		const priorityOrder: Record<RenderPriority, number> = {
			high: 0,
			normal: 1,
			low: 2,
		};

		this.queue.sort((a, b) => {
			// First by priority
			const priorityDiff =
				priorityOrder[a.priority] - priorityOrder[b.priority];
			if (priorityDiff !== 0) return priorityDiff;

			// Then by age (older first)
			return a.addedAt - b.addedAt;
		});
	}

	private processQueue(): void {
		// Fill up to max concurrent
		while (
			this.activeRenders.size < this.maxConcurrent &&
			this.queue.length > 0
		) {
			const request = this.queue.shift()!;
			this.startRender(request);
		}
	}

	private async startRender(request: QueuedRequest): Promise<void> {
		const {
			id,
			url,
			page,
			canvas,
			scale,
			abortController,
			onProgress,
			onComplete,
		} = request;

		// Track as active
		const activeRender: ActiveRender = {
			request,
			renderTask: null,
			documentPromise: null,
		};
		this.activeRenders.set(id, activeRender);

		try {
			// Check if aborted
			if (abortController.signal.aborted) {
				throw new Error("Aborted");
			}

			onProgress?.("loading");

			// Get or load document
			let pdf = this.documentCache.get(url);
			if (!pdf) {
				const loadingTask = pdfjsLib.getDocument({
					url,
					...PDFJS_CONFIG,
				});

				activeRender.documentPromise = loadingTask.promise;
				pdf = await loadingTask.promise;

				// Cache the document and its loading task (needed to destroy it later)
				this.documentCache.set(url, pdf);
				this.loadingTaskCache.set(url, loadingTask);
			}

			// Check if aborted
			if (abortController.signal.aborted) {
				throw new Error("Aborted");
			}

			// Get page
			const pdfPage = await pdf.getPage(page);

			// Check if aborted
			if (abortController.signal.aborted) {
				throw new Error("Aborted");
			}

			onProgress?.("rendering");

			// Calculate viewport
			const viewport = pdfPage.getViewport({ scale });

			// Set canvas size
			canvas.width = viewport.width;
			canvas.height = viewport.height;

			// Get context
			const context = canvas.getContext("2d");
			if (!context) {
				throw new Error("Could not get canvas context");
			}

			// Start render
			// Type assertion needed due to pdfjs-dist type definition variations
			const renderTask = pdfPage.render({
				canvasContext: context,
				viewport,
			} as Parameters<PDFPageProxy["render"]>[0]);

			activeRender.renderTask = renderTask;

			await renderTask.promise;

			// Check if aborted (render might have completed but request was cancelled)
			if (abortController.signal.aborted) {
				throw new Error("Aborted");
			}

			onProgress?.("complete");
			onComplete?.(true);
		} catch (error) {
			const err = error as Error;

			// Don't report cancellation as an error
			if (
				err.name === "RenderingCancelledException" ||
				err.message === "Aborted"
			) {
				onComplete?.(false);
			} else {
				console.error(`[PDFRenderService] Render failed for ${id}:`, err);
				onComplete?.(false, err);
			}
		} finally {
			// Remove from active
			this.activeRenders.delete(id);

			// Process next in queue
			this.processQueue();
		}
	}
}

// Separate cache for dimension queries (to avoid interference with rendering)
const documentCacheForDimensions: Map<string, PDFDocumentProxy> = new Map();

/**
 * Get page dimensions for a PDF document.
 * Returns dimensions in PDF points (1 point = 1/72 inch).
 */
export async function getPageDimensions(
	url: string,
	pageNumber: number = 1
): Promise<{ width: number; height: number } | null> {
	try {
		// Try to get from cache first
		let pdf = documentCacheForDimensions.get(url);
		if (!pdf) {
			const loadingTask = pdfjsLib.getDocument({
				url,
				...PDFJS_CONFIG,
			});
			pdf = await loadingTask.promise;
			documentCacheForDimensions.set(url, pdf);
		}

		const page = await pdf.getPage(pageNumber);
		const viewport = page.getViewport({ scale: 1 });

		return {
			width: viewport.width,
			height: viewport.height,
		};
	} catch (err) {
		console.error("[PDFRenderService] Failed to get page dimensions:", err);
		return null;
	}
}

/**
 * Get the total number of pages in a PDF document.
 */
export async function getPageCount(url: string): Promise<number | null> {
	try {
		// Try to get from cache first
		let pdf = documentCacheForDimensions.get(url);
		if (!pdf) {
			const loadingTask = pdfjsLib.getDocument({
				url,
				...PDFJS_CONFIG,
			});
			pdf = await loadingTask.promise;
			documentCacheForDimensions.set(url, pdf);
		}

		return pdf.numPages;
	} catch (err) {
		console.error("[PDFRenderService] Failed to get page count:", err);
		return null;
	}
}

// ============================================================================
// Singleton Export
// ============================================================================

/**
 * Singleton PDF render service instance.
 * Shared across all PDF nodes for coordinated rendering.
 */
export const PDFRenderService = new PDFRenderServiceImpl();
