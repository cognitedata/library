import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { copyToClipboard } from "@/utils/clipboard";
import {
	COPY_CONFIRMATION_DURATION_MS,
	useCopyCanvasLink,
} from "./useCopyCanvasLink";

// vi.mock: clipboard is a browser API wrapper (not DI); stub avoids real clipboard access.
vi.mock("@/utils/clipboard", () => ({
	copyToClipboard: vi.fn(() => Promise.resolve()),
}));

describe(useCopyCanvasLink.name, () => {
	beforeEach(() => {
		vi.spyOn(console, "error").mockImplementation(() => {});
		vi.mocked(copyToClipboard).mockResolvedValue(undefined);
	});

	afterEach(() => {
		vi.useRealTimers();
		vi.restoreAllMocks();
		document
			.querySelector('[role="dialog"][aria-labelledby="manual-copy-title"]')
			?.parentElement?.remove();
	});

	it("should mark copied after a successful clipboard write and reset after the timeout", async () => {
		vi.useFakeTimers();
		vi.mocked(copyToClipboard).mockResolvedValue(undefined);
		const { result } = renderHook(() =>
			useCopyCanvasLink("https://example.test/c")
		);

		await act(async () => {
			await result.current.copyLink();
		});

		expect(copyToClipboard).toHaveBeenCalledWith("https://example.test/c");
		expect(result.current.copied).toBe(true);

		await act(async () => {
			vi.advanceTimersByTime(COPY_CONFIRMATION_DURATION_MS);
		});

		expect(result.current.copied).toBe(false);
	});

	it("should offer a manual copy dialog when clipboard write fails and the user confirms", async () => {
		const copyText = vi.fn(() => Promise.reject(new Error("blocked")));
		const confirm = vi.fn(() => true);
		const showManualCopy = vi.fn();
		const { result } = renderHook(() =>
			useCopyCanvasLink("https://example.test/c", {
				copyText,
				confirm,
				showManualCopy,
			})
		);

		await act(async () => {
			await result.current.copyLink();
		});

		expect(confirm).toHaveBeenCalled();
		expect(showManualCopy).toHaveBeenCalledWith("https://example.test/c");
		expect(result.current.copied).toBe(false);
	});

	it("should not show a manual dialog when the user cancels after copy failure", async () => {
		const copyText = vi.fn(() => Promise.reject(new Error("blocked")));
		const confirm = vi.fn(() => false);
		const showManualCopy = vi.fn();
		const { result } = renderHook(() =>
			useCopyCanvasLink("https://example.test/c", {
				copyText,
				confirm,
				showManualCopy,
			})
		);

		await act(async () => {
			await result.current.copyLink();
		});

		expect(showManualCopy).not.toHaveBeenCalled();
		expect(result.current.copied).toBe(false);
	});

	it("should clear a pending confirmation timeout when copy succeeds again", async () => {
		vi.useFakeTimers();
		const copyText = vi.fn(() => Promise.resolve());
		const { result } = renderHook(() =>
			useCopyCanvasLink("https://example.test/c", { copyText })
		);

		await act(async () => {
			await result.current.copyLink();
		});
		expect(result.current.copied).toBe(true);

		await act(async () => {
			await result.current.copyLink();
			vi.advanceTimersByTime(COPY_CONFIRMATION_DURATION_MS - 1);
		});
		expect(result.current.copied).toBe(true);

		await act(async () => {
			vi.advanceTimersByTime(1);
		});
		expect(result.current.copied).toBe(false);
	});

	it("should clear the confirmation timeout on unmount", async () => {
		vi.useFakeTimers();
		const copyText = vi.fn(() => Promise.resolve());
		const { result, unmount } = renderHook(() =>
			useCopyCanvasLink("https://example.test/c", { copyText })
		);

		await act(async () => {
			await result.current.copyLink();
		});
		expect(result.current.copied).toBe(true);

		unmount();
		expect(() => {
			vi.advanceTimersByTime(COPY_CONFIRMATION_DURATION_MS);
		}).not.toThrow();
	});

	it("should open the default manual-copy dialog when clipboard write fails and the user confirms", async () => {
		const copyText = vi.fn(() => Promise.reject(new Error("blocked")));
		const { result } = renderHook(() =>
			useCopyCanvasLink("https://example.test/c", {
				copyText,
				confirm: () => true,
			})
		);

		await act(async () => {
			await result.current.copyLink();
		});

		const textarea = document.querySelector(
			"#manual-copy-text"
		) as HTMLTextAreaElement | null;
		expect(textarea).not.toBeNull();
		expect(textarea?.value).toBe("https://example.test/c");
		await waitFor(() => {
			expect(document.activeElement).toBe(textarea);
		});
	});

	it("should set the link as textarea value rather than interpolating it into HTML", async () => {
		const copyText = vi.fn(() => Promise.reject(new Error("blocked")));
		const canvasLink = `https://example.test/c"><img src=x onerror=alert(1)>`;
		const { result } = renderHook(() =>
			useCopyCanvasLink(canvasLink, {
				copyText,
				confirm: () => true,
			})
		);

		await act(async () => {
			await result.current.copyLink();
		});

		const textarea = document.querySelector(
			"#manual-copy-text"
		) as HTMLTextAreaElement | null;
		expect(textarea?.value).toBe(canvasLink);
		expect(document.querySelector("img")).toBeNull();
	});

	it("should unmount the default manual-copy dialog when Close is clicked", async () => {
		const copyText = vi.fn(() => Promise.reject(new Error("blocked")));
		const { result } = renderHook(() =>
			useCopyCanvasLink("https://example.test/c", {
				copyText,
				confirm: () => true,
			})
		);

		await act(async () => {
			await result.current.copyLink();
		});

		const closeButton = document.querySelector(
			'[role="dialog"] button'
		) as HTMLButtonElement | null;
		expect(closeButton).not.toBeNull();
		closeButton?.click();

		expect(document.querySelector("#manual-copy-text")).toBeNull();
	});

	it("should not open the default dialog when the user cancels confirm", async () => {
		const copyText = vi.fn(() => Promise.reject(new Error("blocked")));
		window.confirm = vi.fn(() => false);
		const { result } = renderHook(() =>
			useCopyCanvasLink("https://example.test/c", { copyText })
		);

		await act(async () => {
			await result.current.copyLink();
		});

		expect(window.confirm).toHaveBeenCalled();
		expect(document.querySelector("#manual-copy-text")).toBeNull();
		expect(result.current.copied).toBe(false);
	});
});
