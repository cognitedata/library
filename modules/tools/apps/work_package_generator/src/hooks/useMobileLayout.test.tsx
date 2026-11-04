import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { renderToString } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
	MOBILE_MEDIA_QUERY,
	UseMobileLayoutContext,
	useMobileLayout,
} from "./useMobileLayout";

function buildMedia(initialMatches: boolean) {
	let matches = initialMatches;
	const listeners = new Set<() => void>();
	const media = {
		get matches() {
			return matches;
		},
		media: MOBILE_MEDIA_QUERY,
		onchange: null,
		addEventListener: vi.fn((_event: string, callback: () => void) => {
			listeners.add(callback);
		}),
		removeEventListener: vi.fn((_event: string, callback: () => void) => {
			listeners.delete(callback);
		}),
		addListener: vi.fn(),
		removeListener: vi.fn(),
		dispatchEvent: vi.fn(() => true),
		setMatches(next: boolean) {
			matches = next;
			for (const listener of listeners) listener();
		},
	};
	return media as MediaQueryList & {
		setMatches: (next: boolean) => void;
	};
}

function renderMobile(matchMedia: (query: string) => MediaQueryList) {
	return renderHook(() => useMobileLayout(), {
		wrapper: ({ children }: { children: ReactNode }) => (
			<UseMobileLayoutContext.Provider value={{ matchMedia }}>
				{children}
			</UseMobileLayoutContext.Provider>
		),
	});
}

describe("useMobileLayout", () => {
	it("returns false when the media query does not match", () => {
		const media = buildMedia(false);
		const matchMedia = vi.fn(() => media);
		const { result } = renderMobile(matchMedia);
		expect(matchMedia).toHaveBeenCalledWith(MOBILE_MEDIA_QUERY);
		expect(result.current).toBe(false);
	});

	it("returns true when the media query matches", () => {
		const media = buildMedia(true);
		const { result } = renderMobile(() => media);
		expect(result.current).toBe(true);
	});

	it("updates when the media query changes and unsubscribes on unmount", () => {
		const media = buildMedia(false);
		const { result, unmount } = renderMobile(() => media);
		act(() => {
			media.setMatches(true);
		});
		expect(result.current).toBe(true);
		unmount();
		expect(media.removeEventListener).toHaveBeenCalled();
	});

	it("reads window.matchMedia when no override is provided", () => {
		const media = buildMedia(false);
		const matchMedia = vi.spyOn(window, "matchMedia").mockReturnValue(media);
		const { result, unmount } = renderHook(() => useMobileLayout());
		expect(matchMedia).toHaveBeenCalledWith(MOBILE_MEDIA_QUERY);
		expect(result.current).toBe(false);
		act(() => {
			media.setMatches(true);
		});
		expect(result.current).toBe(true);
		unmount();
		expect(media.removeEventListener).toHaveBeenCalled();
	});

	it("stays desktop when matchMedia is unavailable", () => {
		const original = window.matchMedia;
		Reflect.deleteProperty(window, "matchMedia");
		try {
			const { result } = renderHook(() => useMobileLayout());
			expect(result.current).toBe(false);
		} finally {
			window.matchMedia = original;
		}
	});

	it("uses the server snapshot during server render", () => {
		vi.spyOn(window, "matchMedia").mockReturnValue(buildMedia(true));
		function Probe() {
			return <span>{useMobileLayout() ? "yes" : "no"}</span>;
		}
		expect(renderToString(<Probe />)).toContain("no");
	});
});

afterEach(() => {
	vi.restoreAllMocks();
});
