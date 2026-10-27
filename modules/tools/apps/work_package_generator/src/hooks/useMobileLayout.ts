import {
	createContext,
	useContext,
	useMemo,
	useSyncExternalStore,
} from "react";

export const MOBILE_MEDIA_QUERY = "(max-width: 816px)";

function unmatchedMediaQueryList(query: string): MediaQueryList {
	return {
		matches: false,
		media: query,
		onchange: null,
		addEventListener: () => undefined,
		removeEventListener: () => undefined,
		dispatchEvent: () => false,
		addListener: () => undefined,
		removeListener: () => undefined,
	};
}

function defaultMatchMedia(query: string): MediaQueryList {
	if (
		typeof window === "undefined" ||
		typeof window.matchMedia !== "function"
	) {
		return unmatchedMediaQueryList(query);
	}
	return window.matchMedia(query);
}

const defaultMobileLayoutDeps = {
	matchMedia: defaultMatchMedia,
};

export type UseMobileLayoutContextType = typeof defaultMobileLayoutDeps;
export const UseMobileLayoutContext = createContext<UseMobileLayoutContextType>(
	defaultMobileLayoutDeps
);

function getServerSnapshot() {
	return false;
}

/** True when viewport width is at most 816px (matches `mobile:` Tailwind variant). */
export function useMobileLayout(): boolean {
	const { matchMedia } = useContext(UseMobileLayoutContext);
	const media = useMemo(() => matchMedia(MOBILE_MEDIA_QUERY), [matchMedia]);
	const subscribe = useMemo(
		() => (callback: () => void) => {
			media.addEventListener("change", callback);
			return () => media.removeEventListener("change", callback);
		},
		[media]
	);
	const getSnapshot = useMemo(() => () => media.matches, [media]);
	return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}
