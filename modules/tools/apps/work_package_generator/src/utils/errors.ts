/**
 * Extracts a human-readable message from an unknown error value.
 * Centralizes the repeated `error instanceof Error ? error.message : fallback` pattern.
 */
export function getErrorMessage(
	error: unknown,
	fallback = "Unknown error"
): string {
	if (error instanceof Error) return error.message;
	if (typeof error === "string") return error;
	if (typeof error === "object" && error !== null && "message" in error) {
		const m = (error as { message?: unknown }).message;
		if (typeof m === "string") return m;
	}
	return fallback;
}

/** Shown when the raw message looks like a URL, path, or build artifact (never user-facing). */
export const USER_FACING_ERROR_FALLBACK =
	"We couldn't complete that action. Please try again.";

const URL_RE = /https?:\/\/[^\s]+/i;
const WINDOWS_PATH_RE = /[A-Za-z]:\\[^\s]+/;
const UNIXLIKE_PATH_RE =
	/(?:^|\s)\/(?:usr|Users|home|var|tmp|opt|app|src|assets)\/[^\s]+/i;
const CHUNK_OR_ASSET_RE =
	/\bchunk-[A-Za-z0-9._-]+\.(?:js|mjs|css)\b|\.(?:css|js|mjs)(?:\?[^\s]*)?\b/i;
const MODULE_HINT_RE =
	/dynamically imported module|import\(\)|preload|Failed to fetch module|Loading chunk /i;
const NODE_MODULES_RE = /node_modules/;
const VITE_FS_RE = /@fs\//;

/**
 * True when a string likely contains URLs, file paths, or bundler internals that should not be shown to users.
 */
export function looksLikeTechnicalErrorMessage(message: string): boolean {
	const m = message.trim();
	if (!m) return false;
	if (URL_RE.test(m)) return true;
	if (WINDOWS_PATH_RE.test(m)) return true;
	if (UNIXLIKE_PATH_RE.test(m)) return true;
	if (CHUNK_OR_ASSET_RE.test(m)) return true;
	if (MODULE_HINT_RE.test(m)) return true;
	if (NODE_MODULES_RE.test(m)) return true;
	if (VITE_FS_RE.test(m)) return true;
	return false;
}

/**
 * Returns a safe string for alerts and inline errors. Technical leaks are replaced with a short generic message.
 */
export function toUserFacingErrorMessage(raw: string): string {
	const trimmed = raw.trim();
	if (!trimmed) return USER_FACING_ERROR_FALLBACK;
	if (looksLikeTechnicalErrorMessage(trimmed))
		return USER_FACING_ERROR_FALLBACK;
	return trimmed;
}
