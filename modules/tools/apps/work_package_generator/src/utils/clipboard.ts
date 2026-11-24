/**
 * Clipboard Utilities
 */

import { logError } from "./logger";

/**
 * Copy text to the clipboard. Uses the Clipboard API first, then
 * `document.execCommand` when that API is missing or rejected.
 */
export async function copyToClipboard(text: string): Promise<void> {
	if (navigator.clipboard?.writeText) {
		try {
			await navigator.clipboard.writeText(text);
			return;
		} catch (error) {
			logError({
				scope: "clipboard",
				message: "Clipboard API rejected the write; trying execCommand",
				cause: error,
			});
		}
	}

	try {
		const el = document.createElement("textarea");
		el.value = text;
		el.style.position = "fixed";
		el.style.left = "-9999px";
		el.style.top = "-9999px";
		el.style.opacity = "0";
		el.setAttribute("readonly", "");
		document.body.appendChild(el);

		el.focus();
		el.select();
		el.setSelectionRange(0, el.value.length);

		const success = document.execCommand("copy");
		document.body.removeChild(el);

		if (!success) {
			throw new Error("Failed to copy text using fallback method");
		}
	} catch (error) {
		logError({
			scope: "clipboard",
			message: "Failed to copy text to clipboard",
			cause: error,
		});
		throw error;
	}
}
