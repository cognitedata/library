/**
 * Clipboard Utilities
 */

/**
 * Copy text to the clipboard. Uses the modern Clipboard API with a
 * `document.execCommand` fallback for environments that don't support it.
 */
export async function copyToClipboard(text: string): Promise<void> {
	// Always use fallback method in restrictive environments like Fusion/Flows iframes
	// where permissions policies may block the modern Clipboard API
	try {
		// Enhanced fallback method using document.execCommand
		const el = document.createElement("textarea");
		el.value = text;
		el.style.position = "fixed";
		el.style.left = "-9999px";
		el.style.top = "-9999px";
		el.style.opacity = "0";
		el.setAttribute("readonly", "");
		document.body.appendChild(el);

		// Focus and select the text
		el.focus();
		el.select();
		el.setSelectionRange(0, el.value.length);

		const success = document.execCommand("copy");
		document.body.removeChild(el);

		if (!success) {
			throw new Error("Failed to copy text using fallback method");
		}
	} catch (error) {
		console.error("Failed to copy text to clipboard:", error);
		throw error;
	}
}
