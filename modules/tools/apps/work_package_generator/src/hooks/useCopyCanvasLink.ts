import { useEffect, useRef, useState } from "react";
import { showManualCopyDialog } from "@/components/work-package/canvas-creation/ManualCopyDialog";
import { copyToClipboard } from "@/utils/clipboard";

/** Duration in milliseconds for showing the "copied" confirmation message */
export const COPY_CONFIRMATION_DURATION_MS = 2000;

type CopyCanvasLinkDeps = {
	copyText: (text: string) => Promise<void>;
	confirm: (message: string) => boolean;
	showManualCopy: (canvasLink: string) => void;
};

const defaultDeps: CopyCanvasLinkDeps = {
	copyText: copyToClipboard,
	confirm: (message) => window.confirm(message),
	showManualCopy: showManualCopyDialog,
};

export function useCopyCanvasLink(
	canvasLink: string,
	overrides?: Partial<CopyCanvasLinkDeps>
) {
	const { copyText, confirm, showManualCopy } = {
		...defaultDeps,
		...overrides,
	};
	const [copied, setCopied] = useState(false);
	const copiedTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

	useEffect(() => {
		return () => {
			if (copiedTimeoutRef.current !== null) {
				clearTimeout(copiedTimeoutRef.current);
				copiedTimeoutRef.current = null;
			}
		};
	}, []);

	const copyLink = async () => {
		try {
			await copyText(canvasLink);
			if (copiedTimeoutRef.current !== null) {
				clearTimeout(copiedTimeoutRef.current);
			}
			setCopied(true);
			copiedTimeoutRef.current = setTimeout(() => {
				copiedTimeoutRef.current = null;
				setCopied(false);
			}, COPY_CONFIRMATION_DURATION_MS);
		} catch (error) {
			const userWantsManualCopy = confirm(
				`Automatic copy failed due to browser restrictions.\n\n` +
					`Would you like to manually copy the link?\n\n` +
					`Click OK to see the link in a dialog where you can select and copy it manually.`
			);

			if (userWantsManualCopy) {
				showManualCopy(canvasLink);
			}

			console.error("Copy failed:", error);
		}
	};

	return { copied, copyLink };
}
