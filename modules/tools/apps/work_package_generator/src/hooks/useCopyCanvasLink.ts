import { useEffect, useRef, useState } from "react";
import { showManualCopyDialog } from "@/components/work-package/canvas-creation/ManualCopyDialog";
import { copyToClipboard } from "@/utils/clipboard";
import { logError } from "@/utils/logger";

/** Duration in milliseconds for showing the "copied" confirmation message */
export const COPY_CONFIRMATION_DURATION_MS = 2000;

type CopyCanvasLinkDeps = {
	copyText: (text: string) => Promise<void>;
	showManualCopy: (canvasLink: string) => void;
};

const defaultDeps: CopyCanvasLinkDeps = {
	copyText: copyToClipboard,
	showManualCopy: showManualCopyDialog,
};

export function useCopyCanvasLink(
	canvasLink: string,
	overrides?: Partial<CopyCanvasLinkDeps>
) {
	const { copyText, showManualCopy } = {
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
			showManualCopy(canvasLink);

			logError({
				scope: "useCopyCanvasLink",
				message: "Copy failed",
				cause: error,
			});
		}
	};

	return { copied, copyLink };
}
