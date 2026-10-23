import { useEffect, useRef } from "react";
import { createRoot } from "react-dom/client";
import {
	Button,
	Dialog,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
	Textarea,
} from "@/components/ui";

export function ManualCopyDialog({
	canvasLink,
	onClose,
}: {
	canvasLink: string;
	onClose: () => void;
}) {
	const textareaRef = useRef<HTMLTextAreaElement>(null);

	useEffect(() => {
		const frame = requestAnimationFrame(() => {
			textareaRef.current?.focus();
			textareaRef.current?.select();
		});
		return () => cancelAnimationFrame(frame);
	}, []);

	return (
		<Dialog open onOpenChange={(isOpen) => !isOpen && onClose()}>
			<DialogContent
				className="max-w-lg"
				showCloseButton={false}
				closeButtonLabel="Close"
				initialFocus={() => textareaRef.current}
			>
				<DialogHeader>
					<DialogTitle id="manual-copy-title">Copy Canvas Link</DialogTitle>
					<DialogDescription>
						Select all text below and copy it (Ctrl+C or Cmd+C):
					</DialogDescription>
				</DialogHeader>
				<Textarea
					id="manual-copy-text"
					ref={textareaRef}
					readOnly
					value={canvasLink}
					rows={3}
					className="font-mono text-xs"
					aria-label="Canvas link"
					onFocus={(event) => event.currentTarget.select()}
				/>
				<DialogFooter>
					<Button type="button" onClick={onClose}>
						Close
					</Button>
				</DialogFooter>
			</DialogContent>
		</Dialog>
	);
}

export function showManualCopyDialog(canvasLink: string): void {
	const container = document.createElement("div");
	document.body.appendChild(container);
	const root = createRoot(container);

	const close = () => {
		root.unmount();
		container.remove();
	};

	root.render(<ManualCopyDialog canvasLink={canvasLink} onClose={close} />);
}
