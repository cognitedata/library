/**
 * Opens the user's mail client with a prefilled subject and body
 * that include the canvas name and link.
 */

import { Mail } from "lucide-react";
import { Button } from "@/components/ui";

interface EmailShareDialogProps {
	canvasLink: string;
	canvasName: string;
}

export function EmailShareDialog({
	canvasLink,
	canvasName,
}: EmailShareDialogProps) {
	const handleEmailShare = () => {
		const subject = encodeURIComponent(`Canvas: ${canvasName}`);
		const body = encodeURIComponent(
			`Check out this canvas: ${canvasName}\n\n${canvasLink}`
		);
		const mailtoLink = `mailto:?subject=${subject}&body=${body}`;
		window.open(mailtoLink, "_blank");
	};

	return (
		<Button
			variant="ghost"
			className="h-auto p-0 text-sm"
			onClick={handleEmailShare}
		>
			<Mail className="h-4 w-4 mr-1" />
			Email link
		</Button>
	);
}
