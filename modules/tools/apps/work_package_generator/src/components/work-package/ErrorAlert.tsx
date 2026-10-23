/**
 * Error Alert Component
 * Displays error messages with dismiss functionality
 */

import { AlertCircle, X } from "lucide-react";
import { Button } from "@/components/ui";
import { toUserFacingErrorMessage } from "@/utils/errors";

interface ErrorAlertProps {
	message: string;
	onDismiss: () => void;
}

export function ErrorAlert({ message, onDismiss }: ErrorAlertProps) {
	const safeMessage = toUserFacingErrorMessage(message);
	return (
		<div
			role="alert"
			className="flex items-start gap-3 rounded-xl border border-destructive/30 bg-destructive/10 p-4 text-sm shadow-sm"
		>
			<AlertCircle
				className="mt-0.5 h-4 w-4 shrink-0 text-destructive"
				aria-hidden
			/>
			<p className="flex-1 text-destructive">{safeMessage}</p>
			<Button
				onClick={onDismiss}
				variant="ghost"
				size="icon-sm"
				className="text-destructive/60 hover:text-destructive shrink-0"
				aria-label="Dismiss error"
			>
				<X className="h-3 w-3" />
			</Button>
		</div>
	);
}
