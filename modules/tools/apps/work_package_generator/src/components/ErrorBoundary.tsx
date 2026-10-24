import { Component, type ErrorInfo, type ReactNode } from "react";
import { Button } from "@/components/ui";
import { logError } from "../utils";

interface ErrorBoundaryProps {
	children: ReactNode;
	fallback?: ReactNode;
}

interface ErrorBoundaryState {
	hasError: boolean;
	error: Error | null;
}

/**
 * Top-level error boundary that catches unhandled render-time exceptions and
 * shows a user-friendly message instead of a blank screen.
 */
export class ErrorBoundary extends Component<
	ErrorBoundaryProps,
	ErrorBoundaryState
> {
	constructor(props: ErrorBoundaryProps) {
		super(props);
		this.state = { hasError: false, error: null };
	}

	static getDerivedStateFromError(error: Error): ErrorBoundaryState {
		return { hasError: true, error };
	}

	override componentDidCatch(error: Error, info: ErrorInfo) {
		logError({
			scope: "ErrorBoundary",
			message: "Unhandled render error",
			context: { componentStack: info.componentStack },
			cause: error,
		});
	}

	override render() {
		if (this.state.hasError) {
			if (this.props.fallback) return this.props.fallback;
			return (
				<div
					role="alert"
					className="flex flex-col items-center justify-center h-screen gap-4 p-8 text-center"
				>
					<p className="text-lg font-semibold text-destructive">
						Something went wrong
					</p>
					{this.state.error && (
						<pre className="max-w-xl text-sm text-muted-foreground whitespace-pre-wrap break-words">
							{this.state.error.message}
						</pre>
					)}
					<Button
						type="button"
						variant="outline"
						onClick={() => this.setState({ hasError: false, error: null })}
					>
						Try again
					</Button>
				</div>
			);
		}
		return this.props.children;
	}
}
