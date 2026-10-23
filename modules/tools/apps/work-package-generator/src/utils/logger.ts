/**
 * Structured error logging for the work-package-generator app.
 * Prefer this over raw console.error so logs have a stable shape for filtering
 * and forwarding, without dumping arbitrary object graphs.
 */

const APP_ID = "work-package-generator";

export type StructuredErrorLog = {
	app: typeof APP_ID;
	level: "error";
	timestamp: string;
	scope: string;
	message: string;
	context?: Record<string, unknown>;
	error?: {
		name: string;
		message: string;
		stack?: string;
	};
};

function serializeCause(cause: unknown): StructuredErrorLog["error"] {
	if (cause === undefined || cause === null) return undefined;
	if (cause instanceof Error) {
		return {
			name: cause.name,
			message: cause.message,
			...(cause.stack ? { stack: cause.stack } : {}),
		};
	}
	if (typeof cause === "object" && cause !== null && "message" in cause) {
		const m = (cause as { message?: unknown }).message;
		if (typeof m === "string") {
			return { name: "Error", message: m };
		}
	}
	return { name: "Unknown", message: String(cause) };
}

/**
 * Logs an error with a fixed schema. Uses console.error with a JSON line so
 * aggregators and grep stay predictable; omit sensitive fields from context.
 */
export function logError(params: {
	scope: string;
	message: string;
	context?: Record<string, unknown>;
	cause?: unknown;
}): void {
	const entry: StructuredErrorLog = {
		app: APP_ID,
		level: "error",
		timestamp: new Date().toISOString(),
		scope: params.scope,
		message: params.message,
		...(params.context && Object.keys(params.context).length > 0
			? { context: params.context }
			: {}),
		...(params.cause !== undefined
			? { error: serializeCause(params.cause) }
			: {}),
	};
	console.error(JSON.stringify(entry));
}
