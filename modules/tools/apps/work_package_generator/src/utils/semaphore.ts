/**
 * AbortError thrown when a queued task is cancelled
 */
export class AbortError extends Error {
	public constructor(message: string = "Aborted") {
		super(message);
		this.name = "AbortError";
	}
}

type PendingTask<AsyncFn, AsyncFnResult> = {
	resolve: (result: AsyncFnResult) => void;
	reject: (error: unknown) => void;
	fn: AsyncFn;
	key?: string;
};

const DEFAULT_MAX_CONCURRENT_TASKS = 15;
const TRANSIENT_HTTP_STATUSES = new Set([408, 425, 429, 500, 502, 503, 504]);
const DEFAULT_MAX_RETRIES = 5;
const DEFAULT_BASE_DELAY_MS = 250;
const DEFAULT_MAX_DELAY_MS = 8_000;
const RETRY_AFTER_JITTER_MS = 250;

export type CdfRetryOptions = {
	maxRetries?: number;
	baseDelayMs?: number;
	maxDelayMs?: number;
	random?: () => number;
	sleep?: (ms: number) => Promise<void>;
};

function defaultSleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}

/**
 * Read an HTTP status from Cognite SDK / fetch-style errors.
 */
export function readHttpStatus(error: unknown): number | undefined {
	if (!isRecord(error)) return undefined;
	if (typeof error.status === "number") return error.status;
	if (typeof error.statusCode === "number") return error.statusCode;
	if (isRecord(error.error) && typeof error.error.status === "number") {
		return error.error.status;
	}
	return undefined;
}

function headerRecord(headers: unknown): Record<string, string> | undefined {
	if (!isRecord(headers)) return undefined;
	if (typeof headers.get === "function") {
		const retryAfter = headers.get("retry-after");
		return typeof retryAfter === "string" ? { "retry-after": retryAfter } : {};
	}
	const out: Record<string, string> = {};
	for (const [key, value] of Object.entries(headers)) {
		if (typeof value === "string") out[key.toLowerCase()] = value;
	}
	return out;
}

/**
 * Parse Retry-After as milliseconds (delta-seconds or HTTP-date).
 */
export function readRetryAfterMs(error: unknown): number | undefined {
	if (!isRecord(error)) return undefined;
	const fromHeaders = headerRecord(error.headers)?.["retry-after"];
	const nested = isRecord(error.error)
		? headerRecord(error.error.headers)?.["retry-after"]
		: undefined;
	const raw = fromHeaders ?? nested;
	if (raw === undefined) return undefined;

	const asSeconds = Number(raw);
	if (Number.isFinite(asSeconds) && asSeconds >= 0) {
		return asSeconds * 1000;
	}
	const asDate = Date.parse(raw);
	if (!Number.isNaN(asDate)) {
		return Math.max(0, asDate - Date.now());
	}
	return undefined;
}

export function isTransientCdfError(error: unknown): boolean {
	if (error instanceof AbortError) return false;
	const status = readHttpStatus(error);
	return status !== undefined && TRANSIENT_HTTP_STATUSES.has(status);
}

/**
 * Full-jitter exponential backoff. When Retry-After is present, wait that long
 * plus a small jitter so concurrent retries do not stampede.
 */
export function computeRetryDelayMs(
	attempt: number,
	options: {
		retryAfterMs?: number;
		baseDelayMs?: number;
		maxDelayMs?: number;
		random?: () => number;
	} = {}
): number {
	const random = options.random ?? Math.random;
	if (options.retryAfterMs !== undefined && options.retryAfterMs >= 0) {
		return options.retryAfterMs + Math.floor(random() * RETRY_AFTER_JITTER_MS);
	}
	const base = options.baseDelayMs ?? DEFAULT_BASE_DELAY_MS;
	const max = options.maxDelayMs ?? DEFAULT_MAX_DELAY_MS;
	const cap = Math.min(max, base * 2 ** attempt);
	return Math.floor(random() * cap);
}

export async function withCdfRetry<T>(
	fn: () => Promise<T>,
	options: CdfRetryOptions = {}
): Promise<T> {
	const maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;
	const sleep = options.sleep ?? defaultSleep;
	let lastError: unknown;

	for (let attempt = 0; attempt <= maxRetries; attempt++) {
		try {
			return await fn();
		} catch (error) {
			lastError = error;
			if (!isTransientCdfError(error) || attempt === maxRetries) {
				throw error;
			}
			const delayMs = computeRetryDelayMs(attempt, {
				retryAfterMs: readRetryAfterMs(error),
				baseDelayMs: options.baseDelayMs,
				maxDelayMs: options.maxDelayMs,
				random: options.random,
			});
			await sleep(delayMs);
		}
	}

	throw lastError;
}

/**
 * QueuedTaskRunner for controlling concurrent operations.
 * Limits concurrent CDF API requests to avoid rate limiting and deadlocks.
 */
export default class QueuedTaskRunner<
	AsyncFn extends () => Promise<AsyncFnResult>,
	AsyncFnResult = Awaited<ReturnType<AsyncFn>>,
> {
	private pendingTasks: PendingTask<AsyncFn, AsyncFnResult>[] = [];
	private currentPendingTasks = 0;
	private readonly maxConcurrentTasks: number;
	private readonly retryOptions: CdfRetryOptions;

	public constructor(
		maxConcurrentTasks: number = DEFAULT_MAX_CONCURRENT_TASKS,
		retryOptions: CdfRetryOptions = {}
	) {
		this.maxConcurrentTasks = maxConcurrentTasks;
		this.retryOptions = retryOptions;
	}

	public schedule(
		fn: AsyncFn,
		options: { key?: string } = {}
	): Promise<AsyncFnResult> {
		return new Promise((resolve, reject) => {
			if (options.key !== undefined) {
				this.pendingTasks
					.filter((task) => task.key === options.key)
					.forEach((task) => {
						task.reject(new AbortError());
					});

				this.pendingTasks = this.pendingTasks.filter(
					(task) => task.key !== options.key
				);
			}

			this.pendingTasks.push({
				resolve,
				reject,
				fn,
				key: options.key,
			});

			void this.attemptConsumingNextTask();
		});
	}

	public async attemptConsumingNextTask(): Promise<void> {
		if (this.pendingTasks.length === 0) return;
		if (this.currentPendingTasks >= this.maxConcurrentTasks) return;

		const pendingTask = this.pendingTasks.shift();
		if (pendingTask === undefined) {
			throw new Error("pendingTask is undefined, this should never happen");
		}

		this.currentPendingTasks++;
		const { fn, resolve, reject } = pendingTask;

		try {
			const result = await withCdfRetry(fn, this.retryOptions);
			resolve(result);
		} catch (e) {
			reject(e);
		} finally {
			this.currentPendingTasks--;
			void this.attemptConsumingNextTask();
		}
	}

	public clearQueue = (): void => {
		for (const task of this.pendingTasks) {
			task.reject(new AbortError("Queue cleared"));
		}
		this.pendingTasks = [];
	};
}

/** Global runner for Data Modeling instances.* reads/writes (except deletes). */
export const cdfTaskRunner = new QueuedTaskRunner(DEFAULT_MAX_CONCURRENT_TASKS);

/** Stricter concurrency for instances.delete per DMS guidance. */
export const cdfDeleteTaskRunner = new QueuedTaskRunner(2);
