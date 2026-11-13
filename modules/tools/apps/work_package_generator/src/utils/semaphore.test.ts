import { describe, expect, it } from "vitest";
import QueuedTaskRunner, {
	AbortError,
	cdfDeleteTaskRunner,
	computeRetryDelayMs,
	isTransientCdfError,
	readHttpStatus,
	readRetryAfterMs,
	withCdfRetry,
} from "./semaphore";

describe("AbortError", () => {
	it("has name AbortError", () => {
		const e = new AbortError("x");
		expect(e.name).toBe("AbortError");
		expect(e.message).toBe("x");
	});
});

describe("QueuedTaskRunner", () => {
	it("runs tasks and respects max concurrency", async () => {
		const runner = new QueuedTaskRunner<() => Promise<number>>(2);
		const order: string[] = [];
		const p1 = runner.schedule(async () => {
			order.push("a-start");
			await new Promise((r) => setTimeout(r, 20));
			order.push("a-end");
			return 1;
		});
		const p2 = runner.schedule(async () => {
			order.push("b-start");
			await new Promise((r) => setTimeout(r, 10));
			order.push("b-end");
			return 2;
		});
		const p3 = runner.schedule(async () => {
			order.push("c");
			return 3;
		});
		await expect(Promise.all([p1, p2, p3])).resolves.toEqual([1, 2, 3]);
		expect(order).toContain("c");
	});

	it("rejects queued task with same key when a new one is scheduled", async () => {
		const runner = new QueuedTaskRunner<() => Promise<string>>(1);
		const blocker = runner.schedule(
			() => new Promise<string>((r) => setTimeout(() => r("done"), 40))
		);
		const superseded = runner.schedule(async () => "old", { key: "k" });
		const winner = runner.schedule(async () => "new", { key: "k" });
		await expect(superseded).rejects.toBeInstanceOf(AbortError);
		await blocker;
		await expect(winner).resolves.toBe("new");
	});

	it("clearQueue removes tasks not yet started", async () => {
		const runner = new QueuedTaskRunner<() => Promise<void>>(1);
		let secondRan = false;
		const first = runner.schedule(
			async () => await new Promise((r) => setTimeout(r, 25))
		);
		const second = runner.schedule(async () => {
			secondRan = true;
		});

		// Attach rejection handler BEFORE any await so the rejection is never unhandled
		const secondCheck = expect(second).rejects.toBeInstanceOf(AbortError);

		runner.clearQueue();
		await first;
		await secondCheck;
		expect(secondRan).toBe(false);
	});

	it("clearQueue rejects all pending tasks with AbortError", async () => {
		const runner = new QueuedTaskRunner<() => Promise<string>>(1);

		const blocker = runner.schedule(
			() => new Promise<string>((r) => setTimeout(() => r("blocker done"), 50))
		);

		const pending1 = runner.schedule(async () => "pending1");
		const pending2 = runner.schedule(async () => "pending2");
		const pending3 = runner.schedule(async () => "pending3");

		// Use allSettled to attach handlers immediately, preventing unhandled rejections
		const allPending = Promise.allSettled([pending1, pending2, pending3]);

		runner.clearQueue();

		await expect(blocker).resolves.toBe("blocker done");

		const results = await allPending;
		expect(results).toHaveLength(3);
		for (const result of results) {
			expect(result.status).toBe("rejected");
			expect((result as PromiseRejectedResult).reason).toBeInstanceOf(
				AbortError
			);
			expect((result as PromiseRejectedResult).reason.message).toBe(
				"Queue cleared"
			);
		}
	});

	it("cdfDeleteTaskRunner is a runner instance", () => {
		expect(cdfDeleteTaskRunner).toBeInstanceOf(QueuedTaskRunner);
	});
});

describe("CDF 429 / transient retry", () => {
	it("reads status and Retry-After seconds", () => {
		const error = Object.assign(new Error("limited"), {
			status: 429,
			headers: { "Retry-After": "2" },
		});
		expect(readHttpStatus(error)).toBe(429);
		expect(readRetryAfterMs(error)).toBe(2000);
		expect(isTransientCdfError(error)).toBe(true);
		expect(isTransientCdfError(new Error("no"))).toBe(false);
		expect(isTransientCdfError(new AbortError())).toBe(false);
	});

	it("reads a nested HTTP-date Retry-After and ignores junk", () => {
		const future = new Date(Date.now() + 5_000).toUTCString();
		expect(
			readRetryAfterMs({
				error: { headers: { "Retry-After": future } },
			})
		).toBeGreaterThan(0);
		expect(
			readRetryAfterMs({ headers: { "Retry-After": "not-a-date" } })
		).toBeUndefined();
		expect(readRetryAfterMs({ headers: ["nope"] })).toBeUndefined();
		expect(readRetryAfterMs("nope")).toBeUndefined();
		expect(
			readRetryAfterMs({
				headers: {
					get: (name: string) => (name === "retry-after" ? "3" : null),
				},
			})
		).toBe(3000);
		expect(
			readRetryAfterMs({
				headers: { get: () => 3 },
			})
		).toBeUndefined();
		expect(readHttpStatus({ statusCode: 503 })).toBe(503);
		expect(readHttpStatus({ error: { status: 429 } })).toBe(429);
		expect(readRetryAfterMs({ headers: { "Retry-After": "-1" } })).toBe(0);
		expect(computeRetryDelayMs(1, { retryAfterMs: -1, random: () => 0 })).toBe(
			0
		);
	});

	it("honors Retry-After before applying jitter", () => {
		expect(
			computeRetryDelayMs(0, { retryAfterMs: 1000, random: () => 0.4 })
		).toBe(1100);
	});

	it("uses full jitter on exponential backoff when Retry-After is absent", () => {
		expect(
			computeRetryDelayMs(2, {
				baseDelayMs: 100,
				maxDelayMs: 10_000,
				random: () => 0.5,
			})
		).toBe(200);
	});

	it("retries 429 then succeeds", async () => {
		let attempts = 0;
		const result = await withCdfRetry(
			async () => {
				attempts += 1;
				if (attempts < 3) {
					throw Object.assign(new Error("limited"), {
						status: 429,
						headers: { "retry-after": "0" },
					});
				}
				return "ok";
			},
			{ random: () => 0, sleep: async () => undefined }
		);
		expect(result).toBe("ok");
		expect(attempts).toBe(3);
	});

	it("does not retry client errors", async () => {
		await expect(
			withCdfRetry(async () => {
				throw Object.assign(new Error("bad"), { status: 400 });
			})
		).rejects.toThrow("bad");
	});

	it("gives up after max retries", async () => {
		let attempts = 0;
		await expect(
			withCdfRetry(
				async () => {
					attempts += 1;
					throw Object.assign(new Error("limited"), { status: 429 });
				},
				{ maxRetries: 2, random: () => 0, sleep: async () => undefined }
			)
		).rejects.toThrow("limited");
		expect(attempts).toBe(3);
	});

	it("retries from QueuedTaskRunner.schedule", async () => {
		const runner = new QueuedTaskRunner<() => Promise<string>>(1, {
			random: () => 0,
			sleep: async () => undefined,
		});
		let attempts = 0;
		await expect(
			runner.schedule(async () => {
				attempts += 1;
				if (attempts === 1) {
					throw Object.assign(new Error("limited"), { status: 503 });
				}
				return "done";
			})
		).resolves.toBe("done");
		expect(attempts).toBe(2);
	});
});
