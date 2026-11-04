/**
 * Factory for CDF scheduling and error logging used by `src/api`.
 * Tests replace the factory instead of mocking the semaphore or logger modules.
 */

import {
	cdfApiFetch as defaultCdfApiFetch,
	parseJsonResponse as defaultParseJsonResponse,
} from "../utils/api-helpers";
import { logError as defaultLogError } from "../utils/logger";
import { cdfDeleteTaskRunner, cdfTaskRunner } from "../utils/semaphore";

export type ScheduledTask = <T>(fn: () => Promise<T>) => Promise<T>;

export type ApiRuntimeDeps = {
	schedule: ScheduledTask;
	scheduleDelete: ScheduledTask;
	logError: typeof defaultLogError;
	cdfApiFetch: typeof defaultCdfApiFetch;
	parseJsonResponse: typeof defaultParseJsonResponse;
};

const defaultDeps: ApiRuntimeDeps = {
	schedule: (fn) => cdfTaskRunner.schedule(fn),
	scheduleDelete: (fn) => cdfDeleteTaskRunner.schedule(fn),
	logError: defaultLogError,
	cdfApiFetch: defaultCdfApiFetch,
	parseJsonResponse: defaultParseJsonResponse,
};

let activeDeps: ApiRuntimeDeps = defaultDeps;

/** Per-call overrides win over the factory default. */
export function resolveApiRuntime(
	overrides?: Partial<ApiRuntimeDeps>
): ApiRuntimeDeps {
	return { ...activeDeps, ...overrides };
}

/** Replace the factory default for the current test (or a nested call scope). */
export function setApiRuntimeDeps(overrides?: Partial<ApiRuntimeDeps>): void {
	activeDeps = { ...defaultDeps, ...overrides };
}

/** Merge overrides onto the deps already active in this test. */
export function patchApiRuntimeDeps(overrides: Partial<ApiRuntimeDeps>): void {
	activeDeps = { ...activeDeps, ...overrides };
}

export function resetApiRuntimeDeps(): void {
	activeDeps = defaultDeps;
}
