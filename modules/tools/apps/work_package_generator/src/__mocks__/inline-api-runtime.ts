import { afterEach, beforeEach, vi } from "vitest";
import { resetApiRuntimeDeps, setApiRuntimeDeps } from "../api/runtime-deps";

/** Run API calls inline and silence structured logs for the current test file. */
export function installInlineApiRuntime(): void {
	beforeEach(() => {
		setApiRuntimeDeps({
			schedule: (fn) => fn(),
			scheduleDelete: (fn) => fn(),
			logError: vi.fn(),
		});
	});
	afterEach(() => {
		resetApiRuntimeDeps();
	});
}
