import type { TestingLibraryMatchers } from "@testing-library/jest-dom/matchers";

/**
 * jest-dom 7 augments Vitest 4's single-parameter `Assertion<T>`.
 * Vitest 5's `Assertion<R, T>` does not merge with that declaration,
 * so the matchers are re-applied here for `tsc`.
 */
declare module "vitest" {
	interface Assertion<R extends void | Promise<void> = void, T = unknown>
		extends TestingLibraryMatchers<T, R> {}

	interface AsymmetricMatchersContaining
		extends TestingLibraryMatchers<unknown, void> {}
}
