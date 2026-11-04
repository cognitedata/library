import { cdfTaskRunner } from "../utils/semaphore";

/** Schedules a CDF call so concurrency and 429 retries stay in one place. */
export type CdfSchedule = <T>(fn: () => Promise<T>) => Promise<T>;

export const defaultCdfSchedule: CdfSchedule = (fn) =>
	cdfTaskRunner.schedule(fn);
