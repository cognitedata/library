import type { CogniteClient } from "@cognite/sdk";
import { cachedTransformationJobs } from "./transformations-cache";
import { toTimestamp } from "@/shared/time-utils";

type TxJob = {
  id?: number | string;
  transformationId?: number | string;
  status?: string;
  startedTime?: number;
  finishedTime?: number;
  createdTime?: number;
  error?: string;
};

type TxJobsResponse = {
  data?: {
    items?: TxJob[];
    nextCursor?: string | null;
  };
};

type TxJobsSdk = Pick<CogniteClient, "project" | "get">;

type StoredState = {
  jobs: TxJob[];
  jobsById: Set<string>;
  cursor?: string;
  coverageStart: number | null;
  lastFetched: number;
};

const JOBS_PAGE_SIZE = "50";
const TRANSFORMATION_JOBS_PARALLEL_REQUESTS = 3;
const CACHE_REFRESH_INTERVAL_MS = 30_000;
const stateByTransformation = new Map<string, StoredState>();
const inFlightByTransformation = new Map<string, Promise<StoredState>>();
let activeRequests = 0;
const requestQueue: Array<() => void> = [];

function stateKey(project: string, transformationId: string): string {
  return `${project}\x1f${transformationId}`;
}

function jobIdentity(job: TxJob): string {
  if (job.id != null) return String(job.id);
  return `${job.startedTime ?? ""}|${job.finishedTime ?? ""}|${job.status ?? ""}`;
}

function jobTime(job: TxJob): number | null {
  const started = toTimestamp(job.startedTime);
  if (started != null) return started;
  const finished = toTimestamp(job.finishedTime);
  if (finished != null) return finished;
  const created = toTimestamp(job.createdTime);
  if (created != null) return created;
  return null;
}

function sortJobsDesc(a: TxJob, b: TxJob): number {
  return (jobTime(b) ?? Number.NEGATIVE_INFINITY) - (jobTime(a) ?? Number.NEGATIVE_INFINITY);
}

function recomputeCoverageStart(jobs: TxJob[]): number | null {
  let min: number | null = null;
  for (const job of jobs) {
    const t = jobTime(job);
    if (t == null) continue;
    if (min == null || t < min) min = t;
  }
  return min;
}

function mergeJobs(state: StoredState, incoming: TxJob[]): void {
  for (const job of incoming) {
    const id = jobIdentity(job);
    if (state.jobsById.has(id)) continue;
    state.jobsById.add(id);
    state.jobs.push(job);
  }
  state.jobs.sort(sortJobsDesc);
  state.coverageStart = recomputeCoverageStart(state.jobs);
}

async function fetchPage(
  sdk: TxJobsSdk,
  transformationId: string,
  limit: string,
  cursor?: string
): Promise<{ items: TxJob[]; nextCursor?: string }> {
  const response = (await withJobsRequestSlot(async () =>
    (await cachedTransformationJobs(sdk, transformationId, limit, cursor)) as TxJobsResponse
  )) as TxJobsResponse;
  return {
    items: response.data?.items ?? [],
    nextCursor:
      response.data?.nextCursor != null && String(response.data.nextCursor).trim() !== ""
        ? String(response.data.nextCursor)
        : undefined,
  };
}

async function withJobsRequestSlot<T>(fn: () => Promise<T>): Promise<T> {
  await acquireRequestSlot();
  try {
    return await fn();
  } finally {
    releaseRequestSlot();
  }
}

function acquireRequestSlot(): Promise<void> {
  if (activeRequests < TRANSFORMATION_JOBS_PARALLEL_REQUESTS) {
    activeRequests += 1;
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    requestQueue.push(() => {
      activeRequests += 1;
      resolve();
    });
  });
}

function releaseRequestSlot(): void {
  activeRequests = Math.max(0, activeRequests - 1);
  const next = requestQueue.shift();
  if (next) next();
}

async function loadWindowIntoState(
  sdk: TxJobsSdk,
  transformationId: string,
  windowStart: number
): Promise<StoredState> {
  const key = stateKey(sdk.project, transformationId);
  const existing = stateByTransformation.get(key);
  const needsRestart = !existing || existing.coverageStart == null || windowStart < existing.coverageStart;
  if (!needsRestart) {
    const now = Date.now();
    if (now - existing.lastFetched > CACHE_REFRESH_INTERVAL_MS) {
      const latestPage = await fetchPage(sdk, transformationId, JOBS_PAGE_SIZE);
      mergeJobs(existing, latestPage.items);
      existing.lastFetched = now;
      stateByTransformation.set(key, existing);
    }
    return existing;
  }
  const probe = await fetchPage(sdk, transformationId, "1");
  const restarted: StoredState = {
    jobs: [],
    jobsById: new Set<string>(),
    cursor: probe.nextCursor,
    coverageStart: null,
    lastFetched: Date.now(),
  };
  mergeJobs(restarted, probe.items);

  const latestTime = restarted.jobs.length > 0 ? jobTime(restarted.jobs[0]) : null;
  if (latestTime == null || windowStart > latestTime) {
    stateByTransformation.set(key, restarted);
    return restarted;
  }

  while (
    restarted.cursor &&
    (restarted.coverageStart == null || restarted.coverageStart > windowStart)
  ) {
    const page = await fetchPage(sdk, transformationId, JOBS_PAGE_SIZE, restarted.cursor);
    restarted.cursor = page.nextCursor;
    mergeJobs(restarted, page.items);
  }

  stateByTransformation.set(key, restarted);
  return restarted;
}

export async function loadTransformationJobsForWindow(args: {
  sdk: TxJobsSdk;
  transformationId: string;
  windowStart: number;
  windowEnd: number;
}): Promise<TxJob[]> {
  const { sdk, transformationId, windowStart, windowEnd } = args;
  const key = stateKey(sdk.project, transformationId);
  const inFlight = inFlightByTransformation.get(key);
  if (inFlight) {
    const state = await inFlight;
    return state.jobs.filter((job) => {
      const t = jobTime(job);
      return t != null && t >= windowStart && t <= windowEnd;
    });
  }
  const request = loadWindowIntoState(sdk, transformationId, windowStart).finally(() => {
    inFlightByTransformation.delete(key);
  });
  inFlightByTransformation.set(key, request);
  const state = await request;
  return state.jobs.filter((job) => {
    const t = jobTime(job);
    return t != null && t >= windowStart && t <= windowEnd;
  });
}

export async function loadLatestTransformationJob(args: {
  sdk: TxJobsSdk;
  transformationId: string;
}): Promise<TxJob | null> {
  const { sdk, transformationId } = args;
  const state = await loadWindowIntoState(sdk, transformationId, Number.MAX_SAFE_INTEGER);
  return state.jobs[0] ?? null;
}
