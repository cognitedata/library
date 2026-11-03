/**
 * QueuedTaskRunner (semaphore) for limiting concurrent CDF API requests.
 * See dm-limits-and-best-practices skill.
 */

import { getAppConfig } from "@/config/appConfig";

class AbortError extends Error {
  public constructor(message = "Aborted") {
    super(message);
    this.name = "AbortError";
  }
}

type PendingTask = {
  resolve: (result: unknown) => void;
  reject: (error: unknown) => void;
  fn: () => Promise<unknown>;
  key?: string;
};

class QueuedTaskRunner {
  private pendingTasks: PendingTask[] = [];
  private currentPendingTasks = 0;
  private readonly maxConcurrentTasks: number;

  public constructor(maxConcurrentTasks: number) {
    this.maxConcurrentTasks = maxConcurrentTasks;
  }

  public schedule<Result>(
    fn: () => Promise<Result>,
    options: { key?: string } = {},
  ): Promise<Result> {
    this.startTrackingTime();

    return new Promise((resolve, reject) => {
      if (options.key !== undefined) {
        this.pendingTasks
          .filter((task) => task.key === options.key)
          .forEach((task) => {
            task.reject(new AbortError());
          });

        this.pendingTasks = this.pendingTasks.filter(
          (task) => task.key !== options.key,
        );
      }

      this.pendingTasks.push({
        resolve: (result) => {
          resolve(result as Result);
        },
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
      const result = await fn();
      resolve(result);
    } catch (e) {
      reject(e);
    } finally {
      this.currentPendingTasks--;
      this.tick();
      void this.attemptConsumingNextTask();
    }
  }

  public clearQueue = (): void => {
    this.pendingTasks = [];
  };

  private startTime: number | null = null;

  private startTrackingTime = (): void => {
    if (this.startTime === null) {
      this.startTime = performance.now();
    }
  };

  private tick = (): void => {
    if (this.pendingTasks.length === 0) {
      this.startTime = null;
    }
  };
}

let cdfRunner: QueuedTaskRunner | undefined;
let deleteRunner: QueuedTaskRunner | undefined;

/** Align with Cognite JS client HTTP concurrency (sdk-core); DMS limits are separate. */
export function getCdfTaskRunner(): QueuedTaskRunner {
  if (!cdfRunner) {
    cdfRunner = new QueuedTaskRunner(getAppConfig().limits.concurrency.default);
  }
  return cdfRunner;
}

export function getCdfDeleteTaskRunner(): QueuedTaskRunner {
  if (!deleteRunner) {
    deleteRunner = new QueuedTaskRunner(
      getAppConfig().limits.concurrency.delete,
    );
  }
  return deleteRunner;
}
