/**
 * Start, poll, and read a Cognite Function call.
 * The JS SDK exposes function CRUD but not call/poll/response — those use REST.
 */

import type { CogniteClient } from "@cognite/sdk";
import { withCdfConcurrency } from "@/utils/cdfRequest";

export type FunctionCallStatus = "Running" | "Completed" | "Failed" | "Timeout";

export interface FunctionCallClient {
  call(
    data: Record<string, unknown>,
    opts?: {
      signal?: AbortSignal;
      onStatus?: (status: FunctionCallStatus) => void;
      onLog?: (message: string) => void;
    },
  ): Promise<unknown>;
}

export interface FunctionCallClientOptions {
  functionExternalId: string;
  pollIntervalMs: number;
  timeoutMs: number;
  maxConsecutivePollErrors: number;
  /** Prefix for error messages, for example "Podcast function". */
  label: string;
  sleep?: (ms: number) => Promise<void>;
}

type FunctionCallRecord = {
  id: number;
  status: FunctionCallStatus;
  functionId: number;
  error?: string;
};

type FunctionCallResponseBody = {
  functionId: number;
  callId: number;
  response?: unknown;
};

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw new DOMException("The operation was aborted", "AbortError");
  }
}

export class CogniteFunctionCallClient implements FunctionCallClient {
  private readonly sleep: (ms: number) => Promise<void>;

  constructor(
    private readonly client: CogniteClient,
    private readonly options: FunctionCallClientOptions,
  ) {
    this.sleep = options.sleep ?? defaultSleep;
  }

  async call(
    data: Record<string, unknown>,
    opts?: {
      signal?: AbortSignal;
      onStatus?: (status: FunctionCallStatus) => void;
      onLog?: (message: string) => void;
    },
  ): Promise<unknown> {
    throwIfAborted(opts?.signal);
    const functionId = await this.resolveFunctionId();
    const nonce = await this.createSessionNonce();
    const started = await this.startCall(functionId, nonce, data);
    opts?.onStatus?.("Running");
    await this.waitForCompletion(functionId, started.id, opts);
    const responseBody = await this.fetchCallResponse(functionId, started.id);
    return responseBody.response;
  }

  private async resolveFunctionId(): Promise<number> {
    const [fn] = await withCdfConcurrency(() =>
      this.client.functions.retrieve([
        { externalId: this.options.functionExternalId },
      ]),
    );
    if (!fn?.id) {
      throw new Error(
        `${this.options.label} not found: ${this.options.functionExternalId}`,
      );
    }
    return fn.id;
  }

  private async createSessionNonce(): Promise<string> {
    try {
      const [session] = await withCdfConcurrency(() =>
        this.client.sessions.create([{ tokenExchange: true }]),
      );
      if (session?.nonce) return session.nonce;
    } catch {
      // Some IdPs do not support token exchange; fall back to one-shot.
    }

    const [session] = await withCdfConcurrency(() =>
      this.client.sessions.create([{ oneshotTokenExchange: true }]),
    );
    if (!session?.nonce) {
      throw new Error(
        `Failed to create a CDF session for the ${this.options.label.toLowerCase()}`,
      );
    }
    return session.nonce;
  }

  private async startCall(
    functionId: number,
    nonce: string,
    data: Record<string, unknown>,
  ): Promise<FunctionCallRecord> {
    const url = `/api/v1/projects/${this.client.project}/functions/${functionId}/call`;
    const response = await withCdfConcurrency(() =>
      this.client.post<FunctionCallRecord>(url, {
        data: { nonce, data },
      }),
    );
    const call = response.data;
    if (!call?.id) {
      throw new Error(`${this.options.label} call did not return a call id`);
    }
    return call;
  }

  private async waitForCompletion(
    functionId: number,
    callId: number,
    opts?: {
      signal?: AbortSignal;
      onStatus?: (status: FunctionCallStatus) => void;
      onLog?: (message: string) => void;
    },
  ): Promise<void> {
    const deadline = Date.now() + this.options.timeoutMs;
    let consecutiveErrors = 0;
    let seenLogLines = 0;

    while (Date.now() < deadline) {
      throwIfAborted(opts?.signal);
      let call: FunctionCallRecord;
      try {
        call = await this.retrieveCall(functionId, callId);
        consecutiveErrors = 0;
      } catch (err) {
        if (err instanceof DOMException && err.name === "AbortError") throw err;
        consecutiveErrors++;
        if (consecutiveErrors >= this.options.maxConsecutivePollErrors) {
          throw new Error(
            `Failed to retrieve ${this.options.label.toLowerCase()} status after ${this.options.maxConsecutivePollErrors} consecutive attempts: ${err instanceof Error ? err.message : String(err)}`,
          );
        }
        await this.sleep(this.options.pollIntervalMs);
        continue;
      }

      if (opts?.onLog) {
        seenLogLines = await this.emitNewLogs(
          functionId,
          callId,
          seenLogLines,
          opts.onLog,
        );
      }

      opts?.onStatus?.(call.status);
      if (call.status === "Completed") return;
      if (call.status === "Failed" || call.status === "Timeout") {
        throw new Error(
          call.error ||
            `${this.options.label} call ${call.status.toLowerCase()}`,
        );
      }
      await this.sleep(this.options.pollIntervalMs);
    }
    throw new Error(
      `${this.options.label} call timed out after ${Math.round(this.options.timeoutMs / 1000)}s`,
    );
  }

  private async emitNewLogs(
    functionId: number,
    callId: number,
    seenLogLines: number,
    onLog: (message: string) => void,
  ): Promise<number> {
    try {
      const url = `/api/v1/projects/${this.client.project}/functions/${functionId}/calls/${callId}/logs`;
      const response = await withCdfConcurrency(() =>
        this.client.get<{ items?: { message?: string }[] }>(url),
      );
      const items = response.data?.items ?? [];
      for (const item of items.slice(seenLogLines)) {
        if (item.message) onLog(item.message);
      }
      return Math.max(seenLogLines, items.length);
    } catch {
      return seenLogLines;
    }
  }

  private async retrieveCall(
    functionId: number,
    callId: number,
  ): Promise<FunctionCallRecord> {
    const url = `/api/v1/projects/${this.client.project}/functions/${functionId}/calls/${callId}`;
    const response = await withCdfConcurrency(() =>
      this.client.get<FunctionCallRecord>(url),
    );
    return response.data;
  }

  private async fetchCallResponse(
    functionId: number,
    callId: number,
  ): Promise<FunctionCallResponseBody> {
    const url = `/api/v1/projects/${this.client.project}/functions/${functionId}/calls/${callId}/response`;
    const response = await withCdfConcurrency(() =>
      this.client.get<FunctionCallResponseBody>(url),
    );
    return response.data;
  }
}
