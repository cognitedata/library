/**
 * Generic Cognite CDF Function caller.
 *
 * Framework-agnostic, no React deps. API responses are validated with Zod;
 * errors propagate as-is: SDK HTTP errors from the client, ZodError on shape
 * mismatch, plain Error for Failed/Timeout status or poll exhaustion.
 *
 * Usage:
 *   const Output = z.object({ success: z.boolean(), report: z.string() });
 *   const result = await callCogniteFunction(client, 12345678, { foo: "bar" }, {
 *     outputSchema: Output, // result is typed z.infer<typeof Output>
 *   });
 */

import { z } from "zod";
import type { CogniteClient } from "@cognite/sdk";

// Response shapes, per CDF API docs — only the fields we use.
const Session = z.object({ items: z.array(z.object({ nonce: z.string() })).nonempty() });
const Call = z.object({ id: z.number() });
const CallStatus = z.object({ status: z.string(), error: z.string().optional() });
const Envelope = z.object({ response: z.unknown() });

export interface CallCogniteFunctionOptions<TOutput = unknown> {
  /** Zod schema for the function's payload; sets the return type and validates at runtime. Omit → unknown. */
  outputSchema?: z.ZodType<TOutput>;
  /** Max poll attempts before giving up. Default 120 (~6 min at 3 s). */
  maxPollAttempts?: number;
  /** Base poll interval in ms (±20 % jitter applied). Default 3000. */
  pollIntervalMs?: number;
  /** Optional AbortSignal to cancel the poll loop. */
  signal?: AbortSignal;
}

const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    if (signal?.aborted) return reject(new DOMException("Aborted", "AbortError"));
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(timer);
      reject(new DOMException("Aborted", "AbortError"));
    });
  });

/**
 * Call a deployed CDF Function: session nonce → invoke → poll → response.
 *
 * @param client     Authenticated CogniteClient.
 * @param functionId Numeric CDF function ID (or its string form), not an external ID.
 * @param data       JSON-serialisable input, passed as the function's `data` arg.
 */
export async function callCogniteFunction<TOutput = unknown>(
  client: CogniteClient,
  functionId: string | number,
  data: unknown,
  opts: CallCogniteFunctionOptions<TOutput> = {},
): Promise<TOutput> {
  const { outputSchema, maxPollAttempts = 120, pollIntervalMs = 3_000, signal } = opts;
  const base = `/api/v1/projects/${client.project}/functions/${functionId}`;

  // 1. Session nonce via token exchange.
  const sessionRes = await client.post(`/api/v1/projects/${client.project}/sessions`, {
    data: { items: [{ tokenExchange: true }] },
  });
  const { items } = Session.parse(sessionRes.data);
  const nonce = items[0].nonce;

  // 2. Invoke.
  const callRes = await client.post(`${base}/call`, { data: { data, nonce } });
  const call = Call.parse(callRes.data);

  // 3. Poll. Only Failed/Timeout/Completed are terminal; any other status
  //    (Running, future values) keeps polling — forward-compatible by design.
  for (let attempt = 0; attempt < maxPollAttempts; attempt++) {
    await sleep(pollIntervalMs * (0.8 + 0.4 * Math.random()), signal); // ±20 % jitter

    const statusRes = await client.get(`${base}/calls/${call.id}`);
    const { status, error } = CallStatus.parse(statusRes.data);

    if (status === "Failed" || status === "Timeout") {
      throw new Error(
        `CDF function ${functionId} (call ${call.id}) ended with status ${status}${error ? `: ${error}` : ""}`,
      );
    }

    if (status === "Completed") {
      // 4. Fetch and validate the response payload.
      const responseRes = await client.get(`${base}/calls/${call.id}/response`);
      const { response } = Envelope.parse(responseRes.data);
      const schema = (outputSchema ?? z.unknown()) as z.ZodType<TOutput>;
      return schema.parse(response);
    }
  }

  throw new Error(
    `CDF function ${functionId} (call ${call.id}) did not complete after ${maxPollAttempts} poll attempts`,
  );
}
