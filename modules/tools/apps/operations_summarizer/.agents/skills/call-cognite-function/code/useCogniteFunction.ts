/**
 * useCogniteFunction — React Query hook for calling any Cognite CDF Function.
 *
 * useMutation is the right primitive here: POST /functions/{id}/call is not
 * idempotent (every call spins a new execution and counts against the
 * 100-concurrent-calls limit), so we never want refetch-on-focus or
 * remount-refetch semantics. React Query owns isPending / error / data /
 * reset, which removes the hand-rolled state and AbortController plumbing.
 *
 * Requires a <QueryClientProvider> above in the tree.
 * Adjust the useCdfClient import to your project's SDK context.
 *
 * Usage:
 *   const Output = z.object({ success: z.boolean(), report: z.string() });
 *
 *   const { call, isLoading, error, result, reset } = useCogniteFunction(
 *     12345678,
 *     { outputSchema: Output },
 *   );
 *
 *   // in an event handler:
 *   const output = await call({ reportId: "abc" }); // typed from Output
 *
 * If a specific function is a pure read and you want caching, skip this hook
 * and wrap the service in useQuery at the call site:
 *
 *   useQuery({
 *     queryKey: ["cdf-fn", functionId, input],
 *     queryFn: () => callCogniteFunction(sdk, functionId, input, { outputSchema }),
 *   });
 */

import { useMutation } from "@tanstack/react-query";
// Adjust this import to your project's SDK context:
import { useCdfClient } from "@/contexts/CdfClientContext";
import {
  callCogniteFunction,
  type CallCogniteFunctionOptions,
} from "./cogniteFunctionService";

export interface UseCogniteFunctionResult<TInput, TOutput> {
  /** Invoke the function. Resolves with the (schema-validated) output, or throws (SDK error, ZodError, or Error). */
  call: (data: TInput) => Promise<TOutput>;
  isLoading: boolean;
  error: string | null;
  result: TOutput | null;
  reset: () => void;
}

/**
 * @param functionId Numeric CDF function ID.
 * @param opts       Output schema and polling config (see CallCogniteFunctionOptions).
 */
export function useCogniteFunction<TInput, TOutput = unknown>(
  functionId: string | number,
  opts: Omit<CallCogniteFunctionOptions<TOutput>, "signal"> = {},
): UseCogniteFunctionResult<TInput, TOutput> {
  const { sdk } = useCdfClient();

  const mutation = useMutation({
    mutationFn: (data: TInput) => {
      if (!sdk) throw new Error("CDF SDK is not available");
      return callCogniteFunction<TOutput>(sdk, functionId, data, opts);
    },
  });

  return {
    call: mutation.mutateAsync,
    isLoading: mutation.isPending,
    error: mutation.error ? mutation.error.message : null,
    result: mutation.data ?? null,
    reset: mutation.reset,
  };
}
