/**
 * Runtime validation for tool arguments using TypeBox Value (CSP-safe; no eval).
 */

import type { TSchema } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

/**
 * Validate and coerce tool arguments against a TypeBox / JSON Schema.
 * Returns coerced args on success. Throws a formatted error on validation failure.
 */
export function validateToolArguments(toolName: string, schema: TSchema, args: unknown): unknown {
  const coerced = Value.Convert(schema, args);
  if (Value.Check(schema, coerced)) return coerced;

  const errors = [...Value.Errors(schema, coerced)]
    .map((e) => `${e.path || "/"} ${e.message}`)
    .join("; ");
  throw new Error(`Tool "${toolName}" received invalid arguments: ${errors}`);
}
