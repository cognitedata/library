/**
 * Python execution primitives for Pyodide-based tool execution.
 *
 * buildWrapper  — combines tool code + args into a single runnable Python snippet
 * formatOutput  — serialises the Pyodide result into a string for the agent
 */

/**
 * Unicode-safe base64 encoding.
 * Encodes to UTF-8 bytes first so non-ASCII characters survive round-tripping.
 */
function toBase64(str: string): string {
  const bytes = new TextEncoder().encode(str);
  let binary = '';
  for (const b of bytes) {
    binary += String.fromCharCode(b);
  }
  return btoa(binary);
}

/**
 * Build a Python wrapper that loads the tool code, parses base64-encoded args,
 * and calls handle(**args). Supports both sync and async handle functions.
 */
export function buildWrapper(code: string, argsJson: string): string {
  const encoded = toBase64(argsJson);
  return `
import json, base64, inspect
_args = json.loads(base64.b64decode("${encoded}").decode("utf-8"))
${code}
async def _exec():
    if "handle" not in globals():
        return {"_error": "No handle() function found in tool code"}
    return await handle(**_args) if inspect.iscoroutinefunction(handle) else handle(**_args)
_r = await _exec()
json.dumps(_r) if _r is not None and not isinstance(_r, str) else _r
`.trimStart();
}

/** Stringify Python result into tool output text. */
export function formatOutput(raw: unknown): string {
  if (raw == null) return '';
  if (typeof raw === 'string') return raw;
  return JSON.stringify(raw);
}
