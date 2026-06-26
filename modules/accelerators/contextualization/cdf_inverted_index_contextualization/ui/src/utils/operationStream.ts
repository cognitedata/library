export class OperationCancelledError extends Error {
  constructor() {
    super("Operation cancelled");
    this.name = "OperationCancelledError";
  }
}

export type OperationStreamHandlers = {
  onLog: (line: string) => void;
  onResult: (data: unknown) => void;
  onError: (detail: string) => void;
};

type StreamEvent =
  | { event: "log"; line: string }
  | { event: "result"; data: unknown }
  | { event: "error"; detail: string }
  | { event: "exit"; code: number };

function parseErrorDetail(text: string): string {
  if (!text) return "Request failed";
  try {
    const data = JSON.parse(text) as { detail?: unknown };
    if (typeof data.detail === "string") return data.detail;
  } catch {
    /* plain text */
  }
  return text;
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

export async function consumeOperationStream(
  url: string,
  body: unknown,
  handlers: OperationStreamHandlers,
  signal?: AbortSignal
): Promise<number> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });

  if (!res.ok) {
    throw new Error(parseErrorDetail(await res.text()));
  }
  if (!res.body) {
    throw new Error("No response body from operation stream");
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let exitCode = 1;

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.trim()) continue;
        const event = JSON.parse(line) as StreamEvent;
        if (event.event === "log") handlers.onLog(String(event.line));
        else if (event.event === "result") handlers.onResult(event.data);
        else if (event.event === "error") handlers.onError(String(event.detail));
        else if (event.event === "exit") exitCode = Number(event.code) || 0;
      }
    }

    if (buffer.trim()) {
      const event = JSON.parse(buffer) as StreamEvent;
      if (event.event === "log") handlers.onLog(String(event.line));
      else if (event.event === "result") handlers.onResult(event.data);
      else if (event.event === "error") handlers.onError(String(event.detail));
      else if (event.event === "exit") exitCode = Number(event.code) || 0;
    }
  } catch (error) {
    if (isAbortError(error)) {
      throw new OperationCancelledError();
    }
    throw error;
  }

  return exitCode;
}
