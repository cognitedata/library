/**
 * useAtlasChat — plug-and-play React hook for Atlas agent conversations.
 *
 * Manages session lifecycle, message state, streaming, and abort support.
 * Separate entry point from core for tree-shaking.
 */

import { useState, useCallback, useRef, useEffect, useMemo } from 'react';
import type { CogniteClient } from '@cognite/sdk';
import { AtlasSession } from './session';
import type { AtlasTool, AtlasResponse, PythonRuntime, ToolCall } from './types';

// ============================================================================
// Types
// ============================================================================

export interface ChatMessage<TContext = unknown> {
  id: string;
  role: 'user' | 'assistant';
  text: string;
  timestamp: Date;
  isStreaming?: boolean;
  /** Tool calls (client-side and server-side) attached to this message */
  toolCalls?: ToolCall[];
  /** App-specific context data, populated via onResponse */
  context?: TContext;
}

export interface UseAtlasChatOptions<TContext = unknown> {
  client: CogniteClient | null;
  agentExternalId: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  tools?: AtlasTool<any, any>[];
  /** Opt-in Python runtime (e.g. Pyodide) — only needed for agents that use Python tools. */
  pythonRuntime?: PythonRuntime;
  /** Messages to show on initial render (e.g. welcome message) */
  initialMessages?: ChatMessage<TContext>[];
  /** Called when a full response is received. Return context to merge into the assistant message. */
  onResponse?: (response: AtlasResponse) => TContext | void;
  /** Called before each send to inject app-level context (e.g. current todo state) into the request. */
  getAppContext?: () => string | undefined;
}

export interface UseAtlasChatReturn<TContext = unknown> {
  /** All messages in the conversation */
  messages: ChatMessage<TContext>[];
  /** Send a user message — automatically creates user + assistant messages, handles streaming */
  send: (text: string) => Promise<void>;
  /** True while the agent is responding */
  isStreaming: boolean;
  /** Current progress text (e.g. "Agent thinking", "Executing: render_widget") */
  progress: string | null;
  /** Error message if last send failed */
  error: string | null;
  /** Clear all messages and reset the session */
  reset: () => void;
  /** Cancel the current streaming response */
  abort: () => void;
  /** Replace messages (e.g. loading conversation history) */
  setMessages: (messages: ChatMessage<TContext>[]) => void;
}

// ============================================================================
// Helpers
// ============================================================================

let messageCounter = 0;

function generateId(): string {
  return `msg-${Date.now()}-${++messageCounter}`;
}

// ============================================================================
// Hook
// ============================================================================

export function useAtlasChat<TContext = unknown>(
  options: UseAtlasChatOptions<TContext>,
): UseAtlasChatReturn<TContext> {
  const { client, agentExternalId, tools, pythonRuntime, initialMessages, onResponse, getAppContext } = options;

  const [messages, setMessages] = useState<ChatMessage<TContext>[]>(initialMessages ?? []);
  const [isStreaming, setIsStreaming] = useState(false);
  const [progress, setProgress] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const sessionRef = useRef<AtlasSession | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const agentExternalIdRef = useRef(agentExternalId);
  const toolsRef = useRef(tools);
  const pythonRuntimeRef = useRef(pythonRuntime);
  const getAppContextRef = useRef(getAppContext);

  // Keep refs updated (array/object identity may change between renders)
  toolsRef.current = tools;
  pythonRuntimeRef.current = pythonRuntime;
  getAppContextRef.current = getAppContext;

  // Stable wrapper — always delegates to the latest getAppContext via ref.
  // Passed to AtlasSession once at creation so the session is never stale.
  const stableGetAppContext = useMemo(
    () => () => getAppContextRef.current?.(),
    [],
  );

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  const getSession = useCallback((): AtlasSession | null => {
    if (!client) return null;

    if (!sessionRef.current || agentExternalIdRef.current !== agentExternalId) {
      sessionRef.current = new AtlasSession({
        client,
        agentExternalId,
        tools: toolsRef.current,
        pythonRuntime: pythonRuntimeRef.current,
        getAppContext: stableGetAppContext,
      });
      agentExternalIdRef.current = agentExternalId;
    }

    return sessionRef.current;
  }, [client, agentExternalId]);

  const send = useCallback(
    async (text: string) => {
      const session = getSession();
      if (!session || isStreaming) return;

      setError(null);
      setIsStreaming(true);
      setProgress('Agent thinking');

      // Add user message
      const userMessage: ChatMessage<TContext> = {
        id: generateId(),
        role: 'user',
        text,
        timestamp: new Date(),
      };

      const assistantId = generateId();
      let accumulatedText = '';
      let assistantCreated = false;

      setMessages((prev) => [...prev, userMessage]);

      const abortController = new AbortController();
      abortRef.current = abortController;

      // ---- Helpers scoped to this send() call ----

      /** Update a single message by id */
      const updateMsg = (id: string, updates: Partial<ChatMessage<TContext>>) => {
        setMessages((prev) =>
          prev.map((m) => (m.id === id ? { ...m, ...updates } : m)),
        );
      };

      /** Finalize the assistant message — update if already created, otherwise add a new one */
      const finalizeAssistant = (fields: Partial<ChatMessage<TContext>>) => {
        if (assistantCreated) {
          updateMsg(assistantId, { isStreaming: false, ...fields });
        } else {
          setMessages((prev) => [
            ...prev,
            {
              id: assistantId,
              role: 'assistant' as const,
              timestamp: new Date(),
              text: '',
              isStreaming: false,
              ...fields,
            },
          ]);
        }
      };

      try {
        const response = await session.send(
          text,
          {
            onProgress: (progressText) => {
              setProgress(progressText);
            },
            onChunk: (chunk) => {
              if (!assistantCreated) {
                assistantCreated = true;
                setMessages((prev) => [
                  ...prev,
                  {
                    id: assistantId,
                    role: 'assistant' as const,
                    text: chunk,
                    timestamp: new Date(),
                    isStreaming: true,
                  },
                ]);
              }
              accumulatedText += chunk;
              updateMsg(assistantId, { text: accumulatedText });
            },
            onToolStart: (toolName) => {
              setProgress(`Executing: ${toolName}`);
            },
          },
          abortController.signal,
        );

        // Finalize assistant message
        finalizeAssistant({
          text:
            response.text ||
            (assistantCreated
              ? undefined
              : "I apologize, but I couldn't generate a response. Please try again."),
          toolCalls:
            response.toolCalls.length > 0
              ? response.toolCalls
              : undefined,
        });

        // Let the app attach context (e.g. applications) to the message
        const ctx = onResponse?.(response);
        if (ctx !== undefined) {
          updateMsg(assistantId, { context: ctx });
        }
      } catch (err) {
        if ((err as Error).name === 'AbortError') {
          // Cancelled by user — finalize any in-progress message
          if (assistantCreated) {
            updateMsg(assistantId, { isStreaming: false });
          }
        } else {
          const errorText =
            err instanceof Error ? err.message : 'Unknown error';
          setError(errorText);
          finalizeAssistant({ text: `Error: ${errorText}` });
        }
      } finally {
        setIsStreaming(false);
        setProgress(null);
        abortRef.current = null;
      }
    },
    [getSession, isStreaming, onResponse],
  );

  const reset = useCallback(() => {
    abortRef.current?.abort();
    setMessages(initialMessages ?? []);
    setIsStreaming(false);
    setProgress(null);
    setError(null);
    sessionRef.current = null;
  }, [initialMessages]);

  const abort = useCallback(() => {
    abortRef.current?.abort();
  }, []);

  return {
    messages,
    send,
    isStreaming,
    progress,
    error,
    reset,
    abort,
    setMessages,
  };
}

