/**
 * AtlasSession — stateful conversation with validated tool execution.
 *
 * Manages the cursor, converts AtlasTool[] to API actions format,
 * validates tool arguments with TypeBox Value, and runs the tool execution loop.
 */

import { AtlasClient } from './client';
import { buildWrapper, formatOutput } from './python';
import { validateToolArguments } from './validation';
import type {
  AtlasTool,
  AtlasToolResult,
  AtlasResponse,
  AtlasSessionConfig,
  AgentToolConfig,
  PythonRuntime,
  StreamCallbacks,
  ChatPayload,
  ApiToolDefinition,
  RawAction,
  RawClientToolAction,
  RawAgentResponse,
  RequestMessage,
  ClientToolActionMessage,
  ToolCall,
} from './types';

const MAX_TURNS = 50;

/**
 * Parse raw arguments from the API (string or object) into a plain object.
 */
function parseArguments(
  raw: string | Record<string, unknown>,
): Record<string, unknown> {
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw);
    } catch {
      return {};
    }
  }
  return raw || {};
}

/**
 * Extract all actions from all messages in a raw response.
 */
function extractActions(raw: RawAgentResponse): RawAction[] {
  return raw.response.messages.flatMap((msg) => msg.actions ?? []);
}

/**
 * Extract server-side tool calls from reasoning blocks in a raw response.
 * Shape: messages[n].reasoning[n].data[n].toolCall
 */
function extractServerToolCalls(raw: RawAgentResponse): ToolCall[] {
  const calls: ToolCall[] = [];
  for (const msg of raw.response.messages) {
    for (const entry of msg.reasoning ?? []) {
      const data = (entry as { data?: unknown[] }).data;
      if (!Array.isArray(data)) continue;
      for (const item of data) {
        const tc = (item as { toolCall?: Record<string, unknown> }).toolCall;
        if (!tc) continue;
        const result = tc.result as Record<string, unknown> | undefined;
        calls.push({
          name: String(tc.name ?? ''),
          toolType: tc.toolType !== undefined ? String(tc.toolType) : undefined,
          input: tc.input,
          output: result?.output !== undefined ? String(result.output) : undefined,
          details: result?.result,
        });
      }
    }
  }
  return calls;
}

/**
 * Execute a server-declared Python tool via the pythonRuntime.
 */
async function executePythonTool(
  action: RawClientToolAction,
  toolConfig: AgentToolConfig,
  pythonRuntime: PythonRuntime | undefined,
  callbacks?: StreamCallbacks,
): Promise<{ result: AtlasToolResult; followup: ClientToolActionMessage }> {
  const toolName = action.clientTool.name;
  const pythonCode = String(toolConfig.configuration?.pythonCode ?? '').trim();

  const fail = (msg: string) => {
    const result: AtlasToolResult = { output: msg };
    callbacks?.onToolEnd?.(toolName, result);
    return { result, followup: createActionReply(action.actionId, result.output) };
  };

  if (!pythonCode) {
    return fail(`ERROR: pythonCode is empty in tool configuration for "${toolName}"`);
  }
  if (!pythonRuntime) {
    return fail(`ERROR: pythonRuntime is required to execute Python tool "${toolName}" but was not provided`);
  }

  try {
    const argsJson = JSON.stringify(parseArguments(action.clientTool.arguments));
    const wrapper = buildWrapper(pythonCode, argsJson);
    const raw = await pythonRuntime.runCodeAsync(wrapper);
    const result: AtlasToolResult = { output: formatOutput(raw) };
    callbacks?.onToolEnd?.(toolName, result);
    return { result, followup: createActionReply(action.actionId, result.output) };
  } catch (err) {
    return fail(`ERROR: ${err instanceof Error ? err.message : String(err)}`);
  }
}

/**
 * Execute a client tool action: validate args, run the tool, return result + followup.
 *
 * Dispatch order:
 *   1. Pre-registered JS tool in the tools Map (client-declared tools)
 *   2. Server-defined Python tool — fetch config from agent API, run via pythonRuntime
 */
async function executeClientTool(
  action: RawClientToolAction,
  tools: Map<string, AtlasTool>,
  fetchToolConfig: (name: string) => Promise<AgentToolConfig | null>,
  pythonRuntime: PythonRuntime | undefined,
  callbacks?: StreamCallbacks,
): Promise<{ result: AtlasToolResult; followup: ClientToolActionMessage }> {
  const toolName = action.clientTool.name;

  callbacks?.onToolStart?.(toolName);

  // 1. Pre-registered JS tool
  const tool = tools.get(toolName);
  if (tool) {
    const args = parseArguments(action.clientTool.arguments);
    let validatedArgs: unknown;
    try {
      validatedArgs = validateToolArguments(toolName, tool.parameters, args);
    } catch (err) {
      const errorOutput = err instanceof Error ? err.message : String(err);
      const result: AtlasToolResult = { output: `ERROR: ${errorOutput}` };
      callbacks?.onToolEnd?.(toolName, result);
      return { result, followup: createActionReply(action.actionId, result.output) };
    }
    const result = await tool.execute(validatedArgs);
    callbacks?.onToolEnd?.(toolName, result);
    return { result, followup: createActionReply(action.actionId, result.output) };
  }

  // 2. Server-declared tool — look up config from agent API
  const toolConfig = await fetchToolConfig(toolName);
  if (toolConfig?.type === 'runPythonCode') {
    return executePythonTool(action, toolConfig, pythonRuntime, callbacks);
  }

  const result: AtlasToolResult = { output: `Unknown client tool: ${toolName}` };
  callbacks?.onToolEnd?.(toolName, result);
  return { result, followup: createActionReply(action.actionId, result.output) };
}

/**
 * Stateful conversation session with validated tool execution.
 */
export class AtlasSession {
  private cursor?: string;
  private readonly client: AtlasClient;
  private readonly agentExternalId: string;
  private readonly tools: Map<string, AtlasTool>;
  private readonly apiActionsOrUndefined: ApiToolDefinition[] | undefined;
  private readonly pythonRuntime: PythonRuntime | undefined;
  private readonly getAppContext: (() => string | undefined) | undefined;
  /** Cached tool configs fetched from the agent API (populated lazily on first Python tool call). */
  private cachedAgentTools: AgentToolConfig[] | undefined;

  constructor(config: AtlasSessionConfig) {
    this.client = new AtlasClient(config.client);
    this.agentExternalId = config.agentExternalId;
    this.tools = new Map((config.tools || []).map((t) => [t.name, t]));
    this.pythonRuntime = config.pythonRuntime;
    this.getAppContext = config.getAppContext;

    // Inline toApiToolDefinition (only used here)
    const apiActions: ApiToolDefinition[] = (config.tools || []).map((tool) => ({
      type: 'clientTool' as const,
      clientTool: {
        name: tool.name,
        description: tool.description,
        parameters: tool.parameters,
      },
    }));
    this.apiActionsOrUndefined = apiActions.length > 0 ? apiActions : undefined;
  }

  /**
   * Send a user message. Handles the full tool execution loop internally.
   */
  async send(
    message: string,
    callbacks?: StreamCallbacks,
    signal?: AbortSignal,
  ): Promise<AtlasResponse> {
    const allToolCalls: ToolCall[] = [];

    const appContext = this.getAppContext?.();
    let payload: ChatPayload = {
      agentExternalId: this.agentExternalId,
      messages: [{ role: 'user', content: { type: 'text', text: message } }],
      actions: this.apiActionsOrUndefined,
      stream: true,
      ...(this.cursor && { cursor: this.cursor }),
      ...(appContext && { contextInformation: { appContext } }),
    };

    for (let turn = 0; turn < MAX_TURNS; turn++) {
      const raw = await this.client.post(payload, this.agentExternalId, callbacks, signal);
      const response = raw.response;

      if (response.type !== 'result') {
        throw new Error(`Unexpected response type: ${response.type}`);
      }

      if (response.cursor) {
        this.cursor = response.cursor;
      }

      // Collect server-side tool calls from reasoning blocks in this turn
      allToolCalls.push(...extractServerToolCalls(raw));

      const actions = extractActions(raw);

      // No actions → conversation turn is done
      if (actions.length === 0) {
        const text = response.messages?.[0]?.content?.text || '';
        return { text, cursor: this.cursor, toolCalls: allToolCalls, raw };
      }

      // Execute actions and build follow-up messages
      const followups: RequestMessage[] = [];

      for (const action of actions) {
        if (action.type === 'clientTool') {
          const { result, followup } = await executeClientTool(
            action,
            this.tools,
            (name) => this.fetchToolConfig(name),
            this.pythonRuntime,
            callbacks,
          );
          allToolCalls.push({ name: action.clientTool.name, output: result.output, details: result.details });
          followups.push(followup);
        } else if (action.type === 'toolConfirmation') {
          const toolName = action.toolConfirmation?.toolName;
          if (toolName) callbacks?.onToolStart?.(toolName);
          followups.push({
            role: 'action',
            type: 'toolConfirmation',
            actionId: action.actionId,
            status: 'ALLOW',
          });
        }
      }

      if (followups.length === 0) {
        const text = response.messages?.[0]?.content?.text || '';
        return { text, cursor: this.cursor, toolCalls: allToolCalls, raw };
      }

      // Prepare the next turn — re-evaluate appContext so it reflects state changes from tool execution
      const updatedAppContext = this.getAppContext?.();
      payload = {
        agentExternalId: this.agentExternalId,
        messages: followups,
        actions: this.apiActionsOrUndefined,
        stream: true,
        cursor: this.cursor,
        ...(updatedAppContext && { contextInformation: { appContext: updatedAppContext } }),
      };
    }

    throw new Error(`Max tool execution turns reached (${MAX_TURNS})`);
  }

  /**
   * Fetch a tool's config from the agent API (lazy, cached per session).
   * Used as a fallback when a clientTool action arrives for an unregistered tool.
   */
  private async fetchToolConfig(toolName: string): Promise<AgentToolConfig | null> {
    if (!this.cachedAgentTools) {
      const agent = await this.client.getAgentById(this.agentExternalId);
      this.cachedAgentTools = agent?.tools ?? [];
    }
    return this.cachedAgentTools.find((t) => t.name === toolName) ?? null;
  }

  /** Reset the session cursor (start a fresh conversation). */
  reset(): void {
    this.cursor = undefined;
  }

  /** Get the current cursor value. */
  getCursor(): string | undefined {
    return this.cursor;
  }

  /** Set the cursor (e.g. when restoring a conversation). */
  setCursor(cursor: string): void {
    this.cursor = cursor;
  }
}

function createActionReply(
  actionId: string,
  text: string,
): ClientToolActionMessage {
  return {
    role: 'action',
    type: 'clientTool',
    actionId,
    content: { type: 'text', text },
    data: [],
  };
}
