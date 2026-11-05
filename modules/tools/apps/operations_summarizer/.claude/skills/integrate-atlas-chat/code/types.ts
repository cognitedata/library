/**
 * Core types for the Atlas Agent client library.
 *
 * This module is self-contained — no imports from outside the library
 * except external packages (@sinclair/typebox, @cognite/sdk).
 */

import type { TSchema, Static } from '@sinclair/typebox';
import type { CogniteClient } from '@cognite/sdk';

// ============================================================================
// Agent Types
// ============================================================================

/** Configuration for a tool stored in the agent's CDF config. */
export interface AgentToolConfig {
  name: string;
  type: string;
  configuration?: {
    pythonCode?: string;
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

export interface Agent {
  externalId: string;
  name: string;
  description?: string;
  model?: string;
  instructions?: string;
  ownerId?: string;
  tools?: AgentToolConfig[];
  createdTime?: number;
  lastUpdatedTime?: number;
}

// ============================================================================
// Tool Types
// ============================================================================

/** Result from executing a tool */
export interface AtlasToolResult<TDetails = unknown> {
  /** Text sent back to the agent as tool output */
  output: string;
  /** Structured data for the app/UI */
  details?: TDetails;
}

/**
 * A client-side tool the Atlas agent can invoke.
 * TypeBox schema for type-safe params + runtime validation via TypeBox Value.
 */
export interface AtlasTool<
  TParameters extends TSchema = TSchema,
  TDetails = unknown,
> {
  name: string;
  description: string;
  parameters: TParameters;
  execute: (
    args: Static<TParameters>,
  ) => AtlasToolResult<TDetails> | Promise<AtlasToolResult<TDetails>>;
}

/** Minimal interface for executing Python code (e.g. Pyodide). */
export interface PythonRuntime {
  runCodeAsync(code: string): Promise<unknown>;
}

// ============================================================================
// Callback Types
// ============================================================================

export interface StreamCallbacks {
  onProgress?: (text: string) => void;
  onChunk?: (text: string) => void;
  onToolStart?: (toolName: string) => void;
  onToolEnd?: (toolName: string, result: AtlasToolResult) => void;
}

// ============================================================================
// Response Types (app-facing)
// ============================================================================

/** A single tool invocation (client-side or server-side), ready for the UI. */
export interface ToolCall {
  /** Friendly display name, e.g. "Find files" */
  name: string;
  /** Server-side tool type, e.g. "queryKnowledgeGraph" */
  toolType?: string;
  /** Raw input arguments */
  input?: unknown;
  /** Text returned to the agent as tool output */
  output?: string;
  /** Structured data for UI rendering */
  details?: unknown;
}

export interface AtlasResponse {
  text: string;
  cursor?: string;
  toolCalls: ToolCall[];
  raw: RawAgentResponse;
}

// ============================================================================
// Config Types
// ============================================================================

export interface AtlasSessionConfig {
  client: CogniteClient;
  agentExternalId: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  tools?: AtlasTool<any, any>[];
  /** Opt-in Python runtime (e.g. Pyodide) — required only for agents that use Python tools. */
  pythonRuntime?: PythonRuntime;
  /** Called before each send to inject app-level context (e.g. current todo state) into the request. */
  getAppContext?: () => string | undefined;
}

// ============================================================================
// API Shared Primitives (maps to cog_ai…session.common)
// ============================================================================

/** Maps to AgentContentDTO */
export interface AgentContent {
  type: string;
  text?: string;
}

/** Maps to InstanceIdDTO */
export interface InstanceId {
  space: string;
  externalId: string;
}

/** Maps to ViewDTO */
export interface View {
  space: string;
  externalId: string;
  version: string;
}

/** Maps to PropertyVal type alias */
export type PropertyVal =
  | string
  | number
  | boolean
  | null
  | Record<string, unknown>
  | (string | null)[]
  | (number | null)[]
  | (boolean | null)[]
  | (Record<string, unknown> | null)[];

// ============================================================================
// Agent Data Types (maps to AgentDataDTO)
// ============================================================================

/** Maps to InstanceDataDTO — the only variant we narrow on in app code. */
export interface InstanceData {
  type: 'instance';
  view: View;
  instanceId: InstanceId;
  properties?: Record<string, PropertyVal>;
}

/**
 * Data items attached to response messages.
 * Only `InstanceData` is typed — other variants pass through as plain objects.
 */
export type AgentData = InstanceData | (Record<string, unknown> & { type: string });

// ============================================================================
// Tool Definition Types (maps to CustomClientActionDTO)
// ============================================================================

/** Maps to clientToolParameters */
export interface ClientToolParameters {
  type: 'object';
  description?: string;
  properties?: Record<string, Record<string, unknown>>;
  required?: string[];
  propertyOrdering?: string[];
}

/** Maps to CustomClientActionDTO */
export interface ApiToolDefinition {
  type: 'clientTool';
  clientTool: {
    name: string;
    description?: string;
    parameters: ClientToolParameters;
  };
}

// ============================================================================
// Request Message Types (maps to RequestMessageDTO)
// ============================================================================

/** Maps to AgentChatMessageUserRequestDTO */
export interface UserRequestMessage {
  role: 'user';
  content: AgentContent;
}

/** Maps to ClientToolCallActionMessageDTO */
export interface ClientToolActionMessage {
  role: 'action';
  type: 'clientTool';
  actionId: string;
  content: AgentContent;
  data: Array<Record<string, unknown>>;
}

/** Maps to UserConfirmationResponseDTO */
export interface UserConfirmationMessage {
  role: 'action';
  type: 'toolConfirmation';
  actionId: string;
  status: 'ALLOW' | 'DENY';
}

/** Maps to UserSessionResponseDTO */
export interface UserSessionMessage {
  role: 'action';
  type: 'userSession';
  actionId: string;
  nonce: string;
}

/** Maps to RequestMessageDTO (discriminated union) */
export type RequestMessage =
  | UserRequestMessage
  | ClientToolActionMessage
  | UserConfirmationMessage
  | UserSessionMessage;

// ============================================================================
// Session Context (maps to AgentSessionContextDTO)
// ============================================================================

export interface AgentSessionContext {
  instanceSpaces?: string[];
  dataModels?: Array<Record<string, unknown>>;
  timeZone?: string;
  appContext?: string;
}

// ============================================================================
// Chat Payload (maps to AgentSessionRequest)
// ============================================================================

export interface ChatPayload {
  agentExternalId?: string;
  messages: RequestMessage[];
  actions?: ApiToolDefinition[];
  contextInformation?: AgentSessionContext;
  cursor?: string;
  stream: boolean;
}

// ============================================================================
// Raw Response Types
// ============================================================================

/** Response action: agent requests client to execute a tool */
export interface RawClientToolAction {
  type: 'clientTool';
  actionId: string;
  clientTool: {
    name: string;
    arguments: string | Record<string, unknown>;
  };
}

/** Response action: agent requests user confirmation */
export interface RawToolConfirmationAction {
  type: 'toolConfirmation';
  actionId: string;
  toolConfirmation?: {
    toolName?: string;
    toolType?: string;
    toolArguments?: Record<string, unknown>;
    toolDescription?: string;
    content?: AgentContent;
  };
}

/** Response action: agent requests user session */
export interface RawUserSessionAction {
  type: 'userSession';
  actionId: string;
}

/** Action from agent response (discriminated by `type`). Unknown types are skipped by the session loop. */
export type RawAction =
  | RawClientToolAction
  | RawToolConfirmationAction
  | RawUserSessionAction;

/** A message in an agent response */
export interface RawMessage {
  content?: AgentContent;
  role: string;
  data?: AgentData[];
  reasoning?: Array<Record<string, unknown>>;
  actions?: RawAction[];
}

export interface RawAgentResponse {
  agentId: string;
  agentExternalId: string;
  response: {
    type: string;
    cursor?: string;
    messages: RawMessage[];
  };
}
