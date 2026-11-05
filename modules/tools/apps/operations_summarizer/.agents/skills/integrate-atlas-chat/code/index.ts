/**
 * Atlas Agent Client — public API.
 *
 * Core library for communicating with Cognite Atlas AI agents.
 * Self-contained, zero imports from outside this directory (except external packages).
 *
 * React hook is a separate import path:
 *   import { useAtlasChat } from '@cognite/dune-utils/atlas-agent/react';
 */

// Core
export { AtlasSession } from './session';
export { AtlasClient } from './client';

// TypeBox re-exports for convenience
export { Type } from '@sinclair/typebox';
export type { Static, TSchema } from '@sinclair/typebox';

// Types
export type {
  Agent,
  AgentToolConfig,
  AtlasTool,
  AtlasToolResult,
  AtlasResponse,
  AtlasSessionConfig,
  ToolCall,
  StreamCallbacks,
  ApiToolDefinition,
  PythonRuntime,
} from './types';
