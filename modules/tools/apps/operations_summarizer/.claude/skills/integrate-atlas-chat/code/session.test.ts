import { describe, it, expect, vi } from 'vitest';
import { AtlasSession } from './session';
import type { AtlasSessionConfig, RawAgentResponse } from './types';

/**
 * Minimal mock that satisfies CogniteClient just enough for AtlasSession.
 * AtlasClient.post is the only call path we exercise, so we stub it via the
 * prototype after construction.
 */
function createMockConfig(
  overrides?: Partial<AtlasSessionConfig>,
): AtlasSessionConfig {
  return {
    client: {} as AtlasSessionConfig['client'],
    agentExternalId: 'test-agent',
    ...overrides,
  };
}

/** Build a raw response with a tool action that requests a client tool call. */
function responseWithToolAction(
  actionId: string,
  toolName: string,
  args: Record<string, unknown>,
): RawAgentResponse {
  return {
    agentId: 'test-agent',
    agentExternalId: 'test-agent',
    response: {
      type: 'result',
      cursor: 'cursor-1',
      messages: [
        {
          role: 'assistant',
          actions: [
            {
              type: 'clientTool',
              actionId,
              clientTool: { name: toolName, arguments: args },
            },
          ],
        },
      ],
    },
  };
}

/** Build a terminal response (no actions). */
function terminalResponse(text: string): RawAgentResponse {
  return {
    agentId: 'test-agent',
    agentExternalId: 'test-agent',
    response: {
      type: 'result',
      cursor: 'cursor-2',
      messages: [{ role: 'assistant', content: { type: 'text', text } }],
    },
  };
}

describe(AtlasSession.name, () => {
  let postSpy: ReturnType<typeof vi.fn>;

  function createSession(config?: Partial<AtlasSessionConfig>): AtlasSession {
    const session = new AtlasSession(createMockConfig(config));

    // Stub the internal client.post so we never hit the network.
    postSpy = vi.fn();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).client = { post: postSpy };

    return session;
  }

  describe('appContext in continuation turns', () => {
    it('includes contextInformation on the initial user message', async () => {
      const session = createSession({
        getAppContext: () => 'todo state here',
      });

      postSpy.mockResolvedValueOnce(terminalResponse('Done'));

      await session.send('hello');

      const payload = postSpy.mock.calls[0][0];
      expect(payload.contextInformation).toEqual({
        appContext: 'todo state here',
      });
    });

    it('includes contextInformation on continuation turns after tool execution', async () => {
      let callCount = 0;
      const session = createSession({
        getAppContext: () => {
          callCount++;
          return `context-v${callCount}`;
        },
        tools: [
          {
            name: 'TestTool',
            description: 'test',
            parameters: { type: 'object', properties: {} },
            execute: () => ({ output: 'ok' }),
          },
        ],
      });

      // Turn 1: agent requests a tool call
      postSpy.mockResolvedValueOnce(
        responseWithToolAction('action-1', 'TestTool', {}),
      );
      // Turn 2: terminal response
      postSpy.mockResolvedValueOnce(terminalResponse('All done'));

      await session.send('do something');

      // First call: initial payload
      expect(postSpy).toHaveBeenCalledTimes(2);
      const initialPayload = postSpy.mock.calls[0][0];
      expect(initialPayload.contextInformation).toEqual({
        appContext: 'context-v1',
      });

      // Second call: continuation payload after tool execution
      const continuationPayload = postSpy.mock.calls[1][0];
      expect(continuationPayload.contextInformation).toEqual({
        appContext: 'context-v2',
      });
    });

    it('omits contextInformation when getAppContext returns undefined', async () => {
      const session = createSession({
        getAppContext: () => undefined,
      });

      postSpy.mockResolvedValueOnce(terminalResponse('Done'));

      await session.send('hello');

      const payload = postSpy.mock.calls[0][0];
      expect(payload.contextInformation).toBeUndefined();
    });

    it('omits contextInformation when getAppContext is not provided', async () => {
      const session = createSession();

      postSpy.mockResolvedValueOnce(terminalResponse('Done'));

      await session.send('hello');

      const payload = postSpy.mock.calls[0][0];
      expect(payload.contextInformation).toBeUndefined();
    });
  });
});
