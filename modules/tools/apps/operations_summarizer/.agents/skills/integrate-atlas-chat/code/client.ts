/**
 * AtlasClient — stateless HTTP/SSE transport layer.
 *
 * Single responsibility: send chat payloads to the Cognite AI agent API
 * and parse the response (JSON or Server-Sent Events).
 */

import type { CogniteClient } from '@cognite/sdk';
import type { Agent, ChatPayload, RawAgentResponse, StreamCallbacks } from './types';

const CDF_API_VERSION = 'alpha';
const AGENTS_API_VERSION = 'beta';

export class AtlasClient {
  private readonly client: CogniteClient;

  constructor(client: CogniteClient) {
    this.client = client;
  }

  async listAgents(): Promise<Agent[]> {
    const { data } = await this.client.get<{ items: Agent[] }>(
      `/api/v1/projects/${this.client.project}/ai/agents`,
      { headers: { 'cdf-version': AGENTS_API_VERSION } },
    );
    return data.items;
  }

  async getAgentById(externalId: string): Promise<Agent | null> {
    const { data } = await this.client.post<{ items: Agent[] }>(
      `/api/v1/projects/${this.client.project}/ai/agents/byids`,
      { data: { items: [{ externalId }] }, headers: { 'cdf-version': AGENTS_API_VERSION } },
    );
    return data.items[0] ?? null;
  }

  /**
   * Post a chat payload and parse the response (JSON or SSE).
   * @param agentExternalId — used as a fallback identifier when the SSE result event omits agent IDs.
   */
  async post(
    payload: ChatPayload,
    agentExternalId: string,
    callbacks?: StreamCallbacks,
    signal?: AbortSignal,
  ): Promise<RawAgentResponse> {
    const url = `${this.client.getBaseUrl()}/api/v1/projects/${this.client.project}/ai/internal/agents/chat`;

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        ...this.client.getDefaultRequestHeaders(),
        'Content-Type': 'application/json',
        'cdf-version': CDF_API_VERSION,
      },
      body: JSON.stringify(payload),
      signal,
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(
        `Agent chat API error: ${response.status} - ${JSON.stringify(errorData)}`,
      );
    }

    const contentType = response.headers.get('content-type') || '';

    if (
      contentType.includes('text/event-stream') ||
      contentType.includes('text/plain')
    ) {
      return this.parseSSE(response, agentExternalId, callbacks);
    }

    return await response.json();
  }

  /**
   * Parse a Server-Sent Events streaming response.
   */
  private async parseSSE(
    response: Response,
    agentExternalId: string,
    callbacks?: StreamCallbacks,
  ): Promise<RawAgentResponse> {
    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error('Response body is not readable');
    }

    const decoder = new TextDecoder();
    let buffer = '';
    let finalResponse: RawAgentResponse | null = null;

    const processLine = (line: string) => {
      if (!line.startsWith('data: ')) return;

      const dataStr = line.slice(6).trim();
      if (dataStr === '[DONE]') return '[DONE]' as const;

      try {
        const data = JSON.parse(dataStr);
        const sseResponse = data.response;

        if (!sseResponse) return;

        if (sseResponse.type === 'progress' && sseResponse.content) {
          callbacks?.onProgress?.(sseResponse.content);
        } else if (
          sseResponse.type === 'responseChunk' &&
          sseResponse.content
        ) {
          callbacks?.onChunk?.(sseResponse.content);
        } else if (sseResponse.type === 'result') {
          finalResponse = {
            agentId: data.agentId || agentExternalId,
            agentExternalId: data.agentExternalId || agentExternalId,
            response: {
              type: 'result',
              cursor: sseResponse.cursor,
              messages: sseResponse.messages || [],
            },
          };
        }
      } catch {
        // Skip unparseable SSE lines
      }
    };

    try {
      while (true) {
        const { done, value } = await reader.read();

        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        if (lines.some((line) => processLine(line) === '[DONE]')) break;
      }
    } finally {
      reader.releaseLock();
    }

    if (!finalResponse) {
      throw new Error('No result response received from streaming API');
    }

    return finalResponse;
  }
}
