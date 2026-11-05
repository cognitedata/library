/**
 * CDF Helper Service for Operations Summarizer.
 * Handles all CDF API interactions including agents and AI operations.
 * Transpiled from Python cdf_helper.py
 */

import type { CogniteClient } from "@cognite/sdk";
import { getAppConfig } from "@/config/appConfig";
import type {
  Agent,
  AgentChatResponse,
  AgentCreateParams,
  AgentsListResponse,
  AIServicesAvailabilityResponse,
  ChatCompletionResponse,
  ChatMessage,
  TokenInspectResponse,
} from "@/types/operations";
import {
  getHttpStatusFromError,
  getRequestIdFromError,
  withCdfConcurrency,
} from "@/utils/cdfRequest";
import { logError } from "@/utils/errorHandling";
import { buildComparisonPrompt } from "./prompts";

/**
 * Value for the `cdf-version` header on AI API requests that require the alpha contract.
 * Bump when Cognite publishes a new required alpha version for these endpoints
 * (`dataModel.cdfAlphaVersionHeader` on the config node).
 */
function cdfAlphaVersionHeader(): string {
  return getAppConfig().dataModel.cdfAlphaVersionHeader;
}

// ============================================================
// Types
// ============================================================

function cogniteThrownMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim() !== "") {
    return error.message;
  }
  if (error !== null && typeof error === "object") {
    const rec = error as Record<string, unknown>;
    if (
      typeof rec.errorMessage === "string" &&
      rec.errorMessage.trim() !== ""
    ) {
      return rec.errorMessage;
    }
    if (typeof rec.message === "string" && rec.message.trim() !== "") {
      return rec.message;
    }
  }
  return "Cognite API Error";
}

/** Thrown when CDF AI HTTP calls fail; preserves HTTP status, request id, and {@link Error.cause}. */
export class CogniteApiException extends Error {
  readonly status?: number;
  readonly requestId?: string;
  readonly response?: Response;

  constructor(
    message: string,
    options: {
      cause?: unknown;
      response?: Response;
      status?: number;
      requestId?: string;
    } = {},
  ) {
    const { cause, response, status, requestId } = options;
    super(message, cause !== undefined ? { cause } : undefined);
    this.name = "CogniteApiException";
    this.response = response;
    this.status = status;
    this.requestId = requestId;
  }

  /** Prefer the standard {@link Error.cause} chain for the SDK error. */
  get originalError(): Error | undefined {
    return this.cause instanceof Error ? this.cause : undefined;
  }
}

type RequestMethod = "GET" | "POST";

// ============================================================
// CDF Helper Class
// ============================================================

export class CDFHelper {
  private client: CogniteClient;

  constructor(client: CogniteClient) {
    this.client = client;
  }

  private get project(): string {
    return this.client.project;
  }

  /**
   * Send a request to Cognite AI API using the SDK's HTTP methods
   */
  private async sendCogAiRequest<T>(
    url: string,
    method: RequestMethod,
    payload?: Record<string, unknown>,
    isAlpha = true,
  ): Promise<T> {
    const headers: Record<string, string> = {};

    if (isAlpha) {
      headers["cdf-version"] = cdfAlphaVersionHeader();
    }

    try {
      return await withCdfConcurrency(async () => {
        let response: { data?: T };
        if (method === "GET") {
          response = await this.client.get(url, { headers });
        } else {
          response = await this.client.post(url, { data: payload, headers });
        }
        return response.data as T;
      });
    } catch (error) {
      const status = getHttpStatusFromError(error);
      const requestId = getRequestIdFromError(error);
      throw new CogniteApiException(cogniteThrownMessage(error), {
        cause: error,
        status,
        requestId,
      });
    }
  }

  // ============================================================
  // Agent Endpoints
  // ============================================================

  /**
   * Call GET /api/v1/token/inspect to get the current token's capabilities.
   * Used to determine if the user has agents:write (can edit agent config).
   */
  async getTokenInspect(): Promise<TokenInspectResponse> {
    const url = "/api/v1/token/inspect";
    return this.sendCogAiRequest<TokenInspectResponse>(
      url,
      "GET",
      undefined,
      false,
    );
  }

  /**
   * Returns true if the current user has agents:write for the current project.
   * Uses token/inspect: looks in response.capabilities for an item where
   * agentsAcl.actions includes "WRITE" and projectScope.projects includes
   * the current CDF client project (this.project).
   */
  async canEditAgentConfig(): Promise<boolean> {
    try {
      const raw = await this.getTokenInspect();
      const capabilities = raw.capabilities ?? [];
      const currentProject = this.project;
      return capabilities.some(
        (item) =>
          item.agentsAcl?.actions?.includes("WRITE") === true &&
          item.projectScope?.projects?.includes(currentProject) === true,
      );
    } catch (error) {
      logError(error, "Checking agent config edit permission");
      return false;
    }
  }

  /**
   * Get all available agents from CDF
   */
  async getAgents(): Promise<{ agents: Agent[]; invalidAgents: string[] }> {
    const url = `/api/v1/projects/${this.project}/ai/agents`;
    const data = await this.sendCogAiRequest<AgentsListResponse>(url, "GET");

    return {
      agents: data.items.map((item) => ({
        externalId: item.externalId,
        name: item.name,
        description: item.description,
        model: item.model,
        instructions: item.instructions,
      })),
      invalidAgents: data.invalidAgents || [],
    };
  }

  /**
   * Get list of supported model names
   */
  async getSupportedModelNames(): Promise<string[]> {
    try {
      const url = `/api/v1/projects/${this.project}/ai/services/availability`;
      const data = await this.sendCogAiRequest<AIServicesAvailabilityResponse>(
        url,
        "GET",
      );

      return data.languageModels.map((lm) => lm.name).sort();
    } catch (error) {
      logError(error, "Getting supported models");
      // Return fallback models
      return [getAppConfig().agent.defaultLanguageModel];
    }
  }

  /**
   * Create or update (upsert) an AI agent in CDF by externalId.
   * The API adds or updates agents; no need to delete before updating.
   */
  async createAgent(params: AgentCreateParams): Promise<string> {
    const url = `/api/v1/projects/${this.project}/ai/agents`;

    const payload = {
      items: [
        {
          externalId: params.externalId,
          name: params.name,
          description: params.description,
          instructions: params.instructions,
          model: params.model,
          tools: params.tools || [],
        },
      ],
    };

    await this.sendCogAiRequest(url, "POST", payload);
    return params.externalId;
  }

  /**
   * Chat with an AI agent
   */
  async agentChat(
    externalId: string,
    messages: ChatMessage[],
    cursor?: string,
  ): Promise<AgentChatResponse> {
    const url = `/api/v1/projects/${this.project}/ai/agents/chat`;

    const payload: Record<string, unknown> = {
      agentId: externalId,
      messages: messages.map((m) => ({
        role: m.role,
        content:
          typeof m.content === "string"
            ? { type: "text", text: m.content }
            : m.content,
      })),
    };

    if (cursor) {
      payload.cursor = cursor;
    }

    return this.sendCogAiRequest<AgentChatResponse>(url, "POST", payload);
  }

  /**
   * Use the chat completion API for LLM-based operations
   */
  async chatCompletion(
    messages: ChatMessage[],
  ): Promise<ChatCompletionResponse> {
    const url = `/api/v1/projects/${this.project}/ai/chat/completions`;

    const payload = {
      messages: messages.map((m) => ({
        role: m.role,
        content: typeof m.content === "string" ? m.content : m.content.text,
      })),
    };

    return this.sendCogAiRequest<ChatCompletionResponse>(
      url,
      "POST",
      payload,
      false,
    );
  }

  /**
   * Compare two summaries using LLM with focus on data accuracy
   */
  async compareSummaries(
    originalSummary: string,
    newSummary: string,
  ): Promise<ChatCompletionResponse> {
    return this.chatCompletion([
      {
        role: "user",
        content: buildComparisonPrompt(originalSummary, newSummary),
      },
    ]);
  }
}

// ============================================================
// Utility Functions
// ============================================================

/**
 * Extract date range from a summary instance external_id
 * Format: ops_summary_{start_date}_{end_date}_{timestamp}
 */
export function extractDateRangeFromSummary(externalId: string): {
  startDate: string | null;
  endDate: string | null;
} {
  const prefix = getAppConfig().externalIdPrefixes.operationSummary;
  const rest = externalId.startsWith(prefix)
    ? externalId.slice(prefix.length)
    : externalId;
  const match = /^(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})/.exec(rest);
  if (!match) return { startDate: null, endDate: null };
  return { startDate: match[1], endDate: match[2] };
}
