/**
 * AI Agent Utilities
 * Transpiled from Python ai_agent.py
 */

import type { CogniteClient } from "@cognite/sdk";
import {
	type AppConfig,
	getAgentExternalId,
	getResourceConfigs,
	resolveAppConfig,
} from "../app-config";
import { renderPromptTemplate } from "../app-config/prompt-template";
import type {
	Asset,
	ResourceInstance,
	Selection,
	TemplateCanvas,
	WorkOrder,
} from "../types";
import {
	extractAssetContext,
	extractWorkOrderContext,
} from "../utils/context-helpers";
import { getErrorMessage } from "../utils/errors";
import { resolveApiRuntime } from "./runtime-deps";

// ============================================================================
// Types
// ============================================================================

interface AgentContext {
	workOrderInstance?: Record<string, unknown>;
	assetInstance?: Record<string, unknown>;
	workOrder?: WorkOrder;
	asset?: Asset;
}

// AI Agent Chat API Types
interface AgentChatMessage {
	role: "user" | "assistant" | "agent";
	content: string | { type: "text"; text: string };
}

interface AgentChatResponse {
	messages: AgentChatMessage[];
}

interface AgentChatResponseWrapper {
	response: AgentChatResponse;
}

// Union type for the different response formats the API might return
type AgentChatApiResponse = AgentChatResponse | AgentChatResponseWrapper;

// ============================================================================
// Context Extraction
// ============================================================================
// Context extraction is now handled by utils/context-helpers

// ============================================================================
// Prompt Building
// ============================================================================

/**
 * Format a key for display (snake_case to Title Case)
 */
function formatKeyForDisplay(key: string): string {
	return key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * Build a context section from a context object
 */
function buildContextSection(
	context: Record<string, string>,
	sectionTitle: string,
	emptyMessage: string
): string {
	if (!Object.keys(context).length) {
		return `${sectionTitle}:\n- ${emptyMessage}`;
	}

	const lines: string[] = [`${sectionTitle}:`];
	for (const [key, value] of Object.entries(context)) {
		if (value && value !== "Unknown") {
			const formattedKey = formatKeyForDisplay(key);
			lines.push(`- ${formattedKey}: ${value}`);
		}
	}

	return lines.join("\n");
}

/**
 * Build context section for work order
 */
function buildWorkOrderContextSection(
	woContext: Record<string, string>
): string {
	return buildContextSection(
		woContext,
		"WORK ORDER CONTEXT",
		"No work order context available"
	);
}

/**
 * Build context section for asset
 */
function buildAssetContextSection(
	assetContext: Record<string, string>
): string {
	return buildContextSection(
		assetContext,
		"ASSET CONTEXT",
		"No asset context available"
	);
}

/**
 * Format resource options for AI prompt
 */
function formatResourceOptions(
	instances: ResourceInstance[],
	resourceType: string,
	appConfig?: AppConfig
): string {
	if (!instances.length) return "No options available";

	const config = getResourceConfigs(appConfig)[resourceType];
	if (!config) return "No options available";

	const options = instances.map((inst, index) => {
		const props: Record<string, unknown> = {};

		for (const prop of config.properties) {
			const value = inst.properties[prop];
			if (value != null) {
				props[prop] = value;
			}
		}

		return `${index + 1}. External ID: ${inst.externalId}\n   Properties: ${JSON.stringify(props)}`;
	});

	return options.join("\n\n");
}

/**
 * Build AI prompt for resource selection
 */
function buildAiPrompt(
	resourceType: string,
	label: string,
	context: AgentContext,
	optionsText: string,
	appConfig?: AppConfig
): string {
	const config = getResourceConfigs(appConfig)[resourceType];
	const resourceName = config?.displayName || resourceType;

	const woContext = extractWorkOrderContext(
		context.workOrder || context.workOrderInstance,
		appConfig
	);
	const assetContext = extractAssetContext(
		context.asset || context.assetInstance,
		appConfig
	);

	const workOrderSection = buildWorkOrderContextSection(woContext);
	const assetSection = buildAssetContextSection(assetContext);

	return renderPromptTemplate(
		resolveAppConfig(appConfig).prompts.resourceSelection,
		{
			workOrderSection,
			assetSection,
			resourceName,
			resourceNameUpper: resourceName.toUpperCase(),
			label,
			resourceType,
			optionsText,
		}
	);
}

// ============================================================================
// Agent API
// ============================================================================

/**
 * Get or create the Work Package Generator agent
 *
 * This function efficiently checks for agent existence by using the byids endpoint
 * to retrieve only the specific agent needed, rather than listing all agents.
 * This approach scales better and avoids pagination issues when there are many agents.
 */
export async function getOrCreateAgent(
	client: CogniteClient,
	project: string,
	appConfig?: AppConfig
): Promise<string> {
	const agent = resolveAppConfig(appConfig).agent;
	const agentExternalId = getAgentExternalId(project, appConfig);
	const useExistingAgent = Boolean(agent.existingAgentExternalId);

	try {
		// First try to retrieve the specific agent by external ID using the byIds endpoint
		const retrieveUrl = `${client.getBaseUrl()}/api/v1/projects/${project}/ai/agents/byids`;
		const { cdfApiFetch, parseJsonResponse } = resolveApiRuntime();
		const retrieveResponse = await cdfApiFetch(client, retrieveUrl, {
			method: "POST",
			body: JSON.stringify({
				ignoreUnknownIds: true, // Don't error if agent doesn't exist
				items: [{ externalId: agentExternalId }],
			}),
		});

		if (retrieveResponse.ok) {
			const retrieveData = await parseJsonResponse<{
				items?: Array<{ externalId: string }>;
			}>(retrieveResponse);

			if (retrieveData.items && retrieveData.items.length > 0) {
				// Agent exists, return its external ID
				return agentExternalId;
			}
		} else if (retrieveResponse.status === 404) {
			// Agent doesn't exist. Creation is below unless an existing id was configured.
		} else if (!useExistingAgent) {
			// Log the retrieval error but continue to creation attempt
			resolveApiRuntime().logError({
				scope: "ai-agent.getOrCreateAgent",
				message: `Failed to retrieve agent, will attempt creation. Status: ${retrieveResponse.status}`,
				context: { project, agentExternalId },
			});
		}

		if (useExistingAgent) {
			const detail = retrieveResponse.ok
				? "was not found"
				: `could not be loaded (HTTP ${retrieveResponse.status})`;
			throw new Error(
				`Configured agent "${agentExternalId}" ${detail}. config.agent.existingAgentExternalId must refer to an agent that already exists. The app does not create one when that value is set.`
			);
		}

		// If retrieve fails or agent not found, try to create it
		const agentsUrl = `${client.getBaseUrl()}/api/v1/projects/${project}/ai/agents`;
		const createResponse = await resolveApiRuntime().cdfApiFetch(
			client,
			agentsUrl,
			{
				method: "POST",
				body: JSON.stringify({
					items: [
						{
							externalId: agentExternalId,
							name: agent.name,
							description: agent.description,
							instructions: agent.instructions,
							model: agent.model,
							runtimeVersion: agent.runtimeVersion,
							tools: agent.tools,
						},
					],
				}),
			}
		);

		if (!createResponse.ok) {
			// If creation fails, check if it's because the agent already exists
			// This handles race conditions where another process might have created it
			if (createResponse.status === 409) {
				// Conflict - agent likely already exists, return the external ID
				return agentExternalId;
			}

			throw new Error(`Failed to create agent: ${createResponse.statusText}`);
		}

		return agentExternalId;
	} catch (error) {
		const msg = getErrorMessage(error);
		throw new Error(`Failed to get or create agent: ${msg}`);
	}
}

/**
 * Query AI agent for selection
 */
export async function queryAiForSelection(
	client: CogniteClient,
	project: string,
	agentExternalId: string,
	prompt: string
): Promise<string | null> {
	try {
		const { cdfApiFetch, parseJsonResponse } = resolveApiRuntime();
		const chatUrl = `${client.getBaseUrl()}/api/v1/projects/${project}/ai/agents/chat`;
		const response = await cdfApiFetch(client, chatUrl, {
			method: "POST",
			body: JSON.stringify({
				agentExternalId,
				messages: [
					{
						role: "user",
						content: { type: "text", text: prompt },
					},
				],
			}),
		});

		if (!response.ok) {
			throw new Error(`Agent chat failed: ${response.statusText}`);
		}

		// The alpha API may wrap messages under `response` or return them flat.
		// `content` may also be a string or an object with a `text` field.
		const data = await parseJsonResponse<AgentChatApiResponse>(response);

		// Extract messages from either response format
		const msgs: AgentChatMessage[] =
			"response" in data ? data.response.messages : data.messages;

		if (!msgs || msgs.length === 0) {
			return null;
		}

		const lastMessage = msgs[msgs.length - 1];
		if (
			!lastMessage ||
			(lastMessage.role !== "assistant" && lastMessage.role !== "agent")
		) {
			return null;
		}

		// Extract text content from the message
		const content = lastMessage.content;
		const text = (
			typeof content === "string"
				? content
				: content && typeof content === "object" && "text" in content
					? (content.text ?? "")
					: ""
		).trim();

		return text || null;
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "ai-agent.queryAiForSelection",
			message: "AI agent chat request failed or response could not be parsed",
			cause: error,
			context: { project, agentExternalIdLen: agentExternalId.length },
		});
		return null;
	}
}

// ============================================================================
// Selection Functions
// ============================================================================

/**
 * AI select the best resource for a template field
 */
export async function aiSelectResource(
	client: CogniteClient,
	project: string,
	agentExternalId: string,
	instances: ResourceInstance[],
	resourceType: string,
	label: string,
	context: AgentContext,
	selectedResources: Set<string>,
	appConfig?: AppConfig
): Promise<Selection | null> {
	if (!instances.length) {
		return null;
	}

	// Filter out already selected resources
	const availableInstances = instances.filter(
		(inst) => !selectedResources.has(inst.externalId)
	);

	if (!availableInstances.length) {
		return null;
	}

	// Format options and build prompt
	const optionsText = formatResourceOptions(
		availableInstances,
		resourceType,
		appConfig
	);
	const prompt = buildAiPrompt(
		resourceType,
		label,
		context,
		optionsText,
		appConfig
	);

	// Query AI
	const selectedExternalId = await queryAiForSelection(
		client,
		project,
		agentExternalId,
		prompt
	);

	if (!selectedExternalId) return null;

	// Strip surrounding quotes the AI may add, then try exact → case-insensitive
	const cleanedId = selectedExternalId.replace(/^["'`]+|["'`]+$/g, "").trim();

	const match =
		availableInstances.find((inst) => inst.externalId === cleanedId) ??
		availableInstances.find(
			(inst) => inst.externalId.toLowerCase() === cleanedId.toLowerCase()
		);

	if (!match) {
		return null;
	}

	return {
		externalId: match.externalId,
		space: match.space,
		name: match.name,
		description: match.description,
	};
}

/**
 * AI select the best canvas template
 */
export async function aiSelectBestCanvasTemplate(
	client: CogniteClient,
	project: string,
	agentExternalId: string,
	canvasList: TemplateCanvas[],
	workOrder: WorkOrder,
	asset: Asset,
	availableResources: Record<string, ResourceInstance[]>,
	appConfig?: AppConfig
): Promise<string | null> {
	if (!canvasList.length) {
		return null;
	}

	// Build context
	const woContext = extractWorkOrderContext(workOrder, appConfig);
	const assetContext = extractAssetContext(asset, appConfig);

	// Build available resources section
	const resourceConfigs = getResourceConfigs(appConfig);
	const resourcesSection = Object.entries(resourceConfigs)
		.map(([typeKey, config]) => {
			const resources = availableResources[typeKey] || [];
			return `- ${config.displayName}: ${resources.length} available`;
		})
		.join("\n");

	// Build canvas options section
	const canvasSection = canvasList
		.map(
			(canvas, index) =>
				`\n${index + 1}. Canvas: ${canvas.name} (ID: ${canvas.externalId})`
		)
		.join("\n");

	const prompt = renderPromptTemplate(
		resolveAppConfig(appConfig).prompts.templateSelection,
		{
			workOrderName: woContext.name || "Unknown Work Order",
			workOrderType: woContext.type || "Unknown Type",
			workOrderDescription: woContext.description || "No description",
			assetName: assetContext.name || "Unknown Asset",
			assetType: assetContext.type || "Unknown Type",
			assetDescription: assetContext.description || "No description",
			resourcesSection,
			canvasSection,
		}
	);

	// Query AI and validate response (consistent with aiSelectResource)
	const selectedId = await queryAiForSelection(
		client,
		project,
		agentExternalId,
		prompt
	);
	if (!selectedId) return null;

	// Strip surrounding quotes the AI may add, then try exact → case-insensitive matching
	const cleanedId = selectedId.replace(/^["'`]+|["'`]+$/g, "").trim();
	if (!cleanedId || cleanedId.toUpperCase() === "NONE") {
		return null;
	}

	const match =
		canvasList.find((canvas) => canvas.externalId === cleanedId) ??
		canvasList.find(
			(canvas) => canvas.externalId.toLowerCase() === cleanedId.toLowerCase()
		);

	return match?.externalId ?? null;
}
