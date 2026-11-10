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

export type AiSelectionResult =
	| { kind: "match"; id: string }
	| { kind: "none" }
	| { kind: "error"; status: number; retryable: boolean };

const CANDIDATE_LIMIT = 100;
const RETRYABLE_STATUSES = new Set([429, 503]);
const AGENT_RETRY_DELAYS_MS = [250, 500];

let agentSlot: Promise<unknown> = Promise.resolve();

function withAgentSlot<T>(fn: () => Promise<T>): Promise<T> {
	const run = agentSlot.then(fn, fn);
	agentSlot = run.then(
		() => undefined,
		() => undefined
	);
	return run;
}

function delay(ms: number): Promise<void> {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}

export function selectionIdFromResult(
	result: AiSelectionResult
): string | null {
	return result.kind === "match" ? result.id : null;
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

function contextSearchText(context: AgentContext): string {
	return JSON.stringify(context).toLowerCase();
}

function rankAndCap<T extends { externalId: string }>(
	items: T[],
	contextText: string,
	haystack: (item: T) => string
): T[] {
	const tokens = contextText
		.split(/[^a-z0-9]+/)
		.filter((token) => token.length > 2);
	const score = (item: T): number => {
		const text = haystack(item);
		return tokens.reduce(
			(total, token) => total + (text.includes(token) ? 1 : 0),
			0
		);
	};
	return [...items]
		.sort(
			(left, right) =>
				score(right) - score(left) ||
				left.externalId.localeCompare(right.externalId)
		)
		.slice(0, CANDIDATE_LIMIT);
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
type LiveAgent = {
	externalId: string;
	model?: string;
	runtimeVersion?: string;
	instructions?: string;
	tools?: unknown;
};

function agentWriteBody(
	agentExternalId: string,
	agent: AppConfig["agent"]
): string {
	return JSON.stringify({
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
	});
}

function configDiff(
	live: LiveAgent,
	agent: AppConfig["agent"]
): Record<string, { from: unknown; to: unknown }> | undefined {
	const comparable = [
		"model",
		"runtimeVersion",
		"instructions",
		"tools",
	] as const;
	const diff: Record<string, { from: unknown; to: unknown }> = {};
	const liveHasFields = comparable.every((key) => live[key] !== undefined);
	if (!liveHasFields) return undefined;
	for (const key of comparable) {
		const from = key === "tools" ? JSON.stringify(live.tools) : live[key];
		const to = key === "tools" ? JSON.stringify(agent.tools) : agent[key];
		if (from !== to) {
			diff[key] = {
				from: key === "tools" ? live.tools : live[key],
				to: key === "tools" ? agent.tools : agent[key],
			};
		}
	}
	return Object.keys(diff).length > 0 ? diff : undefined;
}

async function syncLiveAgent(
	client: CogniteClient,
	project: string,
	agentExternalId: string,
	live: LiveAgent,
	agent: AppConfig["agent"],
	useExistingAgent: boolean
): Promise<void> {
	if (useExistingAgent) return;
	const diff = configDiff(live, agent);
	if (!diff) return;
	const { cdfApiFetch, logError } = resolveApiRuntime();
	logError({
		scope: "ai-agent.syncAgentConfig",
		message: "Live agent config differs from app config",
		context: diff,
	});
	const updateUrl = `${client.getBaseUrl()}/api/v1/projects/${project}/ai/agents/update`;
	const updateResponse = await cdfApiFetch(client, updateUrl, {
		method: "POST",
		body: agentWriteBody(agentExternalId, agent),
	});
	if (!updateResponse.ok) {
		logError({
			scope: "ai-agent.syncAgentConfig",
			message: `Agent update failed with HTTP ${updateResponse.status}. Keeping the existing agent.`,
			context: { project, status: updateResponse.status },
		});
	}
}

export async function getOrCreateAgent(
	client: CogniteClient,
	project: string,
	appConfig?: AppConfig
): Promise<string> {
	return withAgentSlot(() =>
		getOrCreateAgentUnlocked(client, project, appConfig)
	);
}

async function getOrCreateAgentUnlocked(
	client: CogniteClient,
	project: string,
	appConfig?: AppConfig
): Promise<string> {
	const agent = resolveAppConfig(appConfig).agent;
	const agentExternalId = getAgentExternalId(project, appConfig);
	const useExistingAgent = Boolean(agent.existingAgentExternalId);

	try {
		const retrieveUrl = `${client.getBaseUrl()}/api/v1/projects/${project}/ai/agents/byids`;
		const { cdfApiFetch, parseJsonResponse } = resolveApiRuntime();
		const retrieveResponse = await cdfApiFetch(client, retrieveUrl, {
			method: "POST",
			body: JSON.stringify({
				ignoreUnknownIds: true,
				items: [{ externalId: agentExternalId }],
			}),
		});

		if (retrieveResponse.ok) {
			const retrieveData = await parseJsonResponse<{
				items?: LiveAgent[];
			}>(retrieveResponse);

			const live = retrieveData.items?.[0];
			if (live) {
				await syncLiveAgent(
					client,
					project,
					agentExternalId,
					live,
					agent,
					useExistingAgent
				);
				return agentExternalId;
			}
		} else if (retrieveResponse.status !== 404) {
			throw new Error(`Failed to load agent: HTTP ${retrieveResponse.status}`);
		}

		if (useExistingAgent) {
			const detail = retrieveResponse.ok
				? "was not found"
				: `could not be loaded (HTTP ${retrieveResponse.status})`;
			throw new Error(
				`Configured agent "${agentExternalId}" ${detail}. config.agent.existingAgentExternalId must refer to an agent that already exists. The app does not create one when that value is set.`
			);
		}

		const agentsUrl = `${client.getBaseUrl()}/api/v1/projects/${project}/ai/agents`;
		const createResponse = await resolveApiRuntime().cdfApiFetch(
			client,
			agentsUrl,
			{
				method: "POST",
				body: agentWriteBody(agentExternalId, agent),
			}
		);

		if (!createResponse.ok) {
			if (createResponse.status === 409) {
				return agentExternalId;
			}

			throw new Error(`Failed to create agent: HTTP ${createResponse.status}`);
		}

		return agentExternalId;
	} catch (error) {
		const msg = getErrorMessage(error);
		throw new Error(`Failed to get or create agent: ${msg}`);
	}
}

function cleanIdToken(raw: string): string {
	return raw.replace(/^["'`]+|["'`]+$/g, "").trim();
}

/**
 * Pull exactly one candidate id out of free text.
 * Bold, `ID:` lines, and fenced JSON count. An id outside the candidate set is rejected.
 */
export function parseCandidateId(
	text: string,
	candidateIds: readonly string[]
): string | null {
	const byLower = new Map(
		candidateIds.map((id) => [id.toLowerCase(), id] as const)
	);
	const canonical = (raw: string): string | undefined => {
		const cleaned = cleanIdToken(raw);
		if (!cleaned || cleaned.toUpperCase() === "NONE") return undefined;
		return byLower.get(cleaned.toLowerCase());
	};
	const isHallucinated = (raw: string): boolean => {
		const cleaned = cleanIdToken(raw);
		if (!cleaned || cleaned.toUpperCase() === "NONE") return false;
		return !byLower.has(cleaned.toLowerCase());
	};

	const structured: string[] = [];
	let hallucinated = false;
	const note = (raw: string): void => {
		const id = canonical(raw);
		if (id) structured.push(id);
		else if (isHallucinated(raw)) hallucinated = true;
	};

	for (const match of text.matchAll(/```(?:json)?\s*([\s\S]*?)```/gi)) {
		const body = match[1]?.trim() ?? "";
		if (!body) continue;
		try {
			const parsed: unknown = JSON.parse(body);
			if (typeof parsed === "string") {
				note(parsed);
			} else if (parsed && typeof parsed === "object") {
				const record = parsed as { id?: unknown; externalId?: unknown };
				const raw = record.externalId ?? record.id;
				if (typeof raw === "string") note(raw);
			}
		} catch {
			note(body);
		}
	}
	for (const match of text.matchAll(/\*\*([^*]+)\*\*/g)) {
		note(match[1] ?? "");
	}
	for (const match of text.matchAll(/(?:^|\n)\s*id\s*:\s*([^\n]+)/gi)) {
		note(match[1] ?? "");
	}

	if (hallucinated) return null;
	const uniqueStructured = [...new Set(structured)];
	if (uniqueStructured.length === 1) return uniqueStructured[0] ?? null;
	if (uniqueStructured.length > 1) return null;

	const found = new Set<string>();
	for (const id of candidateIds) {
		const escaped = id.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
		const pattern = new RegExp(
			`(^|[^A-Za-z0-9_-])${escaped}(?=$|[^A-Za-z0-9_-])`,
			"i"
		);
		if (pattern.test(text)) found.add(byLower.get(id.toLowerCase()) ?? id);
	}
	if (found.size === 1) {
		const [only] = found;
		return only ?? null;
	}
	return null;
}

function errorResult(status: number): AiSelectionResult {
	return {
		kind: "error",
		status,
		retryable: status === 0 || RETRYABLE_STATUSES.has(status),
	};
}

async function postAgentChat(
	client: CogniteClient,
	project: string,
	agentExternalId: string,
	prompt: string
): Promise<AiSelectionResult> {
	const { cdfApiFetch, parseJsonResponse } = resolveApiRuntime();
	const chatUrl = `${client.getBaseUrl()}/api/v1/projects/${project}/ai/agents/chat`;
	let response: Awaited<ReturnType<typeof cdfApiFetch>> | undefined;
	for (let attempt = 0; attempt <= AGENT_RETRY_DELAYS_MS.length; attempt += 1) {
		response = await cdfApiFetch(client, chatUrl, {
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
		if (!RETRYABLE_STATUSES.has(response.status) || response.ok) break;
		const wait = AGENT_RETRY_DELAYS_MS[attempt];
		if (wait === undefined) break;
		await delay(wait);
	}
	if (!response?.ok) {
		return errorResult(response?.status ?? 0);
	}

	const data = await parseJsonResponse<AgentChatApiResponse>(response);
	const msgs: AgentChatMessage[] =
		"response" in data ? data.response.messages : data.messages;
	const lastMessage = msgs?.[msgs.length - 1];
	if (
		!lastMessage ||
		(lastMessage.role !== "assistant" && lastMessage.role !== "agent")
	) {
		return { kind: "none" };
	}
	const content = lastMessage.content;
	const text = (
		typeof content === "string"
			? content
			: content && typeof content === "object" && "text" in content
				? (content.text ?? "")
				: ""
	).trim();
	return text ? { kind: "match", id: text } : { kind: "none" };
}

/**
 * Query AI agent for selection.
 * The returned id is the raw assistant text. Callers validate it against candidates.
 */
export async function queryAiForSelection(
	client: CogniteClient,
	project: string,
	agentExternalId: string,
	prompt: string,
	candidateIds: readonly string[] = []
): Promise<AiSelectionResult> {
	try {
		const result = await withAgentSlot(() =>
			postAgentChat(client, project, agentExternalId, prompt)
		);
		if (result.kind !== "match") return result;
		if (candidateIds.length === 0) {
			return { kind: "match", id: result.id.trim() };
		}
		const id = parseCandidateId(result.id, candidateIds);
		return id ? { kind: "match", id } : { kind: "none" };
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "ai-agent.queryAiForSelection",
			message: "AI agent chat request failed or response could not be parsed",
			cause: error,
			context: { project, agentExternalIdLen: agentExternalId.length },
		});
		return errorResult(0);
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

	const rankedInstances = rankAndCap(
		availableInstances,
		contextSearchText(context),
		(inst) =>
			`${inst.externalId} ${inst.name} ${inst.description ?? ""} ${JSON.stringify(inst.properties)}`.toLowerCase()
	);

	// Format options and build prompt
	const optionsText = formatResourceOptions(
		rankedInstances,
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

	const selectedExternalId = selectionIdFromResult(
		await queryAiForSelection(
			client,
			project,
			agentExternalId,
			prompt,
			rankedInstances.map((inst) => inst.externalId)
		)
	);

	if (!selectedExternalId) return null;

	const match = rankedInstances.find(
		(inst) => inst.externalId === selectedExternalId
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

	const rankedCanvases = rankAndCap(
		canvasList,
		`${workOrder.name ?? ""} ${asset.assetName ?? ""}`.toLowerCase(),
		(canvas) => `${canvas.externalId} ${canvas.name}`.toLowerCase()
	);

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
	const canvasSection = rankedCanvases
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

	const selectedId = selectionIdFromResult(
		await queryAiForSelection(
			client,
			project,
			agentExternalId,
			prompt,
			rankedCanvases.map((canvas) => canvas.externalId)
		)
	);
	if (!selectedId) return null;

	const match = rankedCanvases.find(
		(canvas) => canvas.externalId === selectedId
	);
	return match?.externalId ?? null;
}
