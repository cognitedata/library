import type { CogniteClient } from "@cognite/sdk";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	AGENT_DESCRIPTION,
	AGENT_NAME,
	createMockAppConfig,
	DEFAULT_AGENT_MODEL,
	DEFAULT_AGENT_RUNTIME_VERSION,
} from "@/__mocks__/app-config";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { installInlineApiRuntime } from "@/__mocks__/inline-api-runtime";
import {
	type AiSelectionResult,
	aiSelectBestCanvasTemplate,
	aiSelectResource,
	getOrCreateAgent,
	queryAiForSelection,
	selectionIdFromResult,
} from "./ai-agent";
import { patchApiRuntimeDeps } from "./runtime-deps";

function selectedExternalId(
	value: Awaited<ReturnType<typeof aiSelectResource>>
): string | undefined {
	if (!value || "kind" in value) return undefined;
	return value.externalId;
}

const cdfApiFetch = vi.fn();
const parseJsonResponse = vi.fn();

installInlineApiRuntime();

describe("ai-agent", () => {
	let client: CogniteClient;

	beforeEach(() => {
		vi.clearAllMocks();
		patchApiRuntimeDeps({ cdfApiFetch, parseJsonResponse });
		client = cdfClientTestDouble({
			getBaseUrl: () => "https://cluster.cognitedata.com",
		});
	});

	describe("getOrCreateAgent", () => {
		it("returns existing agent id when listed", async () => {
			cdfApiFetch.mockResolvedValueOnce({ ok: true });
			parseJsonResponse.mockResolvedValueOnce({
				items: [{ externalId: "proj_work_package_generator_helper_agent" }],
			});
			const id = await getOrCreateAgent(client, "proj");
			expect(id).toBe("proj_work_package_generator_helper_agent");
		});

		it("creates agent when missing", async () => {
			cdfApiFetch
				.mockResolvedValueOnce({ ok: true })
				.mockResolvedValueOnce({ ok: true });
			parseJsonResponse.mockResolvedValueOnce({ items: [] });
			const id = await getOrCreateAgent(client, "proj");
			expect(id).toBe("proj_work_package_generator_helper_agent");
			expect(cdfApiFetch).toHaveBeenLastCalledWith(
				client,
				expect.stringContaining("/ai/agents"),
				expect.objectContaining({
					method: "POST",
					body: JSON.stringify({
						items: [
							{
								externalId: "proj_work_package_generator_helper_agent",
								name: AGENT_NAME,
								description: AGENT_DESCRIPTION,
								instructions: "",
								model: DEFAULT_AGENT_MODEL,
								runtimeVersion: DEFAULT_AGENT_RUNTIME_VERSION,
								tools: [],
							},
						],
					}),
				})
			);
		});

		it("does not create when existingAgentExternalId is missing", async () => {
			const config = createMockAppConfig();
			config.agent = {
				...config.agent,
				existingAgentExternalId: "bogus-agent",
			};
			cdfApiFetch.mockResolvedValueOnce({ ok: true });
			parseJsonResponse.mockResolvedValueOnce({ items: [] });
			await expect(getOrCreateAgent(client, "proj", config)).rejects.toThrow(
				/bogus-agent/
			);
			expect(cdfApiFetch).toHaveBeenCalledTimes(1);
		});

		it("throws when create fails", async () => {
			cdfApiFetch
				.mockResolvedValueOnce({ ok: true })
				.mockResolvedValueOnce({ ok: false, statusText: "Bad" });
			parseJsonResponse.mockResolvedValueOnce({ items: [] });
			await expect(getOrCreateAgent(client, "proj")).rejects.toThrow(
				"Failed to create agent"
			);
		});

		it("throws on list parse failure", async () => {
			cdfApiFetch.mockRejectedValueOnce(new Error("net"));
			await expect(getOrCreateAgent(client, "proj")).rejects.toThrow(
				"Failed to get or create agent"
			);
		});

		it("N17: creates the agent only when retrieve returns 404", async () => {
			cdfApiFetch
				.mockResolvedValueOnce({ ok: false, status: 404 })
				.mockResolvedValueOnce({ ok: true, status: 201 });
			await expect(getOrCreateAgent(client, "proj")).resolves.toBe(
				"proj_work_package_generator_helper_agent"
			);
			expect(cdfApiFetch).toHaveBeenCalledTimes(2);
		});

		it("N17: does not create the agent on 401, 403, or 500", async () => {
			for (const status of [401, 403, 500]) {
				cdfApiFetch.mockReset();
				cdfApiFetch.mockResolvedValueOnce({ ok: false, status });
				await expect(getOrCreateAgent(client, "proj")).rejects.toThrow(
					String(status)
				);
				expect(cdfApiFetch).toHaveBeenCalledTimes(1);
			}
		});

		it("returns existing agent id when create returns 409", async () => {
			cdfApiFetch
				.mockResolvedValueOnce({ ok: false, status: 404 })
				.mockResolvedValueOnce({ ok: false, status: 409 });
			await expect(getOrCreateAgent(client, "proj")).resolves.toBe(
				"proj_work_package_generator_helper_agent"
			);
			expect(cdfApiFetch).toHaveBeenCalledTimes(2);
		});

		it("B-D2: updates the agent via POST /ai/agents and logs the config diff", async () => {
			const config = createMockAppConfig();
			config.agent = { ...config.agent, model: "configured-model" };
			const logError = vi.fn();
			patchApiRuntimeDeps({ cdfApiFetch, parseJsonResponse, logError });
			cdfApiFetch
				.mockResolvedValueOnce({ ok: true, status: 200 })
				.mockResolvedValueOnce({ ok: true, status: 200 });
			parseJsonResponse.mockResolvedValueOnce({
				items: [
					{
						externalId: "proj_work_package_generator_helper_agent",
						model: "live-model",
						runtimeVersion: config.agent.runtimeVersion,
						instructions: config.agent.instructions,
						tools: config.agent.tools,
					},
				],
			});
			await expect(getOrCreateAgent(client, "proj", config)).resolves.toBe(
				"proj_work_package_generator_helper_agent"
			);
			expect(cdfApiFetch).toHaveBeenCalledTimes(2);
			expect(cdfApiFetch).toHaveBeenNthCalledWith(
				2,
				client,
				"https://cluster.cognitedata.com/api/v1/projects/proj/ai/agents",
				{
					method: "POST",
					body: JSON.stringify({
						items: [
							{
								externalId: "proj_work_package_generator_helper_agent",
								name: config.agent.name,
								description: config.agent.description,
								instructions: config.agent.instructions,
								model: "configured-model",
								runtimeVersion: config.agent.runtimeVersion,
								tools: config.agent.tools,
							},
						],
					}),
				}
			);
			expect(logError).toHaveBeenCalledWith(
				expect.objectContaining({
					scope: "ai-agent.syncAgentConfig",
					context: expect.objectContaining({
						model: { from: "live-model", to: "configured-model" },
					}),
				})
			);
		});

		it("B-D2: keeps the existing agent when the update is forbidden", async () => {
			const config = createMockAppConfig();
			config.agent = { ...config.agent, instructions: "updated instructions" };
			const logError = vi.fn();
			patchApiRuntimeDeps({ cdfApiFetch, parseJsonResponse, logError });
			cdfApiFetch
				.mockResolvedValueOnce({ ok: true, status: 200 })
				.mockResolvedValueOnce({ ok: false, status: 403 });
			parseJsonResponse.mockResolvedValueOnce({
				items: [
					{
						externalId: "proj_work_package_generator_helper_agent",
						model: config.agent.model,
						runtimeVersion: config.agent.runtimeVersion,
						instructions: "old instructions",
						tools: [],
					},
				],
			});
			await expect(getOrCreateAgent(client, "proj", config)).resolves.toBe(
				"proj_work_package_generator_helper_agent"
			);
			expect(logError).toHaveBeenCalledWith(
				expect.objectContaining({
					message: expect.stringMatching(/403/),
				})
			);
		});
	});

	describe("queryAiForSelection", () => {
		it("C16: returns an error result with the HTTP status", async () => {
			cdfApiFetch.mockResolvedValueOnce({
				ok: false,
				status: 400,
				statusText: "Bad Request",
			});
			const result = await queryAiForSelection(client, "p", "agent", "prompt", [
				"id-1",
			]);
			expect(result).toEqual({
				kind: "error",
				status: 400,
				retryable: false,
			});
			expect(JSON.stringify(result)).not.toContain("Bad Request");
			expect(selectionIdFromResult(result)).toBeNull();
		});

		it("extracts text from nested response.messages", async () => {
			cdfApiFetch.mockResolvedValueOnce({ ok: true, status: 200 });
			parseJsonResponse.mockResolvedValueOnce({
				response: {
					messages: [{ role: "assistant", content: { text: "  id-1  " } }],
				},
			});
			await expect(
				queryAiForSelection(client, "p", "a", "pr", ["id-1"])
			).resolves.toEqual({ kind: "match", id: "id-1" });
		});

		it("handles flat messages and string content", async () => {
			cdfApiFetch.mockResolvedValueOnce({ ok: true, status: 200 });
			parseJsonResponse.mockResolvedValueOnce({
				messages: [{ role: "assistant", content: "plain" }],
			});
			await expect(
				queryAiForSelection(client, "p", "a", "pr", ["plain"])
			).resolves.toEqual({ kind: "match", id: "plain" });
		});

		it("C16: returns an error result when the request throws", async () => {
			cdfApiFetch.mockRejectedValueOnce(new Error("e"));
			const result = await queryAiForSelection(client, "p", "a", "pr", [
				"id-1",
			]);
			expect(result).toEqual({ kind: "error", status: 0, retryable: true });
			expect(selectionIdFromResult(result)).toBeNull();
		});

		it("C16: selectionIdFromResult returns only a match id", () => {
			const match: AiSelectionResult = { kind: "match", id: "abc" };
			const none: AiSelectionResult = { kind: "none" };
			expect(selectionIdFromResult(match)).toBe("abc");
			expect(selectionIdFromResult(none)).toBeNull();
		});

		it("C17: extracts one id from bold, ID prefix, or fenced JSON", async () => {
			const candidates = ["pump-a", "valve-b"];
			const replies = [
				"Use **pump-a** for this work order.",
				"ID: valve-b",
				'```json\n{"externalId":"pump-a"}\n```',
			];
			for (const content of replies) {
				cdfApiFetch.mockResolvedValueOnce({ ok: true, status: 200 });
				parseJsonResponse.mockResolvedValueOnce({
					messages: [{ role: "assistant", content }],
				});
			}
			await expect(
				queryAiForSelection(client, "p", "a", "pr", candidates)
			).resolves.toEqual({ kind: "match", id: "pump-a" });
			await expect(
				queryAiForSelection(client, "p", "a", "pr", candidates)
			).resolves.toEqual({ kind: "match", id: "valve-b" });
			await expect(
				queryAiForSelection(client, "p", "a", "pr", candidates)
			).resolves.toEqual({ kind: "match", id: "pump-a" });
		});

		it("C17: rejects a hallucinated id and more than one candidate id", async () => {
			cdfApiFetch.mockResolvedValue({ ok: true, status: 200 });
			parseJsonResponse.mockResolvedValueOnce({
				messages: [{ role: "assistant", content: "ID: not-a-candidate" }],
			});
			await expect(
				queryAiForSelection(client, "p", "a", "pr", ["pump-a"])
			).resolves.toEqual({ kind: "none" });

			parseJsonResponse.mockResolvedValueOnce({
				messages: [
					{ role: "assistant", content: "Either **pump-a** or **valve-b**." },
				],
			});
			await expect(
				queryAiForSelection(client, "p", "a", "pr", ["pump-a", "valve-b"])
			).resolves.toEqual({ kind: "none" });
		});

		it("N17: retries 429 and 503 with backoff", async () => {
			vi.useFakeTimers();
			cdfApiFetch
				.mockResolvedValueOnce({ ok: false, status: 429, statusText: "Slow" })
				.mockResolvedValueOnce({ ok: false, status: 503, statusText: "Down" })
				.mockResolvedValueOnce({ ok: true, status: 200 });
			parseJsonResponse.mockResolvedValueOnce({
				messages: [{ role: "assistant", content: "id-1" }],
			});
			const pending = queryAiForSelection(client, "p", "a", "pr", ["id-1"]);
			await vi.runAllTimersAsync();
			await expect(pending).resolves.toEqual({ kind: "match", id: "id-1" });
			expect(cdfApiFetch).toHaveBeenCalledTimes(3);
			vi.useRealTimers();
		});

		it("N17: runs slot calls one at a time", async () => {
			let releaseFirst: () => void = () => undefined;
			const gate = new Promise<void>((resolve) => {
				releaseFirst = resolve;
			});
			let started = 0;
			cdfApiFetch.mockImplementation(async () => {
				started += 1;
				if (started === 1) await gate;
				return { ok: true, status: 200 };
			});
			parseJsonResponse.mockResolvedValue({
				messages: [{ role: "assistant", content: "id-1" }],
			});
			const first = queryAiForSelection(client, "p", "a", "pr", ["id-1"]);
			const second = queryAiForSelection(client, "p", "a", "pr", ["id-1"]);
			await Promise.resolve();
			expect(started).toBe(1);
			releaseFirst();
			await expect(Promise.all([first, second])).resolves.toEqual([
				{ kind: "match", id: "id-1" },
				{ kind: "match", id: "id-1" },
			]);
			expect(started).toBe(2);
		});
	});

	describe("aiSelectResource", () => {
		const inst = {
			externalId: "r1",
			space: "s",
			name: "N",
			description: "d",
			properties: {},
		};

		it("returns null for empty or all selected", async () => {
			expect(
				await aiSelectResource(client, "p", "a", [], "file", "L", {}, new Set())
			).toBeNull();
			expect(
				await aiSelectResource(
					client,
					"p",
					"a",
					[inst],
					"file",
					"L",
					{},
					new Set(["r1"])
				)
			).toBeNull();
		});

		it("returns match when AI responds with id", async () => {
			cdfApiFetch.mockResolvedValue({ ok: true });
			parseJsonResponse.mockResolvedValue({
				messages: [{ role: "assistant", content: "r1" }],
			});
			const sel = await aiSelectResource(
				client,
				"p",
				"a",
				[inst],
				"file",
				"lbl",
				{ workOrder: { externalId: "w", space: "s" } },
				new Set()
			);
			expect(selectedExternalId(sel)).toBe("r1");
		});

		it("matches case-insensitive and strips quotes", async () => {
			cdfApiFetch.mockResolvedValue({ ok: true });
			parseJsonResponse.mockResolvedValue({
				messages: [{ role: "assistant", content: '"R1"' }],
			});
			const sel = await aiSelectResource(
				client,
				"p",
				"a",
				[inst],
				"file",
				"lbl",
				{},
				new Set()
			);
			expect(selectedExternalId(sel)).toBe("r1");
		});

		it("C17: ranks and caps the prompt at about 100 candidates", async () => {
			const instances = Array.from({ length: 120 }, (_, index) => ({
				externalId: `res-${String(index).padStart(3, "0")}`,
				space: "s",
				name: "Generic",
				description: "d",
				properties: {},
			}));
			const ranked = instances[119];
			if (!ranked) throw new Error("Expected a ranked candidate");
			ranked.name = "Boiler overhaul specialist";
			cdfApiFetch.mockResolvedValueOnce({ ok: true, status: 200 });
			parseJsonResponse.mockResolvedValueOnce({
				messages: [{ role: "assistant", content: ranked.externalId }],
			});
			const sel = await aiSelectResource(
				client,
				"p",
				"a",
				instances,
				"file",
				"lbl",
				{ workOrder: { externalId: "w", space: "s", name: "Boiler overhaul" } },
				new Set()
			);
			expect(selectedExternalId(sel)).toBe(ranked.externalId);
			const call = cdfApiFetch.mock.calls[0];
			if (!call) throw new Error("Expected a CDF API call");
			const body = JSON.parse((call[2] as { body: string }).body) as {
				messages: { content: { text: string } }[];
			};
			const prompt = body.messages[0]?.content.text ?? "";
			expect(prompt).toContain(ranked.externalId);
			expect(prompt.match(/External ID:/g)?.length).toBe(100);
			expect(prompt).not.toContain("res-110");
		});

		it("returns null when AI picks unknown id", async () => {
			cdfApiFetch.mockResolvedValue({ ok: true });
			parseJsonResponse.mockResolvedValue({
				messages: [{ role: "assistant", content: "nope" }],
			});
			expect(
				await aiSelectResource(
					client,
					"p",
					"a",
					[inst],
					"file",
					"lbl",
					{},
					new Set()
				)
			).toBeNull();
		});

		it("C16: returns the agent error instead of an empty match", async () => {
			cdfApiFetch.mockResolvedValueOnce({
				ok: false,
				status: 400,
				statusText: "Bad Request",
			});
			await expect(
				aiSelectResource(client, "p", "a", [inst], "file", "lbl", {}, new Set())
			).resolves.toEqual({ kind: "error", status: 400, retryable: false });
		});
	});

	describe("aiSelectBestCanvasTemplate", () => {
		const wo = { externalId: "w", space: "s", name: "W" };
		const asset = { assetExternalId: "a", space: "s" };

		it("returns null for empty list", async () => {
			expect(
				await aiSelectBestCanvasTemplate(client, "p", "ag", [], wo, asset, {})
			).toBeNull();
		});

		it("queries AI even for a single canvas", async () => {
			cdfApiFetch.mockResolvedValueOnce({ ok: true });
			parseJsonResponse.mockResolvedValueOnce({
				messages: [{ role: "assistant", content: "c1" }],
			});
			await expect(
				aiSelectBestCanvasTemplate(
					client,
					"p",
					"ag",
					[
						{
							externalId: "c1",
							space: "s",
							name: "T",
							visibility: "public",
						},
					],
					wo,
					asset,
					{}
				)
			).resolves.toBe("c1");
			expect(cdfApiFetch).toHaveBeenCalled();
		});

		it("returns null when AI declines with NONE", async () => {
			cdfApiFetch.mockResolvedValueOnce({ ok: true });
			parseJsonResponse.mockResolvedValueOnce({
				messages: [{ role: "assistant", content: "NONE" }],
			});
			const id = await aiSelectBestCanvasTemplate(
				client,
				"p",
				"ag",
				[
					{
						externalId: "c1",
						space: "s",
						name: "A",
						visibility: "public",
					},
					{
						externalId: "c2",
						space: "s",
						name: "B",
						visibility: "public",
					},
				],
				wo,
				asset,
				{ file: [] }
			);
			expect(id).toBeNull();
			const call = cdfApiFetch.mock.calls[0];
			if (!call) throw new Error("Expected a CDF API call");
			const body = JSON.parse((call[2] as { body: string }).body) as {
				messages: { content: { text: string } }[];
			};
			expect(body.messages[0]?.content.text).toContain("NONE");
		});

		it("returns null when AI declines with quoted none", async () => {
			cdfApiFetch.mockResolvedValueOnce({ ok: true });
			parseJsonResponse.mockResolvedValueOnce({
				messages: [{ role: "assistant", content: '"none"' }],
			});
			await expect(
				aiSelectBestCanvasTemplate(
					client,
					"p",
					"ag",
					[
						{
							externalId: "c1",
							space: "s",
							name: "A",
							visibility: "public",
						},
					],
					wo,
					asset,
					{}
				)
			).resolves.toBeNull();
		});

		it("calls query for multiple canvases", async () => {
			cdfApiFetch.mockResolvedValueOnce({ ok: true });
			parseJsonResponse.mockResolvedValueOnce({
				messages: [{ role: "assistant", content: "c2" }],
			});
			const id = await aiSelectBestCanvasTemplate(
				client,
				"p",
				"ag",
				[
					{
						externalId: "c1",
						space: "s",
						name: "A",
						visibility: "public",
					},
					{
						externalId: "c2",
						space: "s",
						name: "B",
						visibility: "public",
					},
				],
				wo,
				asset,
				{ file: [] }
			);
			expect(id).toBe("c2");
		});
	});
});
