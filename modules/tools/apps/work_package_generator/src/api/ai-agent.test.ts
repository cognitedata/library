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
	aiSelectBestCanvasTemplate,
	aiSelectResource,
	getOrCreateAgent,
	queryAiForSelection,
} from "./ai-agent";
import { patchApiRuntimeDeps } from "./runtime-deps";

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
	});

	describe("queryAiForSelection", () => {
		it("returns null when not ok", async () => {
			cdfApiFetch.mockResolvedValueOnce({ ok: false, statusText: "x" });
			await expect(
				queryAiForSelection(client, "p", "agent", "prompt")
			).resolves.toBeNull();
		});

		it("extracts text from nested response.messages", async () => {
			cdfApiFetch.mockResolvedValueOnce({ ok: true });
			parseJsonResponse.mockResolvedValueOnce({
				response: {
					messages: [{ role: "assistant", content: { text: "  id-1  " } }],
				},
			});
			await expect(queryAiForSelection(client, "p", "a", "pr")).resolves.toBe(
				"id-1"
			);
		});

		it("handles flat messages and string content", async () => {
			cdfApiFetch.mockResolvedValueOnce({ ok: true });
			parseJsonResponse.mockResolvedValueOnce({
				messages: [{ role: "assistant", content: "plain" }],
			});
			await expect(queryAiForSelection(client, "p", "a", "pr")).resolves.toBe(
				"plain"
			);
		});

		it("returns null on error", async () => {
			cdfApiFetch.mockRejectedValueOnce(new Error("e"));
			await expect(
				queryAiForSelection(client, "p", "a", "pr")
			).resolves.toBeNull();
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
			expect(sel?.externalId).toBe("r1");
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
			expect(sel?.externalId).toBe("r1");
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
