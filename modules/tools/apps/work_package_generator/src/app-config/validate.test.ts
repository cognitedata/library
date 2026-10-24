import { describe, expect, it } from "vitest";
import rawConfig from "../__mocks__/app-config.fixture.json";
import { AppConfigError } from "./errors";
import { parseConfigNode } from "./validate";

describe(parseConfigNode.name, () => {
	it("rejects a payload that is not an object", () => {
		expect(() => parseConfigNode(null)).toThrow(AppConfigError);
		expect(() => parseConfigNode("app")).toThrow(/Invalid WPG config/);
	});

	it("ignores an unknown top-level property", () => {
		const parsed = parseConfigNode({ ...rawConfig, extra: true });
		expect(parsed.agent.externalIdSuffix).toBe(
			"work_package_generator_helper_agent"
		);
	});

	it("rejects an unknown key inside a section", () => {
		const raw = structuredClone(rawConfig);
		(raw.limits as { pageSize?: number }).pageSize = 10;
		expect(() => parseConfigNode(raw)).toThrow(
			/WPGConfig\.limits\.pageSize: unknown key/
		);
	});

	it("rejects an unsupported schema version", () => {
		expect(() => parseConfigNode({ ...rawConfig, schemaVersion: 9 })).toThrow(
			/unsupported version 9/
		);
	});

	it("accepts an empty fallback organization as omitted", () => {
		const parsed = parseConfigNode({
			...rawConfig,
			fusion: { ...rawConfig.fusion, fallbackOrganization: "" },
		});
		expect(parsed.fusion.fallbackOrganization).toBeUndefined();
	});

	it("rejects a missing or mistyped section", () => {
		let rejected = 0;
		for (const key of Object.keys(rawConfig)) {
			if (key === "schemaVersion") continue;
			const missing = structuredClone(rawConfig);
			delete (missing as Record<string, unknown>)[key];
			try {
				parseConfigNode(missing);
			} catch (error) {
				expect(error).toBeInstanceOf(AppConfigError);
				rejected += 1;
			}

			const mistyped = structuredClone(rawConfig);
			(mistyped as Record<string, unknown>)[key] = "nope";
			try {
				parseConfigNode(mistyped);
			} catch (error) {
				expect(error).toBeInstanceOf(AppConfigError);
				rejected += 1;
			}
		}
		expect(rejected).toBeGreaterThan(0);
	});

	it("rejects invalid nested values across every section", () => {
		const raw = structuredClone(rawConfig) as Record<string, unknown>;
		const agent = raw.agent as Record<string, unknown>;
		agent.existingAgentExternalId = "   ";
		agent.instructions = "";
		agent.tools = [{ name: "" }];
		const templates = raw.templates as Record<string, unknown>;
		const base = (templates.baseTemplate as Record<string, unknown>) ?? {};
		base.visibility = "shared";
		templates.baseTemplate = base;
		const fusion = raw.fusion as Record<string, unknown>;
		fusion.hostSuffix = "fusion.cognite.com";
		fusion.fallbackOrganization = "   ";
		const ui = raw.ui as Record<string, unknown>;
		ui.stepperLabels = ["only-one"];
		ui.workOrderStatusOptions = [""];
		ui.templateInstructionExamples = { extra: true };
		ui.terms = { extra: true };
		const cluster = raw.cluster as Record<string, unknown>;
		cluster.hostAliases = { "  ": "az", "westeurope-1": "" };
		const canvas = raw.canvas as Record<string, unknown>;
		const style = (canvas.style as Record<string, unknown>) ?? {};
		style.borderWidth = 1.5;
		style.borderRadius = -1;
		canvas.style = style;
		canvas.visibility = "shared";

		expect(() => parseConfigNode(raw)).toThrow(AppConfigError);
	});

	it("keeps a named existing agent and a private base template", () => {
		const raw = structuredClone(rawConfig);
		(
			raw.agent as { existingAgentExternalId?: string }
		).existingAgentExternalId = "agent-already-there";
		raw.templates.baseTemplate.visibility = "private";
		raw.agent.instructions = "";
		const parsed = parseConfigNode(raw);
		expect(parsed.agent.existingAgentExternalId).toBe("agent-already-there");
		expect(parsed.templates.baseTemplate.visibility).toBe("private");
		expect(parsed.agent.instructions).toBe("");
	});

	it("rejects an empty tools list, a missing view, and a non-string alias", () => {
		const source = structuredClone(rawConfig);
		const invalidAsset: Record<string, unknown> = {
			...source.dataModel.views.asset,
			externalId: 1,
		};
		expect(() =>
			parseConfigNode({
				...source,
				dataModel: {
					...source.dataModel,
					views: { ...source.dataModel.views, asset: invalidAsset },
				},
			})
		).toThrow(AppConfigError);

		const badAlias = structuredClone(rawConfig);
		(badAlias.cluster as { hostAliases: Record<string, unknown> }).hostAliases =
			{ host: 4 };
		expect(() => parseConfigNode(badAlias)).toThrow(/hostAliases/);

		const badTools = structuredClone(rawConfig);
		(badTools.agent as { tools: unknown[] }).tools = [null];
		expect(() => parseConfigNode(badTools)).toThrow(/tools/);
	});

	it("reads a fallback organization", () => {
		const parsed = parseConfigNode({
			...rawConfig,
			fusion: { ...rawConfig.fusion, fallbackOrganization: "cog-demo" },
		});
		expect(parsed.fusion.fallbackOrganization).toBe("cog-demo");
	});
});
