import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

function readModuleFile(relativePath: string): string {
	return readFileSync(
		path.resolve(import.meta.dirname, "..", relativePath),
		"utf8"
	);
}

describe("auth group capabilities", () => {
	const group = readModuleFile("auth/apps.operations-summarizer.Group.yaml");

	it.each([
		"agentsAcl",
		"dataModelsAcl",
		"dataModelInstancesAcl",
		"filesAcl",
		"functionsAcl",
		"sessionsAcl",
	])("grants %s", (acl) => {
		expect(group).toContain(`${acl}:`);
	});

	it("is no longer a placeholder", () => {
		expect(group).not.toMatch(/placeholder/i);
	});

	it("keeps agents:write out of the app group", () => {
		const agentsBlock = /- agentsAcl:[\s\S]*?(?=\n {2}- \w+Acl:|$)/.exec(
			group
		)?.[0];
		expect(agentsBlock).toBeDefined();
		expect(agentsBlock).toContain("READ");
		expect(agentsBlock).toContain("RUN");
		expect(agentsBlock).not.toContain("WRITE");
	});
});

describe("agent identity in Toolkit config", () => {
	it("uses ops_agent_external_id in the Agent YAML and the config node", () => {
		const agent = readModuleFile("agents/operation_summarizer.Agent.yaml");
		const node = readModuleFile(
			"data_modeling/ops_summarizer_config.Node.yaml"
		);
		expect(agent).toContain("externalId: '{{ ops_agent_external_id }}'");
		expect(node).toContain('externalId: "{{ ops_agent_external_id }}"');
		expect(node).not.toContain("externalIdSuffix");
	});

	it("needs no cdf_project variable", () => {
		for (const file of [
			"agents/operation_summarizer.Agent.yaml",
			"data_modeling/ops_summarizer_config.Node.yaml",
			"default.config.yaml",
		]) {
			expect(readModuleFile(file)).not.toMatch(/cdf_project/i);
		}
	});

	it("defaults the variable", () => {
		expect(readModuleFile("default.config.yaml")).toContain(
			"ops_agent_external_id: operation_summarizer_helper_agent"
		);
	});
});
