import { describe, expect, it } from "vitest";
import { createMockAppConfig } from "@/__mocks__/app-config";
import {
	cdfClusterQueryHost,
	extractClusterFromUrl as extractClusterFromConfig,
} from "@/app-config";
import { extractClusterFromUrl, generateCanvasLink } from "./index";

describe("extractClusterFromUrl", () => {
	const config = createMockAppConfig();

	it("resolves configured host aliases, including non-Azure clusters", () => {
		expect(extractClusterFromUrl).toBe(extractClusterFromConfig);

		for (const [host, clusterId] of Object.entries(
			config.clusterSettings.hostAliases
		)) {
			expect(extractClusterFromUrl(`https://${host}`)).toBe(clusterId);
			expect(extractClusterFromUrl(`https://${host}/`)).toBe(clusterId);
			expect(cdfClusterQueryHost(clusterId)).toBe(host);
		}
	});

	it("extracts the cluster id from a public CDF host on any cloud", () => {
		const clusterIds = ["az-eastus-1", "aw-dub-gp-001", "asia-northeast1-1"];

		for (const clusterId of clusterIds) {
			const host = `${clusterId}.${config.clusterSettings.apiHostSuffix}`;
			expect(extractClusterFromUrl(`https://${host}`)).toBe(clusterId);
			expect(extractClusterFromUrl(`https://${host}/`)).toBe(clusterId);
			expect(cdfClusterQueryHost(clusterId)).toBe(host);
		}
	});

	it("uses the configured alias host in Fusion canvas links", () => {
		for (const [host, clusterId] of Object.entries(
			config.clusterSettings.hostAliases
		)) {
			const url = generateCanvasLink("cid", "proj", clusterId, "acme");
			expect(url).toContain(`cluster=${host}`);
		}
	});
});
