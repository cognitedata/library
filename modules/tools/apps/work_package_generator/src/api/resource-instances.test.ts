import { describe, expect, it } from "vitest";
import { FILE_VIEW_ID } from "@/__mocks__/app-config";
import {
	itemsToResourceInstances,
	resolveResourceConfig,
} from "./resource-instances";

describe("resolveResourceConfig", () => {
	it("returns null for an unknown resource type", () => {
		expect(resolveResourceConfig("unknown")).toBeNull();
	});
});

describe("itemsToResourceInstances", () => {
	it("drops items that are not nodes", () => {
		expect(
			itemsToResourceInstances([null, "not-a-node"], FILE_VIEW_ID)
		).toEqual([]);
	});

	it("maps a node using the given view", () => {
		const viewKey = `${FILE_VIEW_ID.externalId}/${FILE_VIEW_ID.version}`;
		const [instance] = itemsToResourceInstances(
			[
				{
					externalId: "file-1",
					space: "files",
					properties: {
						[FILE_VIEW_ID.space]: {
							[viewKey]: { name: "P&ID", description: "Drawing" },
						},
					},
				},
			],
			FILE_VIEW_ID
		);

		expect(instance).toEqual({
			externalId: "file-1",
			space: "files",
			name: "P&ID",
			description: "Drawing",
			properties: { name: "P&ID", description: "Drawing" },
		});
	});
});
