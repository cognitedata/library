import { describe, expect, it } from "vitest";
import { DEFAULT_CANVAS_DIMENSIONS } from "@/__mocks__/app-config";
import type { Asset, CanvasItem, TemplateInstance, WorkOrder } from "../types";

const tmpl = {
	...DEFAULT_CANVAS_DIMENSIONS,
	properties: {} as Record<string, unknown>,
};

import {
	applyAssetReplacement,
	applySelectionReplacement,
	applyWorkOrderReplacement,
	isMainAsset,
	isMainWorkOrder,
	isSelectableResourceSlot,
} from "./canvas-item-helpers";

describe("isMainAsset / isMainWorkOrder", () => {
	const createInstance = (viewId: string, label: string): TemplateInstance => ({
		label,
		viewExternalId: viewId,
		viewSpace: "s",
		viewVersion: "v1",
		...tmpl,
	});

	it("detects main asset or equipment", () => {
		expect(isMainAsset(createInstance("CogniteAsset", "Main thing"))).toBe(
			true
		);
		expect(isMainAsset(createInstance("CogniteEquipment", "Main thing"))).toBe(
			true
		);
		expect(isMainAsset(createInstance("Other", "Main thing"))).toBe(false);
	});

	it("detects main work order view", () => {
		expect(
			isMainWorkOrder(createInstance("CogniteActivity", "Main thing"))
		).toBe(true);
		expect(isMainWorkOrder(createInstance("X", "Main thing"))).toBe(false);
	});

	it("correctly identifies main instances with word boundaries", () => {
		// These should match - valid "main" instances
		expect(isMainAsset(createInstance("CogniteAsset", "main item"))).toBe(true);
		expect(isMainAsset(createInstance("CogniteAsset", "Main Item"))).toBe(true);
		expect(isMainAsset(createInstance("CogniteAsset", "MAIN"))).toBe(true);
		expect(isMainAsset(createInstance("CogniteAsset", "main"))).toBe(true);
		expect(
			isMainAsset(createInstance("CogniteAsset", "The main component"))
		).toBe(true);

		// These should NOT match - false positives prevented by word boundaries
		expect(isMainAsset(createInstance("CogniteAsset", "Maintenance"))).toBe(
			false
		);
		expect(isMainAsset(createInstance("CogniteAsset", "Mainland"))).toBe(false);
		expect(isMainAsset(createInstance("CogniteAsset", "Domain"))).toBe(false);
		expect(isMainAsset(createInstance("CogniteAsset", "Remain"))).toBe(false);
		expect(isMainAsset(createInstance("CogniteAsset", ""))).toBe(false);
	});
});

describe("isSelectableResourceSlot", () => {
	const createInstance = (viewId: string, label: string): TemplateInstance => ({
		label,
		viewExternalId: viewId,
		viewSpace: "s",
		viewVersion: "v1",
		...tmpl,
	});

	it("matches configured resource views used as AI slots", () => {
		expect(
			isSelectableResourceSlot(createInstance("CogniteFile", "P&ID"))
		).toBe(true);
		expect(
			isSelectableResourceSlot(createInstance("CogniteTimeSeries", "Sensor"))
		).toBe(true);
		expect(
			isSelectableResourceSlot(createInstance("CogniteAsset", "Related asset"))
		).toBe(true);
	});

	it("does not match auto items or unknown views", () => {
		expect(
			isSelectableResourceSlot(createInstance("CogniteAsset", "Main asset"))
		).toBe(false);
		expect(
			isSelectableResourceSlot(
				createInstance("CogniteActivity", "Main work order")
			)
		).toBe(false);
		expect(
			isSelectableResourceSlot(createInstance("CustomCanvasNote", "Legend"))
		).toBe(false);
		expect(isSelectableResourceSlot(createInstance("", "Empty"))).toBe(false);
	});
});

describe("apply replacements", () => {
	const fdm: CanvasItem = {
		type: "fdmInstance",
		containerId: "c",
		instanceExternalId: "old",
		instanceSpace: "os",
		viewExternalId: "v",
		viewSpace: "vs",
		viewVersion: "v1",
	};

	const annotation: CanvasItem = {
		type: "CanvasAnnotation",
		containerId: "c",
		annotationType: "a",
	};

	it("applyAssetReplacement", () => {
		const asset: Asset = {
			assetExternalId: "a1",
			space: "sp",
		};
		const next = applyAssetReplacement(fdm, asset);
		expect(next.type).toBe("fdmInstance");
		if (next.type === "fdmInstance") {
			expect(next.instanceExternalId).toBe("a1");
			expect(next.instanceSpace).toBe("sp");
		}
		expect(
			applyAssetReplacement(fdm, { assetExternalId: "", space: "s" })
		).toBe(fdm);
	});

	it("applyWorkOrderReplacement", () => {
		const wo: WorkOrder = { externalId: "wo", space: "ws" };
		const next = applyWorkOrderReplacement(fdm, wo);
		if (next.type === "fdmInstance") {
			expect(next.instanceExternalId).toBe("wo");
		}
		expect(applyWorkOrderReplacement(fdm, { externalId: "", space: "s" })).toBe(
			fdm
		);
	});

	it("applySelectionReplacement sets label from name", () => {
		const next = applySelectionReplacement(fdm, {
			externalId: "sel",
			space: "ss",
			name: "Nice",
		});
		if (next.type === "fdmInstance") {
			expect(next.label).toBe("Nice");
		}
	});

	it("leaves non-fdm items unchanged", () => {
		expect(
			applyAssetReplacement(annotation, { assetExternalId: "a", space: "s" })
		).toBe(annotation);
	});
});
