import { describe, expect, it } from "vitest";
import { DEFAULT_CANVAS_CONFIG } from "@/__mocks__/app-config";
import type { Asset, ResourceInstance, WorkOrder } from "@/types";
import { buildBaseTemplateItems } from "./base-template";

const CARD_W = 280;
const CARD_H = 120;
const GAP = 16;
const COLS = 2;
const SECTION_GAP = 32;
const SECTION_W = COLS * CARD_W + (COLS - 1) * GAP;

const workOrder: WorkOrder = {
	externalId: "wo1",
	space: "s",
	name: "Work Order",
};
const asset: Asset = { assetExternalId: "a1", space: "s", assetName: "Asset" };

function resource(externalId: string, name: string): ResourceInstance {
	return { externalId, space: "s", name, properties: {} };
}

describe("buildBaseTemplateItems", () => {
	it("creates resource type groups from candidates", () => {
		// Arrange
		const candidatesByType = {
			activity: [resource("wo1", "Work Order")],
			asset: [resource("a1", "Asset")],
			file: [resource("f1", "File 1")],
		};

		// Act
		const items = buildBaseTemplateItems(
			DEFAULT_CANVAS_CONFIG,
			workOrder,
			asset,
			candidatesByType
		);

		// Assert
		expect(items.length).toBeGreaterThan(0);
		const annotations = items.filter((i) => i.type === "CanvasAnnotation");
		expect(annotations).toHaveLength(3);
		expect(
			annotations.every((a) => a.annotationType === "stickyAnnotation")
		).toBe(true);

		// Sections are laid out left-to-right (activity, asset, file alphabetically)
		const activitiesHeader = annotations.find(
			(a) => a.properties?.text === "Activities"
		);
		const assetsHeader = annotations.find(
			(a) => a.properties?.text === "Assets"
		);
		const filesHeader = annotations.find((a) => a.properties?.text === "Files");
		expect(activitiesHeader?.properties).toMatchObject({
			x: 0,
			y: 0,
			width: SECTION_W,
			height: 48,
			text: "Activities",
		});
		expect(assetsHeader?.properties).toMatchObject({
			x: SECTION_W + SECTION_GAP,
			y: 0,
			width: SECTION_W,
		});
		expect(filesHeader?.properties).toMatchObject({
			x: 2 * (SECTION_W + SECTION_GAP),
			y: 0,
			width: SECTION_W,
			height: 48,
			text: "Files",
		});

		const instances = items.filter((i) => i.type === "fdmInstance");
		const instanceIds = instances.map((i) =>
			i.type === "fdmInstance" ? i.instanceExternalId : undefined
		);
		expect(instanceIds).toEqual(expect.arrayContaining(["wo1", "a1", "f1"]));
		const fileCard = instances.find(
			(i) => i.type === "fdmInstance" && i.instanceExternalId === "f1"
		);
		expect(fileCard?.x).toBe(2 * (SECTION_W + SECTION_GAP));
	});

	it("places cards in a 2-column grid within each section", () => {
		// Arrange
		const candidatesByType = {
			file: [
				resource("f1", "File 1"),
				resource("f2", "File 2"),
				resource("f3", "File 3"),
			],
		};

		// Act
		const items = buildBaseTemplateItems(
			DEFAULT_CANVAS_CONFIG,
			workOrder,
			asset,
			candidatesByType
		);

		// Assert
		const cards = items.filter((i) => i.type === "fdmInstance") as Array<{
			x?: number;
			y?: number;
			instanceExternalId?: string;
		}>;
		expect(cards).toHaveLength(3);
		const byId = Object.fromEntries(
			cards.map((c) => [c.instanceExternalId, c])
		);
		// Row 0: f1 left, f2 right
		expect(byId.f1.x).toBe(0);
		expect(byId.f2.x).toBe(CARD_W + GAP);
		expect(byId.f1.y).toBe(byId.f2.y);
		// Row 1: f3 left, below f1
		expect(byId.f3.x).toBe(0);
		expect(byId.f3.y).toBe((byId.f1.y ?? 0) + CARD_H + GAP);
	});

	it("skips empty candidate lists and unknown resource types", () => {
		// Arrange
		const candidatesByType = {
			file: [],
			unknownType: [resource("u1", "Unknown")],
		};

		// Act
		const items = buildBaseTemplateItems(
			DEFAULT_CANVAS_CONFIG,
			workOrder,
			asset,
			candidatesByType
		);

		// Assert
		expect(items).toHaveLength(0);
	});

	it("uses properties.name then externalId for labels when name is missing", () => {
		// Arrange
		const candidatesByType = {
			file: [
				{
					externalId: "f-props",
					space: "s",
					properties: { name: "From Properties" },
				},
				{ externalId: "f-id-only", space: "s", properties: {} },
			],
		};

		// Act
		const items = buildBaseTemplateItems(
			DEFAULT_CANVAS_CONFIG,
			workOrder,
			asset,
			candidatesByType
		);

		// Assert
		const cards = items.filter((i) => i.type === "fdmInstance");
		expect(cards).toHaveLength(2);
		expect(
			cards.find(
				(c) => c.type === "fdmInstance" && c.instanceExternalId === "f-props"
			)?.label
		).toBe("From Properties");
		expect(
			cards.find(
				(c) => c.type === "fdmInstance" && c.instanceExternalId === "f-id-only"
			)?.label
		).toBe("f-id-only");
	});
});
