import { describe, expect, it } from "vitest";
import { DEFAULT_CANVAS_CONFIG } from "@/__mocks__/app-config";
import { buildBaseTemplateItems } from "./base-template";

const CARD_W = 280;
const CARD_H = 120;
const GAP = 16;
const COLS = 2;
const SECTION_GAP = 32;
const SECTION_W = COLS * CARD_W + (COLS - 1) * GAP;

describe("buildBaseTemplateItems", () => {
	it("creates resource type groups from candidates", () => {
		const items = buildBaseTemplateItems(
			DEFAULT_CANVAS_CONFIG,
			{ externalId: "wo1", space: "s", name: "Work Order" },
			{ assetExternalId: "a1", space: "s", assetName: "Asset" },
			{
				activity: [
					{
						externalId: "wo1",
						space: "s",
						name: "Work Order",
						properties: {},
					},
				],
				asset: [
					{
						externalId: "a1",
						space: "s",
						name: "Asset",
						properties: {},
					},
				],
				file: [
					{
						externalId: "f1",
						space: "s",
						name: "File 1",
						properties: {},
					},
				],
			}
		);

		expect(items.length).toBeGreaterThan(0);
		const annotations = items.filter((i) => i.type === "CanvasAnnotation");
		expect(annotations).toHaveLength(3);
		expect(
			annotations.every((a) => a.annotationType === "stickyAnnotation")
		).toBe(true);
		expect(annotations.some((a) => a.properties?.text === "Activities")).toBe(
			true
		);
		expect(annotations.some((a) => a.properties?.text === "Assets")).toBe(true);
		expect(annotations.some((a) => a.properties?.text === "Files")).toBe(true);

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
		expect(
			instances.some(
				(i) => i.type === "fdmInstance" && i.instanceExternalId === "wo1"
			)
		).toBe(true);
		expect(
			instances.some(
				(i) => i.type === "fdmInstance" && i.instanceExternalId === "a1"
			)
		).toBe(true);
		expect(
			instances.some(
				(i) => i.type === "fdmInstance" && i.instanceExternalId === "f1"
			)
		).toBe(true);

		const fileCard = instances.find(
			(i) => i.type === "fdmInstance" && i.instanceExternalId === "f1"
		);
		expect(fileCard?.x).toBe(2 * (SECTION_W + SECTION_GAP));
	});

	it("places cards in a 2-column grid within each section", () => {
		const items = buildBaseTemplateItems(
			DEFAULT_CANVAS_CONFIG,
			{ externalId: "wo1", space: "s", name: "Work Order" },
			{ assetExternalId: "a1", space: "s", assetName: "Asset" },
			{
				file: [
					{
						externalId: "f1",
						space: "s",
						name: "File 1",
						properties: {},
					},
					{
						externalId: "f2",
						space: "s",
						name: "File 2",
						properties: {},
					},
					{
						externalId: "f3",
						space: "s",
						name: "File 3",
						properties: {},
					},
				],
			}
		);

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
		const items = buildBaseTemplateItems(
			DEFAULT_CANVAS_CONFIG,
			{ externalId: "wo1", space: "s", name: "Work Order" },
			{ assetExternalId: "a1", space: "s", assetName: "Asset" },
			{
				file: [],
				unknownType: [
					{
						externalId: "u1",
						space: "s",
						name: "Unknown",
						properties: {},
					},
				],
			}
		);

		expect(items).toHaveLength(0);
	});

	it("uses properties.name then externalId for labels when name is missing", () => {
		const items = buildBaseTemplateItems(
			DEFAULT_CANVAS_CONFIG,
			{ externalId: "wo1", space: "s", name: "Work Order" },
			{ assetExternalId: "a1", space: "s", assetName: "Asset" },
			{
				file: [
					{
						externalId: "f-props",
						space: "s",
						properties: { name: "From Properties" },
					},
					{
						externalId: "f-id-only",
						space: "s",
						properties: {},
					},
				],
			}
		);

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
