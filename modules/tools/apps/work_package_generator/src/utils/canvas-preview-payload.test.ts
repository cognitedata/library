import { describe, expect, it, vi } from "vitest";
import { CANVAS_SPACE_INSTANCE, FILE_VIEW_ID } from "@/__mocks__/app-config";
import type { CanvasItem } from "@/types";
import { canvasItemsToPreviewAggregateParams } from "./canvas-preview-payload";

// vi.mock: UUID is non-deterministic; fixed value keeps preview annotation IDs assertable.
vi.mock("./uuid", () => ({
	generateUuid: vi.fn().mockReturnValue("anno-uuid"),
}));

describe("canvasItemsToPreviewAggregateParams", () => {
	it("skips null items and incomplete fdm instances", () => {
		const items: (CanvasItem | null)[] = [
			null,
			{
				type: "fdmInstance",
				containerId: "c",
				instanceExternalId: undefined,
				instanceSpace: "s",
				viewExternalId: "v",
				viewSpace: "vs",
			},
		];
		const { fdmContainers, annotations } =
			canvasItemsToPreviewAggregateParams(items);
		expect(fdmContainers).toHaveLength(0);
		expect(annotations).toHaveLength(0);
	});

	it("maps fdmInstance with defaults", () => {
		const items: CanvasItem[] = [
			{
				type: "fdmInstance",
				containerId: "c",
				instanceExternalId: "ie",
				instanceSpace: "is",
				viewExternalId: "ve",
				viewSpace: "vs",
				viewVersion: undefined,
				label: undefined,
			},
		];
		const { fdmContainers } = canvasItemsToPreviewAggregateParams(items);
		expect(fdmContainers).toHaveLength(1);
		expect(fdmContainers[0].externalId).toBe("ie");
		expect(fdmContainers[0].properties.viewVersion).toBe(FILE_VIEW_ID.version);
		expect(fdmContainers[0].properties.label).toBe("");
	});

	it("uses explicit viewVersion when provided", () => {
		const items: CanvasItem[] = [
			{
				type: "fdmInstance",
				containerId: "c",
				instanceExternalId: "ie",
				instanceSpace: "is",
				viewExternalId: "ve",
				viewSpace: "vs",
				viewVersion: "v2",
				label: "Custom Label",
			},
		];
		const { fdmContainers } = canvasItemsToPreviewAggregateParams(items);
		expect(fdmContainers).toHaveLength(1);
		expect(fdmContainers[0].properties.viewVersion).toBe("v2");
		expect(fdmContainers[0].properties.label).toBe("Custom Label");
	});

	it("maps CanvasAnnotation", () => {
		const items: CanvasItem[] = [
			{
				type: "CanvasAnnotation",
				containerId: "c",
				annotationType: "text",
				properties: { t: 1 },
			},
		];
		const { annotations } = canvasItemsToPreviewAggregateParams(items);
		expect(annotations).toHaveLength(1);
		expect(annotations[0].externalId).toBe("anno-uuid");
		expect(annotations[0].space).toBe(CANVAS_SPACE_INSTANCE);
		expect(annotations[0].properties.annotationType).toBe("text");
	});

	it("uses consistent space configuration across item types", () => {
		const items: CanvasItem[] = [
			{
				type: "fdmInstance",
				containerId: "fdm-container",
				instanceExternalId: "instance-1",
				instanceSpace: "custom-instance-space",
				viewExternalId: "view-1",
				viewSpace: "view-space",
			},
			{
				type: "CanvasAnnotation",
				containerId: "annotation-container",
				annotationType: "note",
			},
		];
		const { fdmContainers, annotations } =
			canvasItemsToPreviewAggregateParams(items);

		// FDM containers use their instance space
		expect(fdmContainers[0].space).toBe("custom-instance-space");

		// Annotations use the configured canvas instance space consistently
		expect(annotations[0].space).toBe(CANVAS_SPACE_INSTANCE);
		expect(annotations[0].space).toBe("IndustrialCanvasInstanceSpace");
	});
});
