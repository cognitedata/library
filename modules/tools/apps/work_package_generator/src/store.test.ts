import { beforeEach, describe, expect, it } from "vitest";
import { DEFAULT_CANVAS_CONFIG } from "@/__mocks__/app-config";
import { PLACEHOLDER_CANVAS_CONFIG, useWorkPackageStore } from "./store";
import type { WorkflowStep } from "./types";

function resetStore() {
	useWorkPackageStore.setState({
		workOrder: null,
		asset: null,
		candidatesByType: {},
		removedCandidateIdsByType: {},
		cachedAssetId: null,
		templateCanvasId: "",
		candidateTemplates: [],
		templateFields: [],
		templateFieldsLoadedFrom: null,
		aiSelections: {},
		aiSelectedCanvasId: null,
		config: DEFAULT_CANVAS_CONFIG,
		currentStep: "work-order-selection" as WorkflowStep,
		isLoading: false,
		loadingMessage: "",
		error: null,
	});
}

describe("useWorkPackageStore", () => {
	beforeEach(() => {
		resetStore();
	});

	it("uses a placeholder canvas config until AppConfigGate syncs", () => {
		useWorkPackageStore.setState({ config: PLACEHOLDER_CANVAS_CONFIG });
		expect(useWorkPackageStore.getState().config).toEqual(
			PLACEHOLDER_CANVAS_CONFIG
		);
	});

	it("sets work order and asset", () => {
		const wo = {
			externalId: "wo-1",
			space: "s",
			name: "N",
		};
		useWorkPackageStore.getState().setWorkOrder(wo);
		useWorkPackageStore.getState().setAsset({
			assetExternalId: "a1",
			space: "s",
		});
		expect(useWorkPackageStore.getState().workOrder).toEqual(wo);
		expect(useWorkPackageStore.getState().asset?.assetExternalId).toBe("a1");
	});

	it("setCandidatesGeneric merges by type key", () => {
		useWorkPackageStore.getState().setCandidatesGeneric("t1", []);
		useWorkPackageStore
			.getState()
			.setCandidatesGeneric("t2", [
				{ externalId: "x", space: "s", properties: {} },
			]);
		expect(useWorkPackageStore.getState().candidatesByType.t2).toHaveLength(1);
	});

	it("manages template and AI state", () => {
		useWorkPackageStore.getState().setTemplateCanvasId("tc1");
		useWorkPackageStore.getState().setCandidateTemplates([]);
		useWorkPackageStore.getState().setTemplateFields([]);
		useWorkPackageStore.getState().setTemplateFieldsLoadedFrom("tc1");
		useWorkPackageStore.getState().setAiSelections({
			foo: { externalId: "x", space: "s" },
		});
		useWorkPackageStore.getState().setAiSelectedCanvasId("c1");
		useWorkPackageStore.getState().clearAiSelections();
		expect(useWorkPackageStore.getState().aiSelections).toEqual({});
		expect(useWorkPackageStore.getState().aiSelectedCanvasId).toBeNull();
	});

	it("setLoading and setError", () => {
		useWorkPackageStore.getState().setLoading(true, "busy");
		expect(useWorkPackageStore.getState().isLoading).toBe(true);
		expect(useWorkPackageStore.getState().loadingMessage).toBe("busy");
		useWorkPackageStore.getState().setError("e");
		expect(useWorkPackageStore.getState().error).toBe("e");
		useWorkPackageStore.getState().setLoading(false);
		expect(useWorkPackageStore.getState().isLoading).toBe(false);
	});

	it("addCandidateToType appends with dedupe", () => {
		const item = { externalId: "r1", space: "s", properties: {} };
		useWorkPackageStore.getState().addCandidateToType("file", item);
		useWorkPackageStore.getState().addCandidateToType("file", item);
		expect(useWorkPackageStore.getState().candidatesByType.file).toHaveLength(
			1
		);
	});

	it("removeCandidateFromType drops the item and records the id", () => {
		const keep = { externalId: "keep", space: "s", properties: {} };
		const drop = { externalId: "drop", space: "s", properties: {} };
		useWorkPackageStore.getState().setCandidatesGeneric("file", [keep, drop]);
		useWorkPackageStore.getState().removeCandidateFromType("file", "drop");
		expect(useWorkPackageStore.getState().candidatesByType.file).toEqual([
			keep,
		]);
		expect(
			useWorkPackageStore.getState().removedCandidateIdsByType.file
		).toEqual(["drop"]);
	});

	it("removeCandidateFromType does not duplicate removed ids", () => {
		useWorkPackageStore
			.getState()
			.setCandidatesGeneric("file", [
				{ externalId: "r1", space: "s", properties: {} },
			]);
		useWorkPackageStore.getState().removeCandidateFromType("file", "r1");
		useWorkPackageStore.getState().removeCandidateFromType("file", "r1");
		expect(
			useWorkPackageStore.getState().removedCandidateIdsByType.file
		).toEqual(["r1"]);
	});

	it("removeCandidateFromType clears matching AI selections", () => {
		useWorkPackageStore
			.getState()
			.setCandidatesGeneric("file", [
				{ externalId: "r1", space: "s", properties: {} },
			]);
		useWorkPackageStore.getState().setSlotSelection("diagram", {
			externalId: "r1",
			space: "s",
			name: "R1",
		});
		useWorkPackageStore.getState().setSlotSelection("other", {
			externalId: "keep",
			space: "s",
		});
		useWorkPackageStore.getState().removeCandidateFromType("file", "r1");
		expect(useWorkPackageStore.getState().aiSelections.diagram).toBeNull();
		expect(useWorkPackageStore.getState().aiSelections.other?.externalId).toBe(
			"keep"
		);
	});

	it("addCandidateToType un-removes a previously dropped id", () => {
		const item = { externalId: "r1", space: "s", properties: {} };
		useWorkPackageStore.getState().setCandidatesGeneric("file", [item]);
		useWorkPackageStore.getState().removeCandidateFromType("file", "r1");
		useWorkPackageStore.getState().addCandidateToType("file", item);
		expect(useWorkPackageStore.getState().candidatesByType.file).toEqual([
			item,
		]);
		expect(
			useWorkPackageStore.getState().removedCandidateIdsByType.file
		).toEqual([]);
	});

	it("resetForNewWorkOrder clears removed candidate ids", () => {
		useWorkPackageStore
			.getState()
			.setCandidatesGeneric("file", [
				{ externalId: "r1", space: "s", properties: {} },
			]);
		useWorkPackageStore.getState().removeCandidateFromType("file", "r1");
		useWorkPackageStore.getState().resetForNewWorkOrder();
		expect(useWorkPackageStore.getState().removedCandidateIdsByType).toEqual(
			{}
		);
	});

	it("setSlotSelection merges and clears", () => {
		useWorkPackageStore.getState().setAiSelections({
			slot1: { externalId: "a", space: "s" },
		});
		useWorkPackageStore.getState().setSlotSelection("slot1", {
			externalId: "b",
			space: "s",
			name: "B",
		});
		expect(useWorkPackageStore.getState().aiSelections.slot1?.externalId).toBe(
			"b"
		);
		useWorkPackageStore.getState().setSlotSelection("slot2", {
			externalId: "c",
			space: "s",
		});
		expect(useWorkPackageStore.getState().aiSelections.slot2?.externalId).toBe(
			"c"
		);
		useWorkPackageStore.getState().setSlotSelection("slot2", null);
		expect(useWorkPackageStore.getState().aiSelections.slot2).toBeNull();
	});

	it("resetForNewWorkOrder clears candidates and selections after manual edits", () => {
		useWorkPackageStore.getState().addCandidateToType("file", {
			externalId: "r1",
			space: "s",
			properties: {},
		});
		useWorkPackageStore.getState().setSlotSelection("slot", {
			externalId: "x",
			space: "s",
		});
		useWorkPackageStore
			.getState()
			.setCandidateTemplates([
				{ externalId: "t", space: "s", name: "T", visibility: "public" },
			]);
		useWorkPackageStore.getState().resetForNewWorkOrder();
		expect(useWorkPackageStore.getState().candidatesByType).toEqual({});
		expect(useWorkPackageStore.getState().aiSelections).toEqual({});
	});

	it("resetForNewWorkOrder keeps candidateTemplates and config", () => {
		useWorkPackageStore.getState().setWorkOrder({
			externalId: "w",
			space: "s",
		});
		useWorkPackageStore
			.getState()
			.setCandidateTemplates([
				{ externalId: "t", space: "s", name: "T", visibility: "public" },
			]);
		const cfg = useWorkPackageStore.getState().config;
		useWorkPackageStore.getState().resetForNewWorkOrder();
		expect(useWorkPackageStore.getState().workOrder).toBeNull();
		expect(useWorkPackageStore.getState().candidateTemplates).toHaveLength(1);
		expect(useWorkPackageStore.getState().config).toEqual(cfg);
	});

	it("setCachedAssetId", () => {
		useWorkPackageStore.getState().setCachedAssetId("aid");
		expect(useWorkPackageStore.getState().cachedAssetId).toBe("aid");
	});

	it("setCurrentStep", () => {
		useWorkPackageStore.getState().setCurrentStep("ai-selection");
		expect(useWorkPackageStore.getState().currentStep).toBe("ai-selection");
	});
});
