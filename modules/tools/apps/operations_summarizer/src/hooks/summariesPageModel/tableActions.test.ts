// Test button hands the chosen summary to the Tests page as its baseline (SPEC US2.4).
import { describe, expect, it, vi } from "vitest";
import type { OperationsAction } from "@/contexts/operations-summarizer/types";
import type { SummaryInstance } from "@/types/operations";
import { encodeTestBaselineSelectionKey } from "@/utils/summaryInstanceMapping";
import { createSummariesTableHandlers } from "./tableActions";

const VIEW_KEY = "cdf_cdm/CogniteFile/v1";

function makeSummary(fileProps: Record<string, unknown>): SummaryInstance {
	return {
		externalId: "ops_summary_2026-11-01_2026-11-24_x",
		space: "ops_summaries",
		createdTime: 1,
		properties: { [VIEW_KEY]: fileProps },
	};
}

function setup() {
	const navigate = vi.fn();
	const dispatch = vi.fn<(action: OperationsAction) => void>();
	const handlers = createSummariesTableHandlers({
		sdk: null,
		deleteMutation: {
			mutateAsync: vi.fn().mockResolvedValue(true),
			isPending: false,
		},
		robustDownloadPdf: vi.fn(),
		robustDownloadMarkdown: vi.fn(),
		downloadOperationSummaryPdfWithSignedUrlFirst: vi.fn(),
		navigate,
		dispatch,
		selectedSummary: null,
		deleteCandidate: null,
		setDeleteError: vi.fn(),
		setDeleteCandidate: vi.fn(),
		setSelectedSummary: vi.fn(),
		setIsModalOpen: vi.fn(),
	});
	return { handlers, navigate, dispatch };
}

describe("handleCreateTest", () => {
	it("sets the baseline summary, period and data types before navigating to Tests", () => {
		const summary = makeSummary({
			tags: ["range:2026-11-01_2026-11-24"],
			description: "OPS:11000\n# Summary",
		});
		const { handlers, navigate, dispatch } = setup();

		handlers.handleCreateTest(summary);

		expect(dispatch.mock.calls.map(([action]) => action)).toEqual([
			{
				type: "SET_TEST_SELECTED_SUMMARY_ID",
				payload: encodeTestBaselineSelectionKey(summary),
			},
			{ type: "SET_TEST_START_DATE", payload: "2026-11-01" },
			{ type: "SET_TEST_END_DATE", payload: "2026-11-24" },
			{ type: "SET_TEST_SELECTED_DATA_TYPES", payload: ["assets", "files"] },
		]);
		expect(navigate).toHaveBeenCalledWith("tests");
		expect(dispatch.mock.invocationCallOrder[0]).toBeLessThan(
			navigate.mock.invocationCallOrder[0]
		);
	});

	it("falls back to the dates in the external id and skips data types without a bitmap", () => {
		const summary = makeSummary({});
		const { handlers, navigate, dispatch } = setup();

		handlers.handleCreateTest(summary);

		expect(dispatch.mock.calls.map(([action]) => action)).toEqual([
			{
				type: "SET_TEST_SELECTED_SUMMARY_ID",
				payload: encodeTestBaselineSelectionKey(summary),
			},
			{ type: "SET_TEST_START_DATE", payload: "2026-11-01" },
			{ type: "SET_TEST_END_DATE", payload: "2026-11-24" },
		]);
		expect(navigate).toHaveBeenCalledWith("tests");
	});
});
