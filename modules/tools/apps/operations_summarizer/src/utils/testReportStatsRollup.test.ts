import { describe, expect, it, vi } from "vitest";
import {
	applyRollupDelta,
	applyTestReportStatsDelta,
	emptyTestReportStatsRollup,
	readOrBackfillTestReportStats,
	TEST_REPORT_STATS_EXTERNAL_ID,
} from "./testReportStatsRollup";

const VIEW_KEY = "cdf_cdm/CogniteFile/v1";

function rollupNode(sourceContext: string) {
	return {
		instanceType: "node" as const,
		externalId: TEST_REPORT_STATS_EXTERNAL_ID,
		space: "ops_summaries",
		properties: { [VIEW_KEY]: { sourceContext } },
	};
}

const stored = {
	totalTests: 2,
	averageScore: 80,
	excellentCount: 1,
	goodCount: 1,
	poorCount: 0,
	averageDataVolume: 15,
	totalScore: 160,
	totalVolume: 30,
};

describe("test report stats rollup", () => {
	it("keeps averages exact when a report is added", () => {
		const next = applyRollupDelta(stored, {
			score: 50,
			volume: 30,
			direction: "add",
		});
		expect(next.totalTests).toBe(3);
		expect(next.totalScore).toBe(210);
		expect(next.totalVolume).toBe(60);
		expect(next.averageScore).toBeCloseTo(70, 5);
		expect(next.averageDataVolume).toBeCloseTo(20, 5);
		expect(next.poorCount).toBe(1);
	});

	it("reads the stored node without listing test reports", async () => {
		const retrieve = vi
			.fn()
			.mockResolvedValue({ items: [rollupNode(JSON.stringify(stored))] });
		const list = vi.fn();
		const stats = await readOrBackfillTestReportStats({
			instances: { retrieve, list, upsert: vi.fn() },
		});
		expect(stats.totalTests).toBe(2);
		expect(stats.averageScore).toBe(80);
		expect(list).not.toHaveBeenCalled();
		expect(retrieve).toHaveBeenCalledTimes(1);
	});

	it("scans once and upserts when the rollup node is missing", async () => {
		const retrieve = vi.fn().mockResolvedValue({ items: [] });
		const list = vi
			.fn()
			.mockResolvedValue({ items: [], nextCursor: undefined });
		const upsert = vi.fn().mockResolvedValue({});
		const stats = await readOrBackfillTestReportStats({
			instances: { retrieve, list, upsert },
		});
		expect(stats).toEqual({
			totalTests: 0,
			averageScore: 0,
			excellentCount: 0,
			goodCount: 0,
			poorCount: 0,
			averageDataVolume: 0,
		});
		expect(list).toHaveBeenCalledTimes(1);
		const payload = upsert.mock.calls[0][0] as {
			items: Array<{ externalId: string }>;
		};
		expect(payload.items[0].externalId).toBe(TEST_REPORT_STATS_EXTERNAL_ID);
		expect(payload.items[0].externalId.startsWith("test_report_")).toBe(false);
		expect(payload.items[0].externalId.startsWith("ops_summary_")).toBe(false);
	});

	it("skips the delta when the rollup node does not exist", async () => {
		const retrieve = vi.fn().mockResolvedValue({ items: [] });
		const upsert = vi.fn();
		await applyTestReportStatsDelta(
			{ instances: { retrieve, upsert } },
			{ score: 90, volume: 1, direction: "add" }
		);
		expect(upsert).not.toHaveBeenCalled();
	});

	describe("overlapping deltas", () => {
		function versionedNode(rollup: typeof stored, version: number) {
			return { ...rollupNode(JSON.stringify(rollup)), version };
		}

		function conflictError() {
			return Object.assign(new Error("version conflict"), { status: 409 });
		}

		it("re-reads and re-applies after a version conflict so both deltas land", async () => {
			let current = versionedNode(stored, 1);
			let hasOtherWriterRun = false;
			const retrieve = vi.fn().mockImplementation(async () => {
				const snapshot = current;
				if (!hasOtherWriterRun) {
					hasOtherWriterRun = true;
					// A concurrent writer lands its delta right after our read.
					current = versionedNode(
						applyRollupDelta(stored, {
							score: 60,
							volume: 10,
							direction: "add",
						}),
						2
					);
				}
				return { items: [snapshot] };
			});
			const upsert = vi.fn().mockImplementation(
				async (payload: {
					items: Array<{
						existingVersion?: number;
						sources: Array<{ properties: { sourceContext: string } }>;
					}>;
				}) => {
					const [item] = payload.items;
					if (item.existingVersion !== current.version) throw conflictError();
					current = versionedNode(
						JSON.parse(item.sources[0].properties.sourceContext),
						current.version + 1
					);
				}
			);

			await applyTestReportStatsDelta(
				{ instances: { retrieve, upsert } },
				{ score: 90, volume: 20, direction: "add" }
			);

			expect(upsert).toHaveBeenCalledTimes(2);
			expect(upsert.mock.calls[0][0].items[0].existingVersion).toBe(1);
			expect(upsert.mock.calls[1][0].items[0].existingVersion).toBe(2);
			const finalRollup = JSON.parse(
				(current.properties[VIEW_KEY] as { sourceContext: string })
					.sourceContext
			);
			expect(finalRollup.totalTests).toBe(4);
			expect(finalRollup.totalScore).toBe(160 + 60 + 90);
			expect(finalRollup.totalVolume).toBe(30 + 10 + 20);
		});

		it("surfaces the conflict after three attempts", async () => {
			const retrieve = vi.fn().mockImplementation(async () => ({
				items: [versionedNode(stored, 1)],
			}));
			const upsert = vi.fn().mockImplementation(async () => {
				throw conflictError();
			});

			await expect(
				applyTestReportStatsDelta(
					{ instances: { retrieve, upsert } },
					{ score: 90, volume: 20, direction: "add" }
				)
			).rejects.toMatchObject({ status: 409 });
			expect(upsert).toHaveBeenCalledTimes(3);
		});
	});

	it("clears the rollup when the last test is removed", () => {
		expect(
			applyRollupDelta(
				{
					...emptyTestReportStatsRollup(),
					totalTests: 1,
					totalScore: 90,
					totalVolume: 4,
					excellentCount: 1,
					averageScore: 90,
					averageDataVolume: 4,
				},
				{ score: 90, volume: 4, direction: "remove" }
			)
		).toEqual(emptyTestReportStatsRollup());
	});
});
