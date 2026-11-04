import { describe, expect, it, vi } from "vitest";
import { cdfClientTestDouble } from "../__mocks__/cdf-client-test-double";

// vi.mock: task runner is a module singleton; inline schedule keeps this unit test synchronous.
vi.mock("./semaphore", () => ({
	cdfTaskRunner: {
		schedule: async <T>(fn: () => Promise<T>) => fn(),
	},
}));

import {
	SOLUTION_TAG_VIEW,
	WORK_PACKAGE_SOLUTION_TAG,
} from "@/__mocks__/app-config";
import {
	ensureWorkPackageSolutionTag,
	readSolutionTagsFromRootProperties,
	withWorkPackageSolutionTag,
	workPackageSolutionTagInstanceRef,
} from "./canvas-solution-tags";

describe(workPackageSolutionTagInstanceRef.name, () => {
	it("returns the stable Work Package tag ref", () => {
		expect(workPackageSolutionTagInstanceRef()).toEqual({
			externalId: "wpg-work-package",
			space: "SolutionTagsInstanceSpace",
		});
	});
});

describe(ensureWorkPackageSolutionTag.name, () => {
	it("upserts the CogniteSolutionTag node named Work Package", async () => {
		const mockUpsert = vi.fn().mockResolvedValue({});
		const client = cdfClientTestDouble({
			instances: { upsert: mockUpsert },
		});

		await ensureWorkPackageSolutionTag(client);

		expect(mockUpsert).toHaveBeenCalledOnce();
		const { items } = mockUpsert.mock.calls[0][0] as {
			items: Array<{
				space: string;
				externalId: string;
				sources: Array<{
					source: { externalId: string; space: string; version: string };
					properties: { name: string };
				}>;
			}>;
		};
		expect(items[0]?.space).toBe(WORK_PACKAGE_SOLUTION_TAG.instanceSpace);
		expect(items[0]?.externalId).toBe(WORK_PACKAGE_SOLUTION_TAG.externalId);
		expect(items[0]?.sources[0]?.source).toEqual({
			type: "view",
			space: SOLUTION_TAG_VIEW.space,
			externalId: SOLUTION_TAG_VIEW.externalId,
			version: SOLUTION_TAG_VIEW.version,
		});
		expect(items[0]?.sources[0]?.properties).toEqual({ name: "Work Package" });
	});
});

describe(readSolutionTagsFromRootProperties.name, () => {
	it("parses solutionTags refs from canvas root properties", () => {
		expect(
			readSolutionTagsFromRootProperties({
				solutionTags: [
					{
						space: "SolutionTagsInstanceSpace",
						externalId: "wpg-work-package",
					},
				],
			})
		).toEqual([
			{
				space: "SolutionTagsInstanceSpace",
				externalId: "wpg-work-package",
			},
		]);
	});

	it("returns undefined when solutionTags is missing or empty", () => {
		expect(readSolutionTagsFromRootProperties({})).toBeUndefined();
		expect(
			readSolutionTagsFromRootProperties({ solutionTags: [] })
		).toBeUndefined();
	});
});

describe(withWorkPackageSolutionTag.name, () => {
	it("adds Work Package when no tags exist", () => {
		expect(withWorkPackageSolutionTag()).toEqual([
			workPackageSolutionTagInstanceRef(),
		]);
	});

	it("keeps existing tags and appends Work Package if missing", () => {
		expect(
			withWorkPackageSolutionTag([
				{ space: "SolutionTagsInstanceSpace", externalId: "other" },
			])
		).toEqual([
			{ space: "SolutionTagsInstanceSpace", externalId: "other" },
			workPackageSolutionTagInstanceRef(),
		]);
	});
});

describe("readSolutionTagsFromRootProperties edge cases", () => {
	it("accepts snake_case external_id refs", () => {
		// Act
		const tags = readSolutionTagsFromRootProperties({
			solutionTags: [{ space: "s", external_id: "tag-1" }],
		});

		// Assert
		expect(tags).toEqual([{ space: "s", externalId: "tag-1" }]);
	});

	it("skips non-object entries and entries missing space or externalId", () => {
		// Act
		const tags = readSolutionTagsFromRootProperties({
			solutionTags: [
				null,
				"text",
				{ externalId: "no-space" },
				{ space: "s" },
				{ space: 1, externalId: 2 },
				{ space: "s", externalId: "ok" },
			],
		});

		// Assert
		expect(tags).toEqual([{ space: "s", externalId: "ok" }]);
	});

	it("returns undefined when no entry is valid", () => {
		expect(
			readSolutionTagsFromRootProperties({ solutionTags: [null, {}] })
		).toBeUndefined();
		expect(
			readSolutionTagsFromRootProperties({ solutionTags: "nope" })
		).toBeUndefined();
	});
});

describe("withWorkPackageSolutionTag when already present", () => {
	it("returns the existing list unchanged", () => {
		// Arrange
		const existing = [
			{ space: "other", externalId: "x" },
			workPackageSolutionTagInstanceRef(),
		];

		// Act
		const result = withWorkPackageSolutionTag(existing);

		// Assert
		expect(result).toBe(existing);
	});

	it("appends when only the space matches", () => {
		// Arrange
		const { space } = workPackageSolutionTagInstanceRef();

		// Act
		const result = withWorkPackageSolutionTag([{ space, externalId: "x" }]);

		// Assert
		expect(result).toHaveLength(2);
	});
});
