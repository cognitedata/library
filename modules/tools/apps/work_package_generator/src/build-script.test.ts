import { describe, expect, it, vi } from "vitest";
import pkg from "../package.json";
import { runBuild } from "../scripts/build.mjs";

describe("package scripts", () => {
	it("B-T1: build runs scripts/build.mjs, checks browser and node tsconfigs, and stops on tsc failure", () => {
		expect(pkg.scripts.build).toBe("node scripts/build.mjs");

		const failingFirstStep = vi.fn().mockReturnValueOnce({ status: 2 });
		expect(runBuild(failingFirstStep)).toBe(2);
		expect(failingFirstStep).toHaveBeenCalledTimes(1);
		expect(failingFirstStep).toHaveBeenNthCalledWith(1, ["tsc", "--noEmit"]);

		const failingNodeTsc = vi
			.fn()
			.mockReturnValueOnce({ status: 0 })
			.mockReturnValueOnce({ status: 1 });
		expect(runBuild(failingNodeTsc)).toBe(1);
		expect(failingNodeTsc).toHaveBeenCalledTimes(2);
		expect(failingNodeTsc).toHaveBeenNthCalledWith(2, [
			"tsc",
			"-p",
			"tsconfig.node.json",
			"--noEmit",
		]);

		const allPassing = vi.fn().mockReturnValue({ status: 0 });
		expect(runBuild(allPassing)).toBe(0);
		expect(allPassing.mock.calls).toEqual([
			[["tsc", "--noEmit"]],
			[["tsc", "-p", "tsconfig.node.json", "--noEmit"]],
			[["vite", "build"]],
		]);
	});
});
