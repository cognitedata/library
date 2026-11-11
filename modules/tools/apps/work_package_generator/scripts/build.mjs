import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

/**
 * @typedef {(args: string[]) => { status: number | null }} CommandRunner
 */

const appRoot = join(dirname(fileURLToPath(import.meta.url)), "..");

/** @type {CommandRunner} */
function defaultRunner(args) {
	const [tool, ...rest] = args;
	const cliRelPath =
		tool === "tsc"
			? "node_modules/typescript/bin/tsc"
			: tool === "vite"
				? "node_modules/vite/bin/vite.js"
				: undefined;
	if (!cliRelPath) {
		throw new Error(`Unsupported build tool: ${String(tool)}`);
	}
	return spawnSync(process.execPath, [join(appRoot, cliRelPath), ...rest], {
		cwd: appRoot,
		stdio: "inherit",
	});
}

/**
 * Run TypeScript checks for browser and Node configs.
 *
 * @param {CommandRunner} [runner]
 * @returns {number}
 */
export function runTypeCheck(runner = defaultRunner) {
	const steps = [
		["tsc", "--noEmit"],
		["tsc", "-p", "tsconfig.node.json", "--noEmit"],
	];

	for (const step of steps) {
		const result = runner(step);
		if (result.status !== 0) {
			return result.status ?? 1;
		}
	}
	return 0;
}

/**
 * Run TypeScript checks for browser and Node configs, then build the Vite bundle.
 * Returns the first non-zero exit code without running later steps.
 *
 * @param {CommandRunner} [runner]
 * @returns {number}
 */
export function runBuild(runner = defaultRunner) {
	const typeCheckStatus = runTypeCheck(runner);
	if (typeCheckStatus !== 0) {
		return typeCheckStatus;
	}
	const viteResult = runner(["vite", "build"]);
	return viteResult.status === 0 ? 0 : (viteResult.status ?? 1);
}

if (
	process.argv[1] &&
	import.meta.url === pathToFileURL(process.argv[1]).href
) {
	const exitCode = process.argv.includes("--type-check-only")
		? runTypeCheck()
		: runBuild();
	if (exitCode !== 0) {
		process.exit(exitCode);
	}
}
