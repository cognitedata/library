import { spawnSync } from "node:child_process";

function run(args) {
	const result = spawnSync("npx", args, { stdio: "inherit", shell: true });
	if (result.status !== 0) {
		process.exit(result.status ?? 1);
	}
}

run(["tsc"]);
run(["vite", "build"]);
