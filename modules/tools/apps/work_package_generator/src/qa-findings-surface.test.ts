import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const appRoot = join(dirname(fileURLToPath(import.meta.url)), "..");

describe("QA finding surface checks", () => {
	it("B-B2: index.html links a favicon", () => {
		const html = readFileSync(join(appRoot, "index.html"), "utf8");
		expect(html).toContain('rel="icon"');
		expect(html).toContain("favicon.svg");
	});

	it("B-B3: index.html does not include a Permissions-Policy meta tag", () => {
		const html = readFileSync(join(appRoot, "index.html"), "utf8");
		expect(html).not.toContain("Permissions-Policy");
	});

	it("N07: muted text tokens stay dark on light backgrounds and light in dark mode", () => {
		const css = readFileSync(join(appRoot, "src/style.css"), "utf8");
		expect(css).toContain("--muted-foreground: oklch(0.45 0 0)");
		expect(css).toContain("--muted-foreground: oklch(0.86 0 0)");
	});

	it("N18: the permission table names file and time series reads", () => {
		const readme = readFileSync(join(appRoot, "README.md"), "utf8");
		expect(readme).toContain("files:read");
		expect(readme).toContain("timeSeries:read");
	});
});
