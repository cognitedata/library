import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const appRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const html = readFileSync(join(appRoot, "index.html"), "utf8");
const readme = readFileSync(join(appRoot, "README.md"), "utf8");
const styleCss = readFileSync(join(appRoot, "src/style.css"), "utf8");
const templateCanvasCardSrc = readFileSync(
	join(
		appRoot,
		"src/components/work-package/template-selection-step/TemplateCanvasCard.tsx"
	),
	"utf8"
);

interface OklchColor {
	l: number;
	c: number;
	h: number;
}

function parseOklch(raw: string): OklchColor {
	const match = /oklch\(\s*([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s*\)/.exec(raw);
	if (!match) {
		throw new Error(`Invalid oklch token: ${raw}`);
	}
	return {
		l: Number(match[1]),
		c: Number(match[2]),
		h: Number(match[3]),
	};
}

function clamp01(value: number): number {
	return Math.min(1, Math.max(0, value));
}

function oklchToLinearRgb({ l, c, h }: OklchColor): [number, number, number] {
	const hueRad = (h * Math.PI) / 180;
	const a = c * Math.cos(hueRad);
	const b = c * Math.sin(hueRad);

	const lPrime = l + 0.3963377774 * a + 0.2158037573 * b;
	const mPrime = l - 0.1055613458 * a - 0.0638541728 * b;
	const sPrime = l - 0.0894841775 * a - 1.291485548 * b;

	const l3 = lPrime ** 3;
	const m3 = mPrime ** 3;
	const s3 = sPrime ** 3;

	const r = +4.0767416621 * l3 - 3.3077115913 * m3 + 0.2309699292 * s3;
	const g = -1.2684380046 * l3 + 2.6097574011 * m3 - 0.3413193965 * s3;
	const bl = -0.0041960863 * l3 - 0.7034186147 * m3 + 1.707614701 * s3;

	return [clamp01(r), clamp01(g), clamp01(bl)];
}

function srgbChannelToLinear(channel255: number): number {
	const c = channel255 / 255;
	return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
}

function hexToLinearRgb(hex: string): [number, number, number] {
	const cleaned = hex.replace(/^#/, "");
	const r = Number.parseInt(cleaned.slice(0, 2), 16);
	const g = Number.parseInt(cleaned.slice(2, 4), 16);
	const b = Number.parseInt(cleaned.slice(4, 6), 16);
	return [
		srgbChannelToLinear(r),
		srgbChannelToLinear(g),
		srgbChannelToLinear(b),
	];
}

function relativeLuminance([r, g, b]: [number, number, number]): number {
	return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function wcagContrastRatio(
	fg: [number, number, number],
	bg: [number, number, number]
): number {
	const lumA = relativeLuminance(fg);
	const lumB = relativeLuminance(bg);
	const lighter = Math.max(lumA, lumB);
	const darker = Math.min(lumA, lumB);
	return (lighter + 0.05) / (darker + 0.05);
}

function extractCssThemeTokens(
	css: string,
	selector: ":root" | ".dark"
): Record<string, string> {
	const blockPattern =
		selector === ":root" ? /:root\s*\{([^}]*)\}/ : /\.dark\s*\{([^}]*)\}/;
	const match = blockPattern.exec(css);
	if (!match?.[1]) {
		throw new Error(`Missing ${selector} block in style.css`);
	}
	const tokens: Record<string, string> = {};
	for (const entry of match[1].matchAll(/--([a-z0-9-]+)\s*:\s*([^;]+);/gi)) {
		const name = entry[1];
		const value = entry[2]?.trim();
		if (name && value) tokens[name] = value;
	}
	return tokens;
}

// Tailwind v4 palette hex values used by TemplateCanvasCard badges.
const TAILWIND_PALETTE_HEX: Record<string, string> = {
	white: "#ffffff",
	"blue-800": "#1e40af",
	"blue-200": "#bfdbfe",
	"blue-950": "#172554",
	"emerald-800": "#065f46",
	"emerald-200": "#a7f3d0",
	"emerald-950": "#022c22",
	"neutral-200": "#e5e5e5",
	"neutral-900": "#171717",
	"neutral-950": "#0a0a0a",
};

function paletteRgb(token: string): [number, number, number] {
	const hex = TAILWIND_PALETTE_HEX[token];
	if (!hex) throw new Error(`Unknown palette token: ${token}`);
	return hexToLinearRgb(hex);
}

describe("QA finding surface checks", () => {
	it("B-B2: index.html links a favicon", () => {
		expect(html).toContain('rel="icon"');
		expect(html).toContain("favicon.svg");
	});

	it("B-B3: index.html does not include a Permissions-Policy meta tag", () => {
		expect(html).not.toContain("Permissions-Policy");
	});

	it("N07: muted text and badge colour pairs meet WCAG AA 4.5:1 contrast in light and dark mode", () => {
		const light = extractCssThemeTokens(styleCss, ":root");
		const dark = extractCssThemeTokens(styleCss, ".dark");

		// Muted text against background, card, and muted surfaces in light mode
		const lightMutedFg = oklchToLinearRgb(
			parseOklch(light["muted-foreground"] ?? "")
		);
		expect(
			wcagContrastRatio(
				lightMutedFg,
				oklchToLinearRgb(parseOklch(light.background ?? ""))
			)
		).toBeGreaterThanOrEqual(4.5);
		expect(
			wcagContrastRatio(
				lightMutedFg,
				oklchToLinearRgb(parseOklch(light.card ?? ""))
			)
		).toBeGreaterThanOrEqual(4.5);
		expect(
			wcagContrastRatio(
				lightMutedFg,
				oklchToLinearRgb(parseOklch(light.muted ?? ""))
			)
		).toBeGreaterThanOrEqual(4.5);

		// Muted text against background, card, and muted surfaces in dark mode
		const darkMutedFg = oklchToLinearRgb(
			parseOklch(dark["muted-foreground"] ?? "")
		);
		expect(
			wcagContrastRatio(
				darkMutedFg,
				oklchToLinearRgb(parseOklch(dark.background ?? ""))
			)
		).toBeGreaterThanOrEqual(4.5);
		expect(
			wcagContrastRatio(
				darkMutedFg,
				oklchToLinearRgb(parseOklch(dark.card ?? ""))
			)
		).toBeGreaterThanOrEqual(4.5);
		expect(
			wcagContrastRatio(
				darkMutedFg,
				oklchToLinearRgb(parseOklch(dark.muted ?? ""))
			)
		).toBeGreaterThanOrEqual(4.5);

		// Secondary badge token pairs (used for SelectionsStep and ResourcesStep badges)
		expect(
			wcagContrastRatio(
				oklchToLinearRgb(parseOklch(light["secondary-foreground"] ?? "")),
				oklchToLinearRgb(parseOklch(light.secondary ?? ""))
			)
		).toBeGreaterThanOrEqual(4.5);
		expect(
			wcagContrastRatio(
				oklchToLinearRgb(parseOklch(dark["secondary-foreground"] ?? "")),
				oklchToLinearRgb(parseOklch(dark.secondary ?? ""))
			)
		).toBeGreaterThanOrEqual(4.5);

		// TemplateCanvasCard "AI Pick", "Selected", and slot badges
		expect(templateCanvasCardSrc).toContain(
			"bg-blue-800 text-white hover:bg-blue-800 dark:bg-blue-200 dark:text-blue-950"
		);
		expect(
			wcagContrastRatio(paletteRgb("white"), paletteRgb("blue-800"))
		).toBeGreaterThanOrEqual(4.5);
		expect(
			wcagContrastRatio(paletteRgb("blue-950"), paletteRgb("blue-200"))
		).toBeGreaterThanOrEqual(4.5);

		expect(templateCanvasCardSrc).toContain(
			"bg-emerald-800 text-white hover:bg-emerald-800 dark:bg-emerald-200 dark:text-emerald-950"
		);
		expect(
			wcagContrastRatio(paletteRgb("white"), paletteRgb("emerald-800"))
		).toBeGreaterThanOrEqual(4.5);
		expect(
			wcagContrastRatio(paletteRgb("emerald-950"), paletteRgb("emerald-200"))
		).toBeGreaterThanOrEqual(4.5);

		expect(templateCanvasCardSrc).toContain(
			"bg-neutral-200 text-neutral-900 border-neutral-500 dark:bg-neutral-200 dark:text-neutral-950"
		);
		expect(
			wcagContrastRatio(paletteRgb("neutral-900"), paletteRgb("neutral-200"))
		).toBeGreaterThanOrEqual(4.5);
		expect(
			wcagContrastRatio(paletteRgb("neutral-950"), paletteRgb("neutral-200"))
		).toBeGreaterThanOrEqual(4.5);
	});

	it("N18: the permission table names file and time series reads", () => {
		expect(readme).toContain("files:read");
		expect(readme).toContain("timeSeries:read");
	});
});
