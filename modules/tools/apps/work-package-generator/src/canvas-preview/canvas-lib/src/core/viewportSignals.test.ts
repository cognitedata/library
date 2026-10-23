import { afterEach, describe, expect, it, vi } from "vitest";

import {
	fidelityLevel,
	fidelitySettings,
	isInteracting,
	isPanning,
	isZooming,
	qualityMultiplier,
	setDownsampleMode,
	setFidelitySettings,
	setForceHighFidelity,
	setMaxDataPoints,
	setQualityBias,
	stableScale,
	updatePanning,
	updateScale,
	updateViewportBounds,
	viewportBounds,
	virtualBounds,
} from "./viewportSignals";

describe("viewportSignals", () => {
	afterEach(() => {
		vi.useRealTimers();
		isZooming.value = false;
		isPanning.value = false;
		setFidelitySettings({
			forceHighFidelity: false,
			qualityBias: 0,
			maxDataPoints: 2000,
			downsampleMode: "lttb",
		});
	});

	it("reports high fidelity when idle", () => {
		expect(fidelityLevel.value).toBe("high");
		expect(qualityMultiplier.value).toBe(1);
	});

	it("drops fidelity while zooming unless forced high", () => {
		isPanning.value = true;
		expect(isInteracting.value).toBe(true);
		expect(fidelityLevel.value).toBe("low");
		isPanning.value = false;
		isZooming.value = true;
		expect(fidelityLevel.value).toBe("low");
		setQualityBias(0.8);
		expect(fidelityLevel.value).toBe("medium");
		setForceHighFidelity(true);
		expect(fidelityLevel.value).toBe("high");
	});

	it("uses medium fidelity when idle and the bias is very low", () => {
		setQualityBias(-0.8);
		expect(fidelityLevel.value).toBe("medium");
		expect(qualityMultiplier.value).toBeCloseTo(2 ** -0.8);
	});

	it("clamps quality bias and data-point limits", () => {
		setQualityBias(4);
		expect(fidelitySettings.value.qualityBias).toBe(1);
		setMaxDataPoints(10);
		expect(fidelitySettings.value.maxDataPoints).toBe(100);
		setDownsampleMode("minmax");
		expect(fidelitySettings.value.downsampleMode).toBe("minmax");
	});

	it("debounces the end of a zoom", () => {
		vi.useFakeTimers();
		updateScale(2);
		expect(isZooming.value).toBe(true);
		updateScale(3);
		vi.advanceTimersByTime(200);
		expect(isZooming.value).toBe(false);
		expect(stableScale.value).toBe(3);
	});

	it("debounces the end of a pan", () => {
		vi.useFakeTimers();
		updatePanning(true);
		expect(isPanning.value).toBe(true);
		updatePanning(true);
		updatePanning(false);
		expect(isPanning.value).toBe(true);
		vi.advanceTimersByTime(100);
		expect(isPanning.value).toBe(false);
	});

	it("stores visible and virtual bounds", () => {
		updateViewportBounds(
			{ x: 10, y: 20, scale: 2 },
			{ width: 200, height: 100 }
		);
		expect(viewportBounds.value).toEqual({
			x: -5,
			y: -10,
			width: 100,
			height: 50,
		});
		expect(virtualBounds.value.width).toBeGreaterThan(100);
	});
});
