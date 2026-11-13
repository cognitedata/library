import { renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
	fidelitySettings,
	isPanning,
	isZooming,
	stableScale,
	viewportScale,
} from "../core/viewportSignals";
import { useProgressiveFidelity } from "./useProgressiveFidelity";

describe("useProgressiveFidelity", () => {
	it("tracks idle, panning, and zooming fidelity", () => {
		const { result, rerender } = renderHook(() =>
			useProgressiveFidelity({ useStableScale: false, maxZoomMultiplier: 2 })
		);
		result.current.setQualityBias(1);
		result.current.setForceHighFidelity(true);
		result.current.setMaxDataPoints(100);
		result.current.setDownsampleMode("minmax");
		isPanning.value = true;
		rerender();
		expect(result.current.isPanning).toBe(true);

		result.current.setForceHighFidelity(false);
		result.current.setQualityBias(0.8);
		isPanning.value = false;
		isZooming.value = true;
		viewportScale.value = 3;
		stableScale.value = 2;
		rerender();
		expect(result.current.fidelity).toBe("medium");

		isZooming.value = false;
		result.current.setQualityBias(-0.8);
		rerender();
		expect(result.current.shouldReduceFidelity).toBe(true);

		fidelitySettings.value = {
			qualityBias: 0,
			forceHighFidelity: false,
			maxDataPoints: 10000,
			downsampleMode: "lttb",
		};
		isPanning.value = false;
		isZooming.value = false;
	});
});
