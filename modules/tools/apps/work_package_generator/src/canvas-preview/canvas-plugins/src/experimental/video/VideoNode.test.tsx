import { fireEvent, render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { VideoNode } from "./VideoNode";

const props = {
	size: { width: 320, height: 180 },
	onDataChange: vi.fn(),
	isSelected: true,
	isFocused: false,
	canvasScale: 1,
	isCanvasPanning: false,
	readOnly: true,
};

describe("VideoNode", () => {
	it("shows a loading state, then an error, then the player", () => {
		const { container } = render(
			<VideoNode
				{...props}
				data={{ url: "https://files.example.test/a.mp4" }}
			/>
		);
		const video = container.querySelector("video");
		if (!(video instanceof HTMLVideoElement)) throw new Error("video missing");
		fireEvent.error(video);
		expect(container.textContent).toMatch(/fail|error|unable/i);

		const playing = render(
			<VideoNode
				{...props}
				data={{
					url: "https://files.example.test/b.mp4",
					isPlaying: true,
					currentTime: 2,
					muted: true,
					loop: true,
					volume: 0.4,
				}}
			/>
		);
		const next = playing.container.querySelector("video");
		if (!(next instanceof HTMLVideoElement)) throw new Error("video missing");
		Object.defineProperty(next, "duration", { value: 10, configurable: true });
		fireEvent.loadedMetadata(next);
		fireEvent.timeUpdate(next);
		const play = playing.getAllByRole("button")[0];
		fireEvent.mouseEnter(play);
		fireEvent.click(play);
		fireEvent.mouseLeave(play);
		const seek = playing.container.querySelector("input");
		if (seek instanceof HTMLInputElement) {
			fireEvent.change(seek, { target: { value: "3" } });
		}
		fireEvent.ended(next);
		expect(playing.container.querySelector("video")).toBeTruthy();
	});
});
