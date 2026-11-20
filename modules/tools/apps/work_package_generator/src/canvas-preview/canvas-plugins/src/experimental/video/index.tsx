/**
 * Video Plugin registration
 */

import type { NodeTypePlugin } from "@dune-canvas/canvas-lib";

import { VideoNode, type VideoNodeData, VideoPlaceholder } from "./VideoNode";

const VideoIcon: React.FC<{ size?: number }> = ({ size = 24 }) => (
	<svg
		width={size}
		height={size}
		viewBox="0 0 24 24"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
	>
		<rect x="2" y="4" width="20" height="16" rx="2" />
		<polygon points="10,8 16,12 10,16" fill="currentColor" stroke="none" />
	</svg>
);

export const videoPlugin: NodeTypePlugin<VideoNodeData> = {
	type: "video",
	displayName: "Video",
	component: VideoNode,
	placeholder: VideoPlaceholder,
	stayMounted: true,
	resizable: {
		horizontal: true,
		vertical: true,
		aspectRatio: "free",
		minSize: { width: 200, height: 150 },
	},
	connectable: true,
	defaultSize: { width: 400, height: 300 },
	defaultData: () => ({
		url: "",
		currentTime: 0,
		isPlaying: false,
		volume: 1,
		muted: false,
		loop: false,
	}),
	icon: VideoIcon,
};

export { VideoNode, type VideoNodeData, VideoPlaceholder };
