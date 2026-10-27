/**
 * Video Node Component
 * Renders a video player with controls
 */

import type {
	NodeComponentProps,
	PlaceholderProps,
} from "@dune-canvas/canvas-lib";
import type React from "react";
import { useEffect, useRef, useState } from "react";

export interface VideoNodeData {
	url: string;
	currentTime?: number;
	isPlaying?: boolean;
	volume?: number;
	muted?: boolean;
	loop?: boolean;
}

export const VideoNode: React.FC<NodeComponentProps<VideoNodeData>> = ({
	data,
	size,
	onDataChange,
	isSelected,
	isFocused,
	canvasScale,
}) => {
	const videoRef = useRef<HTMLVideoElement>(null);
	const dataRef = useRef(data);
	dataRef.current = data;
	const onDataChangeRef = useRef(onDataChange);
	onDataChangeRef.current = onDataChange;
	const [isLoading, setIsLoading] = useState(true);
	const [error, setError] = useState<string | null>(null);
	const [duration, setDuration] = useState(0);
	const [currentTime, setCurrentTime] = useState(data.currentTime || 0);
	const [isPlaying, setIsPlaying] = useState(data.isPlaying || false);

	useEffect(() => {
		if (!data.url) {
			setError("No video URL provided");
			setIsLoading(false);
		}
	}, [data.url]);

	useEffect(() => {
		const video = videoRef.current;
		if (!video) return;

		const handleLoadedMetadata = () => {
			setDuration(video.duration);
			setIsLoading(false);
			if (dataRef.current.currentTime) {
				video.currentTime = dataRef.current.currentTime;
			}
		};

		const handleTimeUpdate = () => {
			setCurrentTime(video.currentTime);
		};

		const handleError = () => {
			setError("Failed to load video");
			setIsLoading(false);
		};

		const handleEnded = () => {
			setIsPlaying(false);
			onDataChangeRef.current({ ...dataRef.current, isPlaying: false });
		};

		video.addEventListener("loadedmetadata", handleLoadedMetadata);
		video.addEventListener("timeupdate", handleTimeUpdate);
		video.addEventListener("error", handleError);
		video.addEventListener("ended", handleEnded);

		return () => {
			video.removeEventListener("loadedmetadata", handleLoadedMetadata);
			video.removeEventListener("timeupdate", handleTimeUpdate);
			video.removeEventListener("error", handleError);
			video.removeEventListener("ended", handleEnded);
		};
	}, [data.url]);

	useEffect(() => {
		const video = videoRef.current;
		if (!video) return;

		if (isPlaying) {
			video.play().catch(() => {
				setIsPlaying(false);
			});
		} else {
			video.pause();
		}
	}, [isPlaying]);

	const togglePlay = (e: React.MouseEvent) => {
		e.stopPropagation();
		const newIsPlaying = !isPlaying;
		setIsPlaying(newIsPlaying);
		onDataChange({ ...data, isPlaying: newIsPlaying });
	};

	const handleSeek = (e: React.ChangeEvent<HTMLInputElement>) => {
		const video = videoRef.current;
		if (!video) return;
		const time = parseFloat(e.target.value);
		video.currentTime = time;
		setCurrentTime(time);
		onDataChange({ ...data, currentTime: time });
	};

	const formatTime = (seconds: number): string => {
		const mins = Math.floor(seconds / 60);
		const secs = Math.floor(seconds % 60);
		return `${mins}:${secs.toString().padStart(2, "0")}`;
	};

	if (error) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					backgroundColor: "#1a1a1a",
					border: "1px solid #333",
					borderRadius: 4,
					color: "#999",
					fontSize: 14,
				}}
			>
				{error}
			</div>
		);
	}

	return (
		<div
			style={{
				width: "100%",
				height: "100%",
				display: "flex",
				flexDirection: "column",
				backgroundColor: "#000",
				borderRadius: 4,
				overflow: "hidden",
			}}
		>
			<div style={{ flex: 1, position: "relative" }}>
				{isLoading && (
					<div
						style={{
							position: "absolute",
							inset: 0,
							display: "flex",
							alignItems: "center",
							justifyContent: "center",
							backgroundColor: "#1a1a1a",
						}}
					>
						<div
							style={{
								width: 32,
								height: 32,
								border: "2px solid #333",
								borderTopColor: "#fff",
								borderRadius: "50%",
								animation: "spin 1s linear infinite",
							}}
						/>
					</div>
				)}
				<video
					ref={videoRef}
					src={data.url}
					style={{
						width: "100%",
						height: "100%",
						objectFit: "contain",
						display: isLoading ? "none" : "block",
					}}
					muted={data.muted ?? false}
					loop={data.loop ?? false}
					playsInline
				/>

				{/* Play/Pause overlay */}
				{!isLoading && (
					<button
						onClick={togglePlay}
						style={{
							position: "absolute",
							top: "50%",
							left: "50%",
							transform: "translate(-50%, -50%)",
							width: 64,
							height: 64,
							borderRadius: "50%",
							backgroundColor: "rgba(0,0,0,0.6)",
							border: "none",
							cursor: "pointer",
							display: "flex",
							alignItems: "center",
							justifyContent: "center",
							opacity: isPlaying ? 0 : 0.9,
							transition: "opacity 0.2s",
						}}
						onMouseEnter={(e) => {
							e.currentTarget.style.opacity = "1";
						}}
						onMouseLeave={(e) => {
							e.currentTarget.style.opacity = isPlaying ? "0" : "0.9";
						}}
					>
						{isPlaying ? (
							<svg width={24} height={24} viewBox="0 0 24 24" fill="white">
								<rect x="6" y="4" width="4" height="16" />
								<rect x="14" y="4" width="4" height="16" />
							</svg>
						) : (
							<svg width={24} height={24} viewBox="0 0 24 24" fill="white">
								<polygon points="5,3 19,12 5,21" />
							</svg>
						)}
					</button>
				)}
			</div>

			{/* Controls */}
			<div
				style={{
					padding: "8px 12px",
					backgroundColor: "#1a1a1a",
					display: "flex",
					alignItems: "center",
					gap: 8,
				}}
			>
				<button
					onClick={togglePlay}
					style={{
						background: "none",
						border: "none",
						cursor: "pointer",
						padding: 4,
					}}
				>
					{isPlaying ? (
						<svg width={16} height={16} viewBox="0 0 24 24" fill="white">
							<rect x="6" y="4" width="4" height="16" />
							<rect x="14" y="4" width="4" height="16" />
						</svg>
					) : (
						<svg width={16} height={16} viewBox="0 0 24 24" fill="white">
							<polygon points="5,3 19,12 5,21" />
						</svg>
					)}
				</button>

				<span style={{ color: "#fff", fontSize: 12, fontFamily: "monospace" }}>
					{formatTime(currentTime)}
				</span>

				<input
					type="range"
					min={0}
					max={duration || 100}
					value={currentTime}
					onChange={handleSeek}
					style={{
						flex: 1,
						height: 4,
						cursor: "pointer",
					}}
				/>

				<span style={{ color: "#fff", fontSize: 12, fontFamily: "monospace" }}>
					{formatTime(duration)}
				</span>
			</div>

			<style>{`
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
      `}</style>
		</div>
	);
};

export const VideoPlaceholder: React.FC<PlaceholderProps<VideoNodeData>> = ({
	size,
}) => {
	return (
		<div
			style={{
				width: size.width,
				height: size.height,
				backgroundColor: "#1a1a1a",
				borderRadius: 4,
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
			}}
		>
			<svg
				width={48}
				height={48}
				viewBox="0 0 24 24"
				fill="none"
				stroke="#666"
				strokeWidth="1.5"
			>
				<rect x="2" y="4" width="20" height="16" rx="2" />
				<polygon points="10,8 16,12 10,16" fill="#666" stroke="none" />
			</svg>
		</div>
	);
};
