/**
 * Image Node Component
 * Renders an image from URL with loading states
 * Supports both direct URL and flexible file source
 */

import type {
	FileSource,
	FlexibleFileData,
	NodeComponentProps,
	PlaceholderProps,
} from "@dune-canvas/canvas-lib";
import { useResolvedFile } from "@dune-canvas/canvas-lib";
import type React from "react";
import { useEffect, useRef, useState } from "react";

// ============================================================================
// Types
// ============================================================================

/**
 * Legacy data format with direct URL
 */
export interface ImageNodeDataDirect {
	url: string;
	alt?: string;
	objectFit?: "contain" | "cover" | "fill" | "none" | "scale-down";
}

/**
 * Combined data type supporting both direct URL and flexible source
 * When `source` is present, URL is resolved via useResolvedFile hook
 * When `url` is present, URL is used directly
 */
export type ImageNodeData = ImageNodeDataDirect | FlexibleFileData;

/**
 * Type guard to check if data has direct URL
 */
function hasDirectUrl(data: ImageNodeData): data is ImageNodeDataDirect {
	return "url" in data && typeof (data as ImageNodeDataDirect).url === "string";
}

/**
 * Type guard to check if data has source for resolution
 */
function hasSource(data: ImageNodeData): data is FlexibleFileData {
	return "source" in data && (data as FlexibleFileData).source !== undefined;
}

// ============================================================================
// Component
// ============================================================================

export const ImageNode: React.FC<NodeComponentProps<ImageNodeData>> = ({
	data,
	size,
	isSelected,
	onSizeChange,
}) => {
	const [isImageLoading, setIsImageLoading] = useState(true);
	const [error, setError] = useState<string | null>(null);

	// Use refs to avoid re-running the effect when these change
	const onSizeChangeRef = useRef(onSizeChange);
	onSizeChangeRef.current = onSizeChange;
	const initialSizeRef = useRef(size);

	// Resolve data source if using FDM reference
	const source: FileSource | undefined = hasSource(data)
		? data.source
		: undefined;
	const {
		url: resolvedUrl,
		isLoading: isResolvingUrl,
		error: resolveError,
	} = useResolvedFile(source);

	// Get URL: either directly from data or from resolved source
	const url: string | null = hasDirectUrl(data) ? data.url : resolvedUrl;

	// Get display properties
	const alt = hasDirectUrl(data) ? data.alt : (data as FlexibleFileData).label;
	const objectFit = hasDirectUrl(data) ? data.objectFit : undefined;

	useEffect(() => {
		if (!url) {
			// Don't set error while still resolving
			if (!isResolvingUrl) {
				setError(resolveError?.message ?? "No image URL provided");
			}
			setIsImageLoading(false);
			return;
		}

		setIsImageLoading(true);
		setError(null);

		const img = new Image();
		img.onload = () => {
			setIsImageLoading(false);

			// If no explicit size set, use natural size (capped)
			const initSize = initialSizeRef.current;
			if (
				onSizeChangeRef.current &&
				initSize.width === 200 &&
				initSize.height === 200
			) {
				const maxDim = 600;
				const scale = Math.min(
					1,
					maxDim / Math.max(img.naturalWidth, img.naturalHeight)
				);
				onSizeChangeRef.current({
					width: img.naturalWidth * scale,
					height: img.naturalHeight * scale,
				});
			}
		};
		img.onerror = () => {
			setError("Failed to load image");
			setIsImageLoading(false);
		};
		img.src = url;

		return () => {
			img.onload = null;
			img.onerror = null;
		};
	}, [url, isResolvingUrl, resolveError]);

	// Combined loading state
	const isLoading = isResolvingUrl || isImageLoading;

	if (error) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					backgroundColor: "#f5f5f5",
					border: "1px solid #e0e0e0",
					borderRadius: 4,
					color: "#666",
					fontSize: 14,
				}}
			>
				{error}
			</div>
		);
	}

	if (isLoading) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					backgroundColor: "#f5f5f5",
					border: "1px solid #e0e0e0",
					borderRadius: 4,
				}}
			>
				<div
					style={{
						width: 24,
						height: 24,
						border: "2px solid #e0e0e0",
						borderTopColor: "#666",
						borderRadius: "50%",
						animation: "spin 1s linear infinite",
					}}
				/>
				<style>{`
          @keyframes spin {
            to { transform: rotate(360deg); }
          }
        `}</style>
			</div>
		);
	}

	return (
		<div
			style={{
				width: "100%",
				height: "100%",
				overflow: "hidden",
				borderRadius: 4,
				backgroundColor: "#f5f5f5",
			}}
		>
			<img
				src={url!}
				alt={alt || "Image"}
				style={{
					width: "100%",
					height: "100%",
					objectFit: objectFit || "contain",
					display: "block",
				}}
				draggable={false}
			/>
		</div>
	);
};

export const ImagePlaceholder: React.FC<PlaceholderProps<ImageNodeData>> = ({
	data,
	size,
}) => {
	// Resolve data source if using FDM reference
	const source: FileSource | undefined = hasSource(data)
		? data.source
		: undefined;
	const { isLoading, error } = useResolvedFile(source);

	// Get label from data (for flexible data format)
	const label = hasSource(data) ? data.label : undefined;

	// Show loading indicator
	if (isLoading) {
		return (
			<div
				style={{
					width: size.width,
					height: size.height,
					backgroundColor: "#f9f9f9",
					border: "1px solid #ddd",
					borderRadius: 4,
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
				}}
			>
				<div style={{ color: "#999", fontSize: 10 }}>Loading...</div>
			</div>
		);
	}

	// Show error state
	if (error) {
		return (
			<div
				style={{
					width: size.width,
					height: size.height,
					backgroundColor: "#fff5f5",
					border: "1px solid #ffcccc",
					borderRadius: 4,
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
				}}
			>
				<div style={{ color: "#cc0000", fontSize: 10 }}>Error</div>
			</div>
		);
	}

	return (
		<div
			style={{
				width: size.width,
				height: size.height,
				backgroundColor: "#f0f0f0",
				borderRadius: 4,
				display: "flex",
				flexDirection: "column",
				alignItems: "center",
				justifyContent: "center",
				gap: 4,
			}}
		>
			<svg
				width={48}
				height={48}
				viewBox="0 0 24 24"
				fill="none"
				stroke="#999"
				strokeWidth="1.5"
			>
				<rect x="3" y="3" width="18" height="18" rx="2" />
				<circle cx="8.5" cy="8.5" r="1.5" />
				<path d="M21 15l-5-5L5 21" />
			</svg>
			{label && (
				<div
					style={{
						color: "#666",
						fontSize: 10,
						maxWidth: "90%",
						textAlign: "center",
						overflow: "hidden",
						textOverflow: "ellipsis",
						whiteSpace: "nowrap",
					}}
				>
					{label}
				</div>
			)}
		</div>
	);
};
