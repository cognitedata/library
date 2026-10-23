/**
 * Image Annotation Node Component
 * Image overlay with rotation support - useful for stamps, icons, etc.
 */

import type {
	NodeComponentProps,
	PlaceholderProps,
} from "@dune-canvas/canvas-lib";
import type React from "react";
import { useEffect, useState } from "react";

export interface ImageAnnotationNodeData {
	url: string;
	alt?: string;
	tintColor?: string;
	opacity?: number;
}

export const ImageAnnotationNode: React.FC<
	NodeComponentProps<ImageAnnotationNodeData>
> = ({ data, size, isSelected, rotation }) => {
	const [isLoading, setIsLoading] = useState(true);
	const [error, setError] = useState<string | null>(null);

	useEffect(() => {
		if (!data.url) {
			setError("No image URL provided");
			setIsLoading(false);
			return;
		}

		setIsLoading(true);
		setError(null);

		const img = new Image();
		img.onload = () => {
			setIsLoading(false);
		};
		img.onerror = () => {
			setError("Failed to load image");
			setIsLoading(false);
		};
		img.src = data.url;

		return () => {
			img.onload = null;
			img.onerror = null;
		};
	}, [data.url]);

	const opacity = data.opacity ?? 1;

	// Apply tint color using CSS filter if provided
	const getImageStyle = (): React.CSSProperties => {
		const baseStyle: React.CSSProperties = {
			width: "100%",
			height: "100%",
			objectFit: "contain",
			display: "block",
			opacity,
		};

		// Note: True color tinting would require SVG filters or canvas manipulation
		// This is a simplified version using opacity
		return baseStyle;
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
					backgroundColor: "rgba(255,0,0,0.1)",
					border: "1px dashed #f44336",
					borderRadius: 4,
					color: "#f44336",
					fontSize: 12,
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
					backgroundColor: "rgba(0,0,0,0.05)",
					borderRadius: 4,
				}}
			>
				<div
					style={{
						width: 20,
						height: 20,
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
				overflow: "visible",
			}}
		>
			{/* Tint overlay if color is specified */}
			{data.tintColor && (
				<div
					style={{
						position: "absolute",
						inset: 0,
						backgroundColor: data.tintColor,
						mixBlendMode: "multiply",
						pointerEvents: "none",
						borderRadius: 4,
					}}
				/>
			)}

			<img
				src={data.url}
				alt={data.alt || "Image annotation"}
				style={getImageStyle()}
				draggable={false}
			/>
		</div>
	);
};

export const ImageAnnotationPlaceholder: React.FC<
	PlaceholderProps<ImageAnnotationNodeData>
> = ({ data, size }) => {
	return (
		<div
			style={{
				width: size.width,
				height: size.height,
				backgroundColor: data.tintColor || "rgba(0,0,0,0.1)",
				borderRadius: 4,
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				opacity: data.opacity ?? 0.5,
			}}
		>
			<svg
				width={24}
				height={24}
				viewBox="0 0 24 24"
				fill="none"
				stroke="#666"
				strokeWidth="1.5"
			>
				<rect x="3" y="3" width="18" height="18" rx="2" />
				<circle cx="8.5" cy="8.5" r="1.5" />
				<path d="M21 15l-5-5L5 21" />
			</svg>
		</div>
	);
};
