/**
 * Sticky Note Node Component
 * Editable sticky note with customizable background color and auto-sizing text
 */

import type {
	NodeComponentProps,
	PlaceholderProps,
} from "@dune-canvas/canvas-lib";
import type React from "react";
import { useEffect, useRef, useState } from "react";

import { useAutoSizedFont } from "./useAutoSizedFont";

export interface StickyNodeData {
	content: string;
	backgroundColor?: string;
	textColor?: string;
	fontSize?: number;
	borderColor?: string;
	borderWidth?: number;
	borderRadius?: number;
}

// ============================================================================
// Constants
// ============================================================================

/** Padding inside the sticky note */
const STICKY_PADDING = 12;

/** Default font size when no content */
const DEFAULT_FONT_SIZE = 14;

// ============================================================================
// Component
// ============================================================================

const STICKY_COLORS = [
	"#fff59d", // Yellow (default)
	"#ffcc80", // Orange
	"#ef9a9a", // Red
	"#ce93d8", // Purple
	"#90caf9", // Blue
	"#a5d6a7", // Green
	"#bcaaa4", // Brown
	"#ffffff", // White
];

export const StickyNode: React.FC<NodeComponentProps<StickyNodeData>> = ({
	data,
	size,
	onDataChange,
	isSelected,
	isFocused,
	readOnly,
}) => {
	const textareaRef = useRef<HTMLTextAreaElement>(null);
	const [isEditing, setIsEditing] = useState(false);

	const backgroundColor = data.backgroundColor || STICKY_COLORS[0];
	const textColor = data.textColor || "#333";
	const borderColor = data.borderColor || "rgba(83, 88, 127, 0.24)";
	const borderWidth = data.borderWidth ?? 2;
	const borderRadius = data.borderRadius ?? 4;

	// Calculate auto-sized font (always enabled when there's content)
	const autoFontSize = useAutoSizedFont({
		text: data.content,
		size,
		padding: STICKY_PADDING + borderWidth,
		enabled: !!data.content,
		defaultFontSize: DEFAULT_FONT_SIZE,
	});

	useEffect(() => {
		if (isFocused && !readOnly) {
			setIsEditing(true);
			setTimeout(() => {
				textareaRef.current?.focus();
			}, 0);
		}
	}, [isFocused, readOnly]);

	const handleDoubleClick = () => {
		if (!readOnly) {
			setIsEditing(true);
			setTimeout(() => {
				textareaRef.current?.focus();
			}, 0);
		}
	};

	const handleBlur = () => {
		setIsEditing(false);
	};

	const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
		onDataChange({ ...data, content: e.target.value });
	};

	const handleKeyDown = (e: React.KeyboardEvent) => {
		// Stop propagation to prevent canvas keyboard shortcuts from triggering while typing
		e.stopPropagation();

		if (e.key === "Escape") {
			setIsEditing(false);
			textareaRef.current?.blur();
		}
	};

	return (
		<div
			style={{
				width: "100%",
				height: "100%",
				backgroundColor,
				border: `${borderWidth}px solid ${borderColor}`,
				borderRadius,
				boxSizing: "border-box",
				display: "flex",
				flexDirection: "column",
				overflow: "hidden",
			}}
		>
			{/* Content */}
			<div
				style={{
					flex: 1,
					padding: STICKY_PADDING,
					overflow: "hidden",
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
				}}
				onDoubleClick={handleDoubleClick}
			>
				{isEditing ? (
					<textarea
						ref={textareaRef}
						value={data.content}
						onChange={handleChange}
						onBlur={handleBlur}
						onKeyDown={handleKeyDown}
						style={{
							width: "100%",
							height: "100%",
							padding: 0,
							border: "none",
							outline: "none",
							resize: "none",
							backgroundColor: "transparent",
							color: textColor,
							fontSize: autoFontSize || DEFAULT_FONT_SIZE,
							fontFamily: "sans-serif",
							lineHeight: 1.5,
							textAlign: "center",
						}}
						placeholder="Type your note..."
					/>
				) : (
					<div
						style={{
							width: "100%",
							color: textColor,
							fontSize: autoFontSize,
							fontFamily: "sans-serif",
							lineHeight: 1.5,
							whiteSpace: "pre-wrap",
							wordBreak: "break-word",
							overflow: "hidden",
							textAlign: "center",
							cursor: readOnly ? "default" : "text",
						}}
					>
						{data.content || (
							<span
								style={{
									color: "rgba(0,0,0,0.3)",
									fontStyle: "italic",
									fontSize: DEFAULT_FONT_SIZE,
								}}
							>
								Double-click to edit...
							</span>
						)}
					</div>
				)}
			</div>
		</div>
	);
};

export const StickyPlaceholder: React.FC<PlaceholderProps<StickyNodeData>> = ({
	data,
	size,
}) => {
	const backgroundColor = data.backgroundColor || STICKY_COLORS[0];
	const borderColor = data.borderColor || "rgba(83, 88, 127, 0.24)";
	const borderWidth = data.borderWidth ?? 2;
	const borderRadius = data.borderRadius ?? 4;

	const text = data.content || "Sticky Note";
	const autoFontSize = useAutoSizedFont({
		text,
		size,
		padding: STICKY_PADDING + borderWidth,
		defaultFontSize: DEFAULT_FONT_SIZE,
	});

	return (
		<div
			style={{
				width: size.width,
				height: size.height,
				backgroundColor,
				border: `${borderWidth}px solid ${borderColor}`,
				borderRadius,
				boxSizing: "border-box",
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				padding: STICKY_PADDING,
			}}
		>
			<span
				style={{
					color: data.textColor || "#333",
					fontSize: autoFontSize,
					fontFamily: "sans-serif",
					lineHeight: 1.5,
					textAlign: "center",
					overflow: "hidden",
					whiteSpace: "pre-wrap",
					wordBreak: "break-word",
				}}
			>
				{text}
			</span>
		</div>
	);
};

export { STICKY_COLORS };
