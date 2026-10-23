/**
 * Label Node Component
 * Text with background, border, and padding - useful for annotations
 */

import type {
	NodeComponentProps,
	PlaceholderProps,
} from "@dune-canvas/canvas-lib";
import type React from "react";
import { useEffect, useRef, useState } from "react";

export interface LabelNodeData {
	text: string;
	fontSize?: number;
	fontWeight?: "normal" | "bold";
	textColor?: string;
	backgroundColor?: string;
	borderColor?: string;
	borderWidth?: number;
	borderRadius?: number;
	padding?: number;
}

export const LabelNode: React.FC<NodeComponentProps<LabelNodeData>> = ({
	data,
	size,
	onDataChange,
	isSelected,
	isFocused,
	readOnly,
}) => {
	const inputRef = useRef<HTMLInputElement>(null);
	const [isEditing, setIsEditing] = useState(false);

	const fontSize = data.fontSize || 14;
	const fontWeight = data.fontWeight || "normal";
	const textColor = data.textColor || "#333";
	const backgroundColor = data.backgroundColor || "#ffffff";
	const borderColor = data.borderColor || "#e0e0e0";
	const borderWidth = data.borderWidth ?? 1;
	const borderRadius = data.borderRadius ?? 4;
	const padding = data.padding ?? 8;

	useEffect(() => {
		if (isFocused && !readOnly) {
			setIsEditing(true);
			setTimeout(() => {
				inputRef.current?.focus();
				inputRef.current?.select();
			}, 0);
		}
	}, [isFocused, readOnly]);

	const handleDoubleClick = () => {
		if (!readOnly) {
			setIsEditing(true);
			setTimeout(() => {
				inputRef.current?.focus();
				inputRef.current?.select();
			}, 0);
		}
	};

	const handleBlur = () => {
		setIsEditing(false);
	};

	const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
		onDataChange({ ...data, text: e.target.value });
	};

	const handleKeyDown = (e: React.KeyboardEvent) => {
		if (e.key === "Enter" || e.key === "Escape") {
			setIsEditing(false);
			inputRef.current?.blur();
		}
	};

	const containerStyle: React.CSSProperties = {
		width: "100%",
		height: "100%",
		backgroundColor,
		border: `${borderWidth}px solid ${borderColor}`,
		borderRadius,
		display: "flex",
		alignItems: "center",
		justifyContent: "center",
		padding,
		boxSizing: "border-box",
		overflow: "hidden",
	};

	if (isEditing) {
		return (
			<div style={containerStyle}>
				<input
					ref={inputRef}
					type="text"
					value={data.text}
					onChange={handleChange}
					onBlur={handleBlur}
					onKeyDown={handleKeyDown}
					style={{
						width: "100%",
						border: "none",
						outline: "none",
						backgroundColor: "transparent",
						textAlign: "center",
						fontSize,
						fontWeight,
						color: textColor,
						fontFamily: "sans-serif",
					}}
				/>
			</div>
		);
	}

	return (
		<div style={containerStyle} onDoubleClick={handleDoubleClick}>
			<span
				style={{
					fontSize,
					fontWeight,
					color: textColor,
					fontFamily: "sans-serif",
					textAlign: "center",
					overflow: "hidden",
					textOverflow: "ellipsis",
					whiteSpace: "nowrap",
					cursor: readOnly ? "default" : "text",
				}}
			>
				{data.text || (
					<span style={{ color: "#999", fontStyle: "italic" }}>Label</span>
				)}
			</span>
		</div>
	);
};

export const LabelPlaceholder: React.FC<PlaceholderProps<LabelNodeData>> = ({
	data,
	size,
}) => {
	return (
		<div
			style={{
				width: size.width,
				height: size.height,
				backgroundColor: data.backgroundColor || "#ffffff",
				border: `${data.borderWidth ?? 1}px solid ${data.borderColor || "#e0e0e0"}`,
				borderRadius: data.borderRadius ?? 4,
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				padding: data.padding ?? 8,
			}}
		>
			<span
				style={{
					fontSize: data.fontSize || 14,
					fontWeight: data.fontWeight || "normal",
					color: data.textColor || "#333",
					textAlign: "center",
				}}
			>
				{data.text || "Label"}
			</span>
		</div>
	);
};
