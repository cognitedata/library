/**
 * Text Node Component
 * Renders editable text with styling options
 */

import type {
	NodeComponentProps,
	PlaceholderProps,
} from "@dune-canvas/canvas-lib";
import type React from "react";
import { useEffect, useRef, useState } from "react";

export interface TextNodeData {
	content: string;
	fontSize?: number;
	fontFamily?: string;
	fontWeight?: "normal" | "bold";
	fontStyle?: "normal" | "italic";
	textAlign?: "left" | "center" | "right";
	color?: string;
	backgroundColor?: string;
}

export const TextNode: React.FC<NodeComponentProps<TextNodeData>> = ({
	data,
	size,
	onDataChange,
	isSelected,
	isFocused,
	readOnly,
}) => {
	const textareaRef = useRef<HTMLTextAreaElement>(null);
	const [isEditing, setIsEditing] = useState(false);

	useEffect(() => {
		if (isFocused && !readOnly) {
			setIsEditing(true);
			setTimeout(() => {
				textareaRef.current?.focus();
				textareaRef.current?.select();
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

	const styles: React.CSSProperties = {
		fontSize: data.fontSize || 16,
		fontFamily: data.fontFamily || "sans-serif",
		fontWeight: data.fontWeight || "normal",
		fontStyle: data.fontStyle || "normal",
		textAlign: data.textAlign || "left",
		color: data.color || "#000",
		backgroundColor: data.backgroundColor || "transparent",
	};

	if (isEditing) {
		return (
			<textarea
				ref={textareaRef}
				value={data.content}
				onChange={handleChange}
				onBlur={handleBlur}
				onKeyDown={handleKeyDown}
				style={{
					width: "100%",
					height: "100%",
					padding: 8,
					border: "none",
					outline: "none",
					resize: "none",
					overflow: "auto",
					...styles,
				}}
			/>
		);
	}

	return (
		<div
			onDoubleClick={handleDoubleClick}
			style={{
				width: "100%",
				height: "100%",
				padding: 8,
				overflow: "auto",
				whiteSpace: "pre-wrap",
				wordBreak: "break-word",
				cursor: readOnly ? "default" : "text",
				...styles,
			}}
		>
			{data.content || (
				<span style={{ color: "#999", fontStyle: "italic" }}>
					Double-click to edit...
				</span>
			)}
		</div>
	);
};

export const TextPlaceholder: React.FC<PlaceholderProps<TextNodeData>> = ({
	data,
	size,
}) => {
	return (
		<div
			style={{
				width: size.width,
				height: size.height,
				backgroundColor: data.backgroundColor || "#f5f5f5",
				borderRadius: 4,
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				padding: 8,
			}}
		>
			<span
				style={{
					fontSize: data.fontSize || 16,
					color: data.color || "#666",
					textAlign: "center",
					overflow: "hidden",
					textOverflow: "ellipsis",
				}}
			>
				{data.content?.slice(0, 50) || "Text"}
			</span>
		</div>
	);
};
