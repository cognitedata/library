/**
 * Comment Node Component
 * Comment marker with author attribution
 */

import type {
	NodeComponentProps,
	PlaceholderProps,
} from "@dune-canvas/canvas-lib";
import type React from "react";
import { useEffect, useRef, useState } from "react";

export interface CommentNodeData {
	text: string;
	author: string;
	timestamp?: string;
	resolved?: boolean;
	backgroundColor?: string;
	textColor?: string;
}

export const CommentNode: React.FC<NodeComponentProps<CommentNodeData>> = ({
	data,
	size,
	onDataChange,
	isSelected,
	isFocused,
	readOnly,
}) => {
	const textareaRef = useRef<HTMLTextAreaElement>(null);
	const [isEditing, setIsEditing] = useState(false);

	const backgroundColor = data.backgroundColor || "#4a67fb";
	const textColor = data.textColor || "#ffffff";

	useEffect(() => {
		if (isFocused && !readOnly && !data.text) {
			setIsEditing(true);
			setTimeout(() => {
				textareaRef.current?.focus();
			}, 0);
		}
	}, [isFocused, readOnly, data.text]);

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
		onDataChange({ ...data, text: e.target.value });
	};

	const handleKeyDown = (e: React.KeyboardEvent) => {
		if (e.key === "Escape") {
			setIsEditing(false);
			textareaRef.current?.blur();
		}
	};

	const toggleResolved = (e: React.MouseEvent) => {
		e.stopPropagation();
		onDataChange({ ...data, resolved: !data.resolved });
	};

	const formatTimestamp = (timestamp?: string): string => {
		if (!timestamp) return "";
		try {
			const date = new Date(timestamp);
			return (
				date.toLocaleDateString() +
				" " +
				date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
			);
		} catch {
			return timestamp;
		}
	};

	return (
		<div
			style={{
				width: "100%",
				height: "100%",
				display: "flex",
				flexDirection: "column",
				borderRadius: "8px 8px 8px 0",
				overflow: "hidden",
				boxShadow: "0 2px 8px rgba(0,0,0,0.2)",
				opacity: data.resolved ? 0.6 : 1,
			}}
		>
			{/* Header */}
			<div
				style={{
					backgroundColor,
					padding: "8px 12px",
					display: "flex",
					alignItems: "center",
					justifyContent: "space-between",
					gap: 8,
				}}
			>
				<div style={{ display: "flex", alignItems: "center", gap: 8, flex: 1 }}>
					{/* Avatar */}
					<div
						style={{
							width: 24,
							height: 24,
							borderRadius: "50%",
							backgroundColor: "rgba(255,255,255,0.3)",
							display: "flex",
							alignItems: "center",
							justifyContent: "center",
							fontSize: 12,
							fontWeight: 600,
							color: textColor,
						}}
					>
						{data.author?.charAt(0).toUpperCase() || "?"}
					</div>

					<div style={{ flex: 1, minWidth: 0 }}>
						<div
							style={{
								color: textColor,
								fontSize: 12,
								fontWeight: 600,
								overflow: "hidden",
								textOverflow: "ellipsis",
								whiteSpace: "nowrap",
							}}
						>
							{data.author || "Anonymous"}
						</div>
						{data.timestamp && (
							<div
								style={{
									color: "rgba(255,255,255,0.7)",
									fontSize: 10,
								}}
							>
								{formatTimestamp(data.timestamp)}
							</div>
						)}
					</div>
				</div>

				{/* Resolve button */}
				<button
					onClick={toggleResolved}
					style={{
						background: "none",
						border: "none",
						cursor: "pointer",
						padding: 4,
						borderRadius: 4,
						display: "flex",
						alignItems: "center",
						justifyContent: "center",
					}}
					title={data.resolved ? "Unresolve" : "Resolve"}
				>
					{data.resolved ? (
						<svg
							width={16}
							height={16}
							viewBox="0 0 24 24"
							fill="none"
							stroke={textColor}
							strokeWidth="2"
						>
							<path d="M20 6L9 17l-5-5" />
						</svg>
					) : (
						<svg
							width={16}
							height={16}
							viewBox="0 0 24 24"
							fill="none"
							stroke={textColor}
							strokeWidth="2"
							opacity="0.6"
						>
							<circle cx="12" cy="12" r="10" />
						</svg>
					)}
				</button>
			</div>

			{/* Content */}
			<div
				style={{
					flex: 1,
					backgroundColor: "#fff",
					padding: 12,
					overflow: "auto",
				}}
				onDoubleClick={handleDoubleClick}
			>
				{isEditing ? (
					<textarea
						ref={textareaRef}
						value={data.text}
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
							fontSize: 13,
							lineHeight: 1.5,
							fontFamily: "sans-serif",
						}}
						placeholder="Write a comment..."
					/>
				) : (
					<div
						style={{
							fontSize: 13,
							lineHeight: 1.5,
							color: "#333",
							whiteSpace: "pre-wrap",
							wordBreak: "break-word",
							cursor: readOnly ? "default" : "text",
							textDecoration: data.resolved ? "line-through" : "none",
						}}
					>
						{data.text || (
							<span style={{ color: "#999", fontStyle: "italic" }}>
								Double-click to add a comment...
							</span>
						)}
					</div>
				)}
			</div>
		</div>
	);
};

export const CommentPlaceholder: React.FC<
	PlaceholderProps<CommentNodeData>
> = ({ data, size }) => {
	return (
		<div
			style={{
				width: size.width,
				height: size.height,
				backgroundColor: data.backgroundColor || "#4a67fb",
				borderRadius: "8px 8px 8px 0",
				boxShadow: "0 2px 8px rgba(0,0,0,0.2)",
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				opacity: data.resolved ? 0.6 : 1,
			}}
		>
			<svg
				width={24}
				height={24}
				viewBox="0 0 24 24"
				fill="none"
				stroke="#fff"
				strokeWidth="2"
			>
				<path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z" />
			</svg>
		</div>
	);
};
