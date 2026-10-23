/**
 * Cause Map Node Component
 * Node for Root Cause Analysis (RCA) diagrams with status indicators
 */

import type {
	NodeComponentProps,
	PlaceholderProps,
} from "@dune-canvas/canvas-lib";
import type React from "react";
import { useEffect, useRef, useState } from "react";

export type CauseMapNodeStatus =
	| "pending"
	| "accepted"
	| "rejected"
	| "possible";

export interface CauseMapNodeData {
	text: string;
	status?: CauseMapNodeStatus;
	backgroundColor?: string;
	borderColor?: string;
	textColor?: string;
	fontSize?: number;
}

const STATUS_COLORS: Record<
	CauseMapNodeStatus,
	{ bg: string; border: string; icon: string }
> = {
	pending: { bg: "#ffffff", border: "#e0e0e0", icon: "#999" },
	accepted: { bg: "#e8f5e9", border: "#4caf50", icon: "#4caf50" },
	rejected: { bg: "#ffebee", border: "#f44336", icon: "#f44336" },
	possible: { bg: "#fff3e0", border: "#ff9800", icon: "#ff9800" },
};

const StatusIcon: React.FC<{ status: CauseMapNodeStatus; size?: number }> = ({
	status,
	size = 16,
}) => {
	const color = STATUS_COLORS[status].icon;

	switch (status) {
		case "accepted":
			return (
				<svg
					width={size}
					height={size}
					viewBox="0 0 24 24"
					fill="none"
					stroke={color}
					strokeWidth="3"
				>
					<path d="M20 6L9 17l-5-5" />
				</svg>
			);
		case "rejected":
			return (
				<svg
					width={size}
					height={size}
					viewBox="0 0 24 24"
					fill="none"
					stroke={color}
					strokeWidth="3"
				>
					<path d="M18 6L6 18M6 6l12 12" />
				</svg>
			);
		case "possible":
			return (
				<svg
					width={size}
					height={size}
					viewBox="0 0 24 24"
					fill="none"
					stroke={color}
					strokeWidth="2"
				>
					<circle cx="12" cy="12" r="10" />
					<path d="M12 16v.01M12 8v4" />
				</svg>
			);
		default:
			return null;
	}
};

export const CauseMapNode: React.FC<NodeComponentProps<CauseMapNodeData>> = ({
	data,
	size: _size,
	onDataChange,
	isSelected: _isSelected,
	isFocused,
	readOnly,
}) => {
	const textareaRef = useRef<HTMLTextAreaElement>(null);
	const [isEditing, setIsEditing] = useState(false);

	const status = data.status || "pending";
	const statusColors = STATUS_COLORS[status];
	const backgroundColor = data.backgroundColor || statusColors.bg;
	const borderColor = data.borderColor || statusColors.border;
	const textColor = data.textColor || "#333";
	const fontSize = data.fontSize || 14;

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
		// Stop propagation to prevent canvas keyboard shortcuts from triggering while typing
		e.stopPropagation();

		if (e.key === "Escape") {
			setIsEditing(false);
			textareaRef.current?.blur();
		}
	};

	const cycleStatus = (e: React.MouseEvent) => {
		e.stopPropagation();
		const statuses: CauseMapNodeStatus[] = [
			"pending",
			"possible",
			"accepted",
			"rejected",
		];
		const currentIndex = statuses.indexOf(status);
		const nextIndex = (currentIndex + 1) % statuses.length;
		onDataChange({ ...data, status: statuses[nextIndex] });
	};

	return (
		<div
			style={{
				width: "100%",
				height: "100%",
				backgroundColor,
				border: `2px solid ${borderColor}`,
				borderRadius: 8,
				display: "flex",
				flexDirection: "column",
				overflow: "hidden",
				boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
			}}
		>
			{/* Status indicator */}
			<div
				style={{
					position: "absolute",
					top: -8,
					right: -8,
					width: 24,
					height: 24,
					borderRadius: "50%",
					backgroundColor: "#fff",
					border: `2px solid ${borderColor}`,
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					cursor: "pointer",
					zIndex: 1,
				}}
				onClick={cycleStatus}
				title={`Status: ${status} (click to cycle)`}
			>
				{status !== "pending" && <StatusIcon status={status} size={14} />}
			</div>

			{/* Content */}
			<div
				style={{
					flex: 1,
					padding: 12,
					overflow: "hidden",
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
							backgroundColor: "transparent",
							color: textColor,
							fontSize,
							fontFamily: "sans-serif",
							lineHeight: 1.4,
							textAlign: "center",
						}}
						placeholder="Enter cause..."
					/>
				) : (
					<div
						style={{
							width: "100%",
							height: "100%",
							display: "flex",
							alignItems: "center",
							justifyContent: "center",
							color: textColor,
							fontSize,
							fontFamily: "sans-serif",
							lineHeight: 1.4,
							textAlign: "center",
							whiteSpace: "pre-wrap",
							wordBreak: "break-word",
							overflow: "hidden",
							cursor: readOnly ? "default" : "text",
						}}
					>
						{data.text || (
							<span style={{ color: "#999", fontStyle: "italic" }}>
								Double-click to edit...
							</span>
						)}
					</div>
				)}
			</div>
		</div>
	);
};

export const CauseMapPlaceholder: React.FC<
	PlaceholderProps<CauseMapNodeData>
> = ({ data, size }) => {
	const status = data.status || "pending";
	const statusColors = STATUS_COLORS[status];

	return (
		<div
			style={{
				width: size.width,
				height: size.height,
				backgroundColor: data.backgroundColor || statusColors.bg,
				border: `2px solid ${data.borderColor || statusColors.border}`,
				borderRadius: 8,
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				padding: 12,
			}}
		>
			<span
				style={{
					color: data.textColor || "#333",
					fontSize: data.fontSize || 14,
					textAlign: "center",
				}}
			>
				{data.text || "Cause Map Node"}
			</span>
		</div>
	);
};

export { STATUS_COLORS };
