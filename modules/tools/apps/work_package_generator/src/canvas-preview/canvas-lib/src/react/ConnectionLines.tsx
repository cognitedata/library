import type React from "react";
import type { CanvasNode, Connection, Point, Size } from "../core";

interface ConnectionLinesProps {
	connections: Connection[];
	nodes: CanvasNode[];
	computedNodeGeometry: Map<string, { position: Point; size: Size }>;
}

export const ConnectionLines: React.FC<ConnectionLinesProps> = ({
	connections,
	nodes,
	computedNodeGeometry,
}) => {
	if (connections.length === 0) return null;

	const nodeMap = new Map(nodes.map((n) => [n.id, n]));

	return (
		<svg
			style={{
				position: "absolute",
				top: 0,
				left: 0,
				width: "100%",
				height: "100%",
				overflow: "visible",
				pointerEvents: "none",
			}}
		>
			{connections.map((conn) => {
				const fromNode = nodeMap.get(conn.from);
				const toNode = nodeMap.get(conn.to);
				if (!fromNode || !toNode) return null;

				const fromGeom = computedNodeGeometry.get(conn.from);
				const toGeom = computedNodeGeometry.get(conn.to);
				if (!fromGeom || !toGeom) return null;

				// Calculate connection points (right side of from, left side of to)
				const fromX = fromGeom.position.x + fromGeom.size.width;
				const fromY = fromGeom.position.y + fromGeom.size.height / 2;
				const toX = toGeom.position.x;
				const toY = toGeom.position.y + toGeom.size.height / 2;

				// Create elbowed path (horizontal first, then vertical, then horizontal)
				const midX = (fromX + toX) / 2;
				const path = `M ${fromX} ${fromY} L ${midX} ${fromY} L ${midX} ${toY} L ${toX} ${toY}`;

				return (
					<g key={conn.id}>
						<path d={path} fill="none" stroke="#333" strokeWidth={2} />
						{/* Arrow at the end */}
						<polygon
							points={`${toX},${toY} ${toX - 8},${toY - 4} ${toX - 8},${toY + 4}`}
							fill="#333"
						/>
					</g>
				);
			})}
		</svg>
	);
};
