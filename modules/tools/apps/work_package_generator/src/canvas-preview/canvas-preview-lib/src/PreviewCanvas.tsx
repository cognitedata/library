import type { CogniteClient } from "@cognite/sdk";
import {
	SDKProvider,
	useDocumentAnnotations,
	useResolvedFile,
	useResolvedTableData,
	useResolvedTimeseries,
} from "@dune-canvas/canvas-app";
import type { Connection, ViewportState } from "@dune-canvas/canvas-lib";
import {
	Canvas,
	ResolverProvider,
	useZoomControls,
	ZoomToolbar,
} from "@dune-canvas/canvas-lib";
import { QueryClientProvider } from "@tanstack/react-query";
import type React from "react";
import { useContext, useRef, useState } from "react";

import { CanvasPreviewContext, createCanvasPreviewCaches } from "./context";
import { ensurePluginsRegistered } from "./register";
import type { CanvasNodeDef } from "./types";

// Register all plugins once at module load time
ensurePluginsRegistered();

const REAL_RESOLVERS = {
	useResolvedFile,
	useResolvedTimeseries,
	useResolvedTableData,
	useDocumentAnnotations,
};

export interface PreviewCanvasProps {
	nodes: CanvasNodeDef[];
	connections?: Connection[];
	viewport?: ViewportState;
	onViewportChange?: (viewport: ViewportState) => void;
	className?: string;
	style?: React.CSSProperties;
	/** Optional CogniteClient — when provided, enables live data resolution for timeseries, files, etc. */
	client?: CogniteClient;
	/** When true, renders a ZoomToolbar in the bottom-right corner */
	showZoomToolbar?: boolean;
	/** When true, automatically zooms to fit all nodes when they first appear */
	fitOnLoad?: boolean;
}

export function PreviewCanvas({
	nodes,
	connections,
	viewport,
	onViewportChange,
	className,
	style,
	client,
	showZoomToolbar,
	fitOnLoad,
}: PreviewCanvasProps) {
	// Check if we're already inside a CanvasPreviewProvider (which supplies a QueryClient)
	const previewCtx = useContext(CanvasPreviewContext);

	// Fallback QueryClient when no provider is present
	const [fallbackQueryClient] = useState(() =>
		previewCtx ? null : createCanvasPreviewCaches().queryClient
	);

	// Internal viewport state for uncontrolled mode (when zoom toolbar / fitOnLoad is used)
	const containerRef = useRef<HTMLDivElement | null>(null);
	const [internalViewport, setInternalViewport] = useState<ViewportState>({
		x: 0,
		y: 0,
		scale: 1,
	});

	const effectiveViewport = viewport ?? internalViewport;
	const handleViewportChange = onViewportChange ?? setInternalViewport;

	const { zoomTo, zoomToFit } = useZoomControls({
		viewport: effectiveViewport,
		setViewport: handleViewportChange,
		containerRef,
		nodes,
		fitOnLoad,
	});

	const needsWrapper = showZoomToolbar || fitOnLoad;

	const canvasElement = (
		<Canvas
			nodes={nodes}
			connections={connections}
			viewport={effectiveViewport}
			onViewportChange={handleViewportChange}
			readOnly={true}
			className={needsWrapper ? undefined : className}
			style={needsWrapper ? undefined : style}
		/>
	);

	const canvas = needsWrapper ? (
		<div
			ref={containerRef}
			className={className}
			style={{ position: "relative", width: "100%", height: "100%", ...style }}
		>
			{canvasElement}
			{showZoomToolbar && (
				<ZoomToolbar
					scale={effectiveViewport.scale}
					onZoomChange={zoomTo}
					onZoomToFit={zoomToFit}
				/>
			)}
		</div>
	) : (
		canvasElement
	);

	let content: React.ReactElement;
	if (client) {
		content = (
			<SDKProvider client={client}>
				<ResolverProvider resolvers={REAL_RESOLVERS}>{canvas}</ResolverProvider>
			</SDKProvider>
		);
	} else {
		content = <ResolverProvider resolvers={{}}>{canvas}</ResolverProvider>;
	}

	// Wrap with a local QueryClientProvider only when not inside CanvasPreviewProvider
	if (fallbackQueryClient) {
		return (
			<QueryClientProvider client={fallbackQueryClient}>
				{content}
			</QueryClientProvider>
		);
	}

	return content;
}
