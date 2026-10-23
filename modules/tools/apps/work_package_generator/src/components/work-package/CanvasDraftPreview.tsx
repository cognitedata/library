import {
	type CanvasNodeDef,
	CanvasPreviewProvider,
	PreviewCanvas,
} from "@dune-canvas/canvas-preview-lib";
import { Loader } from "@/components/ui";
import { useCdfClientInfo } from "@/hooks/useClientInfo";

export interface CanvasDraftPreviewProps {
	nodes: CanvasNodeDef[] | undefined;
	isLoading: boolean;
	error: Error | null;
}

export function CanvasDraftPreview({
	nodes,
	isLoading,
	error,
}: CanvasDraftPreviewProps) {
	const { client } = useCdfClientInfo();

	if (isLoading) {
		return (
			<div className="flex items-center gap-2 justify-center py-8 text-sm text-muted-foreground">
				<Loader size={16} />
				Building preview…
			</div>
		);
	}

	if (error) {
		return (
			<p className="text-xs text-destructive text-center py-4">
				{error.message || "Unable to load canvas preview."}
			</p>
		);
	}

	if (!nodes?.length) {
		return (
			<p className="text-xs text-muted-foreground text-center py-4">
				Nothing to preview yet.
			</p>
		);
	}

	return (
		<CanvasPreviewProvider>
			<PreviewCanvas
				nodes={nodes}
				client={client ?? undefined}
				showZoomToolbar
				fitOnLoad
				style={{
					width: "100%",
					height: "420px",
					borderRadius: "0.5rem",
					overflow: "hidden",
				}}
			/>
		</CanvasPreviewProvider>
	);
}
