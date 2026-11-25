/**
 * Injectable dependencies for {@link useSummariesPageModel}.
 * Tests supply custom hooks/functions via {@link SummariesPageModelDepsProvider}.
 */

import type { ReactNode } from "react";
import { SummariesPageModelDepsContext } from "@/contexts/summariesPageModelDeps";
import {
	type SummariesListPipelineValue,
	useDefaultSummariesListPipeline,
} from "@/hooks/useDefaultSummariesListPipeline";
import { useRobustDownload } from "@/hooks/useRobustDownload";
import { useSummaryGeneration } from "@/hooks/useSummaryGeneration";
import {
	downloadAsMarkdown,
	downloadOperationSummaryPdfWithSignedUrlFirst,
} from "@/utils/summaryDownload";

function useDefaultSummariesRobustDownload() {
	return useRobustDownload({
		preserveState: true,
		fallbackToMarkdown: true,
	});
}

export type SummariesRobustDownloadValue = ReturnType<
	typeof useDefaultSummariesRobustDownload
>;

export type SummaryGenerationVmValue = ReturnType<typeof useSummaryGeneration>;

export interface SummariesPageModelDeps {
	useSummariesListPipeline: () => SummariesListPipelineValue;
	useSummaryGeneration: () => SummaryGenerationVmValue;
	useSummariesRobustDownload: () => SummariesRobustDownloadValue;
	downloadAsMarkdown: typeof downloadAsMarkdown;
	downloadOperationSummaryPdfWithSignedUrlFirst: typeof downloadOperationSummaryPdfWithSignedUrlFirst;
}

const DEFAULT_SUMMARIES_PAGE_MODEL_DEPS: SummariesPageModelDeps = {
	useSummariesListPipeline: useDefaultSummariesListPipeline,
	useSummaryGeneration,
	useSummariesRobustDownload: useDefaultSummariesRobustDownload,
	downloadAsMarkdown,
	downloadOperationSummaryPdfWithSignedUrlFirst,
};

export function SummariesPageModelDepsProvider({
	children,
	deps,
}: {
	children: ReactNode;
	deps?: SummariesPageModelDeps;
}) {
	const value = deps ?? DEFAULT_SUMMARIES_PAGE_MODEL_DEPS;
	return (
		<SummariesPageModelDepsContext.Provider value={value}>
			{children}
		</SummariesPageModelDepsContext.Provider>
	);
}
