import type { ChangeEvent, Dispatch } from "react";
import { useMemo } from "react";
import { getDisplayName } from "@/config/dataTypes";
import type {
  OperationsAction,
  OperationsState,
} from "@/contexts/operations-summarizer/types";
import type { DataTypeName } from "@/types/operations";
import { toCalendarIsoDate } from "@/utils/dates";
import { generateFilename, stripHtmlTags } from "@/utils/formatting";

export function formatDateForInput(date: Date): string {
  return toCalendarIsoDate(date);
}

export function derivePreservedConfig(
  generationError: unknown,
  state: OperationsState,
) {
  if (!generationError) return undefined;

  const selectedTypes = Object.entries(state.summaryContents)
    .filter(([_, selected]) => selected)
    .map(([type, _]) => getDisplayName(type));

  return {
    dateRange: `${formatDateForInput(state.summaryPeriodStart)} to ${formatDateForInput(state.summaryPeriodEnd)}`,
    dataTypes: selectedTypes,
    agent: state.selectedAgent || "No agent selected",
  };
}

export function useSummariesCreationComputed(state: OperationsState) {
  const selectedCount = useMemo(
    () => Object.values(state.summaryContents).filter(Boolean).length,
    [state.summaryContents],
  );

  const daysSelected = useMemo(
    () =>
      Math.ceil(
        (state.summaryPeriodEnd.getTime() -
          state.summaryPeriodStart.getTime()) /
          (1000 * 60 * 60 * 24),
      ),
    [state.summaryPeriodStart, state.summaryPeriodEnd],
  );

  return { selectedCount, daysSelected };
}

interface CreationHandlersDeps {
  state: OperationsState;
  dispatch: Dispatch<OperationsAction>;
  generate: () => Promise<{
    markdown: string;
    generatedAt: Date;
    dataTypeTruncation: Partial<
      Record<DataTypeName, { limit: number; hadMoreInCdf: boolean }>
    >;
    cdfResult?: {
      external_id?: string;
      fusion_url?: string;
      file_name?: string;
    };
  } | null>;
  downloadAsMarkdown: (content: string, filename: string) => void;
  setSavedPdfInfo: (
    info: {
      fileName: string;
      fileId: string | null;
      fusionUrl?: string;
    } | null,
  ) => void;
  resolveFusionFileUrl?: (externalId: string) => Promise<string | undefined>;
}

export function createSummariesCreationHandlers(deps: CreationHandlersDeps) {
  const {
    state,
    dispatch,
    generate,
    downloadAsMarkdown,
    setSavedPdfInfo,
    resolveFusionFileUrl,
  } = deps;

  const handleStartDateChange = (e: ChangeEvent<HTMLInputElement>) => {
    if (!e.target.value) return;
    const date = new Date(`${e.target.value}T00:00:00`);
    if (Number.isNaN(date.getTime())) return;
    dispatch({ type: "SET_SUMMARY_PERIOD_START", payload: date });
    if (date > state.summaryPeriodEnd) {
      dispatch({ type: "SET_SUMMARY_PERIOD_END", payload: date });
    }
  };

  const handleEndDateChange = (e: ChangeEvent<HTMLInputElement>) => {
    if (!e.target.value) return;
    const date = new Date(`${e.target.value}T00:00:00`);
    if (Number.isNaN(date.getTime())) return;
    if (date < state.summaryPeriodStart) return;
    dispatch({ type: "SET_SUMMARY_PERIOD_END", payload: date });
  };

  const handleGenerateSummary = async () => {
    if (!state.selectedAgent) return;
    setSavedPdfInfo(null);

    try {
      const result = await generate();
      if (result) {
        dispatch({ type: "SET_SHOW_SUMMARY", payload: true });
        dispatch({
          type: "SET_GENERATED_SUMMARY",
          payload: {
            workOrders: [],
            instances: [],
            fullText: result.markdown,
            generatedAt: result.generatedAt,
            dataTypeTruncation: result.dataTypeTruncation,
          },
        });
        if (result.cdfResult?.external_id) {
          const externalId = result.cdfResult.external_id;
          let fusionUrl: string | undefined;
          if (resolveFusionFileUrl) {
            try {
              fusionUrl = await resolveFusionFileUrl(externalId);
            } catch (error) {
              console.error("Failed to build Fusion file URL:", error);
            }
          }
          setSavedPdfInfo({
            fileName: result.cdfResult.file_name ?? "Operations summary",
            fileId: externalId,
            fusionUrl,
          });
        }
      }
    } catch (error) {
      console.error("Error generating summary:", error);
    }
  };

  const handleDownloadMarkdown = () => {
    if (!state.generatedSummary) return;
    const cleanText = stripHtmlTags(state.generatedSummary.fullText);
    const filename = generateFilename(
      "operation_summary",
      state.summaryPeriodStart,
      state.summaryPeriodEnd,
      "md",
    );
    downloadAsMarkdown(cleanText, filename);
  };

  return {
    handleStartDateChange,
    handleEndDateChange,
    handleGenerateSummary,
    handleDownloadMarkdown,
  };
}
