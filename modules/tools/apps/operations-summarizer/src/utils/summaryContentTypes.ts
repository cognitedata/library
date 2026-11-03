import { getDisplayName } from "@/config/dataTypes";
import type { SummaryInstance } from "@/types/operations";
import {
  SUMMARY_CONTENT_TYPE_BITMAP_ORDER,
  stripSummaryContentTypeBitmapLine,
} from "@/utils/summaryListQuery";

/** Parse content types from summary properties with fallback to description parsing. */
export function getContentTypes(summary: SummaryInstance): string[] {
  try {
    const props = summary.properties?.cdf_cdm?.["CogniteFile/v1"] as
      Record<string, unknown> | undefined;
    const metadata = props?.metadata as { contentTypes?: string } | undefined;
    const contentTypesStr = metadata?.contentTypes;
    if (typeof contentTypesStr === "string" && contentTypesStr.trim()) {
      return contentTypesStr
        .split(",")
        .map((t) => getDisplayName(t.trim()))
        .filter(Boolean);
    }
    const rawDescription = (props?.description as string) || "";
    const ops = rawDescription.match(/^OPS:([01]+)\n/);
    if (ops) {
      const bits = ops[1];
      const types = SUMMARY_CONTENT_TYPE_BITMAP_ORDER.filter(
        (_, i) => bits[i] === "1",
      )
        .map((t) => getDisplayName(t))
        .filter(Boolean);
      return types.length > 0 ? types : ["All Types"];
    }
    // Legacy fallback: keyword heuristics on stripped description
    const description = stripSummaryContentTypeBitmapLine(rawDescription);
    const types: string[] = [];
    if (description.toLowerCase().includes("asset")) types.push("Assets");
    if (description.toLowerCase().includes("file")) types.push("Files");
    if (description.toLowerCase().includes("equipment"))
      types.push("Equipment");
    if (
      description.toLowerCase().includes("time") ||
      description.toLowerCase().includes("series")
    )
      types.push("Time Series");
    if (
      description.toLowerCase().includes("work") ||
      description.toLowerCase().includes("order")
    )
      types.push("Work Orders");
    return types.length > 0 ? types : ["All Types"];
  } catch {
    return ["All Types"];
  }
}
