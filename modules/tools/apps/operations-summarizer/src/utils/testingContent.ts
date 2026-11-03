/**
 * Content resolution for testing: extract summary content from node properties
 * or fetch from CDF file. Testable and reusable from the tests page flow.
 */

import type { CogniteClient, NodeDefinition } from "@cognite/sdk";
import type { SummaryInstance } from "@/types/operations";
import { VIEW_IDS } from "@/types/operations";
import { withCdfConcurrency } from "@/utils/cdfRequest";
import { nodeToSummaryInstance } from "@/utils/summaryInstanceMapping";
import { extractSummaryContent } from "./summaryDownload";

// Patterns that indicate a description field contains markdown summary content
// rather than a short plain-text label. Ordered from most to least specific.
const MARKDOWN_INDICATORS = [
  /^#/m, // ATX heading (# or ##)
  /\*\*/, // bold
  /\n- /, // unordered list (dash)
  /\n\* /, // unordered list (asterisk)
  /\n\d+\. /, // ordered list
  /\n> /, // blockquote
];

/**
 * Multi-fallback search for summary content in node properties.
 * 1. extractSummaryContent (standard view-based extraction)
 * 2. Loop all properties for sourceContext, content, or description (markdown-looking)
 * 3. Direct on-summary sourceContext
 * Returns content string or null if none found.
 */
function getSummaryContentFromProperties(
  summary: SummaryInstance,
): string | null {
  const extracted = extractSummaryContent(summary);
  if (extracted.content?.trim()) {
    return extracted.content;
  }

  if (summary.properties && typeof summary.properties === "object") {
    for (const [_space, views] of Object.entries(summary.properties)) {
      if (typeof views !== "object" || views === null) continue;
      for (const [, viewProps] of Object.entries(
        views as Record<string, unknown>,
      )) {
        if (typeof viewProps !== "object" || viewProps === null) continue;
        const props = viewProps as Record<string, unknown>;
        if (
          props.sourceContext &&
          typeof props.sourceContext === "string" &&
          props.sourceContext.trim().length > 0
        ) {
          return props.sourceContext;
        }
        if (
          props.content &&
          typeof props.content === "string" &&
          props.content.trim().length > 0
        ) {
          return props.content;
        }
        if (
          props.description &&
          typeof props.description === "string" &&
          props.description.trim().length > 0 &&
          MARKDOWN_INDICATORS.some((re) => re.test(props.description as string))
        ) {
          return props.description;
        }
      }
    }
  }

  const topLevel = summary.sourceContext;
  if (topLevel?.trim()) {
    return topLevel;
  }

  return null;
}

type SummaryContentSdk = Partial<Pick<CogniteClient, "files" | "instances">>;

/**
 * Picker/query rows omit `sourceContext`; load full CogniteFile view via retrieve.
 */
async function fetchMarkdownViaInstanceRetrieve(
  summary: SummaryInstance,
  sdk: Pick<CogniteClient["instances"], "retrieve">,
): Promise<string | null> {
  const viewId = VIEW_IDS.SUMMARY_OUTPUT_FILE;
  try {
    const res = await withCdfConcurrency(() =>
      sdk.retrieve({
        items: [
          {
            instanceType: "node",
            space: summary.space,
            externalId: summary.externalId,
          },
        ],
        sources: [
          {
            source: {
              type: "view",
              space: viewId.space,
              externalId: viewId.externalId,
              version: viewId.version,
            },
          },
        ],
      }),
    );
    const first = res.items?.[0];
    if (!first || typeof first !== "object") return null;
    const mapped = nodeToSummaryInstance(first as NodeDefinition);
    return getSummaryContentFromProperties(mapped);
  } catch (error) {
    console.error("retrieve summary for test baseline failed:", error);
    return null;
  }
}

/**
 * Fetch file content from CDF via download link.
 * Returns text content or null (e.g. for PDFs or on error).
 */
async function fetchFileContentFromCdf(
  filesApi: CogniteClient["files"],
  space: string,
  externalId: string,
): Promise<string | null> {
  try {
    const results = await withCdfConcurrency(() =>
      filesApi.getDownloadUrls([{ instanceId: { space, externalId } }]),
    );

    const downloadUrl = results[0]?.downloadUrl;
    if (!downloadUrl) return null;

    const contentResponse = await fetch(downloadUrl);
    if (!contentResponse.ok) return null;

    const contentType = contentResponse.headers.get("content-type") || "";
    if (contentType.includes("application/pdf")) {
      return null;
    }

    return await contentResponse.text();
  } catch (error) {
    console.error("Error fetching file content:", error);
    return null;
  }
}

/**
 * Resolve original summary content: try properties first, then CDF file fetch.
 */
export async function resolveSummaryContent(
  summary: SummaryInstance,
  sdk: SummaryContentSdk | null,
  space: string,
): Promise<string> {
  let content = getSummaryContentFromProperties(summary);

  if (!content?.trim() && sdk?.instances?.retrieve) {
    const retrieved = await fetchMarkdownViaInstanceRetrieve(
      summary,
      sdk.instances,
    );
    if (retrieved?.trim()) {
      content = retrieved;
    }
  }

  if (!content?.trim() && sdk?.files) {
    const fileContent = await fetchFileContentFromCdf(
      sdk.files,
      space,
      summary.externalId,
    );
    if (fileContent?.trim()) {
      content = fileContent;
    }
  }

  return content?.trim() || "";
}
