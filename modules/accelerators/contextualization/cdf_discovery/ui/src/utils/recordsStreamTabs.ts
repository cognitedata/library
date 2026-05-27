import type { OpenTarget, RecordsStreamDocumentTab } from "../types/discoveryNodes";
import { openTargetKey, tabLabelForTarget } from "../types/discoveryNodes";
import { DEFAULT_PAGE_SIZE } from "./pagination";

export function recordsStreamTabKey(target: OpenTarget): string {
  return `records:${openTargetKey(target)}`;
}

export function createRecordsStreamTab(
  streamExternalId: string,
  label?: string
): RecordsStreamDocumentTab {
  return {
    kind: "records_stream",
    id: recordsStreamTabKey({ type: "record_stream", stream_external_id: streamExternalId }),
    label: label?.trim() || tabLabelForTarget({ type: "record_stream", stream_external_id: streamExternalId }),
    streamExternalId,
    streamDetail: null,
    items: [],
    columns: [],
    cursor: null,
    loading: false,
    error: null,
    pageSize: DEFAULT_PAGE_SIZE,
    pageIndex: 0,
    selectedRowIndex: null,
  };
}
