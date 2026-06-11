import type { PatternDraft } from "@/shared/utils/patternManagement";

export interface ScopePreviewRow {
  patternScope: string;
  primaryScopeValue: string;
  secondaryScopeValue?: string;
  fileEntities: number;
  assetEntities: number;
  queryLabel: string;
}

export interface EditablePattern extends PatternDraft {
  id: string;
  isNew?: boolean;
}

export type PatternSortField = "sample" | "scope" | "resourceType" | "annotationType";
export type PatternSortDirection = "asc" | "desc";

export interface PatternSortState {
  field: PatternSortField | null;
  direction: PatternSortDirection;
}

export const PAGE_SIZE_OPTIONS = ["25", "50", "100", "200"];
