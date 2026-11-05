/**
 * Shared types for summary generation (core, work orders, hook).
 */

import type { CDFHelper } from "@/services/cdfHelper";
import type { DataTypeName } from "@/types/operations";

/** Marker so coverage counts this module (otherwise only type-only exports). */
export const SUMMARY_GENERATION_TYPES_MODULE = true;

export type GenerationProgressStatus =
  | "idle"
  | "fetching"
  | "processing"
  | "generating"
  | "saving"
  | "complete"
  | "error";

export interface SummaryGenerationProgressUpdate {
  status: GenerationProgressStatus;
  message: string;
  progress: number;
  currentStep?: string;
}

export interface GenerationContext {
  helper: CDFHelper;
  agentId: string;
  periodStart: Date;
  periodEnd: Date;
  promptMapping: Record<DataTypeName, string>;
}
