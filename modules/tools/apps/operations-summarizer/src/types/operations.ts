/**
 * TypeScript interfaces for the Operations Summarizer application.
 * Transpiled from Python data structures.
 */

// ============================================================
// View ID Types
// ============================================================

export interface ViewId {
  space: string;
  externalId: string;
  version: string;
}

export { VIEW_IDS } from "@/config/viewIds";

// ============================================================
// Data Type Configuration
// ============================================================

export type DataTypeName =
  "assets" | "files" | "work_orders" | "time_series" | "equipment";

export type WorkOrderSubcategory =
  "completed_work_orders" | "ongoing_work_orders" | "scheduled_work_orders";

/** A flat row of CDF instance properties as returned by the DM API. */
export type InstanceRow = Record<string, string | number | boolean | null>;

export interface TableColumn {
  header: string;
  property: string;
  formatter?: (value: unknown) => string;
  default?: string;
}

export interface DataTypeConfig {
  name: DataTypeName;
  displayName: string;
  viewId: ViewId;
  promptTemplate: string;
  tableColumns: TableColumn[];
  isSpecialCase: boolean;
  subcategories?: WorkOrderSubcategory[];
}

// ============================================================
// Agent Types
// ============================================================

export interface Agent {
  externalId: string;
  name: string;
  description?: string;
  instructions?: string;
  model?: string;
}

export interface AgentCreateParams {
  externalId: string;
  name: string;
  description: string;
  instructions: string;
  model: string;
  tools?: unknown[];
}

// ============================================================
// Summary Types
// ============================================================

export interface SummaryContents {
  assets: boolean;
  files: boolean;
  work_orders: boolean;
  time_series: boolean;
  equipment: boolean;
}

export interface SummaryPeriod {
  startDate: Date;
  endDate: Date;
}

export interface SummaryInstance {
  externalId: string;
  space: string;
  createdTime: number | Date;
  properties: Record<string, Record<string, unknown>>;
  /** Some list/API shapes expose file or summary text at the top level. */
  sourceContext?: string;
}

export interface InstanceSummary {
  dataType: DataTypeName;
  table: string;
  text: string;
  instances: CdfInstance[];
}

/** Same shape as {@link InstanceSummary} without per-type label (work orders are handled separately). */
export type WorkOrderSummary = Omit<InstanceSummary, "dataType">;

export interface GeneratedSummary {
  workOrders: WorkOrderSummary[];
  instances: InstanceSummary[];
  fullText: string;
  generatedAt: Date;
  /** Set when the function stopped early because more instances remained in CDF. */
  dataTypeTruncation?: Partial<
    Record<DataTypeName, { limit: number; hadMoreInCdf: boolean }>
  >;
}

// ============================================================
// Test Result Types
// ============================================================

export interface TestResultData {
  originalSummaryId: string;
  originalSummarySpace: string;
  originalContent: string;
  newSummary: string;
  similarityScore: number;
  dataAccuracyScore: number;
  originalDataCounts: DataCounts;
  newDataCounts: DataCounts;
  comparisonAnalysis: string;
  timestamp: string; // ISO string for serialization
}

/** Single comparison test run (used on Testing page). */
export interface ComparisonTestResult {
  originalSummary: SummaryInstance;
  originalContent: string;
  newSummary: string;
  similarityScore: number;
  dataAccuracyScore: number;
  originalDataCounts: DataCounts;
  newDataCounts: DataCounts;
  comparisonAnalysis: string;
  timestamp: Date;
}

/** Progress state for test generation UI. */
export interface TestGenerationProgress {
  status:
    "idle" | "fetching" | "generating" | "comparing" | "complete" | "error";
  message: string;
  progress: number;
  currentStep?: string;
}

// ============================================================
// CDF Instance Types
// ============================================================

export interface CdfInstance {
  externalId: string;
  space: string;
  createdTime?: number;
  properties: Record<string, Record<string, unknown>>;
}

export interface CdfFileProperties {
  name?: string;
  description?: string;
  mimeType?: string;
}

// ============================================================
// Test Types
// ============================================================

export interface TestResults {
  originalSummary: string;
  newSummary: string;
  comparisonText: string;
  similarityScore: number | null;
  dataAccuracyScore: number | null;
  llmScore: number | null;
  scoreSource: string;
  startDate: string;
  endDate: string;
  testTimestamp: string;
}

export interface DataCounts {
  assets: number;
  files: number;
  work_orders: number;
  time_series: number;
  equipment: number;
}

/** Keys of {@link DataCounts} in stable order (for UI tables and accuracy math). */
export const DATA_COUNT_KEYS: (keyof DataCounts)[] = [
  "assets",
  "files",
  "work_orders",
  "time_series",
  "equipment",
];

export interface TestReport {
  name: string;
  description: string;
  score: number | null;
  startDate: string;
  endDate: string;
  createdTime: string;
  dataCounts: DataCounts;
  summaryInstance: SummaryInstance;
}

export interface TestStatistics {
  totalTests: number;
  avgScore: number;
  excellentTests: number;
  poorTests: number;
  avgAssets: number;
  avgFiles: number;
  avgEquipment: number;
  avgWorkOrders: number;
}

// ============================================================
// Filter Types
// ============================================================

export interface DateRangeFilter {
  enabled: boolean;
  startDate: Date | null;
  endDate: Date | null;
}

export interface ContentTypeFilter {
  enabled: boolean;
  selectedTypes: DataTypeName[];
}

// ============================================================
// Pagination Types
// ============================================================

export interface PaginationState {
  currentPage: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
}

export interface PaginatedResult<T> {
  items: T[];
  pagination: PaginationState;
}

// ============================================================
// Chat Types
// ============================================================

export interface ChatMessage {
  role: "user" | "assistant" | "system";
  content: string | { type: string; text: string };
}

export interface ChatCompletionResponse {
  choices: Array<{
    message: {
      content: string;
    };
  }>;
}

// ============================================================
// CDF API Response Types
// ============================================================

/**
 * Response from the agents list API.
 */
export interface AgentsListResponse {
  items: Array<{
    externalId: string;
    name: string;
    description?: string;
    model?: string;
    instructions?: string;
  }>;
  invalidAgents?: string[];
}

/**
 * Response from the AI services availability API.
 */
export interface AIServicesAvailabilityResponse {
  languageModels: Array<{
    name: string;
    provider?: string;
    capabilities?: string[];
  }>;
}

/**
 * Response from the agent chat API.
 */
export interface AgentChatResponse {
  response: {
    messages: Array<{
      role: "assistant";
      content: {
        type: string;
        text: string;
      };
    }>;
  };
  cursor?: string;
}

/**
 * One capability scope item from the token inspect API (response.capabilities[]).
 * Each item can have various *Acl keys (e.g. agentsAcl, filesAcl) and projectScope.
 * Used to determine if the user has agents:write (WRITE in agentsAcl.actions)
 * for the current project (projectScope.projects matches CDF client project).
 */
export interface TokenInspectCapabilityItem {
  agentsAcl?: {
    actions: string[];
    scope?: { all?: Record<string, unknown> };
  };
  projectScope?: {
    projects: string[];
  };
  [key: string]: unknown;
}

/**
 * Response from GET /api/v1/token/inspect.
 * - subject: token subject
 * - projects: list of { projectUrlName, groups } for project membership
 * - capabilities: list of capability items; user can edit agent config if any item
 *   has agentsAcl.actions including "WRITE" and projectScope.projects includes
 *   the current CDF client project.
 */
export interface TokenInspectResponse {
  subject?: string;
  projects?: Array<{ projectUrlName: string; groups: number[] }>;
  capabilities: TokenInspectCapabilityItem[];
}

/**
 * Response from the files download link API.
 */
export interface FilesDownloadLinkResponse {
  items: Array<{
    downloadUrl: string;
    externalId?: string;
  }>;
}

/**
 * Response from instances list API.
 */
export interface InstancesListResponse {
  items: Array<{
    externalId: string;
    space: string;
    createdTime?: number;
    lastUpdatedTime?: number;
    properties?: Record<string, Record<string, unknown>>;
  }>;
  nextCursor?: string;
}

/**
 * File upload response from the files API.
 */
export interface FileUploadResponse {
  items: Array<{
    externalId: string;
    space: string;
    uploadUrl?: string;
    createdTime?: number;
  }>;
}

// ============================================================
// Score Types
// ============================================================

export type ScoreCategory = "excellent" | "good" | "fair" | "poor";

export interface ScoreDisplay {
  category: ScoreCategory;
  emoji: string;
  description: string;
  className: string;
}
