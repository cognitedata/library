import { QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { CdfClientProvider } from "@/contexts/CdfClientContext";
import { createTestQueryClient } from "@/test-utils/wrappers";
import type {
  ComparisonTestResult,
  DataTypeName,
  SummaryInstance,
} from "@/types/operations";

// react-pdf loads a PDF.js worker and canvas that jsdom does not provide.
vi.mock("react-pdf", () => ({
  pdfjs: { GlobalWorkerOptions: { workerSrc: "" } },
  Document: ({ children }: { children?: ReactNode }) => (
    <div data-testid="mock-pdf-doc">{children}</div>
  ),
  Page: () => <div data-testid="mock-pdf-page" />,
}));

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
// Stub the Fusion host so unit tests do not boot the real App SDK.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: () => ({
    project: "p",
    get: vi.fn().mockResolvedValue({ data: { items: [], invalidAgents: [] } }),
    post: vi.fn().mockResolvedValue({ data: { items: [] } }),
    instances: { list: vi.fn() },
  }),
}));

import { SummaryDetailModal } from "./SummaryDetailModal";
import { SummaryResultPanel } from "./SummaryResultPanel";
import { TestConfigCard } from "./TestConfigCard";
import { TestReportModal } from "./TestReportModal";
import { TestResultsPanel } from "./TestResultsPanel";

const VIEW = "cdf_cdm/CogniteFile/v1";

function CdfQueryProviders({ children }: { children: ReactNode }) {
  const qc = createTestQueryClient();
  return (
    <QueryClientProvider client={qc}>
      <CdfClientProvider>{children}</CdfClientProvider>
    </QueryClientProvider>
  );
}

const baseSummary: SummaryInstance = {
  externalId: "ops_summary_2024-01-01_2024-01-31_x",
  space: "ops_summaries",
  createdTime: 1_700_000_000_000,
  properties: {
    [VIEW]: { name: "S", content: "# Hello\nWorld" },
  },
};

const counts = {
  assets: 1,
  files: 0,
  work_orders: 0,
  time_series: 0,
  equipment: 0,
};

const comparisonResult: ComparisonTestResult = {
  originalSummary: baseSummary,
  originalContent: "# Old",
  newSummary: "# New",
  similarityScore: 90,
  dataAccuracyScore: 88,
  originalDataCounts: counts,
  newDataCounts: counts,
  comparisonAnalysis: "OK",
  timestamp: new Date(),
};

describe("page panels smoke", () => {
  it("renders SummaryResultPanel", () => {
    render(
      <SummaryResultPanel
        generatedSummary={{
          fullText: "# Gen",
          generatedAt: new Date(),
        }}
        periodStart={new Date("2024-01-01")}
        periodEnd={new Date("2024-01-31")}
        summaryContents={{
          assets: true,
          files: false,
          work_orders: false,
          time_series: false,
          equipment: false,
        }}
        savedPdfInfo={null}
        isSaving={false}
        onSave={async () => {}}
        onDownload={() => {}}
      />,
    );
    expect(screen.getByText(/Generated:/i)).toBeInTheDocument();
  });

  it("renders TestConfigCard", () => {
    render(
      <TestConfigCard
        selectedSummary={baseSummary}
        selectedDataTypes={["assets"] as DataTypeName[]}
        testStartDate="2024-01-01"
        testEndDate="2024-01-31"
        isRunning={false}
        generationProgress={{ status: "idle", message: "", progress: 0 }}
        testError={null}
        onChangeStartDate={() => {}}
        onChangeEndDate={() => {}}
        onToggleDataType={() => {}}
        onSetDataTypes={() => {}}
        onChangeSummary={() => {}}
        onRunTest={() => {}}
        onDismissError={() => {}}
      />,
    );
    expect(screen.getByRole("heading", { name: "S" })).toBeInTheDocument();
  });

  it("renders TestResultsPanel", () => {
    render(
      <TestResultsPanel
        testResult={comparisonResult}
        testStartDate="2024-01-01"
        testEndDate="2024-01-31"
        selectedDataTypes={["assets"]}
        isSaving={false}
        yamlExported={false}
        yamlContent=""
        onSave={async () => {}}
        onExportYaml={() => {}}
        onCopyYaml={async () => {}}
        onDownloadYaml={() => {}}
        onClear={() => {}}
      />,
    );
    expect(
      screen.getByRole("heading", { name: /test results/i }),
    ).toBeInTheDocument();
  });

  it("renders TestReportModal when open", () => {
    render(
      <TestReportModal
        isOpen
        onClose={() => {}}
        selectedReport={baseSummary}
      />,
    );
    expect(screen.getByRole("dialog")).toBeTruthy();
  });

  it("renders SummaryDetailModal when open", () => {
    render(
      <CdfQueryProviders>
        <SummaryDetailModal
          isOpen
          onClose={() => {}}
          selectedSummary={baseSummary}
          onDownloadMd={() => {}}
          onDownloadPdf={() => {}}
        />
      </CdfQueryProviders>,
    );
    expect(screen.getByRole("dialog")).toBeTruthy();
  });
});
