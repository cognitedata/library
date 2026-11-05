import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ComparisonTestResult, DataTypeName } from "@/types/operations";
import { TestResultsPanel } from "./TestResultsPanel";

const VIEW = "cdf_cdm/CogniteFile/v1";

const baseResult: ComparisonTestResult = {
  originalSummary: {
    externalId: "ops_summary_2024-01-01_2024-01-31_x",
    space: "ops_summaries",
    createdTime: 1_700_000_000_000,
    properties: { [VIEW]: { name: "Original", content: "# Original" } },
  },
  originalContent: "# Original summary content",
  newSummary: "# New summary content",
  similarityScore: 88,
  dataAccuracyScore: 92,
  originalDataCounts: {
    assets: 10,
    files: 5,
    work_orders: 3,
    time_series: 2,
    equipment: 1,
  },
  newDataCounts: {
    assets: 12,
    files: 5,
    work_orders: 3,
    time_series: 2,
    equipment: 1,
  },
  comparisonAnalysis: "Minor differences found in assets.",
  timestamp: new Date("2024-01-15T10:00:00Z"),
};

const defaultHandlers = {
  onSave: vi.fn().mockResolvedValue(undefined),
  onExportYaml: vi.fn(),
  onCopyYaml: vi.fn().mockResolvedValue(undefined),
  onDownloadYaml: vi.fn(),
  onClear: vi.fn(),
};

function renderPanel(
  overrides: Partial<{
    testResult: ComparisonTestResult;
    isSaving: boolean;
    yamlExported: boolean;
    yamlContent: string;
    saveError: string | null;
    selectedDataTypes: DataTypeName[];
  }> = {},
) {
  return render(
    <TestResultsPanel
      testResult={baseResult}
      testStartDate="2024-01-01"
      testEndDate="2024-01-31"
      selectedDataTypes={["assets", "files"] as DataTypeName[]}
      isSaving={false}
      yamlExported={false}
      yamlContent=""
      saveError={null}
      {...defaultHandlers}
      {...overrides}
    />,
  );
}

describe("TestResultsPanel", () => {
  describe("header info", () => {
    it("renders similarity and accuracy score badges", () => {
      renderPanel();
      expect(screen.getByText("88% Similarity")).toBeInTheDocument();
      expect(screen.getByText("92% Accuracy")).toBeInTheDocument();
    });

    it("renders the test period", () => {
      renderPanel();
      expect(screen.getByText(/2024-01-01 — 2024-01-31/)).toBeInTheDocument();
    });

    it("renders selected data types", () => {
      renderPanel();
      // getDisplayName maps "assets" → "Assets", "files" → "Files"
      expect(
        screen.getByText(/Assets.*Files|Files.*Assets/),
      ).toBeInTheDocument();
    });
  });

  describe("button callbacks", () => {
    it("calls onSave when Save is clicked", () => {
      renderPanel();
      fireEvent.click(screen.getByRole("button", { name: /^save$/i }));
      expect(defaultHandlers.onSave).toHaveBeenCalled();
    });

    it("calls onExportYaml when Download report is clicked", () => {
      renderPanel();
      fireEvent.click(screen.getByRole("button", { name: /download report/i }));
      expect(defaultHandlers.onExportYaml).toHaveBeenCalled();
    });

    it("calls onClear when Clear is clicked", () => {
      renderPanel();
      fireEvent.click(screen.getByRole("button", { name: /^clear$/i }));
      expect(defaultHandlers.onClear).toHaveBeenCalled();
    });

    it("calls onCopyYaml when Copy is clicked (yamlExported=true)", () => {
      renderPanel({ yamlExported: true, yamlContent: "yaml: content" });
      fireEvent.click(screen.getByRole("button", { name: /^copy$/i }));
      expect(defaultHandlers.onCopyYaml).toHaveBeenCalled();
    });

    it("calls onDownloadYaml when Download is clicked (yamlExported=true)", () => {
      renderPanel({ yamlExported: true, yamlContent: "yaml: content" });
      fireEvent.click(screen.getByRole("button", { name: /^download$/i }));
      expect(defaultHandlers.onDownloadYaml).toHaveBeenCalled();
    });
  });

  describe("conditional rendering", () => {
    it("disables Save button while isSaving", () => {
      renderPanel({ isSaving: true });
      // When saving, a Loader icon renders inside the button alongside "Save",
      // so the accessible name becomes "Loader Save" — match via /save/i.
      expect(screen.getByRole("button", { name: /save/i })).toBeDisabled();
    });

    it("hides Copy and Download buttons when yamlExported is false", () => {
      renderPanel({ yamlExported: false });
      expect(
        screen.queryByRole("button", { name: /^copy$/i }),
      ).not.toBeInTheDocument();
      expect(
        screen.queryByRole("button", { name: /^download$/i }),
      ).not.toBeInTheDocument();
    });

    it("shows YAML textarea when yamlExported is true", () => {
      renderPanel({ yamlExported: true, yamlContent: "name: my-test" });
      expect(screen.getByDisplayValue("name: my-test")).toBeInTheDocument();
    });

    it("shows saveError message when provided", () => {
      renderPanel({ saveError: "Network request failed" });
      expect(screen.getByText("Network request failed")).toBeInTheDocument();
    });
  });

  describe("data count comparison table", () => {
    it("shows a change delta for mismatched counts", () => {
      renderPanel();
      // assets: orig=10, new=12 → diff=+2
      expect(screen.getByText("+2")).toBeInTheDocument();
    });

    it("shows a Match badge for unchanged counts", () => {
      renderPanel();
      // files, work_orders, time_series, equipment all unchanged → multiple Match badges
      const matches = screen.getAllByText("✓ Match");
      expect(matches.length).toBeGreaterThan(0);
    });

    it("shows a negative delta when new counts drop", () => {
      renderPanel({
        testResult: {
          ...baseResult,
          originalDataCounts: { ...baseResult.originalDataCounts, assets: 10 },
          newDataCounts: { ...baseResult.newDataCounts, assets: 7 },
        },
      });
      expect(screen.getByText("-3")).toBeInTheDocument();
    });
  });

  describe("analysis and summary comparison", () => {
    it("renders comparison analysis markdown", () => {
      renderPanel();
      expect(
        screen.getByText(/Minor differences found in assets/i),
      ).toBeInTheDocument();
    });

    it("shows fallback when comparison analysis is missing", () => {
      renderPanel({
        testResult: { ...baseResult, comparisonAnalysis: "" },
      });
      expect(
        screen.getByText(/No comparison analysis available/i),
      ).toBeInTheDocument();
    });

    it("expands Summary Comparison and shows original/new content", () => {
      renderPanel();
      fireEvent.click(
        screen.getByRole("button", { name: /summary comparison/i }),
      );
      expect(screen.getByText("Original Summary")).toBeInTheDocument();
      expect(screen.getByText("New Summary")).toBeInTheDocument();
      expect(
        screen.getByText("# Original summary content"),
      ).toBeInTheDocument();
      expect(screen.getByText("# New summary content")).toBeInTheDocument();
    });

    it("shows empty content fallbacks in Summary Comparison", () => {
      renderPanel({
        testResult: {
          ...baseResult,
          originalContent: "",
          newSummary: "",
        },
      });
      fireEvent.click(
        screen.getByRole("button", { name: /summary comparison/i }),
      );
      expect(screen.getAllByText("No content available").length).toBe(2);
    });
  });
});
