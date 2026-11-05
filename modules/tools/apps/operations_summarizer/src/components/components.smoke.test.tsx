import { QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { DeleteConfirmation } from "@/components/common/ConfirmationDialog";
import {
  DataTypeBadges,
  DataTypeControl,
} from "@/components/common/DataTypeControl";
import { DownloadErrorDialog } from "@/components/common/DownloadErrorDialog";
import { ErrorRecoveryDialog } from "@/components/common/ErrorRecoveryDialog";
import { FilterBar } from "@/components/common/FilterBar";
import { ProgressBar } from "@/components/common/ProgressBar";
import {
  Pagination,
  ResponsiveTable,
} from "@/components/common/ResponsiveTable";
import { Loader } from "@/components/Loader";
import { Navigation } from "@/components/layout/Navigation";
import { SummaryCard } from "@/components/summaries/SummaryCard";
import { SummaryTable } from "@/components/summaries/SummaryTable";
import { TestingInfoDialog } from "@/components/TestingInfoDialog";
import { TestCard } from "@/components/tests/TestCard";
import { TestTable } from "@/components/tests/TestTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { CollapsibleSection } from "@/components/ui/collapsible-section";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  MultiSelectDropdown,
  MultiSelectWithBadges,
} from "@/components/ui/multiselect-dropdown";
import { Skeleton } from "@/components/ui/skeleton";
import { Textarea } from "@/components/ui/textarea";
import { CdfClientProvider } from "@/contexts/CdfClientContext";
import { NavigationProvider, OperationsProvider } from "@/contexts/index";
import { createTestQueryClient } from "@/test-utils/wrappers";
import type { SummaryInstance } from "@/types/operations";

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
// Stub the Fusion host so unit tests do not boot the real App SDK.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: () => ({
    project: "p",
    get: vi.fn().mockResolvedValue({ data: { items: [], invalidAgents: [] } }),
    post: vi.fn().mockResolvedValue({ data: {} }),
    instances: { list: vi.fn() },
  }),
}));

const VIEW = "cdf_cdm/CogniteFile/v1";

const summaryRow: SummaryInstance = {
  externalId: "ops_summary_2024-01-01_2024-01-31_x",
  space: "ops_summaries",
  createdTime: 1,
  properties: {
    [VIEW]: {
      name: "N",
      content: "Similarity Score: 80/100\nTotal Data: 5",
    },
  },
};

function AppShell({ children }: { children: ReactNode }) {
  const qc = createTestQueryClient();
  return (
    <QueryClientProvider client={qc}>
      <CdfClientProvider>
        <NavigationProvider>
          <OperationsProvider>{children}</OperationsProvider>
        </NavigationProvider>
      </CdfClientProvider>
    </QueryClientProvider>
  );
}

describe("components smoke", () => {
  it("Skeleton", () => {
    render(<Skeleton data-testid="sk" className="h-4 w-20" />);
    expect(screen.getByTestId("sk")).toBeInTheDocument();
  });

  it("Loader", () => {
    render(<Loader data-testid="ld" />);
    expect(screen.getByTestId("ld")).toBeInTheDocument();
  });

  it("Navigation", () => {
    render(
      <AppShell>
        <Navigation />
      </AppShell>,
    );
    expect(screen.getByText("Summaries")).toBeInTheDocument();
  });

  it("Badge and Button", () => {
    render(
      <div>
        <Badge>tag</Badge>
        <Button type="button">Go</Button>
      </div>,
    );
    expect(screen.getByText("tag")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Go" })).toBeInTheDocument();
  });

  it("Card primitives", () => {
    render(
      <Card>
        <CardHeader>
          <CardTitle>T</CardTitle>
        </CardHeader>
        <CardContent>c</CardContent>
      </Card>,
    );
    expect(screen.getByText("T")).toBeInTheDocument();
  });

  it("Label Input Textarea", () => {
    render(
      <div>
        <Label htmlFor="i">L</Label>
        <Input id="i" />
        <Textarea defaultValue="hi" />
      </div>,
    );
    expect(screen.getByLabelText("L")).toBeInTheDocument();
    expect(screen.getByDisplayValue("hi")).toBeInTheDocument();
  });

  it("CollapsibleSection", () => {
    render(
      <CollapsibleSection title="Sec" defaultOpen>
        <p>inner</p>
      </CollapsibleSection>,
    );
    expect(screen.getByText("inner")).toBeInTheDocument();
  });

  it("Dialog", () => {
    render(
      <Dialog open>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Dtitle</DialogTitle>
          </DialogHeader>
        </DialogContent>
      </Dialog>,
    );
    expect(screen.getByText("Dtitle")).toBeInTheDocument();
  });

  it("MultiSelectDropdown and MultiSelectWithBadges", () => {
    const opts = [
      { value: "a", label: "A" },
      { value: "b", label: "B" },
    ];
    render(
      <div className="p-4 space-y-4">
        <MultiSelectDropdown
          options={opts}
          selectedValues={["a"]}
          onSelectionChange={() => {}}
          placeholder="Pick"
        />
        <MultiSelectWithBadges
          options={opts}
          selectedValues={[]}
          onSelectionChange={() => {}}
          placeholder="Pick2"
        />
      </div>,
    );
    expect(screen.getByText("A")).toBeInTheDocument();
    expect(screen.getByText("Pick2")).toBeInTheDocument();
  });

  it("FilterBar ProgressBar ResponsiveTable Pagination", () => {
    render(
      <div>
        <FilterBar
          dateRange={{
            startDate: "2024-01-01",
            endDate: "2024-01-31",
            onStartDateChange: () => {},
            onEndDateChange: () => {},
          }}
        />
        <ProgressBar progress={50} message="m" />
        <ResponsiveTable
          table={
            <table>
              <tbody>
                <tr>
                  <td>t</td>
                </tr>
              </tbody>
            </table>
          }
          cards={<div>card</div>}
        />
        <Pagination
          currentPage={1}
          totalPages={3}
          totalItems={10}
          hasNextPage
          hasPreviousPage={false}
          onNextPage={() => {}}
          onPreviousPage={() => {}}
          cursorMode
        />
      </div>,
    );
    expect(screen.getByText("m")).toBeInTheDocument();
    expect(screen.getByText("t")).toBeInTheDocument();
  });

  it("DataTypeControl and DataTypeBadges", () => {
    render(
      <div>
        <DataTypeControl
          selectedTypes={["assets"]}
          onToggle={() => {}}
          availableTypes={["assets", "files"]}
        />
        <DataTypeBadges selectedTypes={["assets"]} />
      </div>,
    );
    expect(screen.getByText(/data types/i)).toBeInTheDocument();
  });

  it("SummaryCard SummaryTable TestCard TestTable", () => {
    render(
      <div>
        <SummaryCard
          summary={summaryRow}
          onView={() => {}}
          onDelete={async () => {}}
        />
        <SummaryTable
          summaries={[summaryRow]}
          currentPage={1}
          totalPages={1}
          totalItems={1}
          hasNextPage={false}
          hasPreviousPage={false}
          onNextPage={() => {}}
          onPreviousPage={() => {}}
        />
        <TestCard
          testReport={summaryRow}
          onView={() => {}}
          onDownload={() => {}}
          onDelete={() => {}}
        />
        <TestTable
          testReports={[summaryRow]}
          currentPage={1}
          totalPages={1}
          totalItems={1}
          hasNextPage={false}
          hasPreviousPage={false}
          onNextPage={() => {}}
          onPreviousPage={() => {}}
        />
      </div>,
    );
    expect(screen.getAllByText(/N|Report|ops_summary/).length).toBeGreaterThan(
      0,
    );
  });

  it("DeleteConfirmation", () => {
    render(
      <DeleteConfirmation
        open
        onOpenChange={() => {}}
        itemType="Summary"
        itemName="N"
        onConfirm={async () => {}}
      />,
    );
    expect(screen.getByText(/delete summary/i)).toBeInTheDocument();
  });

  it("DownloadErrorDialog", () => {
    render(
      <DownloadErrorDialog
        open
        onOpenChange={() => {}}
        error="e"
        summary={summaryRow}
        onRetry={async () => {}}
        onDownloadMarkdown={async () => {}}
        onClose={() => {}}
      />,
    );
    expect(screen.getByText(/download failed/i)).toBeInTheDocument();
  });

  it("ErrorRecoveryDialog", () => {
    render(
      <ErrorRecoveryDialog
        open
        onOpenChange={() => {}}
        error="e2"
        canRetry
        onRetry={async () => {}}
        onResetToIdle={() => {}}
        onClose={() => {}}
      />,
    );
    expect(screen.getByText("e2")).toBeInTheDocument();
  });

  it("TestingInfoDialog", () => {
    render(<TestingInfoDialog open onOpenChange={() => {}} />);
    expect(
      screen.getByText(/how to use the testing feature/i),
    ).toBeInTheDocument();
  });
});
