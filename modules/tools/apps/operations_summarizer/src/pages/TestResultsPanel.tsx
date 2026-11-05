/**
 * Test Results Panel — shows data comparison table, analysis text,
 * summary comparison, and YAML export after a comparison test run.
 */

import { CollapsibleSection } from "@/components/ui/collapsible-section";
import { Textarea } from "@/components/ui/textarea";
import type { ComparisonTestResult, DataTypeName } from "@/types/operations";
import { markdownToHtml } from "@/utils/formatting";
import {
  TestDataCountTable,
  TestResultsHeader,
} from "./tests/TestResultsSections";

export interface TestResultsPanelProps {
  testResult: ComparisonTestResult;
  testStartDate: string;
  testEndDate: string;
  selectedDataTypes: DataTypeName[];
  isSaving: boolean;
  yamlExported: boolean;
  yamlContent: string;
  onSave: () => Promise<void>;
  onExportYaml: () => void;
  onCopyYaml: () => Promise<void>;
  onDownloadYaml: () => void;
  onClear: () => void;
  saveError?: string | null;
}

export function TestResultsPanel(props: TestResultsPanelProps) {
  const { testResult, yamlExported, yamlContent } = props;
  return (
    <div className="flex flex-col min-h-0 max-h-[min(78dvh,calc(100vh-10rem))] sm:max-h-[calc(100vh-220px)]">
      <TestResultsHeader {...props} />
      <div className="flex-1 min-h-0 overflow-auto bg-muted/30 border-x border-b border-border rounded-b-lg p-3 sm:p-4 space-y-4">
        <CollapsibleSection title="Data Count Comparison" defaultOpen={true}>
          <TestDataCountTable testResult={testResult} />
        </CollapsibleSection>

        <CollapsibleSection
          title="Detailed Comparison Analysis"
          defaultOpen={true}
        >
          <div className="mt-3 bg-muted/50 rounded-lg p-4 text-sm">
            {testResult.comparisonAnalysis ? (
              <div
                className="prose prose-sm dark:prose-invert max-w-none
                  prose-headings:font-semibold prose-headings:text-foreground
                  prose-h3:text-base prose-h3:mt-4 prose-h3:mb-2
                  prose-p:text-sm prose-p:text-muted-foreground prose-p:my-2
                  prose-li:text-sm prose-li:text-muted-foreground
                  prose-strong:text-foreground
                  prose-table:text-sm prose-th:bg-muted/50 prose-th:px-3 prose-th:py-2
                  prose-td:px-3 prose-td:py-2 prose-td:border-b"
                // biome-ignore lint/security/noDangerouslySetInnerHtml: sanitized via markdownToHtml/DOMPurify
                dangerouslySetInnerHTML={{
                  __html: markdownToHtml(testResult.comparisonAnalysis),
                }}
              />
            ) : (
              <p className="text-muted-foreground">
                No comparison analysis available.
              </p>
            )}
          </div>
        </CollapsibleSection>

        <CollapsibleSection title="Summary Comparison" defaultOpen={false}>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-3">
            <div>
              <p className="font-medium text-sm mb-2 text-muted-foreground">
                Original Summary
              </p>
              <div className="bg-muted/50 rounded-lg p-3 max-h-64 overflow-y-auto border">
                <pre className="text-xs whitespace-pre-wrap font-mono">
                  {testResult.originalContent || "No content available"}
                </pre>
              </div>
            </div>
            <div>
              <p className="font-medium text-sm mb-2 text-muted-foreground">
                New Summary
              </p>
              <div className="bg-muted/50 rounded-lg p-3 max-h-64 overflow-y-auto border">
                <pre className="text-xs whitespace-pre-wrap font-mono">
                  {testResult.newSummary || "No content available"}
                </pre>
              </div>
            </div>
          </div>
        </CollapsibleSection>

        {yamlExported && (
          <div className="border rounded-lg p-3 bg-muted/30">
            <Textarea
              value={yamlContent}
              readOnly
              className="font-mono text-xs h-48 mb-2"
            />
            <p className="text-xs text-muted-foreground">
              Import this YAML into eval-ui for automated evaluations.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
