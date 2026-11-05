import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { vividScoreBadgeClassName } from "@/utils/testReportUtils";

export function TestingInfoSections() {
  return (
    <div className="px-6 py-4 space-y-6 text-sm overflow-y-auto max-h-[calc(85vh-100px)]">
      <div>
        <h3 className="font-semibold text-foreground mb-3">
          Step-by-Step Guide
        </h3>
        <ol className="space-y-2 text-muted-foreground">
          <li className="flex gap-2">
            <span className="font-medium text-foreground shrink-0">1.</span>
            <span>
              <strong>Filter Summaries (Optional):</strong> Use the date range
              filter to narrow down available summaries by their date range
            </span>
          </li>
          <li className="flex gap-2">
            <span className="font-medium text-foreground shrink-0">2.</span>
            <span>
              <strong>Select Summary:</strong> Choose a past operation summary
              from the dropdown to use as your baseline reference
            </span>
          </li>
          <li className="flex gap-2">
            <span className="font-medium text-foreground shrink-0">3.</span>
            <span>
              <strong>Auto-populate Dates:</strong> Test period dates are
              automatically populated from the selected summary (you can still
              change them)
            </span>
          </li>
          <li className="flex gap-2">
            <span className="font-medium text-foreground shrink-0">4.</span>
            <span>
              <strong>Configure Test Period:</strong> Adjust the start and end
              dates for the test comparison if needed
            </span>
          </li>
          <li className="flex gap-2">
            <span className="font-medium text-foreground shrink-0">5.</span>
            <span>
              <strong>Select Data Types:</strong> Choose which data types to
              include in the test (Assets, Files, Work Orders, Time Series,
              Equipment)
            </span>
          </li>
          <li className="flex gap-2">
            <span className="font-medium text-foreground shrink-0">6.</span>
            <span>
              <strong>Run Test:</strong> Click "Run Test" to generate a new
              summary and compare it against the original
            </span>
          </li>
          <li className="flex gap-2">
            <span className="font-medium text-foreground shrink-0">7.</span>
            <span>
              <strong>Review Results:</strong> Analyze the similarity score,
              data count comparison, and detailed AI analysis
            </span>
          </li>
          <li className="flex gap-2">
            <span className="font-medium text-foreground shrink-0">8.</span>
            <span>
              <strong>Save to CDF:</strong> Click "Save Results" to store the
              comparison report in CDF for future reference
            </span>
          </li>
          <li className="flex gap-2">
            <span className="font-medium text-foreground shrink-0">9.</span>
            <span>
              <strong>Download report:</strong> Export the test case for eval-ui
              integration or download for local review
            </span>
          </li>
        </ol>
      </div>

      <div className="p-3 bg-muted/50 rounded-lg border">
        <h4 className="font-medium text-foreground mb-1">
          About Date Filtering
        </h4>
        <p className="text-muted-foreground">
          The date filter helps you find summaries within a specific time
          period. When you select a filtered summary, the Test Period dates will
          be automatically set to match the selected summary's date range.
        </p>
      </div>

      <div className="p-3 bg-blue-500/10 rounded-lg border border-blue-500/20">
        <h4 className="font-medium text-foreground mb-1">
          About eval-ui Export
        </h4>
        <p className="text-muted-foreground">
          When you export a test case, it is automatically saved to the
          agent-evals space in CDF where our evaluation system can access it.
          The eval-ui app will validate data accuracy, coverage, response
          quality, and consistency using the exported test configuration and
          expected results.
        </p>
      </div>

      <div>
        <h3 className="font-semibold text-foreground mb-3">
          Similarity Score Interpretation
        </h3>
        <div className="space-y-2">
          <div className="flex items-center gap-3">
            <Badge
              variant="success"
              className={cn(
                "w-20 justify-center",
                vividScoreBadgeClassName("success"),
              )}
            >
              85-100
            </Badge>
            <span className="text-muted-foreground">
              Excellent consistency - summaries are very similar
            </span>
          </div>
          <div className="flex items-center gap-3">
            <Badge
              variant="success"
              className={cn(
                "w-20 justify-center",
                vividScoreBadgeClassName("success"),
              )}
            >
              75-84
            </Badge>
            <span className="text-muted-foreground">
              Good consistency - minor differences detected
            </span>
          </div>
          <div className="flex items-center gap-3">
            <Badge
              variant="warning"
              className={cn(
                "w-20 justify-center",
                vividScoreBadgeClassName("warning"),
              )}
            >
              60-74
            </Badge>
            <span className="text-muted-foreground">
              Fair consistency - some notable differences
            </span>
          </div>
          <div className="flex items-center gap-3">
            <Badge
              variant="error"
              className={cn(
                "w-20 justify-center",
                vividScoreBadgeClassName("error"),
              )}
            >
              &lt;60
            </Badge>
            <span className="text-muted-foreground">
              Poor consistency - significant differences found
            </span>
          </div>
        </div>
      </div>

      <div>
        <h3 className="font-semibold text-foreground mb-3">
          Data Comparison Table
        </h3>
        <p className="text-muted-foreground mb-3">
          The table shows count differences between original and new summaries
          for each data type, helping identify if the regenerated summary found
          different amounts of data.
        </p>
        <div className="space-y-1">
          <div className="flex items-center gap-2 text-muted-foreground">
            <span className="text-emerald-600 font-medium">Green (+)</span>
            <span>More items found in new summary</span>
          </div>
          <div className="flex items-center gap-2 text-muted-foreground">
            <span className="text-red-600 font-medium">Red (-)</span>
            <span>Fewer items found in new summary</span>
          </div>
          <div className="flex items-center gap-2 text-muted-foreground">
            <span className="text-muted-foreground font-medium">Gray (0)</span>
            <span>Same count in both summaries</span>
          </div>
        </div>
      </div>

      <div className="p-3 bg-purple-500/10 rounded-lg border border-purple-500/20">
        <h4 className="font-medium text-foreground mb-2">
          Testing Best Practices
        </h4>
        <ul className="space-y-1 text-muted-foreground">
          <li>
            Use the same date range as the original summary for accurate
            comparison
          </li>
          <li>Test periodically to monitor AI consistency over time</li>
          <li>
            Download report files for integration with automated testing
            pipelines
          </li>
          <li>
            Needs attention: scores below 70% to identify potential issues
          </li>
          <li>Save test results to CDF for historical tracking and auditing</li>
        </ul>
      </div>
    </div>
  );
}
