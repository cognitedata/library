import { useMemo, useState } from "react";
import { useAppSdk } from "@/providers/AppSdkProvider";
import {
  FileTable,
  type FileSortDirection,
  type FileSortField,
} from "@/pages/PipelineHealth/components/FileTable";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/shared/components/ui/tabs";
import { Badge } from "@/shared/components/ui/badge";
import { Button } from "@/shared/components/ui/button";
import { Input } from "@/shared/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/shared/components/ui/select";
import {
  FileText,
  Play,
  CheckCircle,
  ArrowUpCircle,
  Filter,
  Loader2,
  Download,
  ChevronDown,
  ChevronUp,
  Search,
  FileSearch,
  Terminal,
  AlertCircle,
} from "lucide-react";
import type { AnnotationState, PipelineConfig } from "@/shared/utils/types";
import { CallerType } from "@/shared/utils/constants";
import { useFunctionLogs, filterLogLines } from "@/pages/PipelineHealth/hooks/usePipelineRuns";
import { WarningCallout } from "@/shared/components/WarningCallout";




interface FileExplorerTabProps {
  annotationStates: AnnotationState[];
  config: PipelineConfig | null;
}

interface FunctionLogContentProps {
  functionId: number | undefined;
  callId: number | undefined;
  callerType: string;
  fileExternalId: string;
}

function FunctionLogContent({
  functionId,
  callId,
  callerType,
  fileExternalId,
}: FunctionLogContentProps) {
  const { sdk } = useAppSdk();
  const { data: logs, isLoading, error } = useFunctionLogs(
    sdk,
    functionId ?? null,
    callId ?? null
  );
  const [showFullLog, setShowFullLog] = useState(false);

  if (!functionId || !callId) {
    return (
      <div className="flex flex-col items-center justify-center py-8 text-muted-foreground">
        <Terminal className="h-8 w-8 mb-2 opacity-30" />
        <p className="text-sm">No {callerType} function run for this file.</p>
      </div>
    );
  }

  const relevantLogs = logs ? filterLogLines(logs, fileExternalId, 2) : "";

  const handleDownload = () => {
    if (!logs) return;
    const blob = new Blob([logs], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${fileExternalId}_${callerType.toLowerCase()}_logs.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-4">
      {/* Function Info */}
      <div className="flex flex-wrap items-center gap-3 text-xs">
        <div className="flex items-center gap-1.5 px-2.5 py-1 bg-muted rounded-md">
          <span className="text-muted-foreground">Function:</span>
          <code className="font-mono">{functionId}</code>
        </div>
        <div className="flex items-center gap-1.5 px-2.5 py-1 bg-muted rounded-md">
          <span className="text-muted-foreground">Call:</span>
          <code className="font-mono">{callId}</code>
        </div>
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-5 w-5 animate-spin text-primary" />
          <span className="ml-2 text-sm text-muted-foreground">Loading logs...</span>
        </div>
      ) : error ? (
        <WarningCallout
          title="Failed to load logs"
          description="The function may not exist or you may not have permission."
        />
      ) : logs ? (
        <div className="space-y-4">
          {/* Relevant Log Entries */}
          {relevantLogs ? (
            <div className="space-y-2">
              <div className="flex items-center gap-2 text-xs font-medium">
                <Search className="h-3.5 w-3.5 text-primary" />
                <span>Relevant Entries</span>
                <Badge variant="info" className="font-mono">
                  {fileExternalId.substring(0, 24)}...
                </Badge>
              </div>
              <div className="rounded-lg border border-primary/20 bg-primary/5 p-3 overflow-x-auto">
                <pre className="text-xs text-foreground font-mono whitespace-pre-wrap max-h-48 overflow-y-auto">
                  {relevantLogs}
                </pre>
              </div>
            </div>
          ) : (
            <div className="rounded-lg border bg-muted/30 p-4">
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Search className="h-4 w-4" />
                <span>No specific log entries found for this file.</span>
              </div>
            </div>
          )}

          {/* Actions */}
          <div className="flex items-center gap-2 text-xs font-medium">
            <Button variant="outline" size="xs" onClick={handleDownload}>
              <Download className="h-3.5 w-3.5 text-primary" />
              Download Log
            </Button>
            <Button
              variant="ghost"
              size="xs"
              onClick={() => setShowFullLog(!showFullLog)}
            >
              {showFullLog ? (
                <>
                  <ChevronUp className="h-3.5 w-3.5 text-primary" />
                  Hide Full Log
                </>
              ) : (
                <>
                  <ChevronDown className="h-3.5 w-3.5 text-primary" />
                  View Full Log
                </>
              )}
            </Button>
          </div>

          {/* Full Log */}
          {showFullLog && (
            <div className="rounded-lg border bg-slate-950 p-3 overflow-x-auto">
              <pre className="text-xs text-emerald-400 font-mono whitespace-pre-wrap max-h-80 overflow-y-auto">
                {logs}
              </pre>
            </div>
          )}
        </div>
      ) : (
        <div className="rounded-lg border bg-muted/30 p-4 text-center text-sm text-muted-foreground">
          No logs available for this function run.
        </div>
      )}
    </div>
  );
}

interface FunctionLogViewerProps {
  file: AnnotationState | null;
}

function FunctionLogViewer({ file }: FunctionLogViewerProps) {
  if (!file) {
    return (
      <Card className="border-dashed">
        <CardContent className="py-12">
          <div className="flex flex-col items-center justify-center py-8 text-muted-foreground">
            <FileSearch className="h-10 w-10 mb-3 opacity-30" />
            <p className="text-sm font-medium">No File Selected</p>
            <p className="text-xs mt-1">Select a file from the table above to view function logs.</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  const fileExternalId =
    file.linkedFile?.externalId || file.fileName || file.externalId;

  const functionConfigs = [
    {
      type: CallerType.LAUNCH,
      icon: <Play className="h-3.5 w-3.5 text-primary" />,
      funcId: file.launchFunctionId,
      callId: file.launchFunctionCallId,
    },
    {
      type: CallerType.FINALIZE,
      icon: <CheckCircle className="h-3.5 w-3.5 text-primary" />,
      funcId: file.finalizeFunctionId,
      callId: file.finalizeFunctionCallId,
    },
    {
      type: CallerType.PREPARE,
      icon: <FileText className="h-3.5 w-3.5 text-primary" />,
      funcId: file.prepareFunctionId,
      callId: file.prepareFunctionCallId,
    },
    {
      type: CallerType.PROMOTE,
      icon: <ArrowUpCircle className="h-3.5 w-3.5 text-primary" />,
      funcId: file.promoteFunctionId,
      callId: file.promoteFunctionCallId,
    },
  ];

  const availableFunctions = functionConfigs.filter(
    (cfg) => cfg.funcId != null && cfg.callId != null
  );

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm font-semibold flex items-center gap-2">
          <Terminal className="h-4 w-4 text-muted-foreground" />
          Function Logs
          <Badge variant="secondary" className="font-mono">
            {(file.fileName || file.linkedFile?.externalId || file.externalId).substring(0, 32)}...
          </Badge>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {availableFunctions.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-8 text-muted-foreground">
            <AlertCircle className="h-8 w-8 mb-2 opacity-30" />
            <p className="text-sm">No function runs available.</p>
            <p className="text-xs mt-1">The file may not have been processed yet.</p>
          </div>
        ) : (
          <Tabs defaultValue={availableFunctions[0]?.type}>
            <TabsList className="h-8 p-0.5 bg-muted/50">
              {availableFunctions.map((cfg) => (
                <TabsTrigger
                  key={cfg.type}
                  value={cfg.type}
                  className="h-7 px-3 text-xs data-[state=active]:shadow-none"
                >
                  {cfg.icon}
                  <span className="ml-1.5">{cfg.type}</span>
                </TabsTrigger>
              ))}
            </TabsList>

            {availableFunctions.map((cfg) => (
              <TabsContent key={cfg.type} value={cfg.type} className="mt-4">
                <FunctionLogContent
                  functionId={cfg.funcId}
                  callId={cfg.callId}
                  callerType={cfg.type}
                  fileExternalId={fileExternalId}
                />
              </TabsContent>
            ))}
          </Tabs>
        )}
      </CardContent>
    </Card>
  );
}

export function FileExplorerTab({
  annotationStates,
  config,
}: FileExplorerTabProps) {
  const [selectedFile, setSelectedFile] = useState<AnnotationState | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [resourceTypeFilter, setResourceTypeFilter] = useState("all");
  const [primaryScopeFilter, setPrimaryScopeFilter] = useState("all");
  const [secondaryScopeFilter, setSecondaryScopeFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [updatedStart, setUpdatedStart] = useState("");
  const [updatedEnd, setUpdatedEnd] = useState("");
  const [sortField, setSortField] = useState<FileSortField | null>(null);
  const [sortDirection, setSortDirection] = useState<FileSortDirection>("asc");

  const resourceTypeLabel = config?.fileResourceProperty || "Resource Type";
  const primaryScopeLabel = config?.primaryScopeProperty || "Primary Scope";
  const secondaryScopeLabel = config?.secondaryScopeProperty || "Secondary Scope";

  const resourceTypeOptions = useMemo(() => {
    const values = new Set<string>();
    annotationStates.forEach((state) => {
      if (state.fileResourceType) values.add(state.fileResourceType);
    });
    return ["all", ...Array.from(values).sort()].map((value) => ({
      value,
      label: value === "all" ? "All" : value,
    }));
  }, [annotationStates]);

  const primaryScopeOptions = useMemo(() => {
    const values = new Set<string>();
    annotationStates.forEach((state) => {
      if (state.filePrimaryScope) values.add(state.filePrimaryScope);
    });
    return ["all", ...Array.from(values).sort()].map((value) => ({
      value,
      label: value === "all" ? "All" : value,
    }));
  }, [annotationStates]);

  const secondaryScopeOptions = useMemo(() => {
    const values = new Set<string>();
    annotationStates.forEach((state) => {
      if (state.fileSecondaryScope) values.add(state.fileSecondaryScope);
    });
    return ["all", ...Array.from(values).sort()].map((value) => ({
      value,
      label: value === "all" ? "All" : value,
    }));
  }, [annotationStates]);

  const statusOptions = useMemo(() => {
    const values = new Set<string>();
    annotationStates.forEach((state) => {
      if (state.annotationStatus) values.add(state.annotationStatus);
    });
    return ["all", ...Array.from(values).sort()].map((value) => ({
      value,
      label: value === "all" ? "All" : value,
    }));
  }, [annotationStates]);

  const filteredStates = useMemo(() => {
    const query = searchQuery.trim().toLowerCase();
    const startDate = updatedStart ? new Date(`${updatedStart}T00:00:00`) : null;
    const endDate = updatedEnd ? new Date(`${updatedEnd}T23:59:59.999`) : null;
    return annotationStates.filter((state) => {
      if (resourceTypeFilter !== "all" && state.fileResourceType !== resourceTypeFilter) {
        return false;
      }
      if (primaryScopeFilter !== "all" && state.filePrimaryScope !== primaryScopeFilter) {
        return false;
      }
      if (secondaryScopeFilter !== "all" && state.fileSecondaryScope !== secondaryScopeFilter) {
        return false;
      }
      if (statusFilter !== "all" && state.annotationStatus !== statusFilter) {
        return false;
      }
      if (startDate && state.lastUpdatedTime < startDate) {
        return false;
      }
      if (endDate && state.lastUpdatedTime > endDate) {
        return false;
      }
      if (!query) return true;

      const name = String(state.fileName || "").toLowerCase();
      const externalId = String(
        state.linkedFile?.externalId || state.externalId || ""
      ).toLowerCase();
      return name.includes(query) || externalId.includes(query);
    });
  }, [
    annotationStates,
    searchQuery,
    resourceTypeFilter,
    primaryScopeFilter,
    secondaryScopeFilter,
    statusFilter,
    updatedStart,
    updatedEnd,
  ]);

  const sortedStates = useMemo(() => {
    if (!sortField) return filteredStates;
    const result = [...filteredStates];

    const compareString = (left?: string, right?: string) =>
      String(left || "").localeCompare(String(right || ""));
    const compareNumber = (left?: number, right?: number) =>
      (left ?? -1) - (right ?? -1);

    result.sort((a, b) => {
      let comparison = 0;
      switch (sortField) {
        case "file":
          comparison = compareString(
            a.fileName || a.linkedFile?.externalId || a.externalId,
            b.fileName || b.linkedFile?.externalId || b.externalId
          );
          break;
        case "updated":
          comparison = a.lastUpdatedTime.getTime() - b.lastUpdatedTime.getTime();
          break;
        case "status":
          comparison = compareString(a.annotationStatus, b.annotationStatus);
          break;
        case "resourceType":
          comparison = compareString(a.fileResourceType, b.fileResourceType);
          break;
        case "primaryScope":
          comparison = compareString(a.filePrimaryScope, b.filePrimaryScope);
          break;
        case "secondaryScope":
          comparison = compareString(a.fileSecondaryScope, b.fileSecondaryScope);
          break;
        case "sourceId":
          comparison = compareString(a.fileSourceId, b.fileSourceId);
          break;
        case "mimeType":
          comparison = compareString(a.fileMimeType, b.fileMimeType);
          break;
        case "pages":
          comparison = compareNumber(a.pageCount, b.pageCount);
          break;
        case "annotated":
          comparison = compareNumber(a.annotatedPageCount, b.annotatedPageCount);
          break;
      }

      return sortDirection === "desc" ? -comparison : comparison;
    });

    return result;
  }, [filteredStates, sortField, sortDirection]);

  const handleSortChange = (field: FileSortField) => {
    if (sortField !== field) {
      setSortField(field);
      setSortDirection("asc");
      return;
    }

    if (sortDirection === "asc") {
      setSortDirection("desc");
      return;
    }

    setSortField(null);
    setSortDirection("asc");
  };

  return (
    <div className="space-y-5">
      {/* File Table */}
      <div>
        <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-3">
          File-Centric Debugging
        </h3>
        <Card className="mb-3">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-semibold flex items-center gap-2">
              <Filter className="h-4 w-4 text-muted-foreground" />
              Filters
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex flex-wrap gap-3">
              <div className="space-y-1.5 flex-1 min-w-[200px] max-w-[320px]">
                <label className="text-xs text-muted-foreground font-medium">Search Files</label>
                <div className="relative">
                  <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                  <Input
                    placeholder="Search by file name or ID..."
                    value={searchQuery}
                    onChange={(event) => setSearchQuery(event.target.value)}
                    className="h-8 text-xs pl-8"
                  />
                </div>
              </div>
              <div className="space-y-1.5 min-w-[160px]">
                <label className="text-xs text-muted-foreground font-medium">Status</label>
                <Select value={statusFilter} onValueChange={setStatusFilter}>
                  <SelectTrigger className="h-8 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {statusOptions.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1.5 min-w-[160px]">
                <label className="text-xs text-muted-foreground font-medium">Updated Start</label>
                <Input
                  type="date"
                  value={updatedStart}
                  onChange={(event) => setUpdatedStart(event.target.value)}
                  className="h-8 text-xs"
                />
              </div>
              <div className="space-y-1.5 min-w-[160px]">
                <label className="text-xs text-muted-foreground font-medium">Updated End</label>
                <Input
                  type="date"
                  value={updatedEnd}
                  onChange={(event) => setUpdatedEnd(event.target.value)}
                  className="h-8 text-xs"
                />
              </div>
              <div className="space-y-1.5 min-w-[160px]">
                <label className="text-xs text-muted-foreground font-medium">
                  {resourceTypeLabel}
                </label>
                <Select value={resourceTypeFilter} onValueChange={setResourceTypeFilter}>
                  <SelectTrigger className="h-8 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {resourceTypeOptions.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1.5 min-w-[160px]">
                <label className="text-xs text-muted-foreground font-medium">
                  {primaryScopeLabel}
                </label>
                <Select value={primaryScopeFilter} onValueChange={setPrimaryScopeFilter}>
                  <SelectTrigger className="h-8 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {primaryScopeOptions.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1.5 min-w-[160px]">
                <label className="text-xs text-muted-foreground font-medium">
                  {secondaryScopeLabel}
                </label>
                <Select value={secondaryScopeFilter} onValueChange={setSecondaryScopeFilter}>
                  <SelectTrigger className="h-8 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {secondaryScopeOptions.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
          </CardContent>
        </Card>
        <FileTable
          data={sortedStates}
          onSelectFile={setSelectedFile}
          selectedFile={selectedFile}
          config={config}
          sortField={sortField}
          sortDirection={sortDirection}
          onSortChange={handleSortChange}
        />
      </div>

      {/* Function Log Viewer */}
      <FunctionLogViewer file={selectedFile} />
    </div>
  );
}







