import { memo, useMemo, useRef, useState, useEffect } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useAppSdk } from "@/providers/AppSdkProvider";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Button } from "@/shared/components/ui/button";
import { Badge } from "@/shared/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/shared/components/ui/select";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  ChevronUp,
  Info,
  Loader2,
  FileText,
  Files,
  Download,
  Clock,
  CheckCircle2,
  XCircle,
  Activity,
} from "lucide-react";
import type { AnnotationState, PipelineConfig, PipelineRun } from "@/shared/utils/types";
import { CallerType } from "@/shared/utils/constants";
import { WarningCallout } from "@/shared/components/WarningCallout";
import {
  usePipelineRuns,
  useFunctionLogs,
  calculateRunMetrics,
} from "@/pages/PipelineHealth/hooks/usePipelineRuns";




interface RunHistoryTabProps {
  annotationStates: AnnotationState[];
  config: PipelineConfig | null;
  pipelineId?: string | null;
}

interface RunItemProps {
  run: PipelineRun;
  annotationStates: AnnotationState[];
  config: PipelineConfig | null;
}

type RunSortField = "createdTime" | "status" | "caller" | "total" | "success" | "failed";
type RunSortDirection = "asc" | "desc";

function RunFunctionLogViewer({
  functionId,
  callId,
  runId,
}: {
  functionId: string | undefined;
  callId: string | undefined;
  runId: string;
}) {
  const { sdk } = useAppSdk();
  const [isExpanded, setIsExpanded] = useState(false);
  const [shouldLoad, setShouldLoad] = useState(false);

  const { data: logs, isLoading, error } = useFunctionLogs(
    shouldLoad ? sdk : null,
    functionId ?? null,
    callId ?? null
  );

  const handleLoadLogs = () => {
    setShouldLoad(true);
    setIsExpanded(true);
  };

  const handleDownload = () => {
    if (!logs) return;
    const blob = new Blob([logs], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `run_${runId}_log.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  if (!functionId || !callId) {
    return (
      <span className="text-[10px] text-muted-foreground italic">
        No function info
      </span>
    );
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <Button
          variant="outline"
          size="xs"
          onClick={() => (shouldLoad ? setIsExpanded(!isExpanded) : handleLoadLogs())}
        >
          <FileText className="h-3 w-3" />
          {shouldLoad ? (isExpanded ? "Hide" : "Show") : "Load"} Logs
          {shouldLoad &&
            (isExpanded ? (
              <ChevronUp className="h-3 w-3" />
            ) : (
              <ChevronDown className="h-3 w-3" />
            ))}
        </Button>
        {logs && (
          <Button variant="ghost" size="xs" onClick={handleDownload}>
            <Download className="h-3 w-3" />
          </Button>
        )}
      </div>

      {isExpanded && (
        <div className="mt-2">
          {isLoading ? (
            <div className="flex items-center gap-2">
              <Loader2 className="h-3 w-3 animate-spin" />
              Loading...
            </div>
          ) : error ? (
            <WarningCallout
              title="Failed to load logs"
              description="The function may not exist or you may not have permission."
            />
          ) : logs ? (
            <div className="rounded-lg border bg-slate-950 p-3 overflow-x-auto">
              <pre className="text-[10px] text-emerald-400 font-mono whitespace-pre-wrap max-h-48 overflow-y-auto">
                {logs}
              </pre>
            </div>
          ) : (
            <div className="text-xs text-muted-foreground p-2 bg-muted rounded-lg">
              No logs found
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function RunFilesViewer({
  run,
  annotationStates,
}: {
  run: PipelineRun;
  annotationStates: AnnotationState[];
  config: PipelineConfig | null;
}) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [files, setFiles] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const { sdk } = useAppSdk();
  const { data: logs } = useFunctionLogs(
    isExpanded ? sdk : null,
    run.functionId ?? null,
    run.callId ?? null
  );

    const extractExternalIdsFromLog = (logText: string | undefined): string[] => {
      if (!logText) return [];
      const regex = /Processing file NodeId\([^)]*external_id=['"]([^'"\)]+)['"]/g;
      const ids = [];
      let match;
      while ((match = regex.exec(logText)) !== null) {
        ids.push(match[1]);
      }
      return ids;
    };

  const handleToggleFiles = () => {
    if (isExpanded) {
      setIsExpanded(false);
      return;
    }
    if (!run.callId) {
      setIsExpanded(!isExpanded);
      return;
    }
    setIsLoading(true);
    setIsExpanded(true);
  };

  useEffect(() => {
    if (!isExpanded) return;
    const matchingFiles: string[] = [];
    try {
      const callIdNum = parseInt(run.callId ?? '', 10);
      for (const state of annotationStates) {
        let matchingCallId: number | undefined;
        if (run.caller === CallerType.FINALIZE) {
          matchingCallId = state.finalizeFunctionCallId;
        } else if (run.caller === CallerType.LAUNCH) {
          matchingCallId = state.launchFunctionCallId;
        } else if (run.caller === CallerType.PREPARE) {
          matchingCallId = state.prepareFunctionCallId;
        } else if (run.caller === CallerType.PROMOTE) {
          matchingCallId = state.promoteFunctionCallId;
        }
        if (matchingCallId === callIdNum) {
          const fileId =
            state.linkedFile?.externalId || state.fileName || state.externalId;
          if (fileId && !matchingFiles.includes(fileId)) {
            matchingFiles.push(fileId);
          }
        }
      }
      if (logs) {
        const idsFromLog = extractExternalIdsFromLog(logs);
        idsFromLog.forEach((id) => {
          if (!matchingFiles.includes(id)) matchingFiles.push(id);
        });
      }
      setFiles(matchingFiles);
    } catch (error) {
      console.error("Failed to load files:", error);
      setFiles([]);
    } finally {
      setIsLoading(false);
    }
  }, [logs, isExpanded, annotationStates, run.callId, run.caller]);

  if (!run.callId) {
    return null;
  }

  return (
    <div className="space-y-2">
      <Button
        variant="outline"
        size="xs"
        onClick={handleToggleFiles}
      >
        <Files className="h-3 w-3" />
        {isExpanded ? "Hide" : "View"} Files
        {isExpanded ? (
          <ChevronUp className="h-3 w-3" />
        ) : (
          <ChevronDown className="h-3 w-3" />
        )}
      </Button>

      {isExpanded && (
        <div className="mt-2">
          {isLoading ? (
            <div className="flex items-center gap-2">
              <Loader2 className="h-3 w-3 animate-spin" />
              Loading...
            </div>
          ) : files.length > 0 ? (
            <div className="rounded-lg border bg-muted/30 p-3 max-h-40 overflow-y-auto">
              <p className="text-[10px] text-muted-foreground italic">
                {files.length} file(s):
              </p>
              <div className="space-y-0.5 font-mono text-[10px]">
                {files.map((file, idx) => (
                  <div key={idx} className="truncate">
                    {file}
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div className="text-xs text-muted-foreground p-2 bg-muted rounded-lg">
              No associated files found
            </div>
          )}
        </div>
      )}
    </div>
  );
}

const RunItem = memo(function RunItem({ run, annotationStates, config }: RunItemProps) {
  const statusVariant = run.status.toLowerCase() === "success" 
    ? "success" 
    : run.status.toLowerCase() === "seen" 
    ? "secondary" 
    : "destructive";

  return (
    <div className="p-4">
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2">
          <Badge variant={statusVariant} className="text-[10px]">
            {run.status}
          </Badge>
          {run.caller && (
            <Badge variant="outline" className="text-[10px]">
              {run.caller}
            </Badge>
          )}
          <span className="text-xs text-muted-foreground p-2 bg-muted rounded-lg">
            {new Date(run.createdTime).toLocaleString()}
          </span>
        </div>
        {run.functionId && (
          <span className="text-[10px] text-muted-foreground italic">
            fn:{run.functionId} / call:{run.callId}
          </span>
        )}
      </div>

      {/* Stats */}
      {run.total !== undefined && (
        <div className="flex gap-4 text-xs">
          <div className="flex items-center gap-1">
            <Activity className="h-3 w-3" />
            <span className="text-muted-foreground">Total:</span>
            <span className="font-medium">{run.total}</span>
          </div>
          {run.success !== undefined && (
            <div className="flex items-center gap-1 text-emerald-600 dark:text-emerald-400">
              <CheckCircle2 className="h-3 w-3" />
              <span>{run.success}</span>
            </div>
          )}
          {run.failed !== undefined && run.failed > 0 && (
            <div className="flex items-center gap-1 text-red-600 dark:text-red-400">
              <XCircle className="h-3 w-3" />
              <span>{run.failed}</span>
            </div>
          )}
        </div>
      )}

      {/* Message */}
      {run.message && (
        <pre className="text-[10px] bg-muted/50 p-2 rounded-lg overflow-x-auto whitespace-pre-wrap max-h-20 font-mono">
          {run.message}
        </pre>
      )}

      {/* Actions */}
      <div className="flex flex-wrap gap-2 pt-2 border-t border-border/30">
        <RunFunctionLogViewer
          functionId={run.functionId}
          callId={run.callId}
          runId={run.id}
        />
        <RunFilesViewer
          run={run}
          annotationStates={annotationStates}
          config={config}
        />
      </div>
    </div>
  );
});

export function RunHistoryTab({
  annotationStates,
  config,
  pipelineId,
}: RunHistoryTabProps) {
  const { sdk } = useAppSdk();
  const { data: runs, isLoading } = usePipelineRuns(sdk, pipelineId ?? null);

  const [timeWindow, setTimeWindow] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [callerFilter, setCallerFilter] = useState("all");
  const [page, setPage] = useState(0);
  const [sortField, setSortField] = useState<RunSortField | null>(null);
  const [sortDirection, setSortDirection] = useState<RunSortDirection>("asc");
  const itemsPerPage = 10;

  const runsData = runs || [];

  const runMetrics = useMemo(() => {
    return calculateRunMetrics(runsData);
  }, [runsData]);

  const timeFilteredRuns = useMemo(() => {
    if (timeWindow === "all") return runsData;

    const now = Date.now();
    const cutoff = {
      "24h": now - 24 * 60 * 60 * 1000,
      "7d": now - 7 * 24 * 60 * 60 * 1000,
      "30d": now - 30 * 24 * 60 * 60 * 1000,
    }[timeWindow];

    if (!cutoff) return runsData;

    return runsData.filter((run) => run.createdTime >= cutoff);
  }, [runsData, timeWindow]);

  const filteredRuns = useMemo(() => {
    return timeFilteredRuns.filter((run) => {
      if (statusFilter !== "all" && run.status.toLowerCase() !== statusFilter)
        return false;
      if (callerFilter !== "all" && run.caller !== callerFilter) return false;
      return true;
    });
  }, [timeFilteredRuns, statusFilter, callerFilter]);

  const sortedRuns = useMemo(() => {
    if (!sortField) return filteredRuns;

    const result = [...filteredRuns];
    result.sort((a, b) => {
      let comparison = 0;
      switch (sortField) {
        case "createdTime":
          comparison = a.createdTime - b.createdTime;
          break;
        case "status":
          comparison = (a.status || "").localeCompare(b.status || "");
          break;
        case "caller":
          comparison = (a.caller || "").localeCompare(b.caller || "");
          break;
        case "total":
          comparison = (a.total ?? -1) - (b.total ?? -1);
          break;
        case "success":
          comparison = (a.success ?? -1) - (b.success ?? -1);
          break;
        case "failed":
          comparison = (a.failed ?? -1) - (b.failed ?? -1);
          break;
      }

      return sortDirection === "desc" ? -comparison : comparison;
    });

    return result;
  }, [filteredRuns, sortField, sortDirection]);

  const paginatedRuns = useMemo(() => {
    const start = page * itemsPerPage;
    return sortedRuns.slice(start, start + itemsPerPage);
  }, [sortedRuns, page]);

  const totalPages = Math.ceil(sortedRuns.length / itemsPerPage);

  const handleFilterChange = (
    setter: React.Dispatch<React.SetStateAction<string>>
  ) => {
    return (value: string) => {
      setter(value);
      setPage(0);
    };
  };

  const toggleSort = (field: RunSortField) => {
    if (sortField !== field) {
      setSortField(field);
      setSortDirection("asc");
      setPage(0);
      return;
    }

    if (sortDirection === "asc") {
      setSortDirection("desc");
      setPage(0);
      return;
    }

    setSortField(null);
    setSortDirection("asc");
    setPage(0);
  };

  const renderSortIcon = (field: RunSortField) => {
    if (sortField !== field) return <ArrowUpDown className="h-3 w-3" />;
    return sortDirection === "asc" ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />;
  };

  const runListRef = useRef<HTMLDivElement | null>(null);
  const runRowVirtualizer = useVirtualizer({
    count: paginatedRuns.length,
    getScrollElement: () => runListRef.current,
    estimateSize: () => 220,
    overscan: 4,
    getItemKey: (index) => {
      const run = paginatedRuns[index];
      return run ? run.id : `${index}`;
    },
  });
  const runVirtualRows = runRowVirtualizer.getVirtualItems();

  return (
    <div className="space-y-5">
      {/* Run Summary Cards */}
      <div>
        <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-3 flex items-center gap-2">
          Run Summary
          {isLoading && <Loader2 className="h-3 w-3 animate-spin" />}
        </h3>
        <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-4">
          {Object.entries(runMetrics).map(([caller, metrics]) => (
            <Card key={caller} className="group">
              <CardContent className="p-4">
                <div className="flex items-center gap-2">
                  <div className="p-1.5 rounded-md bg-primary/10 group-hover:bg-primary/20 transition-colors">
                    <Clock className="h-3.5 w-3.5 text-primary" />
                  </div>
                  <span className="text-xs font-semibold">{caller}</span>
                </div>
                <div className="grid grid-cols-3 gap-2 text-center">
                  <div>
                    <p className="text-lg font-bold">{metrics.processed.toLocaleString()}</p>
                    <p className="text-[9px] uppercase tracking-wide text-muted-foreground">Files</p>
                  </div>
                  <div>
                    <p className="text-lg font-bold text-emerald-600">{metrics.success.toLocaleString()}</p>
                    <p className="text-[9px] uppercase tracking-wide text-muted-foreground">Success</p>
                  </div>
                  <div>
                    <p className="text-lg font-bold text-emerald-600">{metrics.failed.toLocaleString()}</p>
                    <p className="text-[9px] uppercase tracking-wide text-muted-foreground">Failed</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>

      {/* Detailed Run History */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm">
            Run History
            <Badge variant="secondary" className="text-[10px]">{filteredRuns.length}</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Filters */}
          <div className="flex flex-wrap gap-3">
            <Select
              value={timeWindow}
              onValueChange={handleFilterChange(setTimeWindow)}
            >
              <SelectTrigger className="w-32 h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Time</SelectItem>
                <SelectItem value="24h">Last 24h</SelectItem>
                <SelectItem value="7d">Last 7d</SelectItem>
                <SelectItem value="30d">Last 30d</SelectItem>
              </SelectContent>
            </Select>
            <Select
              value={statusFilter}
              onValueChange={handleFilterChange(setStatusFilter)}
            >
              <SelectTrigger className="w-28 h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Status</SelectItem>
                <SelectItem value="success">Success</SelectItem>
                <SelectItem value="failure">Failure</SelectItem>
                <SelectItem value="seen">Seen</SelectItem>
              </SelectContent>
            </Select>
            <Select
              value={callerFilter}
              onValueChange={handleFilterChange(setCallerFilter)}
            >
              <SelectTrigger className="w-28 h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Types</SelectItem>
                {Object.values(CallerType).map((type) => (
                  <SelectItem key={type} value={type}>
                    {type}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="flex flex-wrap gap-2 text-[10px]">
            <Button variant="outline" size="xs" onClick={() => toggleSort("createdTime")}>
              Created
              {renderSortIcon("createdTime")}
            </Button>
            <Button variant="outline" size="xs" onClick={() => toggleSort("status")}>
              Status
              {renderSortIcon("status")}
            </Button>
            <Button variant="outline" size="xs" onClick={() => toggleSort("caller")}>
              Type
              {renderSortIcon("caller")}
            </Button>
            <Button variant="outline" size="xs" onClick={() => toggleSort("total")}>
              Total
              {renderSortIcon("total")}
            </Button>
            <Button variant="outline" size="xs" onClick={() => toggleSort("success")}>
              Success
              {renderSortIcon("success")}
            </Button>
            <Button variant="outline" size="xs" onClick={() => toggleSort("failed")}>
              Failed
              {renderSortIcon("failed")}
            </Button>
          </div>

          {/* Run List */}
          {isLoading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="h-6 w-6 animate-spin text-primary" />
            </div>
          ) : paginatedRuns.length === 0 ? (
            <div className="flex flex-col items-center py-12 text-muted-foreground">
              <Info className="h-8 w-8 mb-2 opacity-30" />
              <p className="text-sm">No runs found</p>
              {runsData.length > 0 && (
                <p className="text-xs mt-1">Try adjusting filters</p>
              )}
            </div>
          ) : (
            <div
              ref={runListRef}
              className="max-h-[520px] overflow-auto"
            >
              <div
                style={{
                  height: runRowVirtualizer.getTotalSize(),
                  position: "relative",
                }}
              >
                {runVirtualRows.map((virtualRow) => {
                  const run = paginatedRuns[virtualRow.index];
                  if (!run) return null;
                  return (
                    <div
                      key={virtualRow.key}
                      ref={runRowVirtualizer.measureElement}
                      data-index={virtualRow.index}
                      style={{
                        position: "absolute",
                        top: 0,
                        left: 0,
                        right: 0,
                        transform: `translateY(${virtualRow.start}px)`,
                        paddingBottom: "12px",
                      }}
                    >
                      <Card className="overflow-hidden">
                        <RunItem
                          run={run}
                          annotationStates={annotationStates}
                          config={config}
                        />
                      </Card>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between pt-3 border-t">
              <Button
                variant="ghost"
                size="xs"
                onClick={() => setPage((p) => Math.max(0, p - 1))}
                disabled={page === 0}
              >
                <ChevronLeft className="h-3.5 w-3.5" />
                Prev
              </Button>
              <span className="text-xs text-muted-foreground p-2 bg-muted rounded-lg">
                {page + 1} / {totalPages}
              </span>
              <Button
                variant="ghost"
                size="xs"
                onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
                disabled={page >= totalPages - 1}
              >
                Next
                <ChevronRight className="h-3.5 w-3.5" />
              </Button>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}



