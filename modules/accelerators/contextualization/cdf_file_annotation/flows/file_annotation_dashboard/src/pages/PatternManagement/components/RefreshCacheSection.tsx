import { memo, useEffect, useMemo, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { CachePreviewSummary } from "@/shared/utils/patternManagement";
import { Badge } from "@/shared/components/ui/badge";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Progress } from "@/shared/components/ui/progress";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/shared/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/shared/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/shared/components/ui/tooltip";
import { AlertCircle, Eye, Loader2, RefreshCw, Save, Trash2 } from "lucide-react";

interface ScopePreviewRow {
  patternScope: string;
  primaryScopeValue: string;
  secondaryScopeValue?: string;
  fileEntities: number;
  discoverFileEntities?: number;
  previewFileEntities?: number;
  assetEntities: number;
  queryLabel: string;
}

interface RefreshCacheSectionProps {
  finalPreview: CachePreviewSummary[];
  finalRowsCount: number;
  handleDiscoverScopes: () => void;
  handleGeneratePreview: () => void;
  handleWriteCacheRows: () => void;
  handleClearCachePreview: () => void;
  isRefreshingScopes: boolean;
  isGeneratingPreview: boolean;
  isWritingCache: boolean;
  refreshMessage: string | null;
  cacheStatus: string | null;
  cacheProgress: number;
  cacheProgressKnown: boolean;
  cacheLogs: string[];
  scopePreview: ScopePreviewRow[];
  isLocalMockMode: boolean;
}

const PAGE_SIZE_OPTIONS = ["25", "50", "100", "200"];

interface CacheRowProps {
  row: ScopePreviewRow & {
    manualPatternSamples?: number;
    autoPatternSamples?: number;
    combinedPatternSamples?: number;
    lastUpdate?: string;
  };
  hasFinalPreview: boolean;
  rowRef: (element: HTMLTableRowElement | null) => void;
  dataIndex: number;
}

const CacheRow = memo(function CacheRow({ row, hasFinalPreview, rowRef, dataIndex }: CacheRowProps) {
  return (
    <TableRow ref={rowRef} data-index={dataIndex}>
      <TableCell className="text-[10px]">
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <span className="font-mono underline underline-offset-2 decoration-dotted cursor-help">
                {row.patternScope}
              </span>
            </TooltipTrigger>
            <TooltipContent>
              <div className="space-y-0.5">
                <div>Primary: {row.primaryScopeValue || "-"}</div>
                <div>Secondary: {row.secondaryScopeValue || "-"}</div>
              </div>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </TableCell>
      <TableCell className="text-[10px]">
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <span className="underline underline-offset-2 decoration-dotted cursor-help">
                {row.fileEntities}
              </span>
            </TooltipTrigger>
            <TooltipContent>
              <div className="space-y-0.5">
                <div>Annotatable Files: {row.discoverFileEntities ?? "-"}</div>
                <div>Entity Files: {row.previewFileEntities ?? row.fileEntities}</div>
                <div>
                  Delta: {row.discoverFileEntities != null && row.previewFileEntities != null
                    ? Math.abs(row.previewFileEntities - row.discoverFileEntities)
                    : "-"}
                </div>
              </div>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </TableCell>
      {hasFinalPreview && (
        <>
          <TableCell className="text-[10px]">{row.assetEntities}</TableCell>
          <TableCell className="text-[10px]">{row.manualPatternSamples ?? 0}</TableCell>
          <TableCell className="text-[10px]">{row.autoPatternSamples ?? 0}</TableCell>
          <TableCell className="text-[10px]">{row.combinedPatternSamples ?? 0}</TableCell>
          <TableCell className="text-[10px]">{row.lastUpdate || "-"}</TableCell>
        </>
      )}
    </TableRow>
  );
});

export function RefreshCacheSection({
  finalPreview,
  finalRowsCount,
  handleDiscoverScopes,
  handleGeneratePreview,
  handleWriteCacheRows,
  handleClearCachePreview,
  isRefreshingScopes,
  isGeneratingPreview,
  isWritingCache,
  refreshMessage,
  cacheStatus,
  cacheProgress,
  cacheProgressKnown,
  cacheLogs,
  scopePreview,
  isLocalMockMode,
}: RefreshCacheSectionProps) {
  const hasFinalPreview = finalPreview.length > 0;
  const scopeMap = useMemo(
    () => new Map(scopePreview.map((row) => [row.patternScope, row])),
    [scopePreview]
  );
  const [cachePageSize, setCachePageSize] = useState("50");
  const [cacheCurrentPage, setCacheCurrentPage] = useState(1);
  const tableRef = useRef<HTMLDivElement | null>(null);

  const mergedRows = useMemo(() => {
    const parseScope = (patternScope: string) => {
      if (!patternScope) return { primary: "", secondary: undefined as string | undefined };
      if (patternScope.includes("_")) {
        const [primary, ...rest] = patternScope.split("_");
        return { primary, secondary: rest.join("_") || undefined };
      }
      return { primary: patternScope, secondary: undefined };
    };

    if (hasFinalPreview) {
      return finalPreview.map((row) => {
        const scopeRow = scopeMap.get(row.patternScope);
        const scopeParts = scopeRow
          ? { primary: scopeRow.primaryScopeValue, secondary: scopeRow.secondaryScopeValue }
          : parseScope(row.patternScope);
        const discoverFileEntities = scopeRow?.fileEntities;
        const previewFileEntities = row.fileEntities ?? scopeRow?.fileEntities ?? 0;
        return {
          patternScope: row.patternScope,
          primaryScopeValue: scopeParts.primary,
          secondaryScopeValue: scopeParts.secondary,
          fileEntities: previewFileEntities,
          discoverFileEntities,
          previewFileEntities,
          assetEntities: row.assetEntities ?? scopeRow?.assetEntities ?? 0,
          queryLabel: scopeRow?.queryLabel ?? "",
          manualPatternSamples: row.manualPatternSamples,
          autoPatternSamples: row.assetPatternSamples + row.filePatternSamples,
          combinedPatternSamples: row.combinedPatternSamples,
          lastUpdate: row.lastUpdate,
        };
      });
    }

    return scopePreview.map((row) => ({
      patternScope: row.patternScope,
      primaryScopeValue: row.primaryScopeValue,
      secondaryScopeValue: row.secondaryScopeValue,
      fileEntities: row.fileEntities,
      discoverFileEntities: row.fileEntities,
      previewFileEntities: undefined,
      assetEntities: row.assetEntities,
      queryLabel: row.queryLabel,
      manualPatternSamples: undefined,
      autoPatternSamples: undefined,
      combinedPatternSamples: undefined,
      lastUpdate: undefined,
    }));
  }, [finalPreview, hasFinalPreview, scopeMap, scopePreview]);

  const cachePageSizeValue = useMemo(() => Number.parseInt(cachePageSize, 10), [cachePageSize]);
  const cacheTotalPages = useMemo(() => {
    return Math.max(1, Math.ceil(mergedRows.length / cachePageSizeValue));
  }, [mergedRows.length, cachePageSizeValue]);

  useEffect(() => {
    if (cacheCurrentPage > cacheTotalPages) {
      setCacheCurrentPage(cacheTotalPages);
    }
  }, [cacheCurrentPage, cacheTotalPages]);

  useEffect(() => {
    setCacheCurrentPage(1);
  }, [cachePageSize, mergedRows.length]);

  const pagedMergedRows = useMemo(() => {
    const startIndex = (cacheCurrentPage - 1) * cachePageSizeValue;
    return mergedRows.slice(startIndex, startIndex + cachePageSizeValue);
  }, [mergedRows, cacheCurrentPage, cachePageSizeValue]);

  const rowVirtualizer = useVirtualizer({
    count: pagedMergedRows.length,
    getScrollElement: () => tableRef.current,
    estimateSize: () => 40,
    overscan: 6,
    getItemKey: (index) => pagedMergedRows[index]?.patternScope ?? `cache-${index}`,
  });
  const virtualRows = rowVirtualizer.getVirtualItems();
  const topSpacer = virtualRows.length > 0 ? virtualRows[0].start : 0;
  const bottomSpacer =
    rowVirtualizer.getTotalSize() -
    (virtualRows.length > 0 ? virtualRows[virtualRows.length - 1].end : 0);

  const cacheRangeLabel = useMemo(() => {
    if (mergedRows.length === 0) return "0 of 0";
    const startIndex = (cacheCurrentPage - 1) * cachePageSizeValue + 1;
    const endIndex = Math.min(cacheCurrentPage * cachePageSizeValue, mergedRows.length);
    return `${startIndex}-${endIndex} of ${mergedRows.length}`;
  }, [mergedRows.length, cacheCurrentPage, cachePageSizeValue]);

  const progressIndicatorClass = cacheProgress >= 100
    ? "bg-emerald-500 from-emerald-500 to-emerald-500"
    : undefined;

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <RefreshCw className="h-4 w-4 text-muted-foreground" />
            Refresh Annotation Entities Cache
          </CardTitle>
          <Badge variant="secondary" className="text-[10px]">
            {mergedRows.length}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" size="xs" onClick={handleDiscoverScopes} disabled={isRefreshingScopes}>
            {isRefreshingScopes ? <Loader2 className="h-3 w-3 animate-spin" /> : <RefreshCw className="h-3 w-3" />}
            Discover Scopes
          </Button>
          <Button
            variant="outline"
            size="xs"
            onClick={handleGeneratePreview}
            disabled={scopePreview.length === 0 || isGeneratingPreview}
          >
            {isGeneratingPreview ? <Loader2 className="h-3 w-3 animate-spin" /> : <Eye className="h-3 w-3" />}
            Generate Preview
          </Button>
          <Button size="xs" onClick={handleWriteCacheRows} disabled={finalRowsCount === 0 || isWritingCache}>
            {isWritingCache ? <Loader2 className="h-3 w-3 animate-spin" /> : <Save className="h-3 w-3" />}
            Write Cache Rows
          </Button>
          <Button
            variant="ghost"
            size="xs"
            onClick={handleClearCachePreview}
            disabled={mergedRows.length === 0}
          >
            <Trash2 className="h-3 w-3" />
            Clear
          </Button>
        </div>

        {refreshMessage && <div className="text-xs text-muted-foreground">{refreshMessage}</div>}

        {cacheStatus && (
          <div className="space-y-2">
            {cacheProgressKnown && (
              <div className="space-y-1">
                <Progress
                  value={cacheProgress}
                  className="h-2"
                  indicatorClassName={progressIndicatorClass}
                />
                <div className="text-[10px] text-muted-foreground">
                  {Math.round(cacheProgress)}%
                </div>
              </div>
            )}
            <div className="text-[10px]">{cacheStatus}</div>
            {cacheLogs.length > 1 && (
              <div className="rounded-md border bg-muted/40 p-2 text-[10px] text-muted-foreground space-y-1">
                {cacheLogs.map((log, idx) => (
                  <div key={`${log}-${idx}`}>{log}</div>
                ))}
              </div>
            )}
          </div>
        )}

        {mergedRows.length > 0 && (
          <div
            ref={tableRef}
            className="rounded-lg border overflow-hidden"
            style={{ maxHeight: "240px", overflow: "auto" }}
          >
            <Table>
              <TableHeader className="sticky top-0 bg-muted/80 backdrop-blur-sm">
                <TableRow>
                  <TableHead className="min-w-[140px]">Key</TableHead>
                  <TableHead className="w-16">Files</TableHead>
                  {hasFinalPreview && (
                    <>
                      <TableHead className="w-16">Assets</TableHead>
                      <TableHead className="w-16">Manual</TableHead>
                      <TableHead className="w-16">Auto</TableHead>
                      <TableHead className="w-20">Combined</TableHead>
                      <TableHead className="w-24">Updated</TableHead>
                    </>
                  )}
                </TableRow>
              </TableHeader>
              <TableBody>
                {topSpacer > 0 && (
                  <TableRow aria-hidden>
                    <TableCell colSpan={hasFinalPreview ? 7 : 2} style={{ height: topSpacer }} />
                  </TableRow>
                )}
                {virtualRows.map((virtualRow) => {
                  const row = pagedMergedRows[virtualRow.index];
                  if (!row) return null;
                  return (
                    <CacheRow
                      key={virtualRow.key}
                      row={row}
                      hasFinalPreview={hasFinalPreview}
                      rowRef={rowVirtualizer.measureElement}
                      dataIndex={virtualRow.index}
                    />
                  );
                })}
                {bottomSpacer > 0 && (
                  <TableRow aria-hidden>
                    <TableCell colSpan={hasFinalPreview ? 7 : 2} style={{ height: bottomSpacer }} />
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        )}

        {mergedRows.length > 0 && (
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="text-[10px]">{cacheRangeLabel} rows</p>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                className="h-7 px-2 text-[10px]"
                onClick={() => setCacheCurrentPage((prev) => Math.max(1, prev - 1))}
                disabled={cacheCurrentPage === 1}
              >
                Prev
              </Button>
              <span className="text-[10px]">
                Page {cacheCurrentPage} of {cacheTotalPages}
              </span>
              <Button
                variant="outline"
                size="sm"
                className="h-7 px-2 text-[10px]"
                onClick={() => setCacheCurrentPage((prev) => Math.min(cacheTotalPages, prev + 1))}
                disabled={cacheCurrentPage === cacheTotalPages}
              >
                Next
              </Button>
              <Select value={cachePageSize} onValueChange={setCachePageSize}>
                <SelectTrigger className="h-7 text-[10px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {PAGE_SIZE_OPTIONS.map((size) => (
                    <SelectItem key={size} value={size}>
                      {size} / page
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        )}

        {isLocalMockMode && (
          <div className="flex items-start gap-2 text-xs text-muted-foreground">
            <AlertCircle className="h-3.5 w-3.5" />
            Cache refresh operations are disabled in mock mode.
          </div>
        )}
      </CardContent>
    </Card>
  );
}
