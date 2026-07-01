import { ArrowDown, ArrowUp, ArrowUpDown, Check, Copy, Info, Loader2, Search, X } from "lucide-react";
import { memo, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { Badge } from "@/shared/components/ui/badge";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Input } from "@/shared/components/ui/input";
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
import { useClickOutside } from "@/shared/hooks/useClickOutside";
import { useClipboard } from "@/shared/hooks/useClipboard";
import { useSortedPagination } from "@/shared/hooks/useSortedPagination";

interface TagEntryData {
  count: number;
  files: Set<string>;
  resourceType?: string;
  normalizedStatus?: string;
}



interface AnnotationTagsCardProps {
  title: string;
  icon: ReactNode;
  badgeVariant: "success" | "warning";
  totalCount: number;
  description: string;
  searchValue: string;
  onSearchChange: (value: string) => void;
  hasFileSelection: boolean;
  isLoading: boolean;
  loadingText: string;
  emptyText: string;
  entries: Array<[string, TagEntryData]>;
  renderStatusBadge: (status?: string) => ReactNode;
  resolveFileInfo: (fileExternalId: string) => { fileName?: string; fileExternalId: string };
  extraNoSelectionContent?: ReactNode;
  viewAll: boolean;
  setViewAll: (v: boolean) => void;
}

type TagSortField = "tag" | "type" | "status" | "count" | "files";
interface TagRowProps {
  tag: string;
  data: TagEntryData;
  renderStatusBadge: (status?: string) => React.ReactNode;
  rowRef: (element: HTMLTableRowElement | null) => void;
  dataIndex: number;
  isFilesOpen: boolean;
  onFilesToggle: (tag: string) => void;
  onFilesClose: () => void;
  resolveFileInfo: (fileExternalId: string) => { fileName?: string; fileExternalId: string };
}

const TagRow = memo(function TagRow({
  tag,
  data,
  renderStatusBadge,
  rowRef,
  dataIndex,
  isFilesOpen,
  onFilesToggle,
  onFilesClose,
  resolveFileInfo,
}: TagRowProps) {
  const filesPanelRef = useRef<HTMLDivElement | null>(null);
  const { copiedValue, copyValue } = useClipboard();

  useClickOutside({
    isEnabled: isFilesOpen,
    ref: filesPanelRef,
    onClickOutside: onFilesClose,
    onEscape: onFilesClose,
  });

  const fileList = useMemo(() => {
    return Array.from(data.files)
      .map((fileExternalId) => resolveFileInfo(fileExternalId))
      .sort((a, b) => (a.fileName || a.fileExternalId).localeCompare(b.fileName || b.fileExternalId));
  }, [data.files, resolveFileInfo]);

  return (
    <TableRow ref={rowRef} data-index={dataIndex}>
      <TableCell className="font-medium text-xs truncate max-w-[120px]">{tag}</TableCell>
      <TableCell>
        <Badge variant="secondary" className="text-[9px]">
          {data.resourceType || "-"}
        </Badge>
      </TableCell>
      <TableCell>{renderStatusBadge(data.normalizedStatus)}</TableCell>
      <TableCell className="text-right font-medium text-xs">{data.count}</TableCell>
      <TableCell className="text-right text-xs text-muted-foreground relative">
        {data.files.size > 0 ? (
          <div ref={filesPanelRef} className="inline-block text-left">
            <button
              type="button"
              className="underline underline-offset-2 hover:text-foreground"
              onClick={() => onFilesToggle(tag)}
            >
              {data.files.size}
            </button>
            {isFilesOpen && (
              <div className="absolute right-0 top-full mt-1 z-30 w-[360px] rounded-md border bg-popover shadow-xl p-2">
                <div className="flex items-center justify-between mb-2">
                  <p className="text-[11px] font-medium text-foreground">{data.files.size} files</p>
                  <button
                    type="button"
                    className="text-muted-foreground hover:text-foreground"
                    onClick={onFilesClose}
                    aria-label="Close files list"
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                </div>
                <div className="max-h-[220px] overflow-y-auto space-y-1">
                  {fileList.map((file) => (
                    <div
                      key={file.fileExternalId}
                      className="rounded border bg-background/70 px-2 py-1.5"
                    >
                      <div className="flex items-start justify-between gap-2">
                        <div className="min-w-0">
                          <p className="text-[11px] font-medium text-foreground truncate">
                            {file.fileName || "Unnamed file"}
                          </p>
                          <p className="text-[10px] text-muted-foreground truncate">
                            {file.fileExternalId}
                          </p>
                        </div>
                        <div className="flex items-center gap-1 shrink-0">
                          <button
                            type="button"
                            className="inline-flex items-center gap-1 rounded border px-1.5 py-0.5 text-[10px] hover:bg-muted"
                            onClick={() => copyValue(file.fileName || "")}
                            disabled={!file.fileName}
                          >
                            {copiedValue === file.fileName ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
                            Name
                          </button>
                          <button
                            type="button"
                            className="inline-flex items-center gap-1 rounded border px-1.5 py-0.5 text-[10px] hover:bg-muted"
                            onClick={() => copyValue(file.fileExternalId)}
                          >
                            {copiedValue === file.fileExternalId ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
                            Id
                          </button>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        ) : (
          <span>{data.files.size}</span>
        )}
      </TableCell>
    </TableRow>
  );
});

const PAGE_SIZE_OPTIONS = [25, 50, 100, 200];

export function AnnotationTagsCard(props: AnnotationTagsCardProps) {
  const {
    title,
    icon,
    badgeVariant,
    totalCount,
    description,
    searchValue,
    onSearchChange,
    hasFileSelection,
    isLoading,
    loadingText,
    emptyText,
    entries,
    renderStatusBadge,
    resolveFileInfo,
    extraNoSelectionContent,
    viewAll,
    setViewAll,
  } = props;
  const [openFilesTag, setOpenFilesTag] = useState<string | null>(null);
  const tableRef = useRef<HTMLDivElement | null>(null);

  const {
    sortField,
    sortDirection,
    currentPage,
    pageSize,
    totalPages,
    startIndex,
    pagedItems: pagedEntries,
    rangeLabel,
    setPageSize,
    setCurrentPage,
    toggleSort,
  } = useSortedPagination<[string, TagEntryData], TagSortField>({
    items: entries,
    resetToken: searchValue,
    initialPageSize: 50,
    compare: ([tagA, dataA], [tagB, dataB], field) => {
      switch (field) {
        case "tag":
          return tagA.localeCompare(tagB);
        case "type":
          return (dataA.resourceType || "").localeCompare(dataB.resourceType || "");
        case "status":
          return (dataA.normalizedStatus || "").localeCompare(dataB.normalizedStatus || "");
        case "count":
          return dataA.count - dataB.count;
        case "files":
          return dataA.files.size - dataB.files.size;
      }
    },
  });

  useEffect(() => {
    setOpenFilesTag(null);
  }, [currentPage, pageSize, sortField, sortDirection, searchValue]);

  const renderSortIcon = (field: TagSortField) => {
    if (sortField !== field) return <ArrowUpDown className="h-3 w-3" />;
    return sortDirection === "asc" ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />;
  };

  const rowVirtualizer = useVirtualizer({
    count: pagedEntries.length,
    getScrollElement: () => tableRef.current,
    estimateSize: () => 36,
    overscan: 6,
    getItemKey: (index) => pagedEntries[index]?.[0] ?? `tag-${startIndex + index}`,
  });
  const virtualRows = rowVirtualizer.getVirtualItems();
  const topSpacerHeight = virtualRows.length > 0 ? virtualRows[0].start : 0;
  const bottomSpacerHeight =
    rowVirtualizer.getTotalSize() -
    (virtualRows.length > 0 ? virtualRows[virtualRows.length - 1].end : 0);

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between w-full">
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            {icon}
            {title}
          </CardTitle>
          {viewAll && (
            <div className="flex-1 flex justify-center">
              <Button
                size="xs"
                variant="outline"
                onClick={() => setViewAll(false)}
              >
                Hide all annotations
              </Button>
            </div>
          )}
          <Badge variant={badgeVariant} className="text-[10px]">
            {totalCount}
          </Badge>
        </div>
        <p className="text-[10px] text-muted-foreground block max-w-full">{description}</p>
      </CardHeader>
      <CardContent>
        {!hasFileSelection ? (
          <div className="flex flex-col items-center py-10 text-muted-foreground">
            <Info className="h-6 w-6 mb-2 opacity-30" />
            <p className="text-xs">Select one file to load tags.</p>
            {extraNoSelectionContent}
          </div>
        ) : isLoading ? (
          <div className="flex flex-col items-center py-10 text-muted-foreground">
            <Loader2 className="h-5 w-5 animate-spin mb-2" />
            <p className="text-xs">{loadingText}</p>
          </div>
        ) : (
          <>
            <div className="mb-3">
              <div className="relative">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                <Input
                  placeholder="Search tags or types..."
                  value={searchValue}
                  onChange={(e) => onSearchChange(e.target.value)}
                  className="h-8 text-xs"
                />
                {searchValue && (
                  <button
                    className="absolute right-2.5 top-1/2 -translate-y-1/2"
                    onClick={() => onSearchChange("")}
                    aria-label="Clear"
                    type="button"
                  >
                    <X className="h-3.5 w-3.5 text-muted-foreground" />
                  </button>
                )}
              </div>
            </div>
            {entries.length === 0 ? (
              <div className="flex flex-col items-center py-10 text-muted-foreground">
                <Info className="h-6 w-6 mb-2 opacity-30" />
                <p className="text-xs">{emptyText}</p>
              </div>
            ) : (
              <div
                ref={tableRef}
                className="rounded-lg border overflow-hidden"
                style={{ maxHeight: "300px", overflow: "auto" }}
              >
              <Table>
                <TableHeader className="sticky top-0 bg-muted/80 backdrop-blur-sm">
                  <TableRow>
                    <TableHead>
                      <button type="button" className="inline-flex items-center gap-1" onClick={() => toggleSort("tag")}> 
                        Tag
                        {renderSortIcon("tag")}
                      </button>
                    </TableHead>
                    <TableHead>
                      <button type="button" className="inline-flex items-center gap-1" onClick={() => toggleSort("type")}> 
                        Type
                        {renderSortIcon("type")}
                      </button>
                    </TableHead>
                    <TableHead>
                      <button type="button" className="inline-flex items-center gap-1" onClick={() => toggleSort("status")}> 
                        Status
                        {renderSortIcon("status")}
                      </button>
                    </TableHead>
                    <TableHead className="text-right w-16">
                      <button type="button" className="inline-flex items-center gap-1" onClick={() => toggleSort("count")}> 
                        Count
                        {renderSortIcon("count")}
                      </button>
                    </TableHead>
                    <TableHead className="text-right w-14">
                      <button type="button" className="inline-flex items-center gap-1" onClick={() => toggleSort("files")}> 
                        Files
                        {renderSortIcon("files")}
                      </button>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {topSpacerHeight > 0 && (
                    <TableRow aria-hidden>
                      <TableCell colSpan={5} style={{ height: topSpacerHeight }} />
                    </TableRow>
                  )}
                  {virtualRows.map((virtualRow) => {
                    const entry = pagedEntries[virtualRow.index];
                    if (!entry) return null;
                    const [tag, data] = entry;
                    return (
                      <TagRow
                        key={virtualRow.key}
                        tag={tag}
                        data={data}
                        renderStatusBadge={renderStatusBadge}
                        rowRef={rowVirtualizer.measureElement}
                        dataIndex={virtualRow.index}
                        isFilesOpen={openFilesTag === tag}
                        onFilesToggle={(clickedTag) => {
                          setOpenFilesTag((current) => (current === clickedTag ? null : clickedTag));
                        }}
                        onFilesClose={() => setOpenFilesTag(null)}
                        resolveFileInfo={resolveFileInfo}
                      />
                    );
                  })}
                  {bottomSpacerHeight > 0 && (
                    <TableRow aria-hidden>
                      <TableCell colSpan={5} style={{ height: bottomSpacerHeight }} />
                    </TableRow>
                  )}
                </TableBody>
              </Table>
              </div>
            )}
          </>
        )}
        {entries.length > 0 && (
          <div className="flex flex-wrap items-center justify-between gap-2 mt-2">
            <span className="text-[10px] text-muted-foreground">{rangeLabel}</span>
            <div className="flex items-center gap-2">
              <Select value={String(pageSize)} onValueChange={(value) => setPageSize(Number.parseInt(value, 10))}>
                <SelectTrigger className="h-7 text-[10px] w-[90px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {PAGE_SIZE_OPTIONS.map((size) => (
                    <SelectItem key={size} value={String(size)}>
                      {size} / page
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button
                variant="outline"
                size="sm"
                className="h-7 px-2 text-[10px]"
                onClick={() => setCurrentPage((prev) => Math.max(1, prev - 1))}
                disabled={currentPage === 1}
              >
                Prev
              </Button>
              <span className="text-[10px] text-muted-foreground">
                Page {currentPage} of {totalPages}
              </span>
              <Button
                variant="outline"
                size="sm"
                className="h-7 px-2 text-[10px]"
                onClick={() => setCurrentPage((prev) => Math.min(totalPages, prev + 1))}
                disabled={currentPage === totalPages}
              >
                Next
              </Button>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
