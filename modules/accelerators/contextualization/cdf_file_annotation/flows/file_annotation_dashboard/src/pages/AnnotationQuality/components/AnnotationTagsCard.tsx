import { ArrowDown, ArrowUp, ArrowUpDown, Info, Loader2, Search, X } from "lucide-react";
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
  extraNoSelectionContent?: ReactNode;
  viewAll: boolean;
  setViewAll: (v: boolean) => void;
}

type TagSortField = "tag" | "type" | "status" | "count" | "files";
type TagSortDirection = "asc" | "desc";

interface TagRowProps {
  tag: string;
  data: TagEntryData;
  renderStatusBadge: (status?: string) => React.ReactNode;
  rowRef: (element: HTMLTableRowElement | null) => void;
  dataIndex: number;
}

const TagRow = memo(function TagRow({ tag, data, renderStatusBadge, rowRef, dataIndex }: TagRowProps) {
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
      <TableCell className="text-right text-xs text-muted-foreground">{data.files.size}</TableCell>
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
    extraNoSelectionContent,
    viewAll,
    setViewAll,
  } = props;
  const [sortField, setSortField] = useState<TagSortField | null>(null);
  const [sortDirection, setSortDirection] = useState<TagSortDirection>("asc");
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const tableRef = useRef<HTMLDivElement | null>(null);

  const sortedEntries = useMemo(() => {
    if (!sortField) return entries;

    const result = [...entries];
    result.sort(([tagA, dataA], [tagB, dataB]) => {
      let comparison = 0;
      switch (sortField) {
        case "tag":
          comparison = tagA.localeCompare(tagB);
          break;
        case "type":
          comparison = (dataA.resourceType || "").localeCompare(dataB.resourceType || "");
          break;
        case "status":
          comparison = (dataA.normalizedStatus || "").localeCompare(dataB.normalizedStatus || "");
          break;
        case "count":
          comparison = dataA.count - dataB.count;
          break;
        case "files":
          comparison = dataA.files.size - dataB.files.size;
          break;
      }
      return sortDirection === "desc" ? -comparison : comparison;
    });

    return result;
  }, [entries, sortField, sortDirection]);

  const totalPages = useMemo(() => {
    return Math.max(1, Math.ceil(sortedEntries.length / pageSize));
  }, [sortedEntries.length, pageSize]);

  useEffect(() => {
    if (currentPage > totalPages) {
      setCurrentPage(totalPages);
    }
  }, [currentPage, totalPages]);

  useEffect(() => {
    setCurrentPage(1);
  }, [sortField, sortDirection, searchValue, pageSize]);

  const startIndex = (currentPage - 1) * pageSize;
  const pagedEntries = useMemo(() => {
    return sortedEntries.slice(startIndex, startIndex + pageSize);
  }, [sortedEntries, startIndex, pageSize]);

  const rangeLabel = useMemo(() => {
    if (sortedEntries.length === 0) return "0 of 0";
    const start = startIndex + 1;
    const end = Math.min(startIndex + pageSize, sortedEntries.length);
    return `${start}-${end} of ${sortedEntries.length}`;
  }, [sortedEntries.length, startIndex, pageSize]);

  const toggleSort = (field: TagSortField) => {
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
        ) : entries.length === 0 ? (
          <div className="flex flex-col items-center py-10 text-muted-foreground">
            <Info className="h-6 w-6 mb-2 opacity-30" />
            <p className="text-xs">{emptyText}</p>
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
