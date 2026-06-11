import { memo, useEffect, useMemo, useRef, useState } from "react";
import type { ChangeEvent } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { PatternDraft } from "@/shared/utils/patternManagement";
import { Badge } from "@/shared/components/ui/badge";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Input } from "@/shared/components/ui/input";
import { MessageBox } from "@/shared/components/MessageBox";
import { EmptyTableRow } from "@/shared/components/EmptyTableRow";
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
import { ArrowDownToLine, FileUp, Loader2, RefreshCw, Trash2 } from "lucide-react";

const PAGE_SIZE_OPTIONS = ["25", "50", "100", "200"];

interface CsvPreviewRowProps {
  row: PatternDraft;
  rowIndex: number;
  onUpdate: (index: number, field: keyof PatternDraft, value: string) => void;
  onRemove: (index: number) => void;
  rowRef: (element: HTMLTableRowElement | null) => void;
  dataIndex: number;
}

const CsvPreviewRow = memo(function CsvPreviewRow({
  row,
  rowIndex,
  onUpdate,
  onRemove,
  rowRef,
  dataIndex,
}: CsvPreviewRowProps) {
  return (
    <TableRow ref={rowRef} data-index={dataIndex}>
      <TableCell>
        <Input
          value={row.sample}
          onChange={(e) => onUpdate(rowIndex, "sample", e.target.value)}
          className="h-7 text-xs font-mono"
        />
      </TableCell>
      <TableCell>
        <Input
          value={row.resourceType}
          onChange={(e) => onUpdate(rowIndex, "resourceType", e.target.value)}
          className="h-7 text-[10px]"
        />
      </TableCell>
      <TableCell>
        <Select
          value={row.annotationType}
          onValueChange={(value) => onUpdate(rowIndex, "annotationType", value)}
        >
          <SelectTrigger className="h-7 text-[10px]">
            <SelectValue placeholder="Type" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="Asset">Asset</SelectItem>
            <SelectItem value="File">File</SelectItem>
          </SelectContent>
        </Select>
      </TableCell>
      <TableCell>
        <Input
          value={row.patternScope}
          onChange={(e) => onUpdate(rowIndex, "patternScope", e.target.value)}
          className="h-7 text-[10px]"
        />
      </TableCell>
      <TableCell>
        <Button
          variant="ghost"
          size="icon"
          className="h-7 w-7"
          onClick={() => onRemove(rowIndex)}
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </TableCell>
    </TableRow>
  );
});

interface ImportCsvSectionProps {
  csvPreview: PatternDraft[];
  csvFileName: string | null;
  csvDefaultScope: string;
  csvText: string | null;
  csvError: string | null;
  csvStageMessage: string | null;
  isCsvStaging: boolean;
  handleCsvFileChange: (event: ChangeEvent<HTMLInputElement>) => void;
  setCsvDefaultScope: (value: string) => void;
  handleCsvReparse: () => void;
  handleCsvUpdate: (index: number, field: keyof PatternDraft, value: string) => void;
  handleCsvRemove: (index: number) => void;
  handleCsvStage: () => void;
  handleCsvClear: () => void;
}

export function ImportCsvSection({
  csvPreview,
  csvFileName,
  csvDefaultScope,
  csvText,
  csvError,
  csvStageMessage,
  isCsvStaging,
  handleCsvFileChange,
  setCsvDefaultScope,
  handleCsvReparse,
  handleCsvUpdate,
  handleCsvRemove,
  handleCsvStage,
  handleCsvClear,
}: ImportCsvSectionProps) {
  const [csvPageSize, setCsvPageSize] = useState("50");
  const [csvCurrentPage, setCsvCurrentPage] = useState(1);
  const csvTableRef = useRef<HTMLDivElement | null>(null);
  const csvFileInputRef = useRef<HTMLInputElement | null>(null);

  const csvPageSizeValue = useMemo(() => Number.parseInt(csvPageSize, 10), [csvPageSize]);
  const csvTotalPages = useMemo(() => {
    return Math.max(1, Math.ceil(csvPreview.length / csvPageSizeValue));
  }, [csvPreview.length, csvPageSizeValue]);

  useEffect(() => {
    if (csvCurrentPage > csvTotalPages) {
      setCsvCurrentPage(csvTotalPages);
    }
  }, [csvCurrentPage, csvTotalPages]);

  useEffect(() => {
    setCsvCurrentPage(1);
  }, [csvPreview.length, csvPageSize]);

  const csvStartIndex = (csvCurrentPage - 1) * csvPageSizeValue;
  const pagedCsvPreview = useMemo(() => {
    return csvPreview.slice(csvStartIndex, csvStartIndex + csvPageSizeValue);
  }, [csvPreview, csvStartIndex, csvPageSizeValue]);

  const csvRowVirtualizer = useVirtualizer({
    count: pagedCsvPreview.length,
    getScrollElement: () => csvTableRef.current,
    estimateSize: () => 44,
    overscan: 6,
    getItemKey: (index) => {
      const row = pagedCsvPreview[index];
      return row ? `${row.sample}-${csvStartIndex + index}` : `${csvStartIndex + index}`;
    },
  });
  const csvVirtualRows = csvRowVirtualizer.getVirtualItems();
  const csvTopSpacer = csvVirtualRows.length > 0 ? csvVirtualRows[0].start : 0;
  const csvBottomSpacer =
    csvRowVirtualizer.getTotalSize() -
    (csvVirtualRows.length > 0 ? csvVirtualRows[csvVirtualRows.length - 1].end : 0);

  const csvRangeLabel = useMemo(() => {
    if (csvPreview.length === 0) return "0 of 0";
    const startIndex = csvStartIndex + 1;
    const endIndex = Math.min(csvStartIndex + csvPageSizeValue, csvPreview.length);
    return `${startIndex}-${endIndex} of ${csvPreview.length}`;
  }, [csvPreview.length, csvStartIndex, csvPageSizeValue]);

  const handleClearCsv = () => {
    handleCsvClear();
    if (csvFileInputRef.current) {
      csvFileInputRef.current.value = "";
    }
  };

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <FileUp className="h-4 w-4 text-muted-foreground" />
            Import CSV
          </CardTitle>
          <Badge variant="secondary" className="text-[10px]">
            {csvPreview.length}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-center gap-3">
          <Input
            ref={csvFileInputRef}
            type="file"
            accept=".csv"
            onChange={handleCsvFileChange}
            className="text-xs"
          />
          {csvFileName && <span className="text-[10px]">{csvFileName}</span>}
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div className="space-y-1">
            <label className="text-[10px]">Default Scope</label>
            <Input
              value={csvDefaultScope}
              onChange={(e) => setCsvDefaultScope(e.target.value)}
              placeholder="scope"
              className="h-8 text-xs"
            />
          </div>
          <div className="flex items-end justify-end">
            <Button variant="outline" size="xs" onClick={handleCsvReparse} disabled={!csvText}>
              <RefreshCw className="h-3 w-3" />
              Reparse
            </Button>
          </div>
        </div>

        {csvError && <MessageBox text={csvError} variant="error" />}

        {csvPreview.length > 0 && (
          <>
            <div
              ref={csvTableRef}
              className="rounded-lg border overflow-hidden"
              style={{ maxHeight: "240px", overflow: "auto" }}
            >
              <Table>
                <TableHeader className="sticky top-0 bg-muted/80 backdrop-blur-sm">
                  <TableRow>
                    <TableHead className="min-w-[120px]">Pattern</TableHead>
                    <TableHead className="w-24">Resource</TableHead>
                    <TableHead className="w-20">Type</TableHead>
                    <TableHead className="w-28">Scope</TableHead>
                    <TableHead className="w-8"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedCsvPreview.length === 0 ? (
                    <EmptyTableRow colSpan={5} message="No CSV rows on this page." />
                  ) : (
                    <>
                      {csvTopSpacer > 0 && (
                        <TableRow aria-hidden>
                          <TableCell colSpan={5} style={{ height: csvTopSpacer }} />
                        </TableRow>
                      )}
                      {csvVirtualRows.map((virtualRow) => {
                        const row = pagedCsvPreview[virtualRow.index];
                        if (!row) return null;
                        const rowIndex = csvStartIndex + virtualRow.index;
                        return (
                          <CsvPreviewRow
                            key={virtualRow.key}
                            row={row}
                            rowIndex={rowIndex}
                            onUpdate={handleCsvUpdate}
                            onRemove={handleCsvRemove}
                            rowRef={csvRowVirtualizer.measureElement}
                            dataIndex={virtualRow.index}
                          />
                        );
                      })}
                      {csvBottomSpacer > 0 && (
                        <TableRow aria-hidden>
                          <TableCell colSpan={5} style={{ height: csvBottomSpacer }} />
                        </TableRow>
                      )}
                    </>
                  )}
                </TableBody>
              </Table>
            </div>
            <div className="flex flex-wrap items-center justify-between gap-2 border border-t-0 rounded-b-lg bg-muted/30 px-3 py-2">
              <span className="text-[10px] text-muted-foreground">{csvRangeLabel}</span>
              <div className="flex items-center gap-2">
                <Select value={csvPageSize} onValueChange={setCsvPageSize}>
                  <SelectTrigger className="h-7 w-[80px] text-[10px]">
                    <SelectValue placeholder="Rows" />
                  </SelectTrigger>
                  <SelectContent>
                    {PAGE_SIZE_OPTIONS.map((size) => (
                      <SelectItem key={size} value={size}>
                        {size} / page
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Button
                  variant="ghost"
                  size="xs"
                  onClick={() => setCsvCurrentPage((prev) => Math.max(1, prev - 1))}
                  disabled={csvCurrentPage === 1}
                >
                  Prev
                </Button>
                <Button
                  variant="ghost"
                  size="xs"
                  onClick={() => setCsvCurrentPage((prev) => Math.min(csvTotalPages, prev + 1))}
                  disabled={csvCurrentPage >= csvTotalPages}
                >
                  Next
                </Button>
              </div>
            </div>
          </>
        )}

        <div className="flex gap-2">
          <Button
            variant="outline"
            size="xs"
            onClick={handleCsvStage}
            disabled={csvPreview.length === 0 || isCsvStaging}
          >
            {isCsvStaging ? (
              <Loader2 className="h-3 w-3 animate-spin" />
            ) : (
              <ArrowDownToLine className="h-3 w-3" />
            )}
            {isCsvStaging ? "Staging" : "Stage to Manual"}
          </Button>
          <div className="flex-1" />
          <Button variant="ghost" size="xs" onClick={handleClearCsv} disabled={csvPreview.length === 0}>
            <Trash2 className="h-3 w-3" />
            Clear
          </Button>
        </div>

        {csvStageMessage && <MessageBox text={csvStageMessage} variant="info" />}
      </CardContent>
    </Card>
  );
}
