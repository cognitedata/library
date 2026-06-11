import { useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Checkbox } from "@/shared/components/ui/checkbox";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/shared/components/ui/table";
import { Progress } from "@/shared/components/ui/progress";
import type { FileAggregation } from "@/shared/utils/types";

interface FileAggregationTableProps {
  data: FileAggregation[];
  onSelectionChange?: (selectedFiles: FileAggregation[]) => void;
}

export function FileAggregationTable({
  data,
  onSelectionChange,
}: FileAggregationTableProps) {
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

  const toggleSelection = (fileId: string) => {
    const newSelection = new Set(selectedIds);
    if (newSelection.has(fileId)) {
      newSelection.delete(fileId);
    } else {
      newSelection.add(fileId);
    }
    setSelectedIds(newSelection);

    const selectedFiles = data.filter((file) => newSelection.has(file.fileExternalId));
    onSelectionChange?.(selectedFiles);
  };

  const sortedData = useMemo(() => {
    return [...data].sort((a, b) => a.coveragePct - b.coveragePct);
  }, [data]);

  if (data.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Files Aggregation</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center justify-center h-32 text-muted-foreground">
            No files match current filters
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">Files Aggregation</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-muted-foreground mb-4">
          {data.length} file{data.length !== 1 ? "s" : ""} found.
          {selectedIds.size > 0 && ` ${selectedIds.size} selected.`}
        </p>
        <div className="rounded-md border" style={{ maxHeight: "400px", overflow: "auto" }}>
          <Table>
            <TableHeader className="sticky top-0 bg-background z-10">
              <TableRow>
                <TableHead className="w-12">Select</TableHead>
                <TableHead>File Name</TableHead>
                <TableHead>Resource Type</TableHead>
                <TableHead className="text-right">Actual</TableHead>
                <TableHead className="text-right">Potential</TableHead>
                <TableHead className="text-right">Total</TableHead>
                <TableHead className="w-40">Coverage</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {sortedData.map((file) => (
                <TableRow
                  key={file.fileExternalId}
                  className="cursor-pointer hover:bg-muted/50"
                  onClick={() => toggleSelection(file.fileExternalId)}
                >
                  <TableCell>
                    <Checkbox
                      checked={selectedIds.has(file.fileExternalId)}
                      onCheckedChange={() => toggleSelection(file.fileExternalId)}
                    />
                  </TableCell>
                  <TableCell className="font-medium">
                    {file.fileName || file.fileExternalId}
                  </TableCell>
                  <TableCell>{file.fileResourceType || "-"}</TableCell>
                  <TableCell className="text-right">
                    {file.actualCount.toLocaleString()}
                  </TableCell>
                  <TableCell className="text-right">
                    {file.potentialCount.toLocaleString()}
                  </TableCell>
                  <TableCell className="text-right">
                    {file.totalPossible.toLocaleString()}
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <Progress value={file.coveragePct} className="h-2 flex-1" />
                      <span className="text-sm text-muted-foreground mb-4">
                        {file.coveragePct.toFixed(1)}%
                      </span>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}
