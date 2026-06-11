import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/shared/components/ui/table";
import { Badge } from "@/shared/components/ui/badge";
import { Progress } from "@/shared/components/ui/progress";
import { mergeClassNames } from "@/shared/utils/classNames";
import { FileAnnotationStatus } from "@/shared/utils/constants";

interface Column<T> {
  key: string;
  label: string;
  render?: (value: unknown, row: T) => React.ReactNode;
  className?: string;
}

interface DataTableProps<T extends Record<string, unknown>> {
  data: T[];
  columns: Column<T>[];
  onRowClick?: (row: T) => void;
  selectedRow?: T | null;
  emptyMessage?: string;
  maxHeight?: string;
}

export function DataTable<T extends Record<string, unknown>>({
  data,
  columns,
  onRowClick,
  selectedRow,
  emptyMessage = "No data available",
  maxHeight = "400px",
}: DataTableProps<T>) {
  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center h-32 text-muted-foreground">
        {emptyMessage}
      </div>
    );
  }

  return (
    <div className="rounded-md border" style={{ maxHeight, overflow: "auto" }}>
      <Table>
        <TableHeader className="sticky top-0 bg-background z-10">
          <TableRow>
            {columns.map((col) => (
              <TableHead key={col.key} className={col.className}>
                {col.label}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((row, idx) => {
            const isSelected =
              selectedRow && Object.keys(row).every((k) => row[k] === selectedRow[k]);
            return (
              <TableRow
                key={idx}
                onClick={() => onRowClick?.(row)}
                className={mergeClassNames(
                  onRowClick && "cursor-pointer hover:bg-muted/50",
                  isSelected && "bg-muted"
                )}
              >
                {columns.map((col) => (
                  <TableCell key={col.key} className={col.className}>
                    {col.render ? col.render(row[col.key], row) : String(row[col.key] ?? "")}
                  </TableCell>
                ))}
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
}

export function renderStatus(value: unknown) {
  const status = String(value);
  const normalized = status.toLowerCase();
  let variant: "default" | "secondary" | "destructive" | "outline" | "success" | "warning" =
    "outline";

  if (status === FileAnnotationStatus.ANNOTATED) {
    variant = "success";
  } else if (status === FileAnnotationStatus.AWAITING || normalized.includes("process")) {
    variant = "warning";
  } else if (status === FileAnnotationStatus.FAILED) {
    variant = "destructive";
  }

  return <Badge variant={variant}>{status}</Badge>;
}

export function renderProgress(value: unknown) {
  const percentage = Number(value) || 0;
  return (
    <div className="flex items-center gap-2">
      <Progress value={percentage} className="h-2 w-20" />
      <span className="text-sm text-muted-foreground">{percentage.toFixed(1)}%</span>
    </div>
  );
}

export function renderDate(value: unknown) {
  if (!value) return "-";
  const date = value instanceof Date ? value : new Date(String(value));
  return date.toLocaleString();
}

export function renderNumber(value: unknown) {
  const num = Number(value);
  if (isNaN(num)) return String(value);
  return num.toLocaleString();
}

