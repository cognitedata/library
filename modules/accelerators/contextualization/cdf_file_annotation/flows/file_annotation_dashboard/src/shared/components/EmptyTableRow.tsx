import { TableCell, TableRow } from "@/shared/components/ui/table";

interface EmptyTableRowProps {
  colSpan: number;
  message: string;
  className?: string;
}

export function EmptyTableRow({ colSpan, message, className }: EmptyTableRowProps) {
  const classes = `text-center py-8 text-muted-foreground text-xs${className ? ` ${className}` : ""}`;

  return (
    <TableRow>
      <TableCell colSpan={colSpan} className={classes}>
        {message}
      </TableCell>
    </TableRow>
  );
}
