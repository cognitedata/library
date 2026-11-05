/**
 * Responsive Table Wrapper Component
 *
 * Switches between table and card layouts based on screen size.
 * Shows table on desktop (md and above), cards on mobile.
 */

import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export interface ResponsiveTableProps {
  /** Desktop table content */
  table: React.ReactNode;
  /** Mobile cards content */
  cards: React.ReactNode;
  /** Custom className */
  className?: string;
}

export function ResponsiveTable({
  table,
  cards,
  className,
}: ResponsiveTableProps) {
  return (
    <div className={cn("", className)}>
      {/* Desktop Table */}
      <div className="hidden md:block">{table}</div>

      {/* Mobile Cards */}
      <div className="block md:hidden space-y-3">{cards}</div>
    </div>
  );
}

/**
 * Props for responsive table implementations
 */
export interface ResponsiveTableImplementationProps<T> {
  /** Items to display */
  items: T[];
  /** Current page number */
  currentPage: number;
  /** Total number of pages */
  totalPages: number;
  /** Total number of items */
  totalItems: number;
  /** Whether there is a next page */
  hasNextPage: boolean;
  /** Whether there is a previous page */
  hasPreviousPage: boolean;
  /** Callback for next page */
  onNextPage: () => void;
  /** Callback for previous page */
  onPreviousPage: () => void;
  /** Loading state */
  isLoading?: boolean;
}

/**
 * Shared pagination component for both table and card views
 */
export interface PaginationProps {
  currentPage: number;
  totalPages: number;
  totalItems: number;
  hasNextPage: boolean;
  hasPreviousPage: boolean;
  onNextPage: () => void;
  onPreviousPage: () => void;
  itemName?: string;
  /** DMS cursor pagination: show controls when prev/next exist; label omits unknown totals. */
  cursorMode?: boolean;
}

export function Pagination({
  currentPage,
  totalPages,
  totalItems,
  hasNextPage,
  hasPreviousPage,
  onNextPage,
  onPreviousPage,
  itemName = "items",
  cursorMode = false,
}: PaginationProps) {
  if (cursorMode) {
    if (!hasNextPage && !hasPreviousPage) return null;
  } else if (totalPages <= 1) {
    return null;
  }

  const label = cursorMode
    ? `Page ${currentPage}${hasNextPage ? " · more available" : ""} (${totalItems} on this page)`
    : `Page ${currentPage} of ${totalPages} (${totalItems} ${itemName})`;

  return (
    <div className="flex flex-wrap items-center justify-between gap-3 px-3 sm:px-4 py-3 bg-muted/30 border-t border-border min-w-0 max-md:flex-col max-md:gap-3">
      <span className="text-sm text-muted-foreground max-md:text-center min-w-0 break-words">
        {label}
      </span>
      <div className="flex flex-wrap items-center gap-2 max-md:justify-center shrink-0">
        <Button
          variant="outline"
          size="sm"
          onClick={onPreviousPage}
          disabled={!hasPreviousPage}
          className="h-7"
        >
          <ChevronLeft className="h-4 w-4" />
          Previous
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={onNextPage}
          disabled={!hasNextPage}
          className="h-7"
        >
          Next
          <ChevronRight className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
