/**
 * Unified Data Type Control Component
 *
 * Provides consistent data type selection across the application.
 * Uses a multiselect dropdown for better UX and space efficiency.
 */

import { useMemo } from "react";
import { Badge } from "@/components/ui/badge";
import {
  MultiSelectDropdown,
  type MultiSelectOption,
} from "@/components/ui/multiselect-dropdown";
import { getDisplayName, getEnabledDataTypes } from "@/config/dataTypes";
import { cn } from "@/lib/utils";

export interface DataTypeControlProps {
  /** Currently selected data types */
  selectedTypes: string[];
  /** Callback when a data type is toggled */
  onToggle: (dataType: string) => void;
  /** Callback when selection changes (bulk operations) */
  onSelectionChange?: (newSelection: string[]) => void;
  /** Label for the control group */
  label?: string;
  /** Size variant */
  size?: "sm" | "md";
  /** Layout variant */
  layout?: "inline" | "grid";
  /** Show Select All/Clear All actions */
  showBulkActions?: boolean;
  /** Disabled state */
  disabled?: boolean;
  /** Custom className */
  className?: string;
  /** Available data types (defaults to enabled types) */
  availableTypes?: string[];
}

export function DataTypeControl({
  selectedTypes,
  onToggle,
  onSelectionChange,
  label = "Data Types",
  size = "md",
  layout = "inline",
  showBulkActions = true,
  disabled = false,
  className,
  availableTypes,
}: DataTypeControlProps) {
  const enabledDataTypes = useMemo(
    () => availableTypes || getEnabledDataTypes(),
    [availableTypes],
  );

  // Convert data types to dropdown options
  const options: MultiSelectOption[] = useMemo(
    () =>
      enabledDataTypes.map((dataType) => ({
        value: dataType,
        label: getDisplayName(dataType),
        disabled: false,
      })),
    [enabledDataTypes],
  );

  const handleSelectionChange = (newSelectedValues: string[]) => {
    // Calculate the number of changes
    const previousSet = new Set(selectedTypes);
    const newSet = new Set(newSelectedValues);
    const addedItems = newSelectedValues.filter(
      (value) => !previousSet.has(value),
    );
    const removedItems = selectedTypes.filter((value) => !newSet.has(value));
    const totalChanges = addedItems.length + removedItems.length;

    // If there are many changes (bulk operation) and we have onSelectionChange, use it
    if (totalChanges > 1 && onSelectionChange) {
      onSelectionChange(newSelectedValues);
      return;
    }

    // Otherwise, use individual toggle approach for single changes
    // Handle newly selected items
    addedItems.forEach((value) => {
      onToggle(value);
    });

    // Handle deselected items
    removedItems.forEach((value) => {
      onToggle(value);
    });
  };

  const dropdownSize = size === "sm" ? "sm" : "md";

  return (
    <div
      className={cn(
        "flex flex-col gap-2 min-w-0 sm:flex-row sm:items-center sm:gap-2",
        className,
      )}
    >
      {/* Label */}
      {label ? (
        <span
          className={cn(
            "text-muted-foreground shrink-0",
            size === "sm" ? "text-xs font-medium" : "text-sm font-medium",
          )}
        >
          {label}:
        </span>
      ) : null}

      {/* MultiSelect Dropdown */}
      <MultiSelectDropdown
        options={options}
        selectedValues={selectedTypes}
        onSelectionChange={handleSelectionChange}
        placeholder="Select data types..."
        variant="outline"
        size={dropdownSize}
        disabled={disabled}
        showSelectAll={showBulkActions}
        showClearAll={showBulkActions}
        maxWidth={layout === "grid" ? "min(100%, 300px)" : "min(100%, 280px)"}
        className="w-full min-w-0 sm:min-w-40"
      />
    </div>
  );
}

/**
 * Compact badge-only display variant for read-only scenarios
 */
export interface DataTypeBadgesProps {
  /** Selected data types to display */
  selectedTypes: string[];
  /** Maximum number of badges to show before truncation */
  maxVisible?: number;
  /** Size variant */
  size?: "sm" | "md";
  /** Custom className */
  className?: string;
  /** Available data types (defaults to enabled types) */
  availableTypes?: string[];
}

export function DataTypeBadges({
  selectedTypes,
  maxVisible = 3,
  size = "sm",
  className,
  availableTypes,
}: DataTypeBadgesProps) {
  const enabledDataTypes = useMemo(
    () => availableTypes || getEnabledDataTypes(),
    [availableTypes],
  );

  // Filter selected types to only include enabled ones
  const filteredSelectedTypes = selectedTypes.filter((type) =>
    enabledDataTypes.includes(type),
  );

  if (filteredSelectedTypes.length === 0) {
    return (
      <span className={cn("text-xs text-muted-foreground", className)}>
        None selected
      </span>
    );
  }

  const visibleTypes = filteredSelectedTypes.slice(0, maxVisible);
  const remainingCount = filteredSelectedTypes.length - maxVisible;

  return (
    <div className={cn("flex gap-1 flex-wrap", className)}>
      {visibleTypes.map((dataType) => (
        <Badge
          key={dataType}
          variant="secondary"
          className={cn(
            size === "sm" ? "text-xs px-1.5 py-0.5" : "text-sm px-2 py-1",
          )}
        >
          {getDisplayName(dataType)}
        </Badge>
      ))}
      {remainingCount > 0 && (
        <Badge
          variant="outline"
          className={cn(
            size === "sm" ? "text-xs px-1.5 py-0.5" : "text-sm px-2 py-1",
          )}
        >
          +{remainingCount}
        </Badge>
      )}
    </div>
  );
}
