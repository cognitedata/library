/**
 * MultiSelect Dropdown — multiple selection via Aura DropdownMenu + checkbox items.
 */

import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@cognite/aura/components";
import { ChevronDown } from "lucide-react";
import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export interface MultiSelectOption {
  value: string;
  label: string;
  disabled?: boolean;
}

export interface MultiSelectDropdownProps {
  /** Available options to select from */
  options: MultiSelectOption[];
  /** Currently selected values */
  selectedValues: string[];
  /** Callback when selection changes */
  onSelectionChange: (values: string[]) => void;
  /** Placeholder text when no items selected */
  placeholder?: string;
  /** Button variant */
  variant?: "default" | "outline" | "ghost";
  /** Button size */
  size?: "sm" | "md" | "lg";
  /** Disabled state */
  disabled?: boolean;
  /** Maximum width for the trigger button */
  maxWidth?: string;
  /** Show clear all button */
  showClearAll?: boolean;
  /** Show select all button */
  showSelectAll?: boolean;
  /** Custom className for the trigger button */
  className?: string;
}

export function MultiSelectDropdown({
  options,
  selectedValues,
  onSelectionChange,
  placeholder = "Select items...",
  variant = "outline",
  size = "md",
  disabled = false,
  maxWidth = "min(100%, 280px)",
  showClearAll = true,
  showSelectAll = true,
  className,
}: MultiSelectDropdownProps) {
  const [open, setOpen] = useState(false);

  const handleToggleOption = (value: string) => {
    const newSelection = selectedValues.includes(value)
      ? selectedValues.filter((v) => v !== value)
      : [...selectedValues, value];
    onSelectionChange(newSelection);
  };

  const handleSelectAll = () => {
    const allValues = options
      .filter((opt) => !opt.disabled)
      .map((opt) => opt.value);
    onSelectionChange(allValues);
  };

  const handleClearAll = () => {
    onSelectionChange([]);
  };

  const selectedLabels = options
    .filter((opt) => selectedValues.includes(opt.value))
    .map((opt) => opt.label);

  const getDisplayText = () => {
    if (selectedValues.length === 0) return placeholder;
    if (selectedValues.length === 1) return selectedLabels[0];
    return `${selectedValues.length} selected`;
  };

  const sizeClasses = {
    sm: "h-8 px-2 text-xs",
    md: "h-10 px-3 text-sm",
    lg: "h-12 px-4 text-base",
  };

  const buttonSize = size === "md" ? "default" : size;

  const enabledCount = options.filter((opt) => !opt.disabled).length;
  const allSelected =
    enabledCount > 0 && selectedValues.length === enabledCount;

  return (
    <DropdownMenu open={open} onOpenChange={setOpen}>
      <DropdownMenuTrigger
        render={
          <Button
            type="button"
            variant={variant}
            size={buttonSize}
            disabled={disabled}
            className={cn(
              "justify-between font-normal min-w-0 w-full max-w-full",
              sizeClasses[size],
              className,
            )}
            style={maxWidth ? { maxWidth } : undefined}
          >
            <span className="truncate flex-1 text-left">
              {getDisplayText()}
            </span>
            <ChevronDown
              className={cn(
                "ml-2 shrink-0 transition-transform duration-200",
                size === "sm" ? "h-3 w-3" : "h-4 w-4",
                open && "rotate-180",
              )}
            />
          </Button>
        }
      />

      <DropdownMenuContent
        align="start"
        sideOffset={4}
        className="min-w-32 max-w-[300px]"
      >
        {(showSelectAll || showClearAll) && (
          <>
            <div className="flex items-center gap-2 px-2 py-1.5">
              {showSelectAll && (
                <button
                  type="button"
                  onClick={handleSelectAll}
                  disabled={allSelected}
                  className="flex-1 rounded px-1 py-0.5 text-left text-xs text-primary transition-colors hover:bg-accent/45 hover:underline disabled:cursor-not-allowed disabled:opacity-50 dark:hover:bg-accent/35"
                >
                  Select All
                </button>
              )}
              {showClearAll && (
                <button
                  type="button"
                  onClick={handleClearAll}
                  disabled={selectedValues.length === 0}
                  className="flex-1 rounded px-1 py-0.5 text-right text-xs text-muted-foreground transition-colors hover:bg-accent/45 hover:underline disabled:cursor-not-allowed disabled:opacity-50 dark:hover:bg-accent/35"
                >
                  Clear All
                </button>
              )}
            </div>
            <DropdownMenuSeparator />
          </>
        )}

        <div className="max-h-60 overflow-y-auto">
          {options.map((option) => (
            <DropdownMenuCheckboxItem
              key={option.value}
              checked={selectedValues.includes(option.value)}
              disabled={option.disabled}
              onCheckedChange={() => {
                if (!option.disabled) handleToggleOption(option.value);
              }}
              className={cn(option.disabled && "opacity-50")}
            >
              {option.label}
            </DropdownMenuCheckboxItem>
          ))}
        </div>

        {selectedValues.length > 0 && (
          <>
            <DropdownMenuSeparator />
            <div className="px-2 py-1.5 text-xs text-muted-foreground">
              {selectedValues.length} of {options.length} selected
            </div>
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

/**
 * Compact multiselect with badge display
 */
export interface MultiSelectWithBadgesProps extends MultiSelectDropdownProps {
  /** Maximum number of badges to show */
  maxBadges?: number;
  /** Show badge for selected count when collapsed */
  showCount?: boolean;
}

export function MultiSelectWithBadges({
  maxBadges = 2,
  showCount = true,
  ...props
}: MultiSelectWithBadgesProps) {
  const { options, selectedValues } = props;

  const selectedOptions = options.filter((opt) =>
    selectedValues.includes(opt.value),
  );
  const visibleBadges = selectedOptions.slice(0, maxBadges);
  const remainingCount = selectedOptions.length - maxBadges;

  return (
    <div className="flex items-center gap-2">
      <MultiSelectDropdown {...props} />

      {selectedValues.length > 0 && (
        <div className="flex items-center gap-1 flex-wrap">
          {visibleBadges.map((option) => (
            <Badge
              key={option.value}
              variant="secondary"
              className="text-xs px-1.5 py-0.5"
            >
              {option.label}
            </Badge>
          ))}
          {remainingCount > 0 && showCount && (
            <Badge variant="outline" className="text-xs px-1.5 py-0.5">
              +{remainingCount}
            </Badge>
          )}
        </div>
      )}
    </div>
  );
}
