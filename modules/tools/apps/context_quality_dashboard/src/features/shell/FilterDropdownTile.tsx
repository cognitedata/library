import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
} from '@cognite/aura/components/select';
import { IconChevronDown } from '@cognite/aura/icons';

export const FILTER_TILE_WIDTH_CLASS = 'w-44 max-w-none shrink-0';

type FilterDropdownTileProps = {
  label: string;
  value: string;
  displayValue: string;
  disabled?: boolean;
  onValueChange: (value: string) => void;
  items: Array<{ value: string; label: string }>;
  triggerTestId?: string;
  triggerWidthClass?: string;
  truncateDisplayValue?: boolean;
};

export function FilterDropdownTile({
  label,
  value,
  displayValue,
  disabled = false,
  onValueChange,
  items,
  triggerTestId,
  triggerWidthClass = FILTER_TILE_WIDTH_CLASS,
  truncateDisplayValue = true,
}: FilterDropdownTileProps) {
  return (
    <Select value={value} onValueChange={onValueChange} disabled={disabled}>
      <SelectTrigger
        size="sm"
        aria-label={label}
        data-testid={triggerTestId}
        className={`${triggerWidthClass} h-9 items-center justify-between gap-2 rounded-lg border border-border bg-background px-3 py-2 text-left shadow-none [&>svg]:hidden${truncateDisplayValue ? '' : ' overflow-visible'}`}
      >
        <span
          className={
            truncateDisplayValue
              ? 'flex min-w-0 flex-1 items-center gap-2 overflow-hidden text-sm'
              : 'flex shrink-0 items-center gap-2 text-sm'
          }
        >
          <span className="shrink-0 text-muted-foreground">{label}</span>
          <span
            className={
              truncateDisplayValue
                ? 'truncate font-medium text-foreground'
                : 'whitespace-nowrap font-medium text-foreground'
            }
          >
            {displayValue}
          </span>
        </span>
        <span className="shrink-0" aria-hidden="true">
          <IconChevronDown className="size-4 text-muted-foreground" />
        </span>
      </SelectTrigger>
      <SelectContent>
        {items.map((item) => (
          <SelectItem key={item.value} value={item.value}>
            {item.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
