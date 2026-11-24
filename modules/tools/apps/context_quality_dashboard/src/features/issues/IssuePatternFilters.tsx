import { Input } from '@cognite/aura/components/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
} from '@cognite/aura/components/select';
import { IconChevronDown } from '@cognite/aura/icons';

import {
  FILTER_ALL,
  formatFilterSelectLabel,
  fromFilterSelectValue,
  toFilterSelectValue,
} from '../shell/formatFilterSelectLabel';

export type IssuePatternFiltersState = {
  dimensionId?: string;
  kind?: string;
  ownerId?: string;
  search: string;
};

type SelectOption = {
  id: string;
  label: string;
};

type IssuePatternFiltersProps = {
  filters: IssuePatternFiltersState;
  dimensionOptions: SelectOption[];
  issueTypeOptions: SelectOption[];
  ownerOptions: SelectOption[];
  onChange: (filters: IssuePatternFiltersState) => void;
  t: (key: string) => string;
};

export function IssuePatternFilters({
  filters,
  dimensionOptions,
  issueTypeOptions,
  ownerOptions,
  onChange,
  t,
}: IssuePatternFiltersProps) {
  return (
    <section
      aria-label={t('issues.filters')}
      className="flex flex-wrap items-end gap-3"
      data-testid="issue-pattern-filters"
    >
      <FilterSelect
        label={t('issues.dimension')}
        value={toFilterSelectValue(filters.dimensionId)}
        options={dimensionOptions}
        allLabel={t('filter.all')}
        onValueChange={(value) => onChange({ ...filters, dimensionId: fromFilterSelectValue(value) })}
      />
      <FilterSelect
        label={t('issues.filterType')}
        value={toFilterSelectValue(filters.kind)}
        options={issueTypeOptions}
        allLabel={t('filter.all')}
        onValueChange={(value) => onChange({ ...filters, kind: fromFilterSelectValue(value) })}
      />
      <FilterSelect
        label={t('issues.owner')}
        value={toFilterSelectValue(filters.ownerId)}
        options={ownerOptions}
        allLabel={t('filter.all')}
        onValueChange={(value) => onChange({ ...filters, ownerId: fromFilterSelectValue(value) })}
      />
      <Input
        aria-label={t('issues.searchPlaceholder')}
        placeholder={t('issues.searchPlaceholder')}
        className="h-9 w-56"
        value={filters.search}
        onChange={(event) => onChange({ ...filters, search: event.target.value })}
      />
    </section>
  );
}

type FilterSelectProps = {
  label: string;
  value: string;
  options: SelectOption[];
  allLabel: string;
  onValueChange: (value: string) => void;
};

export function FilterSelect({ label, value, options, allLabel, onValueChange }: FilterSelectProps) {
  const displayValue = formatFilterSelectLabel(value, options, allLabel);

  return (
    <Select value={value} onValueChange={onValueChange}>
      <SelectTrigger
        size="sm"
        aria-label={label}
        className="h-9 w-44 items-center justify-between gap-2 rounded-lg border border-border bg-background px-3 py-2 text-left shadow-none [&>svg]:hidden"
      >
        <span className="flex min-w-0 flex-1 items-center gap-2 overflow-hidden text-sm">
          <span className="shrink-0 text-muted-foreground">{label}</span>
          <span className="truncate font-medium text-foreground">{displayValue}</span>
        </span>
        <span className="shrink-0" aria-hidden="true">
          <IconChevronDown className="size-4 text-muted-foreground" />
        </span>
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={FILTER_ALL}>{allLabel}</SelectItem>
        {options.map((option) => (
          <SelectItem key={option.id} value={option.id}>
            {option.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
