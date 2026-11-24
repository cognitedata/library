import type { ReactNode } from 'react';

import type { FilterKey } from '../../app/state/types';
import type { SliceFilters, TrendWindow } from '../../types';

import { appContentHorizontalInsetClassName } from './appContentHorizontalInset';
import { FilterDropdownTile } from './FilterDropdownTile';
import {
  FILTER_ALL,
  formatFilterSelectLabel,
  fromFilterSelectValue,
  toFilterSelectValue,
} from './formatFilterSelectLabel';
import { isTrendWindowValue, TREND_WINDOWS } from './pageIds';
import type { FilterOption } from './useAppShellViewModel';

type FilterBarProps = {
  filters: SliceFilters;
  spaces: FilterOption[];
  /** The locations to choose from. The tile is only shown when there are some. */
  locations?: FilterOption[];
  trendWindow: TrendWindow;
  isLoading: boolean;
  onFilterChange: (key: FilterKey, value?: string) => void;
  onTrendWindowChange: (window: TrendWindow) => void;
  trailing?: ReactNode;
  leading?: ReactNode;
  showFilterFields?: boolean;
  t: (key: string) => string;
};

export function FilterBar({
  filters,
  spaces,
  locations = [],
  trendWindow,
  isLoading,
  onFilterChange,
  onTrendWindowChange,
  trailing,
  leading,
  showFilterFields = true,
  t,
}: FilterBarProps) {
  return (
    <section
      aria-label="Global filters"
      className={`flex flex-wrap items-center justify-between gap-4 border-b border-border bg-card-background py-4 lg:flex-nowrap ${appContentHorizontalInsetClassName}`}
    >
      {showFilterFields ? (
      <div className="flex flex-wrap items-end gap-3 lg:flex-nowrap">
        <FilterTile
          label={t('filter.space')}
          value={toFilterSelectValue(filters.facilityId)}
          options={spaces}
          disabled={isLoading}
          allLabel={t('filter.all')}
          onValueChange={(value) => onFilterChange('facilityId', fromFilterSelectValue(value))}
        />
        {locations.length > 0 ? (
          <FilterTile
            label={t('filter.location')}
            value={toFilterSelectValue(filters.locationId)}
            options={locations}
            disabled={isLoading}
            allLabel={t('filter.all')}
            onValueChange={(value) => onFilterChange('locationId', fromFilterSelectValue(value))}
          />
        ) : null}
        <TrendWindowTile
          label={t('trend.window')}
          trendWindow={trendWindow}
          disabled={isLoading}
          onTrendWindowChange={onTrendWindowChange}
          t={t}
        />
      </div>
      ) : leading !== undefined ? (
        leading
      ) : (
        <div />
      )}

      <div className="flex flex-wrap items-center gap-4">{trailing}</div>
    </section>
  );
}

type FilterTileProps = {
  label: string;
  value: string;
  options: FilterOption[];
  allLabel: string;
  disabled: boolean;
  onValueChange: (value: string) => void;
};

function FilterTile({
  label,
  value,
  options,
  allLabel,
  disabled,
  onValueChange,
}: FilterTileProps) {
  const displayValue = formatFilterSelectLabel(value, options, allLabel);
  const items = [
    { value: FILTER_ALL, label: allLabel },
    ...options.map((option) => ({ value: option.id, label: option.label })),
  ];

  return (
    <FilterDropdownTile
      label={label}
      value={value}
      displayValue={displayValue}
      disabled={disabled}
      onValueChange={onValueChange}
      items={items}
    />
  );
}

type TrendWindowTileProps = {
  label: string;
  trendWindow: TrendWindow;
  disabled: boolean;
  onTrendWindowChange: (window: TrendWindow) => void;
  t: (key: string) => string;
};

function TrendWindowTile({
  label,
  trendWindow,
  disabled,
  onTrendWindowChange,
  t,
}: TrendWindowTileProps) {
  const items = TREND_WINDOWS.map((window) => ({
    value: window,
    label: t(`trend.${window}`),
  }));

  return (
    <FilterDropdownTile
      label={label}
      value={trendWindow}
      displayValue={t(`trend.${trendWindow}`)}
      disabled={disabled}
      onValueChange={(value) => {
        if (isTrendWindowValue(value)) {
          onTrendWindowChange(value);
        }
      }}
      items={items}
    />
  );
}
