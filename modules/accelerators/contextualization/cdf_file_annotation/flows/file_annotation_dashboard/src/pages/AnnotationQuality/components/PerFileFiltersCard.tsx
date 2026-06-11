import { ArrowUpDown, Filter, Search } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Input } from "@/shared/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/shared/components/ui/select";
import type { PipelineConfig } from "@/shared/utils/types";

interface FilterOption {
  value: string;
  label: string;
}

interface PerFileFiltersCardProps {
  config: PipelineConfig | null;
  searchQuery: string;
  onSearchQueryChange: (value: string) => void;
  coverageRange: string;
  onCoverageRangeChange: (value: string) => void;
  sortOption: string;
  onSortOptionChange: (value: string) => void;
  resourceTypeFilter: string;
  onResourceTypeFilterChange: (value: string) => void;
  primaryScopeFilter: string;
  onPrimaryScopeFilterChange: (value: string) => void;
  secondaryScopeFilter: string;
  onSecondaryScopeFilterChange: (value: string) => void;
  coverageOptions: FilterOption[];
  sortOptions: FilterOption[];
  resourceTypeOptions: FilterOption[];
  primaryScopeOptions: FilterOption[];
  secondaryScopeOptions: FilterOption[];
}

export function PerFileFiltersCard({
  config,
  searchQuery,
  onSearchQueryChange,
  coverageRange,
  onCoverageRangeChange,
  sortOption,
  onSortOptionChange,
  resourceTypeFilter,
  onResourceTypeFilterChange,
  primaryScopeFilter,
  onPrimaryScopeFilterChange,
  secondaryScopeFilter,
  onSecondaryScopeFilterChange,
  coverageOptions,
  sortOptions,
  resourceTypeOptions,
  primaryScopeOptions,
  secondaryScopeOptions,
}: PerFileFiltersCardProps) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm font-semibold flex items-center gap-2">
          <Filter className="h-4 w-4 text-muted-foreground" />
          Filters
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap gap-3">
          <div className="space-y-1.5 flex-1 min-w-[200px] max-w-[320px]">
            <label className="text-xs text-muted-foreground font-medium">Search Files</label>
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
              <Input
                placeholder="Search by file name or ID..."
                value={searchQuery}
                onChange={(e) => onSearchQueryChange(e.target.value)}
                className="h-8 text-xs"
              />
            </div>
          </div>
          <div className="space-y-1.5 min-w-[160px]">
            <label className="text-xs text-muted-foreground font-medium">Coverage Range</label>
            <Select value={coverageRange} onValueChange={onCoverageRangeChange}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {coverageOptions.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1.5 min-w-[180px]">
            <label className="text-xs text-muted-foreground font-medium flex items-center gap-1">
              <ArrowUpDown className="h-3 w-3" />
              Sort By
            </label>
            <Select value={sortOption} onValueChange={onSortOptionChange}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {sortOptions.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        <div className="flex flex-wrap gap-3 pt-2 border-t">
          <div className="space-y-1.5 min-w-[160px]">
            <label className="text-xs text-muted-foreground font-medium">
              {config?.fileResourceProperty || "File Resource Property"}
            </label>
            <Select value={resourceTypeFilter} onValueChange={onResourceTypeFilterChange}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {resourceTypeOptions.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          {config?.primaryScopeProperty && (
            <div className="space-y-1.5 min-w-[160px]">
              <label className="text-xs text-muted-foreground font-medium">
                {config.primaryScopeProperty}
              </label>
              <Select value={primaryScopeFilter} onValueChange={onPrimaryScopeFilterChange}>
                <SelectTrigger className="h-8 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {primaryScopeOptions.map((opt) => (
                    <SelectItem key={opt.value} value={opt.value}>
                      {opt.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}
          {config?.secondaryScopeProperty && (
            <div className="space-y-1.5 min-w-[160px]">
              <label className="text-xs text-muted-foreground font-medium">
                {config.secondaryScopeProperty}
              </label>
              <Select value={secondaryScopeFilter} onValueChange={onSecondaryScopeFilterChange}>
                <SelectTrigger className="h-8 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {secondaryScopeOptions.map((opt) => (
                    <SelectItem key={opt.value} value={opt.value}>
                      {opt.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
