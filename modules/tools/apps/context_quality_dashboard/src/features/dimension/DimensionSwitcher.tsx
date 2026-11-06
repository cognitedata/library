import { Button } from '@cognite/aura/components/button';
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from '@cognite/aura/components/command';
import { Popover, PopoverContent, PopoverTrigger } from '@cognite/aura/components/popover';
import { IconChevronDown } from '@cognite/aura/icons';
import { useMemo, useState } from 'react';

import {
  decodeDimensionsScopeValue,
  dimensionsScopeSearchValue,
  encodeDimensionsScopeValue,
} from './dimensionScope';
import type { DimensionViewModel } from './useDimensionViewModel';

type DimensionSwitcherProps = {
  viewModel: DimensionViewModel;
};

export function DimensionSwitcher({ viewModel }: DimensionSwitcherProps) {
  const [open, setOpen] = useState(false);
  const label = viewModel.t('dimension.label');

  const modelGroups = useMemo(
    () =>
      viewModel.configuredModels.filter((model) =>
        model.groups.some((group) => group.metrics.length > 0)
      ),
    [viewModel.configuredModels]
  );

  const onSelect = (value: string): void => {
    const modelId =
      modelGroups
        .flatMap((model) => model.groups.map((group) => ({ model, group })))
        .find(({ group }) => encodeDimensionsScopeValue({
          kind: 'configured',
          groupId: group.id,
          modelId: group.modelId,
        }) === value)?.group.modelId ?? undefined;

    const scope = decodeDimensionsScopeValue(value, modelId);
    if (scope === undefined) {
      return;
    }
    viewModel.selectDimensionsScope(scope);
    setOpen(false);
  };

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        render={
          <Button
            variant="outline"
            size="sm"
            aria-label={label}
            data-testid="dimension-switcher"
            className="h-9 w-auto max-w-none shrink-0 justify-between gap-2 overflow-visible rounded-lg border-border bg-background px-3 py-2 font-normal shadow-none"
          >
            <span className="flex shrink-0 items-center gap-2 text-sm">
              <span className="shrink-0 text-muted-foreground">{label}</span>
              <span className="whitespace-nowrap font-medium text-foreground">{viewModel.scopeDisplayValue}</span>
            </span>
            <IconChevronDown className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
          </Button>
        }
      />
      <PopoverContent className="w-80 p-0" align="start">
        <Command>
          <CommandInput placeholder={viewModel.t('dimensions.switcher.searchPlaceholder')} />
          <CommandList>
            <CommandEmpty>{viewModel.t('dimensions.switcher.emptySearch')}</CommandEmpty>
            <CommandGroup heading={viewModel.t('dimensions.scope.overview')}>
              {viewModel.allDimensionIds.map((dimensionId) => {
                const dimensionLabel = viewModel.t(`dimension.${dimensionId}`);
                const value = encodeDimensionsScopeValue({ kind: 'builtin', dimensionId });
                return (
                  <CommandItem
                    key={value}
                    value={dimensionsScopeSearchValue({ dimensionLabel })}
                    data-value={value}
                    onSelect={() => {
                      onSelect(value);
                    }}
                  >
                    {dimensionLabel}
                  </CommandItem>
                );
              })}
            </CommandGroup>
            {modelGroups.length > 0 ? (
              <>
                <CommandSeparator />
                {modelGroups.map((model) => (
                  <CommandGroup key={model.id} heading={model.label}>
                    {model.groups.map((group) => {
                      const value = encodeDimensionsScopeValue({
                        kind: 'configured',
                        groupId: group.id,
                        modelId: group.modelId,
                      });
                      return (
                        <CommandItem
                          key={value}
                          value={dimensionsScopeSearchValue({
                            modelLabel: model.label,
                            viewLabel: group.viewLabel,
                            groupId: group.id,
                          })}
                          data-value={value}
                          onSelect={() => {
                            onSelect(value);
                          }}
                        >
                          {group.viewLabel}
                        </CommandItem>
                      );
                    })}
                  </CommandGroup>
                ))}
              </>
            ) : null}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
