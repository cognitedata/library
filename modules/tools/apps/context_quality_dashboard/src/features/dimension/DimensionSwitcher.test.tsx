import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { translate } from '../../i18n/messages';
import type { DimensionId } from '../../types';

import { DimensionSwitcher } from './DimensionSwitcher';
import type { DimensionViewModel } from './useDimensionViewModel';

describe(DimensionSwitcher.name, () => {
  it('shows the translated dimension name in the trigger, not the raw id', () => {
    render(<DimensionSwitcher viewModel={mockViewModel({ dimensionId: 'files' })} />);

    expect(screen.getByLabelText('Dimension')).toHaveTextContent('Files');
    expect(screen.getByLabelText('Dimension')).not.toHaveTextContent('files');
  });

  it('lists Overview dimensions and configured model groups when opened', async () => {
    const user = userEvent.setup();
    render(
      <DimensionSwitcher
        viewModel={mockViewModel({
          dimensionId: 'asset-hierarchy',
          configuredModels: [
            {
              id: 'a:Model:v1',
              label: 'CogniteCore',
              groups: [
                {
                  id: 'a:Model:v1/Widget',
                  modelId: 'a:Model:v1',
                  modelLabel: 'CogniteCore',
                  viewLabel: 'CogniteAsset',
                  metrics: [
                    {
                      label: 'owner',
                      modelId: 'a:Model:v1',
                      view: 'Widget',
                      totalCount: 10,
                      linkedCount: 9,
                      coveragePercent: 90,
                    },
                  ],
                },
              ],
            },
          ],
        })}
      />
    );

    await user.click(screen.getByLabelText('Dimension'));

    expect(screen.getByText('DIMENSIONS OVERVIEW')).toBeInTheDocument();
    expect(screen.getByText('CogniteCore')).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'CogniteAsset' })).toBeInTheDocument();
  });

  it('calls selectDimensionsScope when a configured view is chosen', async () => {
    const user = userEvent.setup();
    const selectDimensionsScope = vi.fn();
    render(
      <DimensionSwitcher
        viewModel={mockViewModel({
          selectDimensionsScope,
          configuredModels: [
            {
              id: 'a:Model:v1',
              label: 'Model',
              groups: [
                {
                  id: 'a:Model:v1/Widget',
                  modelId: 'a:Model:v1',
                  modelLabel: 'Model',
                  viewLabel: 'Widget',
                  metrics: [
                    {
                      label: 'owner',
                      modelId: 'a:Model:v1',
                      view: 'Widget',
                      totalCount: 1,
                      linkedCount: 1,
                      coveragePercent: 100,
                    },
                  ],
                },
              ],
            },
          ],
        })}
      />
    );

    await user.click(screen.getByLabelText('Dimension'));
    await user.click(screen.getByRole('option', { name: 'Widget' }));

    expect(selectDimensionsScope).toHaveBeenCalledWith({
      kind: 'configured',
      groupId: 'a:Model:v1/Widget',
      modelId: 'a:Model:v1',
    });
  });
});

type MockViewModelOptions = {
  dimensionId?: DimensionId;
  configuredModels?: DimensionViewModel['configuredModels'];
  selectDimensionsScope?: DimensionViewModel['selectDimensionsScope'];
  scopeDisplayValue?: string;
  dimensionsScope?: DimensionViewModel['dimensionsScope'];
};

function mockViewModel(options: MockViewModelOptions = {}): DimensionViewModel {
  const dimensionId = options.dimensionId ?? 'asset-hierarchy';
  return {
    dimensionId,
    allDimensionIds: [
      'asset-hierarchy',
      'equipment-asset',
      'time-series',
      'files',
      'annotations',
      'maintenance',
      'model-3d',
    ],
    selectDimension: vi.fn(),
    selectDimensionsScope: options.selectDimensionsScope ?? vi.fn(),
    dimensionsScope: options.dimensionsScope ?? 'builtin',
    configuredModels: options.configuredModels ?? [],
    configuredGroupId: undefined,
    scopeDisplayValue:
      options.scopeDisplayValue ?? translate(`dimension.${dimensionId}`, 'en'),
    t: (key: string) => translate(key, 'en'),
  } as unknown as DimensionViewModel;
}
