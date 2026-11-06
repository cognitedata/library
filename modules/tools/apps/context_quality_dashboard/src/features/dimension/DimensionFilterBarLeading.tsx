import { DimensionSwitcher } from './DimensionSwitcher';
import { useDimensionViewModel } from './useDimensionViewModel';

export function DimensionFilterBarLeading() {
  const viewModel = useDimensionViewModel();
  return <DimensionSwitcher viewModel={viewModel} />;
}
