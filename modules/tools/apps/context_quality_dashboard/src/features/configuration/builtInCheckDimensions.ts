import type { DimensionId } from '../../types';

/** Built-in contextualisation dimensions measured without a YAML configuration file. */
export const BUILT_IN_CHECK_DIMENSION_IDS: readonly DimensionId[] = [
  'asset-hierarchy',
  'equipment-asset',
  'time-series',
  'maintenance',
  'file-annotation',
  'model-3d',
  'files',
];
