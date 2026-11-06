import type {
  Alert,
  AppConfigDto,
  Asset,
  DataOwner,
  Dimension,
  Discipline,
  Division,
  Equipment,
  ExclusionBucket,
  ExclusionRule,
  Facility,
  FileAnnotation,
  FileRecord,
  Insight,
  MaintenanceRecord,
  Model3DObject,
  TimeSeries,
  TrendSnapshot,
} from '../types';

import { MOCK_ALERTS } from './alerts';
import {
  DATA_OWNERS,
  DIMENSIONS,
  DISCIPLINES,
  DIVISIONS,
  FACILITIES,
  OWNER_PREFIX_RULES,
} from './catalog';
import { DEFAULT_APP_CONFIG } from './config';
import {
  MOCK_ASSETS,
  MOCK_EQUIPMENT,
  MOCK_FILE_ANNOTATIONS,
  MOCK_FILES,
  MOCK_MAINTENANCE,
  MOCK_MODEL_3D,
  MOCK_TIME_SERIES,
} from './entities';
import { DEFAULT_EXCLUSION_RULES, SYSTEM_EXCLUSION_BUCKETS } from './exclusions';
import { MOCK_INSIGHTS } from './insights';
import { MOCK_SNAPSHOTS } from './snapshots';
import { MOCK_STATE_FIXTURES, type MockStateFixtures } from './states';

export type MockDataset = {
  facilities: readonly Facility[];
  divisions: readonly Division[];
  disciplines: readonly Discipline[];
  owners: readonly DataOwner[];
  dimensions: readonly Dimension[];
  ownerPrefixRules: readonly { prefix: string; ownerId: string }[];
  assets: readonly Asset[];
  equipment: readonly Equipment[];
  timeSeries: readonly TimeSeries[];
  maintenance: readonly MaintenanceRecord[];
  fileAnnotations: readonly FileAnnotation[];
  model3d: readonly Model3DObject[];
  files: readonly FileRecord[];
  alerts: readonly Alert[];
  insights: readonly Insight[];
  snapshots: readonly TrendSnapshot[];
  systemExclusionBuckets: readonly ExclusionBucket[];
  defaultExclusionRules: readonly ExclusionRule[];
  defaultConfig: AppConfigDto;
  /** Demo-only fixtures for not-applicable metrics, untiered entities and run warnings. */
  states: MockStateFixtures;
};

export const MOCK_DATASET: MockDataset = {
  facilities: FACILITIES,
  divisions: DIVISIONS,
  disciplines: DISCIPLINES,
  owners: DATA_OWNERS,
  dimensions: DIMENSIONS,
  ownerPrefixRules: OWNER_PREFIX_RULES,
  assets: MOCK_ASSETS,
  equipment: MOCK_EQUIPMENT,
  timeSeries: MOCK_TIME_SERIES,
  maintenance: MOCK_MAINTENANCE,
  fileAnnotations: MOCK_FILE_ANNOTATIONS,
  model3d: MOCK_MODEL_3D,
  files: MOCK_FILES,
  alerts: MOCK_ALERTS,
  insights: MOCK_INSIGHTS,
  snapshots: MOCK_SNAPSHOTS,
  systemExclusionBuckets: SYSTEM_EXCLUSION_BUCKETS,
  defaultExclusionRules: DEFAULT_EXCLUSION_RULES,
  defaultConfig: DEFAULT_APP_CONFIG,
  states: MOCK_STATE_FIXTURES,
};

export function countMockEntities(dataset: MockDataset = MOCK_DATASET): Record<string, number> {
  return {
    facilities: dataset.facilities.length,
    divisions: dataset.divisions.length,
    disciplines: dataset.disciplines.length,
    owners: dataset.owners.length,
    dimensions: dataset.dimensions.length,
    assets: dataset.assets.length,
    equipment: dataset.equipment.length,
    timeSeries: dataset.timeSeries.length,
    maintenance: dataset.maintenance.length,
    fileAnnotations: dataset.fileAnnotations.length,
    model3d: dataset.model3d.length,
    files: dataset.files.length,
    alerts: dataset.alerts.length,
    insights: dataset.insights.length,
    snapshots: dataset.snapshots.length,
    exclusionRules: dataset.defaultExclusionRules.length,
  };
}
