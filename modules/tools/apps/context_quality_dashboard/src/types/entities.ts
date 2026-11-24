import type { Tier } from './tier';

export type EntitySlice = {
  facilityId: string;
  divisionId: string;
  disciplineId: string;
  ownerId: string;
  tier: Tier;
};

export type Facility = {
  id: string;
  name: string;
};

/** A location filter of the project that covers whole facilities: a named group of them, possibly inside a parent. */
export type Location = {
  id: string;
  name: string;
  parentId: string | null;
  facilityIds: readonly string[];
};

export type Division = {
  id: string;
  name: string;
};

export type Discipline = {
  id: string;
  name: string;
};

export type DataOwner = {
  id: string;
  name: string;
  contactLabel: string;
};

export type Dimension = {
  id: string;
  name: string;
  description: string;
  /** Whether the dimension appears in overview and navigation when enabled in config. */
  optional: boolean;
};

export type AssetStatus = 'active' | 'retired';

export type Asset = EntitySlice & {
  id: string;
  externalId: string;
  name: string;
  parentId: string | null;
  assetType: string | null;
  status: AssetStatus;
  hierarchyDepth: number;
  isOrphan: boolean;
};

export type EquipmentStatus = 'active' | 'decommissioned';

export type Equipment = EntitySlice & {
  id: string;
  externalId: string;
  name: string;
  equipmentType: string;
  linkedAssetId: string | null;
  manufacturer: string;
  serialNumber: string | null;
  status: EquipmentStatus;
  candidateAssetId: string | null;
  candidateMatchPercent: number | null;
};

export type TimeSeriesKind = 'physical' | 'calculated' | 'virtual';

export type TimeSeries = EntitySlice & {
  id: string;
  externalId: string;
  name: string;
  tagPrefix: string;
  linkedAssetId: string | null;
  kind: TimeSeriesKind;
  lastUpdatedAt: string;
  inferredTag: string | null;
  inferredFromField: 'name' | 'description' | null;
};

export type MaintenanceKind = 'work-order' | 'notification';

export type MaintenanceRecord = EntitySlice & {
  id: string;
  externalId: string;
  kind: MaintenanceKind;
  linkedAssetId: string | null;
  linkedEquipmentId: string | null;
  linkedNotificationId: string | null;
  status: string;
  completed: boolean;
};

export type AnnotationStatus = 'approved' | 'rejected' | 'pending-review' | 'suggested';

export type FileAnnotation = EntitySlice & {
  id: string;
  fileId: string;
  fileName: string;
  annotationType: string;
  confidence: number;
  status: AnnotationStatus;
  linkedAssetId: string | null;
  patternDetectedTag: string | null;
  ageDays: number;
};

export type Model3DType = 'CAD' | 'PointCloud' | 'Image360';

export type Model3DObject = EntitySlice & {
  id: string;
  externalId: string;
  name: string;
  linkedAssetId: string | null;
  modelType: Model3DType;
  bboxComplete: boolean;
};

export type FileRecord = EntitySlice & {
  id: string;
  externalId: string;
  name: string;
  category: string | null;
  linkedAssetId: string | null;
  isUploaded: boolean;
  isLocationCentric: boolean;
  userExcluded: boolean;
};

export type MaintenanceRecordEntity = MaintenanceRecord;
