import type { AppConfigDto } from '../types';

import { DEFAULT_EXCLUSION_RULES } from './exclusions';

export const DEFAULT_APP_CONFIG: AppConfigDto = {
  maintenanceViewsAvailable: true,
  failureTaxonomyAvailable: true,
  fileAnnotationAcceleratorAvailable: true,
  lifecycleMetadataAvailable: true,
  model3dAvailable: true,
  fileAnnotationDimensionEnabled: true,
  maintenanceDimensionEnabled: true,
  tieringEnabled: true,
  viewBindings: [
    {
      objectType: 'CogniteAsset',
      space: 'cdf_cdm',
      externalId: 'CogniteAsset',
      version: 'v1',
    },
    {
      objectType: 'CogniteMaintenanceOrder',
      space: 'cdf_idm',
      externalId: 'CogniteMaintenanceOrder',
      version: 'v1',
    },
    {
      objectType: 'CogniteDiagramAnnotation',
      space: 'cdf_cdm',
      externalId: 'CogniteDiagramAnnotation',
      version: 'v1',
    },
  ],
  ownerMappings: [
    { id: 'map-pi-comp3', prefix: 'PI-COMP3-', ownerId: 'instrumentation-lead' },
    { id: 'map-pid', prefix: 'P&ID-', ownerId: 'pid-review-lead' },
    { id: 'map-3d', prefix: '3D-', ownerId: 'model-3d-lead' },
  ],
  exclusionRules: [...DEFAULT_EXCLUSION_RULES],
  scheduleLabel: 'Every Sunday at 02:00 UTC',
  notificationRecipients: [
    'sap.steward@example.test',
    'instrumentation.lead@example.test',
    'pid.review@example.test',
  ],
};
