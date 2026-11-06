import type { AppConfigDto } from '../../types';

/**
 * The starting settings when the dashboard reads a deployed backend. They describe nothing about any project:
 * no owner mappings, exclusion rules, recipients or schedule. Only `tieringEnabled` is a real view setting, and it starts off: nothing is scored by tier until it is turned on.
 */
export const NEUTRAL_CONFIG: AppConfigDto = {
  maintenanceViewsAvailable: false,
  failureTaxonomyAvailable: false,
  fileAnnotationAcceleratorAvailable: false,
  lifecycleMetadataAvailable: false,
  model3dAvailable: false,
  fileAnnotationDimensionEnabled: false,
  maintenanceDimensionEnabled: false,
  tieringEnabled: false,
  viewBindings: [],
  ownerMappings: [],
  exclusionRules: [],
  scheduleLabel: '',
  notificationRecipients: [],
};
