/**
 * What the connected backend can actually do. The interface of the dashboard services is wider than what the
 * deployed metrics backend implements, so the UI asks here before it offers an action or a page.
 */
export type BackendCapabilities = {
  /** The backend computes and stores alerts. */
  alerts: boolean;
  /** The backend computes and stores insights. */
  insights: boolean;
  /** The backend can notify an owner about entities. */
  notifyOwner: boolean;
  /** The backend can start a metrics run for one dimension or metric. */
  rerun: boolean;
  /** Exclusion rules, owner mappings, feature flags and recipients can be saved. */
  configurationEditing: boolean;
  /** Customer-defined quality rules can be saved and evaluated. */
  qualityRules: boolean;
};

export type BackendInfo = {
  kind: 'cdf' | 'mock';
  capabilities: BackendCapabilities;
};

export const FULL_CAPABILITIES: BackendCapabilities = {
  alerts: true,
  insights: true,
  notifyOwner: true,
  rerun: true,
  configurationEditing: true,
  qualityRules: true,
};

/** What the deployed metrics stream provides today: read-only scores, trends, issues and classifications. */
export const CDF_CAPABILITIES: BackendCapabilities = {
  alerts: false,
  insights: false,
  notifyOwner: false,
  rerun: false,
  configurationEditing: false,
  qualityRules: false,
};
