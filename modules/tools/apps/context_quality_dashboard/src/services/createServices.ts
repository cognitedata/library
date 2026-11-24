import type { HostAppAPI } from '@cognite/app-sdk';

import { hostApiHolder } from '../app/hostApiHolder';

import { CDF_CAPABILITIES, type BackendInfo } from './backendCapabilities';
import type { CatalogService } from './CatalogService';
import { CdfCatalogService } from './cdf/CdfCatalogService';
import { CdfConfigurationService } from './cdf/CdfConfigurationService';
import { CdfQualityDashboardService } from './cdf/CdfQualityDashboardService';
import { HttpExportService } from './cdf/HttpExportService';
import { HttpRunService } from './cdf/HttpRunService';
import { NEUTRAL_CONFIG } from './cdf/neutralConfig';
import { QualityStreamReader } from './cdf/QualityStreamReader';
import { HttpRecordsClient } from './cdf/recordsClient';
import type { BackendConfig, StreamConfig, WorkflowConfig } from './cdf/streamConfig';
import { StreamPreflight } from './cdf/streamPreflight';
import type { ConfigurationService } from './ConfigurationService';
import type { ExportService } from './ExportService';
import type { QualityDashboardService } from './QualityDashboardService';
import type { RunService } from './RunService';
import { createSharedConfigState } from './sharedConfigState';

export type AppServices = {
  qualityDashboardService: QualityDashboardService;
  configurationService: ConfigurationService;
  catalogService: CatalogService;
  exportService: ExportService;
  /** Starts a metrics run. Null when the deployment names no workflow, so the dashboard only reads. */
  runService: RunService | null;
  backend: BackendInfo;
};

type CdfServicesDeps = {
  stream: StreamConfig;
  workflow: WorkflowConfig | null;
  getHostApi: () => HostAppAPI | null;
  fetchFn: typeof fetch;
  now: () => number;
};

/**
 * Services that read an already deployed backend through the Records API. Nothing in here creates, changes or
 * deletes backend resources, and nothing from the mock layer is imported or constructed.
 */
export function createCdfServices(deps: CdfServicesDeps): AppServices {
  const configState = createSharedConfigState(NEUTRAL_CONFIG);
  const preflight = new StreamPreflight({
    getHostApi: deps.getHostApi,
    fetchFn: deps.fetchFn,
    config: deps.stream,
  });
  const reader = new QualityStreamReader(
    new HttpRecordsClient({
      getHostApi: deps.getHostApi,
      fetchFn: deps.fetchFn,
      config: deps.stream,
      now: deps.now,
      windowDays: () => preflight.windowDays(),
    }),
    deps.now
  );
  return {
    qualityDashboardService: new CdfQualityDashboardService({
      reader,
      now: deps.now,
      getTieringEnabled: () => configState.current.tieringEnabled,
    }),
    configurationService: new CdfConfigurationService({ configState }),
    catalogService: new CdfCatalogService({ reader }),
    exportService: new HttpExportService({
      getHostApi: deps.getHostApi,
      fetchFn: deps.fetchFn,
      latestRunId: async () => (await reader.latestRun()).runId,
    }),
    runService:
      deps.workflow === null
        ? null
        : new HttpRunService({ getHostApi: deps.getHostApi, fetchFn: deps.fetchFn, workflow: deps.workflow }),
    backend: { kind: 'cdf', capabilities: CDF_CAPABILITIES },
  };
}

/**
 * Builds the services for a resolved deployment configuration. The mock layer is loaded on demand, so a build
 * that reads a deployed backend never executes it.
 */
export async function loadServices(config: BackendConfig): Promise<AppServices> {
  if (config.backend === 'mock') {
    const { createMockServices } = await import('./mock/createMockServices');
    return createMockServices();
  }
  return createCdfServices({
    stream: config.stream,
    workflow: config.workflow,
    getHostApi: () => hostApiHolder.current,
    fetchFn: (input, init) => fetch(input, init),
    now: () => Date.now(),
  });
}
