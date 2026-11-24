import { MOCK_DATASET } from '../../mock/dataset';
import { FULL_CAPABILITIES } from '../backendCapabilities';
import type { AppServices } from '../createServices';
import { createSharedConfigState } from '../sharedConfigState';

import type { LatencyMode } from './latency';
import { MockCatalogService } from './MockCatalogService';
import { MockConfigurationService } from './MockConfigurationService';
import { MockExportService } from './MockExportService';
import { MockQualityDashboardService } from './MockQualityDashboardService';
import { MockRunService } from './MockRunService';

export type CreateMockServicesOptions = {
  latency?: LatencyMode;
};

/** The demo dataset behind the same service interfaces. Only used when the deployment asks for it explicitly. */
export function createMockServices(options: CreateMockServicesOptions = {}): AppServices {
  const latency = options.latency ?? 'random';
  const configState = createSharedConfigState(MOCK_DATASET.defaultConfig);

  return {
    qualityDashboardService: new MockQualityDashboardService({
      dataset: MOCK_DATASET,
      getConfig: () => configState.current,
      getQualityRules: () => configState.qualityRules,
      latency,
    }),
    configurationService: new MockConfigurationService({ configState, latency }),
    catalogService: new MockCatalogService({ latency }),
    exportService: new MockExportService(),
    runService: new MockRunService(),
    backend: { kind: 'mock', capabilities: FULL_CAPABILITIES },
  };
}
