import type { HostAppAPI } from '@cognite/app-sdk';
import { describe, expect, it, vi } from 'vitest';

import { CDF_CAPABILITIES, FULL_CAPABILITIES } from './backendCapabilities';
import { CdfCatalogService } from './cdf/CdfCatalogService';
import { CdfConfigurationService } from './cdf/CdfConfigurationService';
import { CdfQualityDashboardService } from './cdf/CdfQualityDashboardService';
import { createCdfServices, loadServices } from './createServices';
import { MockCatalogService } from './mock/MockCatalogService';
import { MockConfigurationService } from './mock/MockConfigurationService';
import { MockQualityDashboardService } from './mock/MockQualityDashboardService';

const stream = {
  streamId: 'cq_stream',
  schemaSpace: 'cq_schema',
  relationshipContainer: 'ContextQualityRelationship',
  windowDays: null,
};

function hostApi(): HostAppAPI {
  return {
    getBaseUrl: async () => 'https://cdf.example.test',
    getAccessToken: async () => 'token-123',
    getProject: async () => 'demo',
  } as Partial<HostAppAPI> as HostAppAPI;
}

describe(createCdfServices.name, () => {
  const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ items: [] }));
  const services = createCdfServices({ stream, workflow: null, getHostApi: () => hostApi(), fetchFn, now: () => 0 });

  it('offers no way to start a run unless the deployment names a workflow', () => {
    const named = createCdfServices({
      stream,
      workflow: { externalId: 'cq-wf', version: 'v1' },
      getHostApi: () => hostApi(),
      fetchFn,
      now: () => 0,
    });

    expect(services.runService).toBeNull();
    expect(named.runService).not.toBeNull();
  });

  it('builds only backend-reading services and no mock service', () => {
    expect(services.qualityDashboardService).toBeInstanceOf(CdfQualityDashboardService);
    expect(services.configurationService).toBeInstanceOf(CdfConfigurationService);
    expect(services.catalogService).toBeInstanceOf(CdfCatalogService);
    expect(services.qualityDashboardService).not.toBeInstanceOf(MockQualityDashboardService);
    expect(services.configurationService).not.toBeInstanceOf(MockConfigurationService);
    expect(services.catalogService).not.toBeInstanceOf(MockCatalogService);
  });

  it('declares what the backend can actually do', () => {
    expect(services.backend).toEqual({ kind: 'cdf', capabilities: CDF_CAPABILITIES });
    expect(Object.values(CDF_CAPABILITIES).every((enabled) => !enabled)).toBe(true);
  });

  it('makes no request until something is read', () => {
    expect(fetchFn).not.toHaveBeenCalled();
  });
});

describe(loadServices.name, () => {
  it('builds the backend-reading services for a cdf configuration', async () => {
    const services = await loadServices({ backend: 'cdf', stream, workflow: null });

    expect(services.backend.kind).toBe('cdf');
    expect(services.qualityDashboardService).toBeInstanceOf(CdfQualityDashboardService);
  });

  it('loads the mock services only for an explicit mock configuration', async () => {
    const services = await loadServices({ backend: 'mock' });

    expect(services.backend).toEqual({ kind: 'mock', capabilities: FULL_CAPABILITIES });
    expect(services.qualityDashboardService).toBeInstanceOf(MockQualityDashboardService);
  });
});
