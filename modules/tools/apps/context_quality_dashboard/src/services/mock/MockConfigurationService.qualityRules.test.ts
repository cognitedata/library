import { describe, expect, it } from 'vitest';

import { DEFAULT_APP_CONFIG } from '../../mock/config';
import { getRelationshipsForView } from '../../mock/discovery';
import type { QualityRuleDraft } from '../../types';
import type { SharedConfigState } from '../sharedConfigState';

import { MockConfigurationService } from './MockConfigurationService';

function createService(): {
  service: MockConfigurationService;
  configState: SharedConfigState;
} {
  const configState: SharedConfigState = {
    current: { ...DEFAULT_APP_CONFIG },
    qualityRules: [],
  };
  const service = new MockConfigurationService({ configState, latency: 0 });
  return { service, configState };
}

const IDI_BOM_SOURCE = { space: 'rmdm', externalId: 'IDI_BOM' };
const maintenanceRelationship = getRelationshipsForView(IDI_BOM_SOURCE).find(
  (entry) => entry.targetDisplayName === 'Maintenance Order'
)!;

function validDraft(overrides: Partial<QualityRuleDraft> = {}): QualityRuleDraft {
  return {
    name: 'IDI BOM Maintenance Order Coverage',
    type: 'relationship-coverage',
    sourceView: IDI_BOM_SOURCE,
    relationship: maintenanceRelationship,
    condition: { operator: 'minCount', value: 1 },
    severity: 'critical',
    ownerId: 'sap-steward',
    tier: 1,
    enabled: true,
    ...overrides,
  };
}

describe('MockConfigurationService quality rules', () => {
  it('creates, updates, toggles, and deletes quality rules in mock state', async () => {
    const { service, configState } = createService();

    const created = await service.createQualityRule(validDraft());
    expect(created.ruleId.length).toBeGreaterThan(0);
    expect(configState.qualityRules).toHaveLength(1);

    const updated = await service.updateQualityRule({
      ...created,
      name: 'Updated rule name',
      enabled: false,
    });
    expect(updated.name).toBe('Updated rule name');
    expect(updated.enabled).toBe(false);

    await service.deleteQualityRule(created.ruleId);
    expect(configState.qualityRules).toHaveLength(0);
  });

  it('previews quality rule metrics using passing / eligible semantics', async () => {
    const { service } = createService();

    const preview = await service.previewQualityRule(validDraft());

    expect(preview.eligibleCount).toBe(1000);
    expect(preview.passingCount).toBe(740);
    expect(preview.issueCount).toBe(260);
    expect(preview.valuePercent).toBe(74);
  });
});
