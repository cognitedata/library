import { describe, expect, it } from 'vitest';

import { getRelationshipsForView } from '../../mock/discovery';

import { computeQualityRulePreview } from './qualityRuleComputation';

const IDI_BOM_SOURCE = { space: 'rmdm', externalId: 'IDI_BOM' };

describe('computeQualityRulePreview', () => {
  const relationships = getRelationshipsForView(IDI_BOM_SOURCE);

  it('returns multiple discovered relationship targets for IDI_BOM', () => {
    const targetNames = relationships.map((entry) => entry.targetDisplayName);
    expect(targetNames).toContain('Asset');
    expect(targetNames).toContain('Equipment');
    expect(targetNames).toContain('Maintenance Order');
    expect(targetNames).toContain('Location');
    expect(targetNames.length).toBeGreaterThanOrEqual(4);
  });

  it('calculates maintenance order coverage preview with passing / eligible semantics', () => {
    const maintenanceOrder = relationships.find(
      (entry) => entry.targetDisplayName === 'Maintenance Order'
    );
    expect(maintenanceOrder).toBeDefined();

    const preview = computeQualityRulePreview(maintenanceOrder!, { operator: 'minCount', value: 1 });

    expect(preview.eligibleCount).toBe(1000);
    expect(preview.passingCount).toBe(740);
    expect(preview.issueCount).toBe(260);
    expect(preview.valuePercent).toBe(74);
  });

  it('calculates asset relationship preview independently', () => {
    const asset = relationships.find((entry) => entry.targetDisplayName === 'Asset');
    expect(asset).toBeDefined();

    const preview = computeQualityRulePreview(asset!, { operator: 'minCount', value: 1 });

    expect(preview.eligibleCount).toBe(1000);
    expect(preview.passingCount).toBe(850);
    expect(preview.valuePercent).toBe(85);
  });

  it('calculates equipment relationship preview independently', () => {
    const equipment = relationships.find((entry) => entry.targetDisplayName === 'Equipment');
    expect(equipment).toBeDefined();

    const preview = computeQualityRulePreview(equipment!, { operator: 'minCount', value: 1 });

    expect(preview.eligibleCount).toBe(1000);
    expect(preview.passingCount).toBe(695);
    expect(preview.valuePercent).toBe(70);
  });

  it('calculates location relationship preview independently', () => {
    const location = relationships.find((entry) => entry.targetDisplayName === 'Location');
    expect(location).toBeDefined();

    const preview = computeQualityRulePreview(location!, { operator: 'minCount', value: 1 });

    expect(preview.eligibleCount).toBe(1000);
    expect(preview.passingCount).toBe(520);
    expect(preview.valuePercent).toBe(52);
  });

  it('uses null valuePercent when eligible count is zero', () => {
    const emptyRelationship = {
      relationshipId: 'empty->target',
      sourceView: IDI_BOM_SOURCE,
      targetView: { space: 'cdf_cdm', externalId: 'Unknown' },
      targetDisplayName: 'Unknown',
      discoveredCount: 0,
    };

    const preview = computeQualityRulePreview(emptyRelationship, { operator: 'minCount', value: 1 });

    expect(preview.eligibleCount).toBe(0);
    expect(preview.passingCount).toBe(0);
    expect(preview.valuePercent).toBeNull();
  });

  it('marks sample rows pass or issue from minCount condition', () => {
    const maintenanceOrder = relationships.find(
      (entry) => entry.targetDisplayName === 'Maintenance Order'
    );
    expect(maintenanceOrder).toBeDefined();

    const preview = computeQualityRulePreview(maintenanceOrder!, { operator: 'minCount', value: 1 });

    expect(preview.sampleRows.some((row) => row.entityLabel === 'BOM-001' && row.status === 'pass')).toBe(
      true
    );
    expect(preview.sampleRows.some((row) => row.entityLabel === 'BOM-002' && row.status === 'issue')).toBe(
      true
    );
  });
});
