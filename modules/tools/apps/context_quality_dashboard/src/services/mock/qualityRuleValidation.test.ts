import { describe, expect, it } from 'vitest';

import { MOCK_DATASET } from '../../mock/dataset';
import { getRelationshipsForView } from '../../mock/discovery';
import type { QualityRuleDraft } from '../../types';

import { isDuplicateQualityRule, validateQualityRuleDraft } from './qualityRuleValidation';

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
    ownerId: MOCK_DATASET.owners[0].id,
    tier: 1,
    enabled: true,
    ...overrides,
  };
}

describe('validateQualityRuleDraft', () => {
  it('rejects missing source view', () => {
    const result = validateQualityRuleDraft(
      validDraft({ sourceView: { space: '', externalId: '' } }),
      [],
      MOCK_DATASET.owners
    );

    expect(result.valid).toBe(false);
    if (!result.valid) {
      expect(result.errors).toContain('source-required');
    }
  });

  it('rejects missing relationship', () => {
    const result = validateQualityRuleDraft(
      validDraft({ relationship: null }),
      [],
      MOCK_DATASET.owners
    );

    expect(result.valid).toBe(false);
    if (!result.valid) {
      expect(result.errors).toContain('relationship-required');
    }
  });

  it('rejects invalid minCount', () => {
    const result = validateQualityRuleDraft(
      validDraft({ condition: { operator: 'minCount', value: 0 } }),
      [],
      MOCK_DATASET.owners
    );

    expect(result.valid).toBe(false);
    if (!result.valid) {
      expect(result.errors).toContain('min-count-invalid');
    }
  });

  it('accepts a valid draft', () => {
    const result = validateQualityRuleDraft(validDraft(), [], MOCK_DATASET.owners);
    expect(result.valid).toBe(true);
  });
});

describe('isDuplicateQualityRule', () => {
  it('detects duplicate source, relationship, and condition', () => {
    const draft = validDraft();
    const duplicate = isDuplicateQualityRule(draft, [
      {
        ruleId: 'existing-rule',
        sourceView: draft.sourceView,
        relationship: draft.relationship,
        condition: draft.condition,
      },
    ]);

    expect(duplicate).toBe(true);
  });

  it('allows different relationships for the same source', () => {
    const assetRelationship = getRelationshipsForView(IDI_BOM_SOURCE).find(
      (entry) => entry.targetDisplayName === 'Asset'
    )!;
    const draft = validDraft({ relationship: assetRelationship });

    const duplicate = isDuplicateQualityRule(draft, [
      {
        ruleId: 'existing-rule',
        sourceView: draft.sourceView,
        relationship: maintenanceRelationship,
        condition: draft.condition,
      },
    ]);

    expect(duplicate).toBe(false);
  });
});
