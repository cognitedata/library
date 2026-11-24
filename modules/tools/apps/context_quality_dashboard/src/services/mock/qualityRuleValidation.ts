import type { DataOwner, QualityRuleDraft, QualityRuleValidationError } from '../../types';

export type QualityRuleValidationResult =
  | { valid: true }
  | { valid: false; errors: QualityRuleValidationError[] };

export function validateQualityRuleDraft(
  draft: QualityRuleDraft,
  existingRuleIds: readonly string[],
  owners: readonly DataOwner[]
): QualityRuleValidationResult {
  const errors: QualityRuleValidationError[] = [];

  if (draft.name.trim().length === 0) {
    errors.push('name-required');
  }

  if (draft.sourceView.space.trim().length === 0 || draft.sourceView.externalId.trim().length === 0) {
    errors.push('source-required');
  }

  if (draft.relationship === null) {
    errors.push('relationship-required');
  }

  if (
    draft.condition.operator !== 'minCount' ||
    !Number.isInteger(draft.condition.value) ||
    draft.condition.value < 1
  ) {
    errors.push('min-count-invalid');
  }

  if (draft.ownerId.trim().length === 0) {
    errors.push('owner-required');
  } else if (!owners.some((owner) => owner.id === draft.ownerId)) {
    errors.push('owner-invalid');
  }

  if (draft.ruleId !== undefined && existingRuleIds.includes(draft.ruleId)) {
    const duplicate = existingRuleIds.filter((id) => id === draft.ruleId).length > 1;
    if (duplicate) {
      errors.push('duplicate-rule');
    }
  }

  if (errors.length > 0) {
    return { valid: false, errors };
  }

  return { valid: true };
}

export function isDuplicateQualityRule(
  draft: QualityRuleDraft,
  existingRules: readonly { ruleId: string; sourceView: QualityRuleDraft['sourceView']; relationship: QualityRuleDraft['relationship']; condition: QualityRuleDraft['condition'] }[]
): boolean {
  if (draft.relationship === null) {
    return false;
  }

  return existingRules.some((rule) => {
    if (draft.ruleId !== undefined && rule.ruleId === draft.ruleId) {
      return false;
    }
    return (
      rule.sourceView.space === draft.sourceView.space &&
      rule.sourceView.externalId === draft.sourceView.externalId &&
      rule.relationship?.relationshipId === draft.relationship?.relationshipId &&
      rule.condition.operator === draft.condition.operator &&
      rule.condition.value === draft.condition.value
    );
  });
}
