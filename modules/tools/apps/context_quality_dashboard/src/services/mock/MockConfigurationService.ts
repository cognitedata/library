import { MOCK_DATASET } from '../../mock/dataset';
import { EXCLUSION_PREVIEW_DELTAS } from '../../mock/exclusions';
import type {
  AppConfigDto,
  ExclusionRuleDraft,
  QualityRule,
  QualityRuleDraft,
  QualityRulePreviewResult,
} from '../../types';
import type { ConfigurationService } from '../ConfigurationService';
import type { SharedConfigState } from '../sharedConfigState';

import { withLatency, type LatencyMode } from './latency';
import { computeQualityRulePreview } from './qualityRuleComputation';
import {
  isDuplicateQualityRule,
  validateQualityRuleDraft,
} from './qualityRuleValidation';

type MockConfigurationServiceOptions = {
  configState: SharedConfigState;
  latency: LatencyMode;
};

function createRuleId(name: string): string {
  const slug = name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
  return `quality-rule-${slug}-${Date.now()}`;
}

function toQualityRule(draft: QualityRuleDraft, ruleId: string): QualityRule {
  if (draft.relationship === null) {
    throw new Error('Relationship is required to save a quality rule');
  }

  return {
    ruleId,
    name: draft.name.trim(),
    type: draft.type,
    sourceView: draft.sourceView,
    relationship: draft.relationship,
    condition: draft.condition,
    severity: draft.severity,
    ownerId: draft.ownerId,
    tier: draft.tier,
    enabled: draft.enabled,
  };
}

export class MockConfigurationService implements ConfigurationService {
  private readonly configState: SharedConfigState;
  private readonly latency: LatencyMode;

  constructor(options: MockConfigurationServiceOptions) {
    this.configState = options.configState;
    this.latency = options.latency;
  }

  getConfig(): Promise<AppConfigDto> {
    return withLatency(() => this.cloneConfig(), this.latency);
  }

  updateConfig(patch: Partial<AppConfigDto>): Promise<AppConfigDto> {
    return withLatency(() => {
      this.configState.current = {
        ...this.configState.current,
        ...patch,
        exclusionRules: patch.exclusionRules ?? this.configState.current.exclusionRules,
        viewBindings: patch.viewBindings ?? this.configState.current.viewBindings,
        ownerMappings: patch.ownerMappings ?? this.configState.current.ownerMappings,
      };
      return this.cloneConfig();
    }, this.latency);
  }

  previewExclusion(rule: ExclusionRuleDraft): Promise<{ additionalExcluded: number }> {
    return withLatency(() => {
      const delta = EXCLUSION_PREVIEW_DELTAS[rule.pattern] ?? 2;
      return { additionalExcluded: delta };
    }, this.latency);
  }

  getQualityRules(): Promise<readonly QualityRule[]> {
    return withLatency(() => this.configState.qualityRules.map((rule) => ({ ...rule })), this.latency);
  }

  createQualityRule(draft: QualityRuleDraft): Promise<QualityRule> {
    return withLatency(() => {
      const validation = validateQualityRuleDraft(
        draft,
        this.configState.qualityRules.map((rule) => rule.ruleId),
        MOCK_DATASET.owners
      );
      if (!validation.valid) {
        throw new Error(`Invalid quality rule: ${validation.errors.join(', ')}`);
      }

      if (isDuplicateQualityRule(draft, this.configState.qualityRules)) {
        throw new Error('A quality rule with the same source, relationship, and condition already exists');
      }

      const rule = toQualityRule(draft, createRuleId(draft.name));
      this.configState.qualityRules = [...this.configState.qualityRules, rule];
      return { ...rule };
    }, this.latency);
  }

  updateQualityRule(rule: QualityRule): Promise<QualityRule> {
    return withLatency(() => {
      const draft: QualityRuleDraft = {
        ruleId: rule.ruleId,
        name: rule.name,
        type: rule.type,
        sourceView: rule.sourceView,
        relationship: rule.relationship,
        condition: rule.condition,
        severity: rule.severity,
        ownerId: rule.ownerId,
        tier: rule.tier,
        enabled: rule.enabled,
      };

      const validation = validateQualityRuleDraft(
        draft,
        this.configState.qualityRules.map((entry) => entry.ruleId),
        MOCK_DATASET.owners
      );
      if (!validation.valid) {
        throw new Error(`Invalid quality rule: ${validation.errors.join(', ')}`);
      }

      const others = this.configState.qualityRules.filter((entry) => entry.ruleId !== rule.ruleId);
      if (isDuplicateQualityRule(draft, others)) {
        throw new Error('A quality rule with the same source, relationship, and condition already exists');
      }

      const next = { ...rule };
      this.configState.qualityRules = this.configState.qualityRules.map((entry) =>
        entry.ruleId === rule.ruleId ? next : entry
      );
      return { ...next };
    }, this.latency);
  }

  deleteQualityRule(ruleId: string): Promise<void> {
    return withLatency(() => {
      this.configState.qualityRules = this.configState.qualityRules.filter(
        (rule) => rule.ruleId !== ruleId
      );
    }, this.latency);
  }

  previewQualityRule(draft: QualityRuleDraft): Promise<QualityRulePreviewResult> {
    return withLatency(() => {
      const validation = validateQualityRuleDraft(
        draft,
        draft.ruleId !== undefined ? [draft.ruleId] : [],
        MOCK_DATASET.owners
      );
      if (!validation.valid) {
        throw new Error(`Invalid quality rule: ${validation.errors.join(', ')}`);
      }

      if (draft.relationship === null) {
        throw new Error('Relationship is required to preview a quality rule');
      }

      return computeQualityRulePreview(draft.relationship, draft.condition);
    }, this.latency);
  }

  private cloneConfig(): AppConfigDto {
    return {
      ...this.configState.current,
      exclusionRules: [...this.configState.current.exclusionRules],
      viewBindings: [...this.configState.current.viewBindings],
      ownerMappings: [...this.configState.current.ownerMappings],
      notificationRecipients: [...this.configState.current.notificationRecipients],
    };
  }
}
