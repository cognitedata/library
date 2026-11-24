import type {
  AppConfigDto,
  ExclusionRuleDraft,
  QualityRule,
  QualityRuleDraft,
  QualityRulePreviewResult,
} from '../types';

export interface ConfigurationService {
  getConfig(): Promise<AppConfigDto>;
  updateConfig(patch: Partial<AppConfigDto>): Promise<AppConfigDto>;
  previewExclusion(rule: ExclusionRuleDraft): Promise<{ additionalExcluded: number }>;
  getQualityRules(): Promise<readonly QualityRule[]>;
  createQualityRule(rule: QualityRuleDraft): Promise<QualityRule>;
  updateQualityRule(rule: QualityRule): Promise<QualityRule>;
  deleteQualityRule(ruleId: string): Promise<void>;
  previewQualityRule(rule: QualityRuleDraft): Promise<QualityRulePreviewResult>;
}
