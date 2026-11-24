import type {
  AppConfigDto,
  ExclusionRuleDraft,
  QualityRule,
  QualityRuleDraft,
  QualityRulePreviewResult,
} from '../../types';
import type { ConfigurationService } from '../ConfigurationService';
import type { SharedConfigState } from '../sharedConfigState';

import { BackendError } from './backendErrors';

type Options = {
  configState: SharedConfigState;
};

function unsupported(operation: string): BackendError {
  return new BackendError(
    'unsupported-operation',
    `${operation} is not available: the connected backend has no configuration storage.`,
    { subject: operation }
  );
}

/**
 * The deployed metrics backend stores no configuration, so nothing here is saved. The only setting the dashboard
 * can honour is the tiering view toggle, which only changes how this session reads the scores. Every other change
 * is rejected instead of being accepted and lost.
 */
export class CdfConfigurationService implements ConfigurationService {
  private readonly configState: SharedConfigState;

  constructor(options: Options) {
    this.configState = options.configState;
  }

  async getConfig(): Promise<AppConfigDto> {
    return { ...this.configState.current };
  }

  async updateConfig(patch: Partial<AppConfigDto>): Promise<AppConfigDto> {
    const unsupportedKeys = Object.keys(patch).filter((key) => key !== 'tieringEnabled');
    if (unsupportedKeys.length > 0) {
      throw unsupported(`Changing ${unsupportedKeys.join(', ')}`);
    }
    if (patch.tieringEnabled !== undefined) {
      this.configState.current = { ...this.configState.current, tieringEnabled: patch.tieringEnabled };
    }
    return this.getConfig();
  }

  async previewExclusion(_rule: ExclusionRuleDraft): Promise<{ additionalExcluded: number }> {
    throw unsupported('Exclusion preview');
  }

  async getQualityRules(): Promise<readonly QualityRule[]> {
    return [];
  }

  async createQualityRule(_rule: QualityRuleDraft): Promise<QualityRule> {
    throw unsupported('Creating a quality rule');
  }

  async updateQualityRule(_rule: QualityRule): Promise<QualityRule> {
    throw unsupported('Updating a quality rule');
  }

  async deleteQualityRule(_ruleId: string): Promise<void> {
    throw unsupported('Deleting a quality rule');
  }

  async previewQualityRule(_rule: QualityRuleDraft): Promise<QualityRulePreviewResult> {
    throw unsupported('Previewing a quality rule');
  }
}
