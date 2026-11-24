import type { AppConfigDto, QualityRule } from '../types';

export type SharedConfigState = {
  current: AppConfigDto;
  qualityRules: QualityRule[];
};

export function createSharedConfigState(initial: AppConfigDto): SharedConfigState {
  return {
    current: { ...initial, exclusionRules: [...initial.exclusionRules] },
    qualityRules: [],
  };
}
