import type { MessageKey } from "../i18n/types";
import { NAMING_DIMENSION_PRESET_ORDER } from "../types/governanceConfig";

type Translate = (key: MessageKey, vars?: Record<string, string | number>) => string;

const PRESET_KEYS = new Set<string>(NAMING_DIMENSION_PRESET_ORDER);

/** User-facing label for a dimension YAML key (preset name or key). */
export function governanceDimensionLabel(key: string, t: Translate): string {
  if (PRESET_KEYS.has(key)) {
    return t(`dimensions.presetName.${key}` as MessageKey);
  }
  return t("dimensions.keyDisplay", { key });
}
