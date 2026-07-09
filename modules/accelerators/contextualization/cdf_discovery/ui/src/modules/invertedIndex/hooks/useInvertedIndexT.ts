import { useCallback } from "react";
import { useAppSettings } from "../../../context/AppSettingsContext";
import type { MessageKey } from "../../../i18n";

/** Scoped translator for inverted index panes (keys live under ``invertedIndex.*``). */
export function useInvertedIndexT() {
  const { t: baseT, ...rest } = useAppSettings();
  const t = useCallback(
    (key: string, params?: Record<string, string | number>) =>
      baseT(`invertedIndex.${key}` as MessageKey, params),
    [baseT]
  );
  return { t, ...rest };
}
