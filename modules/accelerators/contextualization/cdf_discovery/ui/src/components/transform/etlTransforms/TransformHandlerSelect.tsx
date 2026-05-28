import type { CSSProperties } from "react";
import {
  TRANSFORM_HANDLER_CATEGORY_DEFS,
  TRANSFORM_HANDLER_DEFINITIONS,
  type DiscoveryTransformHandlerId,
} from "../etlHandlerRegistry";

type Props = {
  value: string;
  onChange: (handler: string) => void;
  unsetLabel?: string;
  className?: string;
  style?: CSSProperties;
  /** @deprecated Use category labels from i18n via TRANSFORM_HANDLER_CATEGORY_DEFS */
  coreGroupLabel?: string;
  /** @deprecated Use category labels from i18n via TRANSFORM_HANDLER_CATEGORY_DEFS */
  eltGroupLabel?: string;
  /** Localized category headings (same order as TRANSFORM_HANDLER_CATEGORY_DEFS). */
  categoryLabels?: Record<string, string>;
};

function handlersForCategory(categoryId: string): DiscoveryTransformHandlerId[] {
  return TRANSFORM_HANDLER_DEFINITIONS.filter((d) => d.category === categoryId).map((d) => d.id);
}

export function TransformHandlerSelect({
  value,
  onChange,
  unsetLabel,
  className = "gov-input",
  style,
  categoryLabels,
}: Props) {
  return (
    <select className={className} style={style} value={value} onChange={(e) => onChange(e.target.value)}>
      {unsetLabel ? <option value="">{unsetLabel}</option> : null}
      {TRANSFORM_HANDLER_CATEGORY_DEFS.map((cat) => {
        const ids = handlersForCategory(cat.id);
        if (!ids.length) return null;
        const label = categoryLabels?.[cat.id]?.trim() || cat.id;
        return (
          <optgroup key={cat.id} label={label}>
            {ids.map((h) => (
              <option key={h} value={h}>
                {h}
              </option>
            ))}
          </optgroup>
        );
      })}
    </select>
  );
}

export function isMultiValueTransformHandler(h: string): h is DiscoveryTransformHandlerId {
  return h === "substitution_variants" || h === "split_string";
}
