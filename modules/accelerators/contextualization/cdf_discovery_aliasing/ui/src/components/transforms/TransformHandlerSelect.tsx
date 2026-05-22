import type { CSSProperties } from "react";
import {
  CORE_TRANSFORM_HANDLER_IDS,
  ELT_TRANSFORM_HANDLER_IDS,
  type DiscoveryTransformHandlerId,
} from "../flow/handlerRegistry";

type Props = {
  value: string;
  onChange: (handler: string) => void;
  unsetLabel?: string;
  coreGroupLabel?: string;
  eltGroupLabel?: string;
  className?: string;
  style?: CSSProperties;
};

export function TransformHandlerSelect({
  value,
  onChange,
  unsetLabel,
  coreGroupLabel = "Core",
  eltGroupLabel = "ELT",
  className = "discovery-select",
  style,
}: Props) {
  return (
    <select className={className} style={style} value={value} onChange={(e) => onChange(e.target.value)}>
      {unsetLabel ? <option value="">{unsetLabel}</option> : null}
      <optgroup label={coreGroupLabel}>
        {CORE_TRANSFORM_HANDLER_IDS.map((h) => (
          <option key={h} value={h}>
            {h}
          </option>
        ))}
      </optgroup>
      <optgroup label={eltGroupLabel}>
        {ELT_TRANSFORM_HANDLER_IDS.map((h) => (
          <option key={h} value={h}>
            {h}
          </option>
        ))}
      </optgroup>
    </select>
  );
}

export function isMultiValueTransformHandler(h: string): h is DiscoveryTransformHandlerId {
  return h === "substitution_variants" || h === "split_string";
}
