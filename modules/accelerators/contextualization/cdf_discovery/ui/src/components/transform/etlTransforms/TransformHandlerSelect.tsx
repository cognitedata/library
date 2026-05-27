import type { CSSProperties } from "react";
import {
  CORE_TRANSFORM_HANDLER_IDS,
  ELT_TRANSFORM_HANDLER_IDS,
  type DiscoveryTransformHandlerId,
} from "../etlHandlerRegistry";

type Props = {
  value: string;
  onChange: (handler: string) => void;
  unsetLabel?: string;
  coreGroupLabel?: string;
  eltGroupLabel?: string;
  className?: string;
  style?: CSSProperties;
};

function HandlerOptions({ ids }: { ids: readonly string[] }) {
  return (
    <>
      {ids.map((h) => (
        <option key={h} value={h}>
          {h}
        </option>
      ))}
    </>
  );
}

export function TransformHandlerSelect({
  value,
  onChange,
  unsetLabel,
  coreGroupLabel,
  eltGroupLabel,
  className = "gov-input",
  style,
}: Props) {
  const coreLabel = coreGroupLabel?.trim() ?? "";
  const eltLabel = eltGroupLabel?.trim() ?? "";

  return (
    <select className={className} style={style} value={value} onChange={(e) => onChange(e.target.value)}>
      {unsetLabel ? <option value="">{unsetLabel}</option> : null}
      {coreLabel ? (
        <optgroup label={coreLabel}>
          <HandlerOptions ids={CORE_TRANSFORM_HANDLER_IDS} />
        </optgroup>
      ) : (
        <HandlerOptions ids={CORE_TRANSFORM_HANDLER_IDS} />
      )}
      {eltLabel ? (
        <optgroup label={eltLabel}>
          <HandlerOptions ids={ELT_TRANSFORM_HANDLER_IDS} />
        </optgroup>
      ) : (
        <HandlerOptions ids={ELT_TRANSFORM_HANDLER_IDS} />
      )}
    </select>
  );
}

export function isMultiValueTransformHandler(h: string): h is DiscoveryTransformHandlerId {
  return h === "substitution_variants" || h === "split_string";
}
