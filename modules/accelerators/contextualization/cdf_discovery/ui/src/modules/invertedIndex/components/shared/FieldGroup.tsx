import type { ReactNode } from "react";
import { useInvertedIndexT } from "../../hooks/useInvertedIndexT";

type Props = {
  label?: string;
  hintKey?: string;
  children: ReactNode;
};

export function FieldGroup({ label, hintKey, children }: Props) {
  const { t } = useInvertedIndexT();
  return (
    <div className="idx-field-group">
      {label ? <p className="idx-field-group__label">{label}</p> : null}
      {hintKey ? <p className="idx-field-hint">{t(hintKey)}</p> : null}
      {children}
    </div>
  );
}
