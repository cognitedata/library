import type { ReactNode } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n";

type Props = {
  label?: string;
  hintKey?: MessageKey;
  children: ReactNode;
};

export function FieldGroup({ label, hintKey, children }: Props) {
  const { t } = useAppSettings();
  return (
    <div className="idx-field-group">
      {label ? <p className="idx-field-group__label">{label}</p> : null}
      {hintKey ? <p className="idx-field-hint">{t(hintKey)}</p> : null}
      {children}
    </div>
  );
}
