import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  INPUT_A_LABEL_CONFIG_KEY,
  INPUT_B_LABEL_CONFIG_KEY,
  type DualInputLabelDefaults,
} from "../../utils/dualInputConnectorLabels";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  defaults: DualInputLabelDefaults;
};

function labelField(
  value: JsonObject,
  onChange: (next: JsonObject) => void,
  configKey: typeof INPUT_A_LABEL_CONFIG_KEY | typeof INPUT_B_LABEL_CONFIG_KEY,
  fieldLabel: string,
  hint: string,
  placeholder: string
) {
  const display = String(value[configKey] ?? "");
  return (
    <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
      {fieldLabel}
      <input
        className="gov-input"
        style={{ marginTop: "0.35rem" }}
        value={display}
        placeholder={placeholder}
        onChange={(e) => {
          const next = { ...value };
          const v = e.target.value;
          if (!v.trim()) {
            delete next[configKey];
          } else {
            next[configKey] = v;
          }
          onChange(next);
        }}
        spellCheck={false}
        autoComplete="off"
      />
      <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
        {hint}
      </span>
    </label>
  );
}

export function DualInputConnectorLabelFields({ value, onChange, defaults }: Props) {
  const { t } = useAppSettings();

  return (
    <fieldset className="transform-node-editor-fields__section" style={{ marginTop: "1rem" }}>
      <legend>{t("transform.dualInput.sectionConnectorLabels")}</legend>
      {labelField(
        value,
        onChange,
        INPUT_A_LABEL_CONFIG_KEY,
        t("transform.dualInput.inputALabel"),
        t("transform.dualInput.inputALabelHint"),
        t(defaults.inputA)
      )}
      {labelField(
        value,
        onChange,
        INPUT_B_LABEL_CONFIG_KEY,
        t("transform.dualInput.inputBLabel"),
        t("transform.dualInput.inputBLabelHint"),
        t(defaults.inputB)
      )}
    </fieldset>
  );
}
