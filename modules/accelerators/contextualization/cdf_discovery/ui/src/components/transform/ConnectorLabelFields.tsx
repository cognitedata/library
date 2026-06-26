import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n";
import type { JsonObject } from "../../types/jsonConfig";
import {
  INPUT_A_LABEL_CONFIG_KEY,
  INPUT_B_LABEL_CONFIG_KEY,
  INPUT_LABEL_CONFIG_KEY,
  OUTPUT_LABEL_CONFIG_KEY,
} from "../../utils/dualInputConnectorLabels";

type ConnectorConfigKey =
  | typeof INPUT_LABEL_CONFIG_KEY
  | typeof OUTPUT_LABEL_CONFIG_KEY
  | typeof INPUT_A_LABEL_CONFIG_KEY
  | typeof INPUT_B_LABEL_CONFIG_KEY;

type ConnectorLabelFieldDef = {
  configKey: ConnectorConfigKey;
  fieldLabelKey: MessageKey;
  hintKey: MessageKey;
  placeholderKey: MessageKey;
  deleteWhenEmpty?: boolean;
  marginTop?: string;
};

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  sectionTitleKey?: MessageKey;
  fields?: ConnectorLabelFieldDef[];
};

export function ConnectorLabelFields({
  value,
  onChange,
  sectionTitleKey = "transform.connector.section",
  fields = [
    {
      configKey: INPUT_LABEL_CONFIG_KEY,
      fieldLabelKey: "transform.connector.inputLabel",
      hintKey: "transform.connector.inputLabelHint",
      placeholderKey: "wfViewer.inputConnector",
      marginTop: "0.5rem",
    },
    {
      configKey: OUTPUT_LABEL_CONFIG_KEY,
      fieldLabelKey: "transform.connector.outputLabel",
      hintKey: "transform.connector.outputLabelHint",
      placeholderKey: "wfViewer.outputConnector",
      marginTop: "0.75rem",
    },
  ],
}: Props) {
  const { t } = useAppSettings();

  return (
    <fieldset className="transform-node-editor-fields__section" style={{ marginTop: "1rem" }}>
      <legend>{t(sectionTitleKey)}</legend>
      {fields.map((field) => (
        <label key={field.configKey} className="gov-label gov-label--block" style={{ marginTop: field.marginTop ?? "0.75rem" }}>
          {t(field.fieldLabelKey)}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value[field.configKey] ?? "")}
            placeholder={t(field.placeholderKey)}
            onChange={(e) => {
              const next = { ...value };
              if (field.deleteWhenEmpty && !e.target.value.trim()) {
                delete next[field.configKey];
              } else {
                next[field.configKey] = e.target.value;
              }
              onChange(next);
            }}
          />
          <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {t(field.hintKey)}
          </span>
        </label>
      ))}
    </fieldset>
  );
}
