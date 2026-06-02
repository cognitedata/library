import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import { INPUT_LABEL_CONFIG_KEY, OUTPUT_LABEL_CONFIG_KEY } from "../../utils/dualInputConnectorLabels";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  showInput?: boolean;
  showOutput?: boolean;
};

export function ConnectorLabelFields({
  value,
  onChange,
  showInput = true,
  showOutput = true,
}: Props) {
  const { t } = useAppSettings();

  return (
    <fieldset className="transform-node-editor-fields__section" style={{ marginTop: "1rem" }}>
      <legend>{t("transform.connector.section")}</legend>
      {showInput ? (
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transform.connector.inputLabel")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value[INPUT_LABEL_CONFIG_KEY] ?? "")}
            placeholder={t("wfViewer.inputConnector")}
            onChange={(e) => onChange({ ...value, [INPUT_LABEL_CONFIG_KEY]: e.target.value })}
          />
          <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {t("transform.connector.inputLabelHint")}
          </span>
        </label>
      ) : null}
      {showOutput ? (
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.connector.outputLabel")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value[OUTPUT_LABEL_CONFIG_KEY] ?? "")}
            placeholder={t("wfViewer.outputConnector")}
            onChange={(e) => onChange({ ...value, [OUTPUT_LABEL_CONFIG_KEY]: e.target.value })}
          />
          <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {t("transform.connector.outputLabelHint")}
          </span>
        </label>
      ) : null}
    </fieldset>
  );
}
