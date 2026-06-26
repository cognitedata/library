import type { JsonObject } from "../../types/jsonConfig";
import { ConnectorLabelFields } from "./ConnectorLabelFields";
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

export function DualInputConnectorLabelFields({ value, onChange, defaults }: Props) {
  return (
    <ConnectorLabelFields
      value={value}
      onChange={onChange}
      sectionTitleKey="transform.dualInput.sectionConnectorLabels"
      fields={[
        {
          configKey: INPUT_A_LABEL_CONFIG_KEY,
          fieldLabelKey: "transform.dualInput.inputALabel",
          hintKey: "transform.dualInput.inputALabelHint",
          placeholderKey: defaults.inputA,
          deleteWhenEmpty: true,
          marginTop: "0.75rem",
        },
        {
          configKey: INPUT_B_LABEL_CONFIG_KEY,
          fieldLabelKey: "transform.dualInput.inputBLabel",
          hintKey: "transform.dualInput.inputBLabelHint",
          placeholderKey: defaults.inputB,
          deleteWhenEmpty: true,
          marginTop: "0.75rem",
        },
      ]}
    />
  );
}
