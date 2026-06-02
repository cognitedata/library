import type { MessageKey } from "../i18n";

export const INPUT_A_LABEL_CONFIG_KEY = "input_a_label";
export const INPUT_B_LABEL_CONFIG_KEY = "input_b_label";
export const INPUT_LABEL_CONFIG_KEY = "input_label";
export const OUTPUT_LABEL_CONFIG_KEY = "output_label";

export type DualInputLabelDefaults = {
  inputA: MessageKey;
  inputB: MessageKey;
};

export function resolveDualInputConnectorLabel(
  config: Record<string, unknown>,
  configKey: typeof INPUT_A_LABEL_CONFIG_KEY | typeof INPUT_B_LABEL_CONFIG_KEY,
  defaultKey: MessageKey,
  t: (key: MessageKey) => string
): string {
  const custom = String(config[configKey] ?? "").trim();
  return custom || t(defaultKey);
}

export function resolveConnectorLabel(
  config: Record<string, unknown>,
  configKey: typeof INPUT_LABEL_CONFIG_KEY | typeof OUTPUT_LABEL_CONFIG_KEY,
  defaultKey: MessageKey,
  t: (key: MessageKey) => string
): string {
  const custom = String(config[configKey] ?? "").trim();
  return custom || t(defaultKey);
}

export function fanoutPlanConnectorLabelDefaults(profile: string): DualInputLabelDefaults {
  if (profile === "file_annotation") {
    return {
      inputA: "transform.fanoutPlan.handle.inputA.context",
      inputB: "transform.fanoutPlan.handle.inputB.files",
    };
  }
  return {
    inputA: "transform.fanoutPlan.handle.inputA",
    inputB: "transform.fanoutPlan.handle.inputB",
  };
}

export function fileAnnotationConnectorLabelDefaults(): DualInputLabelDefaults {
  return {
    inputA: "transform.fileAnnotation.handle.entities",
    inputB: "transform.fileAnnotation.handle.files",
  };
}
