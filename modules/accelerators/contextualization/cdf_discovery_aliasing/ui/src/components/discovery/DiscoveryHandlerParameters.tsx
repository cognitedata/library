import type { MessageKey } from "../../i18n/types";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  handler: string;
  t: TFn;
};

/** Short hints for optional rule-level `parameters` YAML (advanced / hints). */
export function DiscoveryHandlerParameters({ handler: _handler, t }: Props) {
  return (
    <p className="discovery-hint" style={{ marginTop: "0.35rem" }}>
      {t("discoveryRules.handlerFields.fieldRuleParametersHint")}
    </p>
  );
}
