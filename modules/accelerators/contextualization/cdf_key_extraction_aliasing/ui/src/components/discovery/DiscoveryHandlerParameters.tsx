import type { MessageKey } from "../../i18n/types";
import { discoveryHandlerKind } from "../../utils/ruleHandlerTemplates";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  handler: string;
  t: TFn;
};

/**
 * Short hints for handler-specific YAML: heuristic uses `parameters`; field rules use `fields[]`.
 */
export function DiscoveryHandlerParameters({ handler, t }: Props) {
  const kind = discoveryHandlerKind(handler);
  if (kind === "heuristic") {
    return (
      <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
        {t("discoveryRules.handlerFields.heuristicParametersHint")}
      </p>
    );
  }
  return (
    <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
      {t("discoveryRules.handlerFields.fieldRuleParametersHint")}
    </p>
  );
}
