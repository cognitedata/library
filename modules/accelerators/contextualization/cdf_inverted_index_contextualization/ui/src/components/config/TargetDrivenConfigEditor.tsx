import { useAppSettings } from "../../context/AppSettingsContext";
import type { SubscriptionConfig, TargetDrivenQueryConfig } from "../../types/invertedIndexConfig";
import { StringListInput } from "./StringListInput";
import { FormPanel } from "../shared/FormPanel";

type Props = {
  query: TargetDrivenQueryConfig;
  subscription: SubscriptionConfig;
  onQueryChange: (next: TargetDrivenQueryConfig) => void;
  onSubscriptionChange: (next: SubscriptionConfig) => void;
};

export function TargetDrivenConfigEditor({
  query,
  subscription,
  onQueryChange,
  onSubscriptionChange,
}: Props) {
  const { t } = useAppSettings();

  return (
    <FormPanel title={t("config.targetDriven.title")} hint={t("config.targetDriven.hint")}>

      <div className="idx-config-subsections--split">
      <section className="idx-config-subsection">
        <h4 className="idx-config-subsection__title">{t("config.targetDriven.queryTerms")}</h4>
        <div className="idx-config-grid">
        <label className="idx-label">
          {t("config.targetDriven.queryProperty")}
          <input
            className="idx-input idx-input--mono"
            value={query.queryProperty}
            onChange={(e) =>
              onQueryChange({ ...query, queryProperty: e.target.value })
            }
            placeholder={t("config.targetDriven.queryPropertyPlaceholder")}
          />
        </label>
        <label className="idx-label idx-config-grid__full">
          {t("config.targetDriven.queryPropertyFallbacks")}
          <StringListInput
            value={query.queryPropertyFallbacks}
            onChange={(queryPropertyFallbacks) =>
              onQueryChange({ ...query, queryPropertyFallbacks })
            }
            placeholder={t("config.targetDriven.queryPropertyFallbacksPlaceholder")}
            mono
          />
        </label>
        <label className="idx-checkbox-label idx-config-grid__full">
          <input
            type="checkbox"
            checked={query.excludeEmptyAliases}
            onChange={(e) =>
              onQueryChange({ ...query, excludeEmptyAliases: e.target.checked })
            }
          />
          {t("config.targetDriven.excludeEmptyAliases")}
        </label>
        <p className="idx-pane__hint idx-config-grid__full">
          {t("config.targetDriven.excludeEmptyAliasesHint")}
        </p>
      </div>
      </section>

      <section className="idx-config-subsection">
        <h4 className="idx-config-subsection__title">{t("config.targetDriven.subscription")}</h4>
      <div className="idx-config-grid">
        <label className="idx-checkbox-label">
          <input
            type="checkbox"
            checked={subscription.enabled}
            onChange={(e) =>
              onSubscriptionChange({ ...subscription, enabled: e.target.checked })
            }
          />
          {t("config.targetDriven.subscriptionEnabled")}
        </label>
        <label className="idx-label">
          {t("config.targetDriven.trigger")}
          <input
            className="idx-input idx-input--mono"
            value={subscription.trigger}
            onChange={(e) =>
              onSubscriptionChange({ ...subscription, trigger: e.target.value })
            }
          />
        </label>
        <label className="idx-label">
          {t("config.targetDriven.watchProperty")}
          <input
            className="idx-input idx-input--mono"
            value={subscription.watchProperty}
            onChange={(e) =>
              onSubscriptionChange({ ...subscription, watchProperty: e.target.value })
            }
          />
        </label>
        <label className="idx-label idx-config-grid__full">
          {t("config.targetDriven.instanceSpaces")}
          <StringListInput
            value={subscription.instanceSpaces}
            onChange={(instanceSpaces) =>
              onSubscriptionChange({ ...subscription, instanceSpaces })
            }
            placeholder={t("config.targetDriven.instanceSpacesPlaceholder")}
            mono
          />
        </label>
        <label className="idx-label idx-config-grid__full">
          {t("config.targetDriven.watchViewKeys")}
          <StringListInput
            value={subscription.watchViewKeys}
            onChange={(watchViewKeys) =>
              onSubscriptionChange({ ...subscription, watchViewKeys })
            }
            placeholder={t("config.targetDriven.watchViewKeysPlaceholder")}
            mono
          />
          <span className="idx-config-hint">{t("config.targetDriven.watchViewKeysHint")}</span>
        </label>
      </div>
      </section>
      </div>
    </FormPanel>
  );
}
