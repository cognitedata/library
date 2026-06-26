import { useAppSettings } from "../../context/AppSettingsContext";
import type { SubscriptionConfig } from "../../types/invertedIndexConfig";
import { StringListInput } from "./StringListInput";

type Props = {
  subscription: SubscriptionConfig;
  instanceSpaces: string[];
  onSubscriptionChange: (next: SubscriptionConfig) => void;
  onInstanceSpacesChange: (next: string[]) => void;
};

export function TargetDrivenConfigEditor({
  subscription,
  instanceSpaces,
  onSubscriptionChange,
  onInstanceSpacesChange,
}: Props) {
  const { t } = useAppSettings();

  return (
    <div className="idx-config-section">
      <h3 className="idx-config-section__title">{t("config.targetDriven.title")}</h3>
      <p className="idx-pane__hint">{t("config.targetDriven.hint")}</p>

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
        <label className="idx-label">
          {t("config.targetDriven.defaultInstanceType")}
          <input
            className="idx-input"
            value={subscription.defaultInstanceType}
            onChange={(e) =>
              onSubscriptionChange({ ...subscription, defaultInstanceType: e.target.value })
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
          {t("config.targetDriven.assetViews")}
          <StringListInput
            value={subscription.assetViews}
            onChange={(assetViews) => onSubscriptionChange({ ...subscription, assetViews })}
            placeholder={t("config.targetDriven.assetViewsPlaceholder")}
          />
        </label>
        <label className="idx-label idx-config-grid__full">
          {t("config.targetDriven.fileViews")}
          <StringListInput
            value={subscription.fileViews}
            onChange={(fileViews) => onSubscriptionChange({ ...subscription, fileViews })}
            placeholder={t("config.targetDriven.fileViewsPlaceholder")}
          />
        </label>
      </div>

      <h4 className="idx-config-subsection__title">{t("config.targetDriven.indexInstanceSpaces")}</h4>
      <p className="idx-pane__hint">{t("config.targetDriven.indexInstanceSpacesHint")}</p>
      <label className="idx-label">
        {t("config.targetDriven.indexInstanceSpaces")}
        <StringListInput
          value={instanceSpaces}
          onChange={onInstanceSpacesChange}
          placeholder={t("config.targetDriven.indexInstanceSpacesPlaceholder")}
          mono
        />
      </label>
    </div>
  );
}
