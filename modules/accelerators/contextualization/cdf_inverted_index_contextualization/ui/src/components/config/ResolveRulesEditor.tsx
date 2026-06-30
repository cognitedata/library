import { useMemo, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  emptyResolveRule,
  parseResolveByIncomingView,
  resolveEntryForView,
  serializeResolveByIncomingView,
  syncResolveMapWithIncomingViews,
  type ResolveByIncomingView,
  type ResolveIncomingViewEntry,
  type ResolveRule,
  type ResolveSideConfig,
  type ResolveSideMode,
} from "../../utils/resolveRules";
import { CollapsibleJson } from "../shared/CollapsibleJson";
import { StringListInput } from "./StringListInput";

type Props = {
  incomingViews: string[];
  value: JsonObject | undefined;
  onChange: (next: JsonObject) => void;
};

function ResolveSideEditor({
  groupName,
  label,
  side,
  onChange,
}: {
  groupName: string;
  label: string;
  side: ResolveSideConfig;
  onChange: (next: ResolveSideConfig) => void;
}) {
  const { t } = useAppSettings();

  const setMode = (mode: ResolveSideMode) => {
    if (mode === "incoming_instance") {
      onChange({ mode: "incoming_instance" });
      return;
    }
    onChange({
      mode: "rules",
      rules: side.mode === "rules" && side.rules.length ? side.rules : [emptyResolveRule()],
    });
  };

  const rules = side.mode === "rules" ? side.rules : [];

  const updateRule = (index: number, patch: Partial<ResolveRule>) => {
    if (side.mode !== "rules") return;
    const nextRules = side.rules.map((rule, i) => (i === index ? { ...rule, ...patch } : rule));
    onChange({ mode: "rules", rules: nextRules });
  };

  const addRule = () => {
    if (side.mode !== "rules") return;
    onChange({ mode: "rules", rules: [...side.rules, emptyResolveRule()] });
  };

  const removeRule = (index: number) => {
    if (side.mode !== "rules") return;
    const nextRules = side.rules.filter((_, i) => i !== index);
    onChange({
      mode: "rules",
      rules: nextRules.length ? nextRules : [emptyResolveRule()],
    });
  };

  return (
    <div className="idx-config-resolve-side">
      <h6 className="idx-config-resolve-side__title">{label}</h6>
      <div className="idx-checkbox-group idx-checkbox-group--inline">
        <label className="idx-checkbox-label">
          <input
            type="radio"
            name={`${groupName}-mode`}
            checked={side.mode === "incoming_instance"}
            onChange={() => setMode("incoming_instance")}
          />
          {t("config.linking.resolveRulesModeIncoming")}
        </label>
        <label className="idx-checkbox-label">
          <input
            type="radio"
            name={`${groupName}-mode`}
            checked={side.mode === "rules"}
            onChange={() => setMode("rules")}
          />
          {t("config.linking.resolveRulesModeRules")}
        </label>
      </div>

      {side.mode === "rules" ? (
        <div className="idx-config-resolve-rules">
          {rules.map((rule, index) => (
            <div key={index} className="idx-config-resolve-rule-card">
              <div className="idx-config-link-card__header">
                <span className="idx-config-inline-label">
                  {t("config.linking.resolveRulesRule", { n: String(index + 1) })}
                </span>
                <button
                  type="button"
                  className="idx-btn idx-btn--ghost"
                  onClick={() => removeRule(index)}
                  disabled={rules.length <= 1}
                >
                  {t("config.linking.resolveRulesRemoveRule")}
                </button>
              </div>
              <div className="idx-config-grid">
                <label className="idx-label idx-config-grid__full">
                  {t("config.linking.resolveRulesWhenReferenceTypes")}
                  <StringListInput
                    value={rule.whenReferenceTypes}
                    onChange={(whenReferenceTypes) =>
                      updateRule(index, { whenReferenceTypes })
                    }
                    placeholder={t("config.linking.resolveRulesWhenReferenceTypesPlaceholder")}
                    mono
                  />
                </label>
                <label className="idx-label">
                  {t("config.linking.resolveRulesSpacePath")}
                  <input
                    className="idx-input idx-input--mono"
                    value={rule.space}
                    onChange={(e) => updateRule(index, { space: e.target.value })}
                    placeholder="reference_space"
                  />
                </label>
                <label className="idx-label">
                  {t("config.linking.resolveRulesExternalIdPath")}
                  <input
                    className="idx-input idx-input--mono"
                    value={rule.externalId}
                    onChange={(e) => updateRule(index, { externalId: e.target.value })}
                    placeholder="reference_external_id"
                  />
                </label>
              </div>
              <fieldset className="idx-config-resolve-fallback">
                <legend>{t("config.linking.resolveRulesFallback")}</legend>
                <div className="idx-config-grid">
                  <label className="idx-label">
                    {t("config.linking.resolveRulesFallbackSpace")}
                    <input
                      className="idx-input idx-input--mono"
                      value={rule.fallback?.space ?? ""}
                      onChange={(e) =>
                        updateRule(index, {
                          fallback: {
                            space: e.target.value,
                            externalId: rule.fallback?.externalId ?? "",
                          },
                        })
                      }
                      placeholder="additional_metadata.file_space"
                    />
                  </label>
                  <label className="idx-label">
                    {t("config.linking.resolveRulesFallbackExternalId")}
                    <input
                      className="idx-input idx-input--mono"
                      value={rule.fallback?.externalId ?? ""}
                      onChange={(e) =>
                        updateRule(index, {
                          fallback: {
                            space: rule.fallback?.space ?? "",
                            externalId: e.target.value,
                          },
                        })
                      }
                      placeholder="additional_metadata.linked_file_extid"
                    />
                  </label>
                </div>
              </fieldset>
            </div>
          ))}
          <button type="button" className="idx-btn idx-btn--ghost" onClick={addRule}>
            {t("config.linking.resolveRulesAddRule")}
          </button>
        </div>
      ) : null}
    </div>
  );
}

function ResolveIncomingViewCard({
  viewKey,
  entry,
  onChange,
}: {
  viewKey: string;
  entry: ResolveIncomingViewEntry;
  onChange: (next: ResolveIncomingViewEntry) => void;
}) {
  const { t } = useAppSettings();

  return (
    <section className="idx-config-subsection idx-config-resolve-incoming">
      <h5 className="idx-config-subsection__title">
        {t("config.linking.resolveRulesIncomingView", { viewKey })}
      </h5>
      <ResolveSideEditor
        groupName={`${viewKey}-forward`}
        label={t("config.linking.resolveRulesForward")}
        side={entry.forward}
        onChange={(forward) => onChange({ ...entry, forward })}
      />
      <ResolveSideEditor
        groupName={`${viewKey}-target`}
        label={t("config.linking.resolveRulesTarget")}
        side={entry.target}
        onChange={(target) => onChange({ ...entry, target })}
      />
    </section>
  );
}

export function ResolveRulesEditor({ incomingViews, value, onChange }: Props) {
  const { t } = useAppSettings();
  const [showJson, setShowJson] = useState(false);

  const parsed = useMemo(
    () => syncResolveMapWithIncomingViews(parseResolveByIncomingView(value), incomingViews),
    [value, incomingViews]
  );

  const emit = (map: ResolveByIncomingView) => {
    onChange(serializeResolveByIncomingView(syncResolveMapWithIncomingViews(map, incomingViews)));
  };

  const updateViewEntry = (viewKey: string, entry: ResolveIncomingViewEntry) => {
    emit({ ...parsed, [viewKey]: entry });
  };

  if (incomingViews.length === 0) {
    return (
      <div className="idx-config-nested">
        <h5 className="idx-config-subsection__title">{t("config.linking.resolveRules")}</h5>
        <p className="idx-pane__hint">{t("config.linking.resolveRulesNoIncomingViews")}</p>
      </div>
    );
  }

  return (
    <div className="idx-config-nested">
      <h5 className="idx-config-subsection__title">{t("config.linking.resolveRules")}</h5>
      <p className="idx-pane__hint">{t("config.linking.resolveRulesHint")}</p>

      {incomingViews.map((viewKey) => (
        <ResolveIncomingViewCard
          key={viewKey}
          viewKey={viewKey}
          entry={resolveEntryForView(parsed, viewKey)}
          onChange={(entry) => updateViewEntry(viewKey, entry)}
        />
      ))}

      <button
        type="button"
        className="idx-config-advanced__toggle"
        aria-expanded={showJson}
        onClick={() => setShowJson((open) => !open)}
      >
        {showJson ? t("config.linking.resolveRulesHideJson") : t("config.linking.resolveRulesShowJson")}
      </button>
      {showJson ? (
        <div className="idx-config-advanced__body">
          <CollapsibleJson data={serializeResolveByIncomingView(parsed)} defaultOpen />
        </div>
      ) : null}
    </div>
  );
}
