import { useAppSettings } from "../../context/AppSettingsContext";
import type { AnnotationIndexConfig } from "../../types/invertedIndexConfig";
import { StringListInput } from "./StringListInput";
import { FormPanel } from "../shared/FormPanel";

type Props = {
  value: AnnotationIndexConfig;
  onChange: (next: AnnotationIndexConfig) => void;
};

function parsePositiveInt(raw: string, fallback: number): number {
  const n = Number.parseInt(raw, 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

export function AnnotationIndexEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const identity = value.identity;

  const setIdentity = (patch: Partial<typeof identity>) => {
    onChange({ ...value, identity: { ...identity, ...patch } });
  };

  return (
    <>
      <FormPanel title={t("config.annotation.title")} hint={t("config.annotation.hint")}>
        <div className="idx-config-grid">
          <label className="idx-label">
            {t("config.annotation.view")}
            <input
              className="idx-input"
              value={value.view}
              onChange={(e) => onChange({ ...value, view: e.target.value })}
            />
          </label>
          <label className="idx-label">
            {t("config.annotation.viewSpace")}
            <input
              className="idx-input idx-input--mono"
              value={value.viewSpace}
              onChange={(e) => onChange({ ...value, viewSpace: e.target.value })}
            />
          </label>
          <label className="idx-label">
            {t("config.annotation.version")}
            <input
              className="idx-input"
              value={value.version}
              onChange={(e) => onChange({ ...value, version: e.target.value })}
            />
          </label>
          <label className="idx-label">
            {t("config.annotation.instanceType")}
            <input
              className="idx-input"
              value={value.instanceType}
              onChange={(e) => onChange({ ...value, instanceType: e.target.value })}
            />
          </label>
          <label className="idx-label">
            {t("config.annotation.textProperty")}
            <input
              className="idx-input idx-input--mono"
              value={value.textProperty}
              onChange={(e) => onChange({ ...value, textProperty: e.target.value })}
            />
          </label>
          <label className="idx-label">
            {t("config.annotation.confidenceProperty")}
            <input
              className="idx-input idx-input--mono"
              value={value.confidenceProperty}
              onChange={(e) => onChange({ ...value, confidenceProperty: e.target.value })}
            />
          </label>
          <label className="idx-label">
            {t("config.annotation.statusProperty")}
            <input
              className="idx-input idx-input--mono"
              value={value.statusProperty}
              onChange={(e) => onChange({ ...value, statusProperty: e.target.value })}
            />
          </label>
          <label className="idx-label">
            {t("config.annotation.pageProperty")}
            <input
              className="idx-input idx-input--mono"
              value={value.pageProperty}
              onChange={(e) => onChange({ ...value, pageProperty: e.target.value })}
            />
          </label>
          <label className="idx-label idx-config-grid__full">
            {t("config.annotation.bboxProperties")}
            <StringListInput
              value={value.bboxProperties}
              onChange={(bboxProperties) => onChange({ ...value, bboxProperties })}
              placeholder={t("config.annotation.bboxPlaceholder")}
              mono
            />
            <span className="idx-config-hint">{t("config.annotation.bboxHint")}</span>
          </label>
        </div>
      </FormPanel>

      <FormPanel
        variant="compact"
        title={t("config.annotation.identity.title")}
        hint={t("config.annotation.identity.hint")}
      >
        <div className="idx-config-grid idx-config-grid--identity">
          <label className="idx-label">
            {t("config.annotation.identity.prefix")}
            <input
              className="idx-input idx-input--mono"
              value={identity.annotationExternalIdPrefix}
              onChange={(e) => setIdentity({ annotationExternalIdPrefix: e.target.value })}
            />
          </label>
          <label className="idx-label">
            {t("config.annotation.identity.termPrefixLength")}
            <input
              className="idx-input"
              type="number"
              min={1}
              max={128}
              value={identity.detectionKeyTermPrefixLength}
              onChange={(e) =>
                setIdentity({
                  detectionKeyTermPrefixLength: parsePositiveInt(
                    e.target.value,
                    identity.detectionKeyTermPrefixLength
                  ),
                })
              }
            />
          </label>
          <label className="idx-label">
            {t("config.annotation.identity.bboxHashDecimals")}
            <input
              className="idx-input"
              type="number"
              min={0}
              max={12}
              value={identity.bboxHashDecimalPlaces}
              onChange={(e) =>
                setIdentity({
                  bboxHashDecimalPlaces: Math.max(
                    0,
                    parsePositiveInt(e.target.value, identity.bboxHashDecimalPlaces)
                  ),
                })
              }
            />
          </label>
          <label className="idx-label">
            {t("config.annotation.identity.hashHexLength")}
            <input
              className="idx-input"
              type="number"
              min={4}
              max={64}
              value={identity.hashHexLength}
              onChange={(e) =>
                setIdentity({
                  hashHexLength: parsePositiveInt(e.target.value, identity.hashHexLength),
                })
              }
            />
          </label>
          <label className="idx-label">
            {t("config.annotation.identity.externalIdLimit")}
            <input
              className="idx-input"
              type="number"
              min={32}
              max={512}
              value={identity.externalIdLimit}
              onChange={(e) =>
                setIdentity({
                  externalIdLimit: parsePositiveInt(e.target.value, identity.externalIdLimit),
                })
              }
            />
          </label>
          <p className="idx-config-hint idx-config-grid__full">
            {t("config.annotation.identity.templatesYamlHint")}
          </p>
        </div>
      </FormPanel>
    </>
  );
}
