import { useAppSettings } from "../../context/AppSettingsContext";
import type { AnnotationIndexConfig } from "../../types/invertedIndexConfig";
import { StringListInput } from "./StringListInput";
import { FormPanel } from "../shared/FormPanel";

type Props = {
  value: AnnotationIndexConfig;
  onChange: (next: AnnotationIndexConfig) => void;
};

export function AnnotationIndexEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();

  return (
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
  );
}
