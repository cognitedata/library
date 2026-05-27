import { useCallback, useEffect, useState } from "react";
import { ModalDialogShell } from "../ModalDialogShell";
import { saveTransformPipelineAsTemplate } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";

const TEMPLATE_ID_RE = /^[a-z][a-z0-9_]{0,127}$/;

type Props = {
  open: boolean;
  pipelineId: string;
  pipelineLabel: string;
  onClose: () => void;
  onSaved: (templateId: string, label: string) => void;
};

function defaultTemplateId(pipelineId: string): string {
  const base = pipelineId.trim();
  if (!base) return "pipeline_template";
  if (base.endsWith("_template")) return base;
  return `${base}_template`;
}

export function SavePipelineAsTemplateDialog({
  open,
  pipelineId,
  pipelineLabel,
  onClose,
  onSaved,
}: Props) {
  const { t } = useAppSettings();
  const [templateId, setTemplateId] = useState("");
  const [label, setLabel] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setTemplateId(defaultTemplateId(pipelineId));
    setLabel(pipelineLabel.trim() ? `${pipelineLabel.trim()} (template)` : "");
    setError(null);
  }, [open, pipelineId, pipelineLabel]);

  const onSubmit = useCallback(async () => {
    const id = templateId.trim();
    const name = label.trim() || id;
    if (!TEMPLATE_ID_RE.test(id)) {
      setError(t("transform.pipelines.idInvalid"));
      return;
    }
    if (!name) {
      setError(t("transform.pipelines.labelRequired"));
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      await saveTransformPipelineAsTemplate(pipelineId, { template_id: id, label: name });
      onSaved(id, name);
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  }, [templateId, label, pipelineId, onClose, onSaved, t]);

  return (
    <ModalDialogShell
      open={open}
      onClose={onClose}
      titleId="save-pipeline-template-title"
      closeOnEscape={!submitting}
      dialogClassName="gov-modal transform-create-pipeline-modal"
    >
        <h2 id="save-pipeline-template-title" className="gov-modal__title">
          {t("transform.templates.saveFromPipelineTitle")}
        </h2>
        <p className="transform-node-editor-modal__hint">{t("transform.templates.saveFromPipelineHint")}</p>
        <label className="gov-label">
          {t("transform.templates.fieldId")}
          <input
            className="gov-input"
            value={templateId}
            onChange={(e) => setTemplateId(e.target.value.toLowerCase())}
            placeholder={t("transform.templates.idPlaceholder")}
            autoComplete="off"
            spellCheck={false}
          />
        </label>
        <label className="gov-label">
          {t("transform.templates.fieldLabel")}
          <input
            className="gov-input"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
            placeholder={t("transform.templates.labelPlaceholder")}
            autoComplete="off"
          />
        </label>
        {error ? (
          <p className="transform-create-pipeline-modal__error" role="alert">
            {error}
          </p>
        ) : null}
        <div className="gov-modal__actions">
          <button type="button" className="disc-btn" onClick={onClose} disabled={submitting}>
            {t("transform.pipelines.cancel")}
          </button>
          <button
            type="button"
            className="disc-btn disc-btn--primary"
            onClick={() => void onSubmit()}
            disabled={submitting}
          >
            {submitting ? t("transform.templates.saving") : t("transform.templates.save")}
          </button>
        </div>
    </ModalDialogShell>
  );
}
