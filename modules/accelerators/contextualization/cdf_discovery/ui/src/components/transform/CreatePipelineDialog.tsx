import { useCallback, useEffect, useState } from "react";
import { createTransformPipeline, fetchTransformTemplates } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";

const PIPELINE_ID_RE = /^[a-z][a-z0-9_]{0,127}$/;

type Props = {
  open: boolean;
  onClose: () => void;
  onCreated: (pipelineId: string, label: string) => void;
  /** Pre-select template when opened from a template drag onto Pipelines. */
  initialTemplateId?: string;
};

export function CreatePipelineDialog({ open, onClose, onCreated, initialTemplateId }: Props) {
  const { t } = useAppSettings();
  const [pipelineId, setPipelineId] = useState("");
  const [label, setLabel] = useState("");
  const [templateId, setTemplateId] = useState("");
  const [templates, setTemplates] = useState<Array<{ id: string; label: string }>>([]);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setPipelineId("");
    setLabel("");
    setTemplateId(initialTemplateId?.trim() ?? "");
    setError(null);
    let cancelled = false;
    void fetchTransformTemplates()
      .then(({ templates: rows }) => {
        if (cancelled) return;
        setTemplates(rows);
      })
      .catch(() => {
        if (!cancelled) setTemplates([]);
      });
    return () => {
      cancelled = true;
    };
  }, [open, initialTemplateId]);

  const onSubmit = useCallback(async () => {
    const id = pipelineId.trim();
    const name = label.trim() || id;
    if (!PIPELINE_ID_RE.test(id)) {
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
      await createTransformPipeline({
        id,
        label: name,
        template_id: templateId.trim() || undefined,
      });
      onCreated(id, name);
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  }, [pipelineId, label, templateId, onClose, onCreated, t]);

  if (!open) return null;

  return (
    <div className="gov-modal-backdrop" role="presentation" onClick={onClose}>
      <div
        className="gov-modal transform-create-pipeline-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="create-pipeline-title"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id="create-pipeline-title" className="gov-modal__title">
          {t("transform.pipelines.newTitle")}
        </h2>
        <label className="gov-label">
          {t("transform.pipelines.fieldId")}
          <input
            className="gov-input"
            value={pipelineId}
            onChange={(e) => setPipelineId(e.target.value.toLowerCase())}
            placeholder={t("transform.pipelines.idPlaceholder")}
            autoComplete="off"
            spellCheck={false}
          />
        </label>
        <label className="gov-label">
          {t("transform.pipelines.fieldLabel")}
          <input
            className="gov-input"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
            placeholder={t("transform.pipelines.labelPlaceholder")}
            autoComplete="off"
          />
        </label>
        <label className="gov-label">
          {t("transform.pipelines.fieldTemplate")}
          <select
            className="gov-input"
            value={templateId}
            onChange={(e) => setTemplateId(e.target.value)}
          >
            <option value="">{t("transform.pipelines.templateNone")}</option>
            {templates.map((row) => (
              <option key={row.id} value={row.id}>
                {row.label}
              </option>
            ))}
          </select>
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
            {submitting ? t("transform.pipelines.creating") : t("transform.pipelines.create")}
          </button>
        </div>
      </div>
    </div>
  );
}
