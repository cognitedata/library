import { useCallback, useEffect, useState } from "react";
import { renameTransformPipeline, renameTransformTemplate } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";

type Props = {
  open: boolean;
  kind: "pipeline" | "template";
  resourceId: string;
  currentLabel: string;
  onClose: () => void;
  onRenamed: (newLabel: string) => void;
};

export function RenameTransformLabelDialog({
  open,
  kind,
  resourceId,
  currentLabel,
  onClose,
  onRenamed,
}: Props) {
  const { t } = useAppSettings();
  const [label, setLabel] = useState(currentLabel);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setLabel(currentLabel);
    setError(null);
  }, [open, currentLabel, resourceId]);

  const onSubmit = useCallback(async () => {
    const name = label.trim();
    if (!name) {
      setError(t("transform.pipelines.labelRequired"));
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      if (kind === "pipeline") {
        await renameTransformPipeline(resourceId, name);
      } else {
        await renameTransformTemplate(resourceId, name);
      }
      onRenamed(name);
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  }, [kind, label, onClose, onRenamed, resourceId, t]);

  if (!open) return null;

  const titleKey =
    kind === "pipeline" ? "transform.pipelines.renameTitle" : "transform.templates.renameTitle";
  const titleId = `rename-transform-${kind}-title`;

  return (
    <div className="gov-modal-backdrop" role="presentation" onClick={onClose}>
      <div
        className="gov-modal transform-rename-label-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id={titleId} className="gov-modal__title">
          {t(titleKey)}
        </h2>
        <label className="gov-label">
          {t("transform.pipelines.fieldLabel")}
          <input
            className="gov-input"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
            placeholder={
              kind === "pipeline"
                ? t("transform.pipelines.labelPlaceholder")
                : t("transform.templates.labelPlaceholder")
            }
            autoComplete="off"
            autoFocus
          />
        </label>
        {error ? (
          <p className="transform-rename-label-modal__error" role="alert">
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
            {submitting ? t("transform.rename.saving") : t("transform.rename.save")}
          </button>
        </div>
      </div>
    </div>
  );
}
