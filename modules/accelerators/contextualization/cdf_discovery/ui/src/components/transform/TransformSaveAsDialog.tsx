import { useCallback, useEffect, useState } from "react";
import {
  saveTransformPipelineAsPipeline,
  saveTransformPipelineAsTemplate,
  saveTransformTemplateAsPipeline,
  saveTransformTemplateAsTemplate,
} from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { TransformCanvasDocument } from "../../types/transformCanvas";

const RESOURCE_ID_RE = /^[a-z][a-z0-9_]{0,127}$/;

export type TransformSaveAsSource =
  | { kind: "pipeline"; pipelineId: string; label: string; scopeSuffix: string }
  | { kind: "template"; templateId: string; label: string };

export type TransformSaveAsResult =
  | { kind: "pipeline"; pipelineId: string; label: string }
  | { kind: "template"; templateId: string; label: string };

type TargetKind = "pipeline" | "template";

type Props = {
  open: boolean;
  source: TransformSaveAsSource;
  getCanvas: () => TransformCanvasDocument;
  onClose: () => void;
  onSaved: (result: TransformSaveAsResult) => void;
};

function defaultTargetId(sourceId: string, target: TargetKind): string {
  const trimmed = sourceId.trim();
  if (!trimmed) return target === "template" ? "workflow_template" : "workflow_copy";
  if (target === "template") {
    if (trimmed.endsWith("_template")) return `${trimmed}_copy`;
    return `${trimmed}_template`;
  }
  if (trimmed.endsWith("_copy")) return `${trimmed}_2`;
  return `${trimmed}_copy`;
}

function defaultTargetLabel(sourceLabel: string, target: TargetKind): string {
  const name = sourceLabel.trim();
  if (!name) return target === "template" ? "Workflow template" : "Workflow copy";
  return target === "template" ? `${name} (template)` : `${name} (copy)`;
}

export function TransformSaveAsDialog({ open, source, getCanvas, onClose, onSaved }: Props) {
  const { t } = useAppSettings();
  const [targetKind, setTargetKind] = useState<TargetKind>("pipeline");
  const [targetId, setTargetId] = useState("");
  const [label, setLabel] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const sourceId = source.kind === "pipeline" ? source.pipelineId : source.templateId;
  const sourceLabel = source.label;

  useEffect(() => {
    if (!open) return;
    setTargetKind("pipeline");
    setError(null);
  }, [open, source.kind]);

  useEffect(() => {
    if (!open) return;
    setTargetId(defaultTargetId(sourceId, targetKind));
    setLabel(defaultTargetLabel(sourceLabel, targetKind));
  }, [open, sourceId, sourceLabel, targetKind]);

  const onSubmit = useCallback(async () => {
    const id = targetId.trim();
    const name = label.trim() || id;
    if (!RESOURCE_ID_RE.test(id)) {
      setError(t("transform.pipelines.idInvalid"));
      return;
    }
    if (!name) {
      setError(t("transform.pipelines.labelRequired"));
      return;
    }
    setSubmitting(true);
    setError(null);
    const canvas = getCanvas();
    try {
      if (source.kind === "pipeline") {
        if (targetKind === "template") {
          await saveTransformPipelineAsTemplate(
            source.pipelineId,
            { template_id: id, label: name, canvas },
            source.scopeSuffix
          );
          onSaved({ kind: "template", templateId: id, label: name });
        } else {
          await saveTransformPipelineAsPipeline(
            source.pipelineId,
            { id, label: name, canvas },
            source.scopeSuffix
          );
          onSaved({ kind: "pipeline", pipelineId: id, label: name });
        }
      } else if (targetKind === "template") {
        await saveTransformTemplateAsTemplate(source.templateId, {
          template_id: id,
          label: name,
          canvas,
        });
        onSaved({ kind: "template", templateId: id, label: name });
      } else {
        await saveTransformTemplateAsPipeline(source.templateId, { id, label: name, canvas });
        onSaved({ kind: "pipeline", pipelineId: id, label: name });
      }
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  }, [targetId, label, targetKind, source, getCanvas, onClose, onSaved, t]);

  if (!open) return null;

  const idLabel =
    targetKind === "template" ? t("transform.templates.fieldId") : t("transform.pipelines.fieldId");
  const idPlaceholder =
    targetKind === "template"
      ? t("transform.templates.idPlaceholder")
      : t("transform.pipelines.idPlaceholder");
  const labelField =
    targetKind === "template" ? t("transform.templates.fieldLabel") : t("transform.pipelines.fieldLabel");
  const labelPlaceholder =
    targetKind === "template"
      ? t("transform.templates.labelPlaceholder")
      : t("transform.pipelines.labelPlaceholder");

  return (
    <div className="gov-modal-backdrop" role="presentation" onClick={onClose}>
      <div
        className="gov-modal transform-create-pipeline-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="transform-save-as-title"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id="transform-save-as-title" className="gov-modal__title">
          {t("transform.saveAs.title")}
        </h2>
        <p className="transform-node-editor-modal__hint">{t("transform.saveAs.hint")}</p>
        <fieldset className="transform-save-as-dialog__targets">
          <legend className="gov-label">{t("transform.saveAs.targetKind")}</legend>
          <label className="transform-save-as-dialog__target-option">
            <input
              type="radio"
              name="transform-save-as-target"
              checked={targetKind === "pipeline"}
              onChange={() => setTargetKind("pipeline")}
            />
            {t("transform.saveAs.targetPipeline")}
          </label>
          <label className="transform-save-as-dialog__target-option">
            <input
              type="radio"
              name="transform-save-as-target"
              checked={targetKind === "template"}
              onChange={() => setTargetKind("template")}
            />
            {t("transform.saveAs.targetTemplate")}
          </label>
        </fieldset>
        <label className="gov-label">
          {idLabel}
          <input
            className="gov-input"
            value={targetId}
            onChange={(e) => setTargetId(e.target.value.toLowerCase())}
            placeholder={idPlaceholder}
            autoComplete="off"
            spellCheck={false}
          />
        </label>
        <label className="gov-label">
          {labelField}
          <input
            className="gov-input"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
            placeholder={labelPlaceholder}
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
            {submitting ? t("transform.saveAs.creating") : t("transform.saveAs.create")}
          </button>
        </div>
      </div>
    </div>
  );
}
