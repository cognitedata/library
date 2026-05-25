import { useCallback, useEffect, useState } from "react";
import { createGovernanceArtifact } from "../../api/governanceDeclared";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { GovernanceArtifactCreateContext } from "../../utils/governanceTreeNew";

const SPACE_ID_RE = /^inst_[a-z][a-z0-9_]{0,126}$/;
const GROUP_NAME_RE = /^gp_[a-z][a-z0-9_]{0,126}$/;

type Props = {
  open: boolean;
  context: GovernanceArtifactCreateContext;
  onClose: () => void;
  onCreated: (rel: string) => void;
};

export function CreateGovernanceArtifactDialog({ open, context, onClose, onCreated }: Props) {
  const { t } = useAppSettings();
  const isSpaces = context.kind === "spaces";
  const [externalId, setExternalId] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [sourceId, setSourceId] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setExternalId("");
    setDisplayName("");
    setSourceId("");
    setError(null);
  }, [open, context.kind, context.parentRel]);

  const onSubmit = useCallback(async () => {
    const id = externalId.trim().toLowerCase();
    if (isSpaces) {
      if (!SPACE_ID_RE.test(id)) {
        setError(t("governance.create.spaceIdInvalid"));
        return;
      }
    } else if (!GROUP_NAME_RE.test(id)) {
      setError(t("governance.create.groupNameInvalid"));
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      const result = await createGovernanceArtifact({
        kind: context.kind,
        external_id: id,
        display_name: isSpaces ? displayName.trim() || id : undefined,
        parent_rel: context.parentRel,
        source_id: !isSpaces && sourceId.trim() ? sourceId.trim() : undefined,
      });
      const rel = result.path ?? "";
      if (!rel) {
        throw new Error("Create succeeded but path was missing");
      }
      onCreated(rel);
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  }, [context, displayName, externalId, isSpaces, onClose, onCreated, sourceId, t]);

  if (!open) return null;

  const titleKey = isSpaces ? "governance.create.spaceTitle" : "governance.create.groupTitle";

  return (
    <div className="gov-modal-backdrop" role="presentation" onClick={onClose}>
      <div
        className="gov-modal transform-create-pipeline-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="create-gov-artifact-title"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id="create-gov-artifact-title" className="gov-modal__title">
          {t(titleKey)}
        </h2>
        <p className="gov-hint">{t("governance.create.parentFolder", { path: context.parentRel })}</p>
        <label className="gov-label">
          {isSpaces ? t("governance.create.spaceId") : t("governance.create.groupName")}
          <input
            className="gov-input"
            value={externalId}
            onChange={(e) => setExternalId(e.target.value.toLowerCase())}
            placeholder={
              isSpaces
                ? t("governance.create.spaceIdPlaceholder")
                : t("governance.create.groupNamePlaceholder")
            }
            autoComplete="off"
            spellCheck={false}
          />
        </label>
        {isSpaces ? (
          <label className="gov-label">
            {t("governance.create.spaceDisplayName")}
            <input
              className="gov-input"
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              placeholder={t("governance.create.spaceDisplayNamePlaceholder")}
              autoComplete="off"
            />
          </label>
        ) : (
          <label className="gov-label">
            {t("governance.create.groupSourceId")}
            <input
              className="gov-input"
              value={sourceId}
              onChange={(e) => setSourceId(e.target.value)}
              placeholder={t("governance.create.groupSourceIdPlaceholder")}
              autoComplete="off"
              spellCheck={false}
            />
          </label>
        )}
        {error ? (
          <p className="disc-banner--error" role="alert">
            {error}
          </p>
        ) : null}
        <div className="gov-modal__actions">
          <button type="button" className="gov-btn" onClick={onClose} disabled={submitting}>
            {t("governance.create.cancel")}
          </button>
          <button
            type="button"
            className="gov-btn gov-btn--primary"
            onClick={() => void onSubmit()}
            disabled={submitting}
          >
            {submitting ? t("governance.create.creating") : t("governance.create.submit")}
          </button>
        </div>
      </div>
    </div>
  );
}
