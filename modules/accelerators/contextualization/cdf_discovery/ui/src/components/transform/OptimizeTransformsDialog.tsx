import { useCallback, useEffect, useMemo, useState } from "react";
import { ModalDialogShell } from "../ModalDialogShell";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n/types";
import type { Node } from "@xyflow/react";
import {
  buildFlowOptimizeCandidateRows,
  flowCanvasNodeLabel,
  type FlowOptimizeCandidate,
  type FlowOptimizeCandidateRow,
} from "./discoverFlowOptimizeCandidates";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  open: boolean;
  onClose: () => void;
  candidates: FlowOptimizeCandidate[];
  getNode: (id: string) => Node | undefined;
  onApply: (approved: FlowOptimizeCandidate[]) => void;
};

export function OptimizeTransformsDialog({ open, onClose, candidates, getNode, onApply }: Props) {
  const { t } = useAppSettings();
  const [rows, setRows] = useState<FlowOptimizeCandidateRow[]>([]);

  useEffect(() => {
    if (!open) return;
    setRows(buildFlowOptimizeCandidateRows(candidates));
  }, [open, candidates]);

  const approvedCount = useMemo(() => rows.filter((r) => r.approved).length, [rows]);

  const setAllApproved = useCallback((approved: boolean) => {
    setRows((prev) => prev.map((r) => ({ ...r, approved })));
  }, []);

  const toggleRow = useCallback((id: string, approved: boolean) => {
    setRows((prev) => prev.map((r) => (r.id === id ? { ...r, approved } : r)));
  }, []);

  const onConfirm = useCallback(() => {
    const approved = rows
      .filter((r) => r.approved)
      .map((r) => ({
        id: r.id,
        nodeKind: r.nodeKind,
        kind: r.kind,
        nodeIds: r.nodeIds,
        anchorNodeId: r.anchorNodeId,
      }));
    onApply(approved);
    onClose();
  }, [rows, onApply, onClose]);

  return (
    <ModalDialogShell
      open={open}
      onClose={onClose}
      titleId="optimize-transforms-title"
      describedBy="optimize-transforms-desc"
      dialogClassName="gov-modal transform-optimize-modal"
    >
      <h2 id="optimize-transforms-title" className="gov-modal__title">
        {t("transform.optimize.title")}
      </h2>
      <p id="optimize-transforms-desc" className="transform-node-editor-modal__hint">
        {t("transform.optimize.description")}
      </p>

      {rows.length === 0 ? (
        <p className="transform-optimize-modal__empty" role="status">
          {t("transform.optimize.empty")}
        </p>
      ) : (
        <>
          <div className="transform-optimize-modal__actions">
            <button type="button" className="disc-btn disc-btn--sm" onClick={() => setAllApproved(true)}>
              {t("transform.optimize.approveAll")}
            </button>
            <button type="button" className="disc-btn disc-btn--sm" onClick={() => setAllApproved(false)}>
              {t("transform.optimize.declineAll")}
            </button>
          </div>
          <ul className="transform-optimize-modal__list">
            {rows.map((row) => {
              const sep = row.kind === "ordered_chain" ? " → " : " · ";
              const label = row.nodeIds.map((id) => flowCanvasNodeLabel(getNode(id), id)).join(sep);
              return (
                <OptimizeCandidateRow
                  key={row.id}
                  row={row}
                  t={t}
                  label={label}
                  onToggle={(approved) => toggleRow(row.id, approved)}
                />
              );
            })}
          </ul>
        </>
      )}

      <div className="gov-modal__footer">
        <button type="button" className="disc-btn" onClick={onClose}>
          {t("btn.cancel")}
        </button>
        <button
          type="button"
          className="disc-btn disc-btn--primary"
          disabled={approvedCount === 0}
          onClick={onConfirm}
        >
          {t("transform.optimize.apply", { count: approvedCount })}
        </button>
      </div>
    </ModalDialogShell>
  );
}

function OptimizeCandidateRow({
  row,
  t,
  label,
  onToggle,
}: {
  row: FlowOptimizeCandidateRow;
  t: TFn;
  label: string;
  onToggle: (approved: boolean) => void;
}) {
  const nodeKindKey =
    row.nodeKind === "score" ? "transform.optimize.nodeKindScore" : "transform.optimize.nodeKindTransform";
  const topologyKey =
    row.kind === "ordered_chain" ? "transform.optimize.kindOrdered" : "transform.optimize.kindParallel";

  return (
    <li className="transform-optimize-modal__item">
      <label className="transform-optimize-modal__item-label">
        <input
          type="checkbox"
          checked={row.approved}
          onChange={(e) => onToggle(e.target.checked)}
        />
        <span>
          <span className="transform-optimize-modal__kind">
            {t(nodeKindKey)} · {t(topologyKey)}
          </span>
          <span className="transform-optimize-modal__nodes">{label}</span>
          {row.conflicts ? (
            <span className="transform-optimize-modal__conflict">{t("transform.optimize.conflict")}</span>
          ) : null}
        </span>
      </label>
    </li>
  );
}
