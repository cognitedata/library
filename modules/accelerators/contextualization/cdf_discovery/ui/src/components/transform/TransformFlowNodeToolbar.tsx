import { NodeToolbar, Position } from "@xyflow/react";
import type { Node } from "@xyflow/react";
import type { MessageKey } from "../../i18n";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  node: Node;
  readOnly?: boolean;
  onOpenEditor: (node: Node) => void;
  onToggleEnabled: (node: Node) => void;
  onCopy: () => void;
};

function isFlowNodeEnabled(data: Record<string, unknown>): boolean {
  return data.canvas_node_enabled !== false;
}

/** Quick actions for a selected pipeline node (React Flow NodeToolbar). */
export function TransformFlowNodeToolbar({
  t,
  node,
  readOnly = false,
  onOpenEditor,
  onToggleEnabled,
  onCopy,
}: Props) {
  const data = (node.data ?? {}) as Record<string, unknown>;
  const enabled = isFlowNodeEnabled(data);

  return (
    <NodeToolbar
      nodeId={node.id}
      position={Position.Top}
      align="center"
      offset={10}
      className="transform-flow-node-toolbar"
    >
      <div className="transform-flow-node-toolbar__inner" role="toolbar" aria-label={t("transform.flow.toolbarAria")}>
        <button
          type="button"
          className="disc-btn disc-btn--sm"
          title={t("transform.inspector.openEditor")}
          aria-label={t("transform.inspector.openEditor")}
          onClick={() => onOpenEditor(node)}
        >
          {t("transform.inspector.openEditor")}
        </button>
        {!readOnly ? (
          <>
            <button
              type="button"
              className="disc-btn disc-btn--sm"
              title={t("transform.flow.ctxMenuCopy")}
              aria-label={t("transform.flow.ctxMenuCopy")}
              onClick={onCopy}
            >
              {t("transform.flow.ctxMenuCopy")}
            </button>
            <button
              type="button"
              className="disc-btn disc-btn--sm"
              title={
                enabled ? t("transform.contextMenu.disableNode") : t("transform.contextMenu.enableNode")
              }
              aria-label={
                enabled ? t("transform.contextMenu.disableNode") : t("transform.contextMenu.enableNode")
              }
              onClick={() => onToggleEnabled(node)}
            >
              {enabled ? t("transform.contextMenu.disableNode") : t("transform.contextMenu.enableNode")}
            </button>
          </>
        ) : null}
      </div>
    </NodeToolbar>
  );
}
