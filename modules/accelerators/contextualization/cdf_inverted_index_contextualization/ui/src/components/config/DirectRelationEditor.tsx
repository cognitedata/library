import { useEffect, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import {
  WRITE_MODES,
  defaultDirectRelationLinkConfig,
  emptyDmViewRef,
  linkDisplayLabel,
  type DirectRelationConfig,
  type DirectRelationLinkConfig,
  type DmViewRef,
  type WriteMode,
} from "../../types/invertedIndexConfig";
import { FormPanel } from "../shared/FormPanel";
import { ResolveRulesEditor } from "./ResolveRulesEditor";
import { StringListInput } from "./StringListInput";
import {
  parseResolveByIncomingView,
  serializeResolveByIncomingView,
  syncResolveMapWithIncomingViews,
} from "../../utils/resolveRules";

type Props = {
  value: DirectRelationConfig;
  onChange: (next: DirectRelationConfig) => void;
};

const WRITE_MODE_LABEL_KEYS = {
  direct_relation: "config.linking.writeModeDirectRelation",
  edge: "config.linking.writeModeEdge",
  diagram_annotation: "config.linking.writeModeDiagramAnnotation",
} as const satisfies Record<WriteMode, string>;

function updateLink(
  cfg: DirectRelationConfig,
  key: string,
  patch: Partial<DirectRelationLinkConfig>
): DirectRelationConfig {
  return {
    ...cfg,
    links: {
      ...cfg.links,
      [key]: { ...cfg.links[key], ...patch },
    },
  };
}

function updateViewRefs(
  refs: Record<string, DmViewRef>,
  key: string,
  patch: Partial<DmViewRef>
): Record<string, DmViewRef> {
  return {
    ...refs,
    [key]: { ...refs[key], ...patch },
  };
}

function orderedLinkKeys(cfg: DirectRelationConfig): string[] {
  const keys = new Set([...cfg.linkOrder, ...Object.keys(cfg.links)]);
  return [...keys].filter((k) => cfg.links[k]);
}

function renameViewRegistryKey(
  cfg: DirectRelationConfig,
  registry: "views" | "edgeViews",
  oldKey: string,
  newKey: string
): DirectRelationConfig {
  const trimmed = newKey.trim();
  if (!trimmed || trimmed === oldKey || cfg[registry][trimmed]) return cfg;
  const { [oldKey]: ref, ...rest } = cfg[registry];
  if (!ref) return cfg;
  const nextRegistry = { ...rest, [trimmed]: ref };
  const nextLinks = Object.fromEntries(
    Object.entries(cfg.links).map(([linkKey, link]) => {
      const patch: Partial<DirectRelationLinkConfig> = {};
      if (registry === "views") {
        if (link.forwardView === oldKey) patch.forwardView = trimmed;
        if (link.targetView === oldKey) patch.targetView = trimmed;
        if (link.incomingViews.includes(oldKey)) {
          patch.incomingViews = link.incomingViews.map((v) => (v === oldKey ? trimmed : v));
        }
      } else if (link.edgeViewKey === oldKey) {
        patch.edgeViewKey = trimmed;
      }
      return [linkKey, Object.keys(patch).length ? { ...link, ...patch } : link];
    })
  );
  return { ...cfg, [registry]: nextRegistry, links: nextLinks };
}

function DmViewRefFields({
  refKey,
  ref,
  onChange,
  onRename,
  onRemove,
  removeLabelKey = "config.linking.removeView",
}: {
  refKey: string;
  ref: DmViewRef | undefined;
  onChange: (next: DmViewRef) => void;
  onRename?: (nextKey: string) => void;
  onRemove?: () => void;
  removeLabelKey?: "config.linking.removeView" | "config.linking.removeEdgeView";
}) {
  const { t } = useAppSettings();
  const [draftKey, setDraftKey] = useState(refKey);

  useEffect(() => {
    setDraftKey(refKey);
  }, [refKey]);

  if (!ref) return null;
  return (
    <div className="idx-config-grid idx-config-grid--view-ref idx-config-view-ref-card">
      <label className="idx-label">
        {t("config.linking.viewKey")}
        <input
          className="idx-input idx-input--mono"
          value={draftKey}
          onChange={(e) => setDraftKey(e.target.value)}
          onBlur={() => onRename?.(draftKey.trim())}
          readOnly={!onRename}
        />
      </label>
      <label className="idx-label">
        {t("config.linking.viewSpace")}
        <input
          className="idx-input idx-input--mono"
          value={ref.space}
          onChange={(e) => onChange({ ...ref, space: e.target.value })}
        />
      </label>
      <label className="idx-label">
        {t("config.linking.viewExternalId")}
        <input
          className="idx-input idx-input--mono"
          value={ref.externalId}
          onChange={(e) => onChange({ ...ref, externalId: e.target.value })}
        />
      </label>
      <label className="idx-label">
        {t("config.linking.viewVersion")}
        <input
          className="idx-input idx-input--mono"
          value={ref.version}
          onChange={(e) => onChange({ ...ref, version: e.target.value })}
        />
      </label>
      {onRemove ? (
        <button type="button" className="idx-btn idx-btn--ghost" onClick={onRemove}>
          {t(removeLabelKey)}
        </button>
      ) : null}
    </div>
  );
}

function LinkCard({
  linkKey,
  link,
  viewKeys,
  edgeViewKeys,
  onChange,
  onRemove,
  onRename,
}: {
  linkKey: string;
  link: DirectRelationLinkConfig;
  viewKeys: string[];
  edgeViewKeys: string[];
  onChange: (next: DirectRelationLinkConfig) => void;
  onRemove: () => void;
  onRename: (nextKey: string) => void;
}) {
  const { t } = useAppSettings();

  const toggleWriteMode = (mode: WriteMode, checked: boolean) => {
    const next = checked
      ? [...new Set([...link.writeModes, mode])]
      : link.writeModes.filter((m) => m !== mode);
    onChange({ ...link, writeModes: next });
  };

  const toggleIncomingView = (viewKey: string, checked: boolean) => {
    const next = checked
      ? [...new Set([...link.incomingViews, viewKey])]
      : link.incomingViews.filter((v) => v !== viewKey);
    const resolveMap = syncResolveMapWithIncomingViews(
      parseResolveByIncomingView(link.resolveByIncomingView),
      next
    );
    onChange({
      ...link,
      incomingViews: next,
      resolveByIncomingView: serializeResolveByIncomingView(resolveMap),
    });
  };

  return (
    <section className="idx-config-subsection idx-config-link-card">
      <div className="idx-config-link-card__header">
        <label className="idx-checkbox-label">
          <input
            type="checkbox"
            checked={link.enabled}
            onChange={(e) => onChange({ ...link, enabled: e.target.checked })}
          />
          <strong>{linkDisplayLabel(linkKey, link)}</strong>
        </label>
        <button type="button" className="idx-btn idx-btn--ghost" onClick={onRemove}>
          {t("config.linking.removeLink")}
        </button>
      </div>

      <div className="idx-config-grid">
        <label className="idx-label">
          {t("config.linking.linkKey")}
          <input
            className="idx-input idx-input--mono"
            value={linkKey}
            onChange={(e) => onRename(e.target.value.trim())}
          />
        </label>
        <label className="idx-label">
          {t("config.linking.linkLabel")}
          <input
            className="idx-input"
            value={link.label ?? ""}
            onChange={(e) => onChange({ ...link, label: e.target.value })}
          />
        </label>
      </div>

      <div className="idx-checkbox-group idx-checkbox-group--inline">
        <span className="idx-config-inline-label">{t("config.linking.writeModes")}</span>
        {WRITE_MODES.map((mode) => (
          <label key={mode} className="idx-checkbox-label">
            <input
              type="checkbox"
              checked={link.writeModes.includes(mode)}
              onChange={(e) => toggleWriteMode(mode, e.target.checked)}
            />
            {t(WRITE_MODE_LABEL_KEYS[mode])}
          </label>
        ))}
      </div>

      <div className="idx-config-grid">
        <label className="idx-label">
          {t("config.linking.forwardView")}
          <select
            className="idx-input"
            value={link.forwardView}
            onChange={(e) => onChange({ ...link, forwardView: e.target.value })}
          >
            {viewKeys.map((key) => (
              <option key={key} value={key}>
                {key}
              </option>
            ))}
          </select>
        </label>
        <label className="idx-label">
          {t("config.linking.targetView")}
          <select
            className="idx-input"
            value={link.targetView}
            onChange={(e) => onChange({ ...link, targetView: e.target.value })}
          >
            {viewKeys.map((key) => (
              <option key={key} value={key}>
                {key}
              </option>
            ))}
          </select>
        </label>
        <label className="idx-label">
          {t("config.linking.property")}
          <input
            className="idx-input idx-input--mono"
            value={link.property}
            onChange={(e) => onChange({ ...link, property: e.target.value })}
          />
        </label>
        <label className="idx-label">
          {t("config.linking.cardinality")}
          <select
            className="idx-input"
            value={link.cardinality}
            onChange={(e) =>
              onChange({
                ...link,
                cardinality: e.target.value === "single" ? "single" : "list",
              })
            }
          >
            <option value="list">{t("config.linking.cardinalityList")}</option>
            <option value="single">{t("config.linking.cardinalitySingle")}</option>
          </select>
        </label>
        {link.cardinality === "single" ? (
          <label className="idx-checkbox-label idx-config-grid__full">
            <input
              type="checkbox"
              checked={link.overwriteExisting}
              onChange={(e) => onChange({ ...link, overwriteExisting: e.target.checked })}
            />
            {t("config.linking.overwriteExisting")}
          </label>
        ) : null}
        <div className="idx-config-grid__full">
          <span className="idx-config-inline-label">{t("config.linking.incomingViews")}</span>
          <div className="idx-checkbox-group idx-checkbox-group--inline">
            {viewKeys.map((key) => (
              <label key={key} className="idx-checkbox-label">
                <input
                  type="checkbox"
                  checked={link.incomingViews.includes(key)}
                  onChange={(e) => toggleIncomingView(key, e.target.checked)}
                />
                {key}
              </label>
            ))}
          </div>
        </div>
        <label className="idx-label idx-config-grid__full">
          {t("config.linking.linkSourceTypes")}
          <StringListInput
            value={link.sourceTypes}
            onChange={(sourceTypes) => onChange({ ...link, sourceTypes })}
            placeholder={t("config.linking.sourceTypesPlaceholder")}
            mono
          />
          <span className="idx-config-hint">{t("config.linking.linkSourceTypesHint")}</span>
        </label>
      </div>

      {link.writeModes.includes("edge") ? (
        <div className="idx-config-nested">
          <h5 className="idx-config-subsection__title">{t("config.linking.edgeSettings")}</h5>
          <label className="idx-label">
            {t("config.linking.edgeViewSelect")}
            <select
              className="idx-input"
              value={link.edgeViewKey}
              onChange={(e) => onChange({ ...link, edgeViewKey: e.target.value })}
            >
              <option value="">—</option>
              {edgeViewKeys.map((key) => (
                <option key={key} value={key}>
                  {key}
                </option>
              ))}
            </select>
          </label>
        </div>
      ) : null}

      {link.writeModes.includes("diagram_annotation") ? (
        <div className="idx-config-nested">
          <h5 className="idx-config-subsection__title">{t("config.linking.diagramAnnotationSettings")}</h5>
          <div className="idx-config-grid">
            <label className="idx-label">
              {t("config.linking.diagramCreateStatus")}
              <input
                className="idx-input"
                value={link.diagramAnnotation.createStatus}
                onChange={(e) =>
                  onChange({
                    ...link,
                    diagramAnnotation: {
                      ...link.diagramAnnotation,
                      createStatus: e.target.value,
                    },
                  })
                }
              />
            </label>
            <label className="idx-label">
              {t("config.linking.diagramAnnotationIdPath")}
              <input
                className="idx-input idx-input--mono"
                value={link.diagramAnnotation.annotationIdPath}
                onChange={(e) =>
                  onChange({
                    ...link,
                    diagramAnnotation: {
                      ...link.diagramAnnotation,
                      annotationIdPath: e.target.value,
                    },
                  })
                }
              />
            </label>
            <label className="idx-checkbox-label">
              <input
                type="checkbox"
                checked={link.diagramAnnotation.updateEndNodeOnly}
                onChange={(e) =>
                  onChange({
                    ...link,
                    diagramAnnotation: {
                      ...link.diagramAnnotation,
                      updateEndNodeOnly: e.target.checked,
                    },
                  })
                }
              />
              {t("config.linking.diagramUpdateEndNodeOnly")}
            </label>
            <label className="idx-checkbox-label">
              <input
                type="checkbox"
                checked={link.diagramAnnotation.fileFromReference}
                onChange={(e) =>
                  onChange({
                    ...link,
                    diagramAnnotation: {
                      ...link.diagramAnnotation,
                      fileFromReference: e.target.checked,
                    },
                  })
                }
              />
              {t("config.linking.diagramFileFromReference")}
            </label>
          </div>
        </div>
      ) : null}

      <ResolveRulesEditor
        incomingViews={link.incomingViews}
        value={
          link.resolveByIncomingView != null &&
          typeof link.resolveByIncomingView === "object" &&
          !Array.isArray(link.resolveByIncomingView)
            ? (link.resolveByIncomingView as Record<string, unknown>)
            : undefined
        }
        onChange={(resolveByIncomingView) => onChange({ ...link, resolveByIncomingView })}
      />
    </section>
  );
}

export function DirectRelationEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const viewKeys = Object.keys(value.views ?? {});
  const edgeViewKeys = Object.keys(value.edgeViews ?? {});
  const linkKeys = orderedLinkKeys(value);

  const addView = () => {
    const key = `view_${viewKeys.length + 1}`;
    onChange({
      ...value,
      views: { ...value.views, [key]: emptyDmViewRef() },
    });
  };

  const removeView = (key: string) => {
    const { [key]: _removed, ...views } = value.views;
    onChange({ ...value, views });
  };

  const renameView = (oldKey: string, newKey: string) => {
    onChange(renameViewRegistryKey(value, "views", oldKey, newKey));
  };

  const addEdgeView = () => {
    const key = `edge_${edgeViewKeys.length + 1}`;
    onChange({
      ...value,
      edgeViews: { ...value.edgeViews, [key]: emptyDmViewRef() },
    });
  };

  const removeEdgeView = (key: string) => {
    const { [key]: _removed, ...edgeViews } = value.edgeViews;
    onChange({ ...value, edgeViews });
  };

  const renameEdgeView = (oldKey: string, newKey: string) => {
    onChange(renameViewRegistryKey(value, "edgeViews", oldKey, newKey));
  };

  const addLink = () => {
    const key = `link_${linkKeys.length + 1}`;
    const fwd = viewKeys[0] ?? "";
    const tgt = viewKeys[1] ?? viewKeys[0] ?? "";
    const link = defaultDirectRelationLinkConfig(fwd, tgt, "");
    onChange({
      ...value,
      linkOrder: [...value.linkOrder, key],
      links: { ...value.links, [key]: link },
    });
  };

  const removeLink = (key: string) => {
    const { [key]: _removed, ...links } = value.links;
    onChange({
      ...value,
      linkOrder: value.linkOrder.filter((k) => k !== key),
      links,
    });
  };

  const renameLink = (oldKey: string, newKey: string) => {
    if (!newKey || newKey === oldKey || value.links[newKey]) return;
    const { [oldKey]: link, ...rest } = value.links;
    if (!link) return;
    onChange({
      ...value,
      linkOrder: value.linkOrder.map((k) => (k === oldKey ? newKey : k)),
      links: { ...rest, [newKey]: link },
    });
  };

  return (
    <FormPanel title={t("config.linking.title")} hint={t("config.linking.hint")}>
      <div className="idx-checkbox-group idx-checkbox-group--inline">
        <label className="idx-checkbox-label">
          <input
            type="checkbox"
            checked={value.enabled}
            onChange={(e) => onChange({ ...value, enabled: e.target.checked })}
          />
          {t("config.linking.enabled")}
        </label>
      </div>

      <div className="idx-config-grid">
        <label className="idx-label">
          {t("config.linking.minConfidence")}
          <input
            className="idx-input"
            type="number"
            min={0}
            max={1}
            step={0.05}
            value={value.minConfidence}
            onChange={(e) => onChange({ ...value, minConfidence: Number(e.target.value) })}
          />
        </label>
        <label className="idx-label">
          {t("config.linking.maxListSize")}
          <input
            className="idx-input"
            type="number"
            min={1}
            step={1}
            value={value.maxListSize}
            onChange={(e) => onChange({ ...value, maxListSize: Number(e.target.value) })}
          />
        </label>
        <label className="idx-label">
          {t("config.linking.requireAnnotationStatus")}
          <select
            className="idx-input"
            value={value.requireAnnotationStatus}
            onChange={(e) => onChange({ ...value, requireAnnotationStatus: e.target.value })}
          >
            <option value="">{t("config.linking.requireAnnotationStatusNone")}</option>
            <option value="Approved">{t("config.linking.requireAnnotationStatusApproved")}</option>
          </select>
        </label>
        <label className="idx-label idx-config-grid__full">
          {t("config.linking.allowedStatuses")}
          <StringListInput
            value={value.allowedAnnotationStatuses}
            onChange={(allowedAnnotationStatuses) =>
              onChange({ ...value, allowedAnnotationStatuses })
            }
            placeholder={t("config.linking.allowedStatusesPlaceholder")}
          />
          <span className="idx-config-hint">{t("config.linking.allowedStatusesHint")}</span>
        </label>
        <label className="idx-label idx-config-grid__full">
          {t("config.linking.sourceTypes")}
          <StringListInput
            value={value.sourceTypes}
            onChange={(sourceTypes) => onChange({ ...value, sourceTypes })}
            placeholder={t("config.linking.sourceTypesPlaceholder")}
            mono
          />
          <span className="idx-config-hint">{t("config.linking.sourceTypesHint")}</span>
        </label>
      </div>

      <section className="idx-config-subsection">
        <div className="idx-config-link-card__header">
          <h4 className="idx-config-subsection__title">{t("config.linking.views")}</h4>
          <button type="button" className="idx-btn idx-btn--ghost" onClick={addView}>
            {t("config.linking.addView")}
          </button>
        </div>
        <p className="idx-pane__hint">{t("config.linking.viewsHint")}</p>
        {viewKeys.length === 0 ? (
          <p className="idx-pane__hint">{t("config.linking.viewsEmpty")}</p>
        ) : null}
        {viewKeys.map((key) => (
          <DmViewRefFields
            key={key}
            refKey={key}
            ref={value.views[key]}
            onChange={(ref) =>
              onChange({ ...value, views: updateViewRefs(value.views, key, ref) })
            }
            onRename={(nextKey) => renameView(key, nextKey)}
            onRemove={() => removeView(key)}
          />
        ))}
      </section>

      <section className="idx-config-subsection">
        <div className="idx-config-link-card__header">
          <h4 className="idx-config-subsection__title">{t("config.linking.edgeViews")}</h4>
          <button type="button" className="idx-btn idx-btn--ghost" onClick={addEdgeView}>
            {t("config.linking.addEdgeView")}
          </button>
        </div>
        <p className="idx-pane__hint">{t("config.linking.edgeViewsHint")}</p>
        {edgeViewKeys.length === 0 ? (
          <p className="idx-pane__hint">{t("config.linking.edgeViewsEmpty")}</p>
        ) : null}
        {edgeViewKeys.map((key) => (
          <DmViewRefFields
            key={key}
            refKey={key}
            ref={value.edgeViews[key]}
            onChange={(ref) =>
              onChange({ ...value, edgeViews: updateViewRefs(value.edgeViews, key, ref) })
            }
            onRename={(nextKey) => renameEdgeView(key, nextKey)}
            onRemove={() => removeEdgeView(key)}
            removeLabelKey="config.linking.removeEdgeView"
          />
        ))}
      </section>

      <section className="idx-config-subsection">
        <div className="idx-config-link-card__header">
          <h4 className="idx-config-subsection__title">{t("config.linking.linkCards")}</h4>
          <button type="button" className="idx-btn idx-btn--ghost" onClick={addLink}>
            {t("config.linking.addLink")}
          </button>
        </div>
        {linkKeys.map((key) => (
          <LinkCard
            key={key}
            linkKey={key}
            link={value.links[key]}
            viewKeys={viewKeys}
            edgeViewKeys={edgeViewKeys}
            onChange={(link) => onChange(updateLink(value, key, link))}
            onRemove={() => removeLink(key)}
            onRename={(nextKey) => renameLink(key, nextKey)}
          />
        ))}
      </section>
    </FormPanel>
  );
}
