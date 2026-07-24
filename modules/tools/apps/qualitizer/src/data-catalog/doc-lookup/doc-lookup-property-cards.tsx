import { Fragment, type ReactNode } from "react";
import { useI18n } from "@/shared/i18n";
import {
  getPropertyColorClasses,
  type DocLookupDataCategory,
  type PropertyColorClasses,
} from "./doc-lookup-colors";
import type {
  AssetHierarchyWarning,
  CogniteAssetHierarchyAnalysis,
  CogniteDescribableData,
  NodeInstanceRef,
} from "./doc-lookup-fetchers";

const CHANGED_PROPERTY_CLASSES: PropertyColorClasses = {
  border: "border-amber-300",
  bg: "bg-amber-50/60",
  label: "text-amber-800",
};

function PropertyTypeCard({
  title,
  colors,
  changed,
  children,
}: {
  title: string;
  colors: PropertyColorClasses;
  changed?: boolean;
  children: ReactNode;
}) {
  const styles = changed ? CHANGED_PROPERTY_CLASSES : colors;

  return (
    <div className={`rounded-lg border px-3 py-2.5 ${styles.border} ${styles.bg}`}>
      <div className={`text-[11px] font-semibold uppercase tracking-wide ${styles.label}`}>
        {title}
      </div>
      <div className="mt-2 flex flex-col gap-2">{children}</div>
    </div>
  );
}

function CardField({
  label,
  colors,
  children,
}: {
  label: string;
  colors: PropertyColorClasses;
  children: ReactNode;
}) {
  return (
    <div className="min-w-0">
      <div className={`text-[10px] font-medium uppercase tracking-wide ${colors.label}`}>{label}</div>
      <div className="mt-0.5 text-sm leading-snug text-slate-900">{children}</div>
    </div>
  );
}

function StringChipList({ items, colors }: { items: string[]; colors: PropertyColorClasses }) {
  return (
    <div className="flex flex-wrap gap-1">
      {items.map((item) => (
        <span
          key={item}
          className={`rounded-full border px-2 py-0.5 font-mono text-[11px] ${colors.border} ${colors.bg} text-slate-900`}
        >
          {item}
        </span>
      ))}
    </div>
  );
}

export function AssetPathBreadcrumb({
  path,
  category,
}: {
  path: NodeInstanceRef[];
  category: DocLookupDataCategory;
}) {
  return (
    <AssetHierarchyBreadcrumb path={path} root={path[0] ?? null} parent={null} category={category} />
  );
}

type HierarchyNodeRole = "root" | "parent" | "current" | "ancestor";

function hierarchyNodeRole(
  ref: NodeInstanceRef,
  index: number,
  path: NodeInstanceRef[],
  root: NodeInstanceRef | null,
  parent: NodeInstanceRef | null
): HierarchyNodeRole {
  if (root !== null && ref.space === root.space && ref.externalId === root.externalId) {
    return "root";
  }
  if (parent !== null && ref.space === parent.space && ref.externalId === parent.externalId) {
    return "parent";
  }
  if (index === path.length - 1) return "current";
  return "ancestor";
}

const HIERARCHY_ROLE_STYLES: Record<
  HierarchyNodeRole,
  { node: string; badge: string; labelKey: string | null }
> = {
  root: {
    node: "border-emerald-300 bg-emerald-100 font-semibold text-emerald-950",
    badge: "bg-emerald-200 text-emerald-900",
    labelKey: "dataCatalog.docLookup.hierarchy.role.root",
  },
  parent: {
    node: "border-blue-300 bg-blue-100 font-medium text-blue-950",
    badge: "bg-blue-200 text-blue-900",
    labelKey: "dataCatalog.docLookup.hierarchy.role.parent",
  },
  current: {
    node: "border-slate-400 bg-slate-200 font-semibold text-slate-950",
    badge: "bg-slate-300 text-slate-900",
    labelKey: "dataCatalog.docLookup.hierarchy.role.self",
  },
  ancestor: {
    node: "border-transparent bg-transparent text-slate-800",
    badge: "",
    labelKey: null,
  },
};

export function AssetHierarchyBreadcrumb({
  path,
  root,
  parent,
  category,
}: {
  path: NodeInstanceRef[];
  root: NodeInstanceRef | null;
  parent: NodeInstanceRef | null;
  category: DocLookupDataCategory;
}) {
  const { t } = useI18n();
  const colors = getPropertyColorClasses("path", category);

  if (path.length === 0) {
    return <p className="text-sm text-slate-500">{t("dataCatalog.docLookup.hierarchy.noPath")}</p>;
  }

  return (
    <div className="flex min-w-0 flex-wrap items-center gap-x-1 gap-y-0.5 text-xs leading-snug">
      {path.map((ref, index) => {
        const role = hierarchyNodeRole(ref, index, path, root, parent);
        const roleStyles = HIERARCHY_ROLE_STYLES[role];
        const showRole = role !== "ancestor";

        return (
          <Fragment key={`${ref.space}:${ref.externalId}:${index}`}>
            {index > 0 ? <span className={`${colors.label} opacity-60`}>→</span> : null}
            <span
              className={`inline-flex items-center gap-1 rounded border px-1.5 py-0.5 font-mono text-xs ${roleStyles.node}`}
              title={`${ref.space} / ${ref.externalId}`}
            >
              {showRole && roleStyles.labelKey !== null ? (
                <span
                  className={`rounded px-1 text-[9px] font-semibold uppercase tracking-wide ${roleStyles.badge}`}
                >
                  {t(roleStyles.labelKey)}
                </span>
              ) : null}
              {ref.externalId}
            </span>
          </Fragment>
        );
      })}
    </div>
  );
}

function hierarchyWarningMessage(
  warning: AssetHierarchyWarning,
  t: (key: string, params?: Record<string, string | number>) => string
): string {
  if (warning.kind === "path_missing") {
    return t("dataCatalog.docLookup.hierarchy.warning.pathMissing");
  }
  if (warning.kind === "parent_missing") {
    return t("dataCatalog.docLookup.hierarchy.warning.parentMissing");
  }
  if (warning.kind === "root_missing") {
    return t("dataCatalog.docLookup.hierarchy.warning.rootMissing");
  }
  if (warning.kind === "parent_not_in_path") {
    return t("dataCatalog.docLookup.hierarchy.warning.parentNotInPath", {
      externalId: warning.parent.externalId,
    });
  }
  return t("dataCatalog.docLookup.hierarchy.warning.rootMismatch", {
    rootExternalId: warning.root.externalId,
    pathRootExternalId: warning.pathRoot.externalId,
  });
}

function hierarchyFieldChanged(changedPaths: string[], field: string): boolean {
  return changedPaths.some(
    (path) => path === field || path.startsWith(`${field}.`) || path.endsWith(`.${field}`)
  );
}

export function CogniteAssetHierarchyCard({
  hierarchy,
  changedPaths,
  category,
}: {
  hierarchy: CogniteAssetHierarchyAnalysis;
  changedPaths: string[];
  category: DocLookupDataCategory;
}) {
  const { t } = useI18n();
  const colors = getPropertyColorClasses("path", category);
  const changed =
    hierarchyFieldChanged(changedPaths, "path") ||
    hierarchyFieldChanged(changedPaths, "Path") ||
    hierarchyFieldChanged(changedPaths, "root") ||
    hierarchyFieldChanged(changedPaths, "parent");
  const styles = changed ? CHANGED_PROPERTY_CLASSES : colors;

  return (
    <div className={`rounded-lg border px-3 py-1.5 ${styles.border} ${styles.bg}`}>
      {hierarchy.warnings.length > 0 ? (
        <div className="mb-1 flex flex-col gap-1">
          {hierarchy.warnings.map((warning, index) => (
            <p
              key={`${warning.kind}-${index}`}
              className="rounded border border-amber-300 bg-amber-50 px-2 py-0.5 text-xs text-amber-950"
            >
              {hierarchyWarningMessage(warning, t)}
            </p>
          ))}
        </div>
      ) : null}
      <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5">
        <span
          className={`shrink-0 text-[11px] font-semibold uppercase tracking-wide ${styles.label}`}
        >
          {t("dataCatalog.docLookup.hierarchy.title")}
        </span>
        {hierarchy.path.length > 0 ? (
          <AssetHierarchyBreadcrumb
            path={hierarchy.path}
            root={hierarchy.root}
            parent={hierarchy.parent}
            category={category}
          />
        ) : (
          <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5 text-xs text-slate-800">
            {hierarchy.root !== null ? (
              <span className="font-mono">
                <span className={`font-sans text-[10px] font-medium uppercase ${styles.label} opacity-75`}>
                  {t("dataCatalog.docLookup.hierarchy.root")}{" "}
                </span>
                {hierarchy.root.space} / {hierarchy.root.externalId}
              </span>
            ) : null}
            {hierarchy.parent !== null ? (
              <span className="font-mono">
                <span className={`font-sans text-[10px] font-medium uppercase ${styles.label} opacity-75`}>
                  {t("dataCatalog.docLookup.hierarchy.parent")}{" "}
                </span>
                {hierarchy.parent.space} / {hierarchy.parent.externalId}
              </span>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
}

export function CogniteAssetPathCard({
  path,
  changed,
  category,
}: {
  path: NodeInstanceRef[];
  changed: boolean;
  category: DocLookupDataCategory;
}) {
  const { t } = useI18n();
  const colors = getPropertyColorClasses("path", category);

  return (
    <PropertyTypeCard title={t("dataCatalog.docLookup.cogniteAssetPath")} colors={colors} changed={changed}>
      <AssetPathBreadcrumb path={path} category={category} />
    </PropertyTypeCard>
  );
}

function describableFieldChanged(changedPaths: string[], field: string): boolean {
  return changedPaths.some(
    (path) => path === field || path.startsWith(`${field}.`) || path.endsWith(`.${field}`)
  );
}

export function CogniteDescribableCard({
  data,
  changedPaths,
  category,
}: {
  data: CogniteDescribableData;
  changedPaths: string[];
  category: DocLookupDataCategory;
}) {
  const { t } = useI18n();
  const colors = getPropertyColorClasses("CogniteDescribable", category);
  const changed =
    describableFieldChanged(changedPaths, "name") ||
    describableFieldChanged(changedPaths, "description") ||
    describableFieldChanged(changedPaths, "tags") ||
    describableFieldChanged(changedPaths, "aliases");
  const styles = changed ? CHANGED_PROPERTY_CLASSES : colors;
  const hasTagsOrAliases = data.tags.length > 0 || data.aliases.length > 0;

  return (
    <div className={`rounded-lg border px-3 py-1.5 ${styles.border} ${styles.bg}`}>
      <div className="flex min-w-0 flex-wrap items-baseline gap-x-2 gap-y-0.5">
        <span
          className={`shrink-0 text-[11px] font-semibold uppercase tracking-wide ${styles.label}`}
        >
          {t("dataCatalog.docLookup.cogniteDescribable")}
        </span>
        {data.name !== null ? (
          <>
            <span className={`text-[10px] font-medium uppercase ${styles.label} opacity-75`}>
              {t("dataCatalog.docLookup.field.name")}
            </span>
            <span className="text-sm font-medium text-slate-900">{data.name}</span>
          </>
        ) : null}
        {data.description !== null ? (
          <>
            <span className={`text-[10px] font-medium uppercase ${styles.label} opacity-75`}>
              {t("dataCatalog.docLookup.field.description")}
            </span>
            <span className="min-w-0 text-sm text-slate-900">{data.description}</span>
          </>
        ) : null}
      </div>
      {hasTagsOrAliases ? (
        <div className="mt-1 flex min-w-0 flex-wrap items-baseline gap-x-2 gap-y-1">
          {data.tags.length > 0 ? (
            <>
              <span className={`text-[10px] font-medium uppercase ${styles.label} opacity-75`}>
                {t("dataCatalog.docLookup.field.tags")}
              </span>
              <StringChipList items={data.tags} colors={colors} />
            </>
          ) : null}
          {data.aliases.length > 0 ? (
            <>
              <span className={`text-[10px] font-medium uppercase ${styles.label} opacity-75`}>
                {t("dataCatalog.docLookup.field.aliases")}
              </span>
              <StringChipList items={data.aliases} colors={colors} />
            </>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
