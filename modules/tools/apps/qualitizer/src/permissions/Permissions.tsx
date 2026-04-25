import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { isForbiddenError } from "@/shared/cdf-errors";
import { useAppSdk } from "@/shared/auth";
import { useI18n } from "@/shared/i18n";
import { useSdkManager } from "@/shared/SdkManager";
import { cachedSecurityGroupsList } from "@/shared/security-groups-cache";
import { Loader } from "@/shared/Loader";
import { usePrivateMode } from "@/shared/PrivateModeContext";
import {
  getActionDisplay,
  getCapability,
  getScopeDisplay,
  normalizeCapability,
} from "@/shared/permissions-utils";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  loadNavState,
  saveNavState,
  type PersistedPermissionsSubView,
} from "@/shared/nav-persistence";
import { PermissionsHelpModal } from "./PermissionsHelpModal";
import { PermissionsCompareHelpModal } from "./PermissionsCompareHelpModal";
import { PermissionsAccessInfoInput } from "./PermissionsAccessInfoInput";
import { PermissionsCrossProject } from "./PermissionsCrossProject";
import { PermissionsGroupJsonModal } from "./PermissionsGroupJsonModal";
import type { AccessInfo, CompareTableRow, UploadedUser } from "./types";
import {
  useCrossProjectMembershipCheck,
  type CrossProjectMembershipSource,
} from "./useCrossProjectMembershipCheck";
import { usePermissionsData } from "./usePermissionsData";

function compareTableRowKey(row: CompareTableRow): string {
  return row.scope === "current" ? `c:${row.group.id}` : `o:${row.projectUrlName}:${row.groupId}`;
}

function userMemberOfGroupInProject(
  user: UploadedUser,
  projectUrlName: string,
  groupId: number
): boolean {
  const p = user.data.projects.find((pr) => pr.projectUrlName === projectUrlName);
  return p?.groups?.includes(groupId) ?? false;
}

function otherProjectGroupLabelKey(projectUrlName: string, groupId: number): string {
  return `${projectUrlName}\x1f${groupId}`;
}

const CROSS_PROJECT_VIEW_TOKEN = "__cross_token__";

function readAccessInfo(parsed: unknown): AccessInfo | null {
  if (!parsed || typeof parsed !== "object") return null;
  const o = parsed as Record<string, unknown>;
  if (typeof o.subject !== "string" || !Array.isArray(o.projects)) return null;
  return parsed as AccessInfo;
}

function isPermissionsSubView(v: unknown): v is PersistedPermissionsSubView {
  return (
    v === "groups" ||
    v === "compare" ||
    v === "crossProject" ||
    v === "spaces" ||
    v === "datasets"
  );
}

function readInitialPermissionsSubView(): PersistedPermissionsSubView {
  const { permissionsSubView } = loadNavState();
  if (isPermissionsSubView(permissionsSubView)) return permissionsSubView;
  return "groups";
}

export function Permissions() {
  const { sdk, isLoading: isDuneLoading } = useAppSdk();
  const { getSdk } = useSdkManager();
  const { t } = useI18n();
  const { isPrivateMode } = usePrivateMode();
  const pc = isPrivateMode ? " private-mask" : "";
  const {
    status,
    errorMessage,
    loadingDetail,
    groups,
    capabilityNames,
    dataSets,
    dataSetAccess,
    spaces,
    spaceAccess,
  } = usePermissionsData({ isDuneLoading, sdk });
  const [users, setUsers] = useState<UploadedUser[]>([]);
  const [currentUser, setCurrentUser] = useState<UploadedUser | null>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [pasteText, setPasteText] = useState("");
  const [pasteDisplayName, setPasteDisplayName] = useState("");
  const [compareGroupSearch, setCompareGroupSearch] = useState("");
  const [compareOnlyUtilized, setCompareOnlyUtilized] = useState(false);
  const [compareIncludeOtherProjects, setCompareIncludeOtherProjects] = useState(false);
  const [compareShowAllGroups, setCompareShowAllGroups] = useState(false);
  const [remoteOtherGroupLabels, setRemoteOtherGroupLabels] = useState<Record<string, string>>({});
  const [remoteOtherGroupPayloads, setRemoteOtherGroupPayloads] = useState<Record<string, unknown>>(
    {}
  );
  const [remoteOtherGroupLabelsStatus, setRemoteOtherGroupLabelsStatus] = useState<
    "idle" | "loading" | "done" | "error"
  >("idle");
  const [remoteOtherGroupForbiddenProjects, setRemoteOtherGroupForbiddenProjects] = useState<string[]>(
    []
  );
  const [groupJsonModal, setGroupJsonModal] = useState<{ title: string; json: string } | null>(null);
  const [showHelp, setShowHelp] = useState(false);
  const [showCompareHelp, setShowCompareHelp] = useState(false);
  const [showLoader, setShowLoader] = useState(false);
  const [crossProjectViewerId, setCrossProjectViewerId] = useState<string>(CROSS_PROJECT_VIEW_TOKEN);
  const [crossProjectAccessExpanded, setCrossProjectAccessExpanded] = useState(false);
  const permissionsTabRef = useRef<PersistedPermissionsSubView>(readInitialPermissionsSubView());

  const prevUploadedUsersCountRef = useRef(0);
  const pendingAutoUtilAfterUploadRef = useRef(false);

  const isPageLoading = isDuneLoading || status === "loading";

  const openGroupDefinitionJson = useCallback((title: string, value: unknown) => {
    setGroupJsonModal({ title, json: JSON.stringify(value, null, 2) });
  }, []);

  useEffect(() => {
    setShowLoader(isPageLoading);
  }, [isPageLoading]);

  useEffect(() => {
    if (isDuneLoading) return;
    let cancelled = false;
    const loadCurrentUser = async () => {
      try {
        const profile = (await sdk.profiles.me()) as {
          displayName?: string;
          givenName?: string;
          surname?: string;
          email?: string;
          userIdentifier?: string;
        };
        const tokenInspect = await sdk.get<{
          subject?: string;
          projects?: Array<{ projectUrlName?: string; groups?: number[] }>;
        }>("/api/v1/token/inspect");
        const projectEntry = tokenInspect.data?.projects?.find(
          (project) => project.projectUrlName === sdk.project
        );
        const projectsFromToken = (tokenInspect.data?.projects ?? [])
          .filter(
            (p): p is { projectUrlName: string; groups?: number[] } =>
              Boolean(p.projectUrlName && typeof p.projectUrlName === "string")
          )
          .map((p) => ({
            projectUrlName: p.projectUrlName,
            groups: p.groups ?? [],
          }));
        const projects =
          projectsFromToken.length > 0
            ? projectsFromToken
            : [
                {
                  projectUrlName: sdk.project,
                  groups: projectEntry?.groups ?? [],
                },
              ];
        const subject =
          tokenInspect.data?.subject ??
          profile.userIdentifier ??
          profile.email ??
          "current-user";
        const label =
          profile.displayName ??
          [profile.givenName, profile.surname].filter(Boolean).join(" ") ??
          profile.email ??
          subject;
        const user: UploadedUser = {
          id: `current-${subject}`,
          label: label ? `${label} ${t("permissions.currentSuffix")}` : t("permissions.currentUser"),
          data: {
            subject,
            projects,
          },
        };
        if (!cancelled) {
          setCurrentUser(user);
        }
      } catch {
        if (!cancelled) {
          setCurrentUser(null);
        }
      }
    };

    loadCurrentUser();
    return () => {
      cancelled = true;
    };
  }, [isDuneLoading, sdk, t]);

  const datasetRows = useMemo(() => {
    return [...dataSets].sort((a, b) =>
      (a.name ?? a.id).toString().localeCompare((b.name ?? b.id).toString())
    );
  }, [dataSets]);

  const spaceRows = useMemo(() => {
    return [...spaces].sort((a, b) =>
      (a.name ?? a.space).toString().localeCompare((b.name ?? b.space).toString())
    );
  }, [spaces]);

  const groupNameMap = useMemo(() => {
    return new Map(
      groups.map((group) => [
        group.id,
        group.name ?? t("permissions.group.fallback", { id: group.id }),
      ])
    );
  }, [groups, t]);

  const comparisonRows = useMemo(() => {
    return [...groups].sort((a, b) =>
      (a.name ?? a.id).toString().localeCompare((b.name ?? b.id).toString())
    );
  }, [groups]);

  const comparisonUsers = useMemo(() => {
    return currentUser ? [currentUser, ...users] : users;
  }, [currentUser, users]);

  const crossProjectSource: CrossProjectMembershipSource = useMemo(() => {
    if (crossProjectViewerId === CROSS_PROJECT_VIEW_TOKEN) return { kind: "token" };
    const u = users.find((x) => x.id === crossProjectViewerId);
    if (!u) return { kind: "token" };
    const projects = (u.data.projects ?? [])
      .filter((p): p is { projectUrlName: string; groups?: number[] } =>
        Boolean(p?.projectUrlName && typeof p.projectUrlName === "string")
      )
      .map((p) => ({
        projectUrlName: p.projectUrlName.trim(),
        groups: [...(p.groups ?? [])],
      }));
    const signature = `${u.id}|${projects
      .map((pr) => `${pr.projectUrlName}:${[...pr.groups].sort((a, b) => a - b).join(",")}`)
      .join("|")}`;
    return { kind: "projects", signature, projects };
  }, [crossProjectViewerId, users]);

  useEffect(() => {
    if (crossProjectViewerId === CROSS_PROJECT_VIEW_TOKEN) return;
    if (!users.some((u) => u.id === crossProjectViewerId)) {
      setCrossProjectViewerId(CROSS_PROJECT_VIEW_TOKEN);
    }
  }, [users, crossProjectViewerId]);

  const crossProjectViewerSummary = useMemo(() => {
    if (crossProjectViewerId === CROSS_PROJECT_VIEW_TOKEN) {
      return t("permissions.crossProject.viewerCurrentUser");
    }
    const u = users.find((x) => x.id === crossProjectViewerId);
    return u ? (u.label || u.data.subject) : t("permissions.crossProject.viewerCurrentUser");
  }, [crossProjectViewerId, users, t]);

  const referencedGroupIds = useMemo(() => {
    const set = new Set<number>();
    for (const user of comparisonUsers) {
      const project = user.data.projects.find((p) => p.projectUrlName === sdk.project);
      for (const gid of project?.groups ?? []) {
        set.add(gid);
      }
    }
    return set;
  }, [comparisonUsers, sdk.project]);

  const otherProjectGroupRows = useMemo((): Extract<CompareTableRow, { scope: "other" }>[] => {
    const seen = new Set<string>();
    const pairs: { projectUrlName: string; groupId: number }[] = [];
    for (const user of comparisonUsers) {
      for (const pr of user.data.projects) {
        const url = pr.projectUrlName?.trim();
        if (!url || url === sdk.project) continue;
        for (const gid of pr.groups ?? []) {
          const k = `${url}:${gid}`;
          if (seen.has(k)) continue;
          seen.add(k);
          pairs.push({ projectUrlName: url, groupId: gid });
        }
      }
    }
    pairs.sort((a, b) => {
      const c = a.projectUrlName.localeCompare(b.projectUrlName);
      return c !== 0 ? c : a.groupId - b.groupId;
    });
    return pairs.map((p) => ({ scope: "other" as const, ...p }));
  }, [comparisonUsers, sdk.project]);

  const hasOtherProjectMemberships = otherProjectGroupRows.length > 0;

  useEffect(() => {
    const prev = prevUploadedUsersCountRef.current;
    if (users.length < prev) {
      prevUploadedUsersCountRef.current = users.length;
      if (users.length === 0) pendingAutoUtilAfterUploadRef.current = false;
      return;
    }
    const combined = groups.length + otherProjectGroupRows.length;
    if (users.length > prev) {
      prevUploadedUsersCountRef.current = users.length;
      if (combined > 100) {
        setCompareOnlyUtilized(true);
        pendingAutoUtilAfterUploadRef.current = false;
      } else {
        pendingAutoUtilAfterUploadRef.current = true;
      }
      return;
    }
    if (pendingAutoUtilAfterUploadRef.current && users.length > 0 && combined > 100) {
      setCompareOnlyUtilized(true);
      pendingAutoUtilAfterUploadRef.current = false;
    }
  }, [users.length, groups.length, otherProjectGroupRows.length]);

  const otherProjectsToResolve = useMemo(() => {
    if (!compareIncludeOtherProjects) return [] as string[];
    return [...new Set(otherProjectGroupRows.map((r) => r.projectUrlName))].sort();
  }, [compareIncludeOtherProjects, otherProjectGroupRows]);

  const otherProjectGroupRowsSignature = useMemo(
    () =>
      otherProjectGroupRows.map((r) => `${r.projectUrlName}:${r.groupId}`).sort().join("|"),
    [otherProjectGroupRows]
  );

  useEffect(() => {
    if (!compareIncludeOtherProjects || otherProjectsToResolve.length === 0) {
      setRemoteOtherGroupLabels({});
      setRemoteOtherGroupPayloads({});
      setRemoteOtherGroupForbiddenProjects([]);
      setRemoteOtherGroupLabelsStatus("idle");
      return;
    }
    let cancelled = false;
    setRemoteOtherGroupLabelsStatus("loading");
    (async () => {
      const next: Record<string, string> = {};
      const nextPayloads: Record<string, unknown> = {};
      const forbidden: string[] = [];
      try {
        for (const projectUrl of otherProjectsToResolve) {
          try {
            const client = getSdk(projectUrl);
            const list = await cachedSecurityGroupsList(client, projectUrl);
            const byId = new Map(list.map((g) => [g.id, g]));
            for (const row of otherProjectGroupRows) {
              if (row.projectUrlName !== projectUrl) continue;
              const g = byId.get(row.groupId);
              const lk = otherProjectGroupLabelKey(row.projectUrlName, row.groupId);
              const label =
                g?.name?.trim() ||
                g?.sourceId?.trim() ||
                t("permissions.group.fallback", { id: row.groupId });
              next[lk] = label;
              nextPayloads[lk] =
                g != null
                  ? { ...g, projectUrlName: row.projectUrlName }
                  : { id: row.groupId, projectUrlName: row.projectUrlName };
            }
          } catch (e) {
            if (isForbiddenError(e)) {
              forbidden.push(projectUrl);
              for (const row of otherProjectGroupRows) {
                if (row.projectUrlName !== projectUrl) continue;
                const lk = otherProjectGroupLabelKey(row.projectUrlName, row.groupId);
                next[lk] = t("permissions.compare.groupDefinitionForbiddenFallback", {
                  id: row.groupId,
                });
                nextPayloads[lk] = {
                  id: row.groupId,
                  projectUrlName: row.projectUrlName,
                  groupDefinitionsForbidden: true,
                };
              }
            } else {
              throw e;
            }
          }
        }
        if (!cancelled) {
          forbidden.sort((a, b) => a.localeCompare(b));
          setRemoteOtherGroupForbiddenProjects(forbidden);
          setRemoteOtherGroupLabels(next);
          setRemoteOtherGroupPayloads(nextPayloads);
          setRemoteOtherGroupLabelsStatus("done");
        }
      } catch {
        if (!cancelled) {
          setRemoteOtherGroupLabels({});
          setRemoteOtherGroupPayloads({});
          setRemoteOtherGroupForbiddenProjects([]);
          setRemoteOtherGroupLabelsStatus("error");
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [compareIncludeOtherProjects, getSdk, otherProjectGroupRowsSignature, otherProjectsToResolve, t]);

  const compareAllRows = useMemo((): CompareTableRow[] => {
    const current: CompareTableRow[] = comparisonRows.map((g) => ({ scope: "current", group: g }));
    if (!compareIncludeOtherProjects) return current;
    return [...current, ...otherProjectGroupRows];
  }, [comparisonRows, compareIncludeOtherProjects, otherProjectGroupRows]);

  const compareFilteredRows = useMemo(() => {
    const q = compareGroupSearch.trim().toLowerCase();
    let rows = compareAllRows;
    if (compareOnlyUtilized) {
      rows = rows.filter((r) => {
        if (r.scope === "other") {
          return comparisonUsers.some((u) =>
            userMemberOfGroupInProject(u, r.projectUrlName, r.groupId)
          );
        }
        return referencedGroupIds.has(r.group.id);
      });
    }
    if (q) {
      rows = rows.filter((r) => {
        if (r.scope === "current") {
          const name = r.group.name ?? t("permissions.group.fallback", { id: r.group.id });
          const hay = `${name} ${r.group.id} ${r.group.sourceId ?? ""}`.toLowerCase();
          return hay.includes(q);
        }
        const resolved = remoteOtherGroupLabels[otherProjectGroupLabelKey(r.projectUrlName, r.groupId)];
        const hay = `${r.projectUrlName} ${r.groupId} ${resolved ?? ""}`.toLowerCase();
        return hay.includes(q);
      });
    }
    return rows;
  }, [
    compareAllRows,
    compareGroupSearch,
    compareOnlyUtilized,
    comparisonUsers,
    referencedGroupIds,
    remoteOtherGroupLabels,
    t,
  ]);

  const compareTableModel = useMemo(() => {
    const filtered = compareFilteredRows;
    const manyProjectGroups = groups.length > 100;
    const searchActive = Boolean(compareGroupSearch.trim());
    if (!manyProjectGroups || compareShowAllGroups || searchActive) {
      return {
        rows: filtered,
        hiddenCount: 0,
        pinnedCount: filtered.filter(
          (r) => r.scope === "other" || referencedGroupIds.has(r.group.id)
        ).length,
        truncated: false,
      };
    }
    const pinned = filtered.filter(
      (r) => r.scope === "other" || referencedGroupIds.has(r.group.id)
    );
    const unpinned = filtered.filter(
      (r) => r.scope === "current" && !referencedGroupIds.has(r.group.id)
    );
    const unpinnedShown = unpinned.slice(0, 100);
    const showKeys = new Set<string>();
    for (const r of pinned) showKeys.add(compareTableRowKey(r));
    for (const r of unpinnedShown) showKeys.add(compareTableRowKey(r));
    const rows = filtered.filter((r) => showKeys.has(compareTableRowKey(r)));
    const hiddenCount = filtered.length - rows.length;
    return {
      rows,
      hiddenCount,
      pinnedCount: pinned.length,
      truncated: hiddenCount > 0,
    };
  }, [
    compareFilteredRows,
    compareGroupSearch,
    compareShowAllGroups,
    groups.length,
    referencedGroupIds,
  ]);

  const handleUserUpload = async (files: FileList | null) => {
    if (!files || files.length === 0) return;
    setUploading(true);
    setUploadError(null);
    const loaded: UploadedUser[] = [];
    for (const file of Array.from(files)) {
      try {
        const text = await file.text();
        let parsed: unknown;
        try {
          parsed = JSON.parse(text);
        } catch {
          throw new Error(t("permissions.upload.invalid", { fileName: file.name }));
        }
        const data = readAccessInfo(parsed);
        if (!data) {
          throw new Error(t("permissions.upload.invalid", { fileName: file.name }));
        }
        const label = file.name.replace(/\.[^/.]+$/, "");
        loaded.push({
          id: `${data.subject}-${label}`,
          label,
          data,
        });
      } catch (error) {
        setUploadError(error instanceof Error ? error.message : t("permissions.compare.error"));
      }
    }
    if (loaded.length > 0) {
      setUsers((prev) => [...prev, ...loaded]);
      if (permissionsTabRef.current === "crossProject") {
        setCrossProjectViewerId(loaded[loaded.length - 1]!.id);
        setCrossProjectAccessExpanded(false);
      }
    }
    setUploading(false);
  };

  const handlePasteAdd = useCallback(() => {
    setUploadError(null);
    let parsed: unknown;
    try {
      parsed = JSON.parse(pasteText.trim());
    } catch {
      setUploadError(t("permissions.paste.invalid"));
      return;
    }
    const data = readAccessInfo(parsed);
    if (!data) {
      setUploadError(t("permissions.paste.invalid"));
      return;
    }
    const label = pasteDisplayName.trim() || data.subject;
    const newUser: UploadedUser = {
      id: `${data.subject}-paste-${Date.now()}`,
      label,
      data,
    };
    setUsers((prev) => [...prev, newUser]);
    if (permissionsTabRef.current === "crossProject") {
      setCrossProjectViewerId(newUser.id);
      setCrossProjectAccessExpanded(false);
    }
    setPasteText("");
    setPasteDisplayName("");
  }, [pasteDisplayName, pasteText, t]);

  const initialPermissionsTab = useMemo(() => readInitialPermissionsSubView(), []);
  const [permissionsTab, setPermissionsTab] =
    useState<PersistedPermissionsSubView>(initialPermissionsTab);
  permissionsTabRef.current = permissionsTab;
  const selectPermissionsTab = useCallback((next: PersistedPermissionsSubView) => {
    setPermissionsTab(next);
    saveNavState({ permissionsSubView: next });
  }, []);

  const crossProjectState = useCrossProjectMembershipCheck(
    permissionsTab === "crossProject",
    sdk,
    getSdk,
    isDuneLoading,
    crossProjectSource
  );

  if (status === "loading") {
    return (
      <div className="flex flex-col gap-1 text-sm text-slate-600">
        <span className="font-medium text-slate-800">{t("permissions.loading")}</span>
        {loadingDetail ? <span className="text-xs text-slate-500">{loadingDetail}</span> : null}
      </div>
    );
  }

  if (status === "error") {
    return (
      <ApiError message={errorMessage ?? t("permissions.error")} />
    );
  }

  return (
    <>
      <section className="flex flex-col gap-4">
      <header className="relative flex flex-col gap-1">
        <h2 className="text-2xl font-semibold text-slate-900">{t("permissions.title")}</h2>
        <p className="text-sm text-slate-500">{t("permissions.subtitle")}</p>
        <button
          type="button"
          className="absolute right-0 top-0 rounded-md bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700"
          onClick={() => setShowHelp(true)}
        >
          {t("shared.help.button")}
        </button>
      </header>
      <nav
        className="flex flex-wrap gap-2 border-b border-slate-200 pb-3"
        aria-label={t("permissions.subNavAria")}
      >
        <button
          type="button"
          onClick={() => selectPermissionsTab("groups")}
          className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
            permissionsTab === "groups"
              ? "bg-slate-900 text-white"
              : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
          }`}
        >
          {t("permissions.subnav.groups")}
        </button>
        <button
          type="button"
          onClick={() => selectPermissionsTab("compare")}
          className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
            permissionsTab === "compare"
              ? "bg-slate-900 text-white"
              : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
          }`}
        >
          {t("permissions.subnav.compare")}
        </button>
        <button
          type="button"
          onClick={() => selectPermissionsTab("crossProject")}
          className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
            permissionsTab === "crossProject"
              ? "bg-slate-900 text-white"
              : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
          }`}
        >
          {t("permissions.subnav.crossProject")}
        </button>
        <button
          type="button"
          onClick={() => selectPermissionsTab("spaces")}
          className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
            permissionsTab === "spaces"
              ? "bg-slate-900 text-white"
              : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
          }`}
        >
          {t("permissions.subnav.spaces")}
        </button>
        <button
          type="button"
          onClick={() => selectPermissionsTab("datasets")}
          className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
            permissionsTab === "datasets"
              ? "bg-slate-900 text-white"
              : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
          }`}
        >
          {t("permissions.subnav.datasets")}
        </button>
      </nav>
      {permissionsTab === "groups" ? (
        <Card>
          <CardHeader>
            <CardTitle>{t("permissions.groups.title")}</CardTitle>
            <CardDescription>
              {t("permissions.groups.description")}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {groups.length === 0 ? (
              <div className="text-sm text-slate-600">{t("permissions.groups.none")}</div>
            ) : (
              <div className="space-y-3">
                <div className="overflow-auto rounded-md border border-slate-200">
                  <table className="w-full border-collapse text-left text-xs">
                    <thead className="bg-slate-50 text-slate-600">
                      <tr>
                        <th className="min-w-[240px] px-2 py-2 font-medium">
                          {t("permissions.table.group")}
                        </th>
                        {capabilityNames.map((name) => (
                          <th
                            key={name}
                            className="h-40 w-10 px-1 pb-4 align-bottom text-center text-[10px] font-medium"
                            title={name}
                          >
                            <span className="inline-block origin-bottom -rotate-45 whitespace-nowrap">
                              {name}
                            </span>
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      {groups.map((group) => (
                        <tr key={group.id}>
                          <td className={`px-2 py-2 text-sm text-slate-800${pc}`}>
                            <button
                              type="button"
                              className={`cursor-pointer text-left font-medium text-slate-800 hover:text-blue-800 hover:underline${pc}`}
                              onClick={() =>
                                openGroupDefinitionJson(
                                  group.name ?? t("permissions.group.fallback", { id: group.id }),
                                  group
                                )
                              }
                            >
                              {group.name ?? t("permissions.group.fallback", { id: group.id })}
                            </button>
                            {group.sourceId ? (
                              <div className="text-[11px] font-normal text-slate-500 whitespace-nowrap">
                                {group.sourceId}
                              </div>
                            ) : null}
                          </td>
                          {capabilityNames.map((capabilityName) => {
                            const rawCap = getCapability(group, capabilityName);
                            if (!rawCap) {
                              return (
                                <td key={capabilityName} className="px-2 py-2 text-center">
                                  -
                                </td>
                              );
                            }
                            const normalized = normalizeCapability(rawCap);
                            const actionDisplay = getActionDisplay(normalized, t);
                            const scopeDisplay = getScopeDisplay(normalized, dataSets, t);
                            return (
                              <td
                                key={capabilityName}
                                className="px-2 py-2 text-center text-xs"
                                title={`${actionDisplay.titleText}\n${scopeDisplay.titleText}`}
                                style={{ backgroundColor: actionDisplay.color }}
                              >
                                <div className="flex items-center justify-center gap-1">
                                  <span className="font-semibold">{actionDisplay.shortText}</span>
                                  <span className="text-[10px] text-slate-600">
                                    {scopeDisplay.shortText}
                                  </span>
                                </div>
                              </td>
                            );
                          })}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="flex flex-wrap items-center gap-2 text-[11px] text-slate-600">
                  <span className="font-medium text-slate-700">{t("permissions.legend.label")}</span>
                  <span className="flex items-center gap-2 rounded-md border border-slate-200 bg-white px-2 py-1">
                    <span className="h-3 w-3 rounded-sm" style={{ backgroundColor: "#bae6fd" }} />
                    R = {t("permissions.legend.read")}
                  </span>
                  <span className="flex items-center gap-2 rounded-md border border-slate-200 bg-white px-2 py-1">
                    <span className="h-3 w-3 rounded-sm" style={{ backgroundColor: "#bbf7d0" }} />
                    W = {t("permissions.legend.write")}
                  </span>
                  <span className="flex items-center gap-2 rounded-md border border-slate-200 bg-white px-2 py-1">
                    <span className="h-3 w-3 rounded-sm" style={{ backgroundColor: "#A6D6D6" }} />
                    R+ = {t("permissions.legend.readplus")}
                  </span>
                  <span className="flex items-center gap-2 rounded-md border border-slate-200 bg-white px-2 py-1">
                    <span className="h-3 w-3 rounded-sm" style={{ backgroundColor: "#86efac" }} />
                    W+ = {t("permissions.legend.writeplus")}
                  </span>
                  <span className="flex items-center gap-2 rounded-md border border-slate-200 bg-white px-2 py-1">
                    <span className="h-3 w-3 rounded-sm" style={{ backgroundColor: "#fef9c3" }} />
                    O = {t("permissions.legend.owner")}
                  </span>
                  <span className="flex items-center gap-2 rounded-md border border-slate-200 bg-white px-2 py-1">
                    <span className="h-3 w-3 rounded-sm" style={{ backgroundColor: "#fed7aa" }} />
                    A = {t("permissions.legend.custom")}
                  </span>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      ) : null}
      {permissionsTab === "compare" ? (
        <Card>
          <CardHeader className="relative">
            <div className="flex flex-col gap-1">
              <CardTitle>{t("permissions.compare.title")}</CardTitle>
              <CardDescription>{t("permissions.compare.membership")}</CardDescription>
            </div>
            <button
              type="button"
              className="absolute right-0 top-0 rounded-md bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700"
              onClick={() => setShowCompareHelp(true)}
            >
              {t("shared.help.button")}
            </button>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col gap-3">
              <PermissionsAccessInfoInput
                idSuffix="compare"
                uploading={uploading}
                onFiles={handleUserUpload}
                pasteText={pasteText}
                onPasteText={setPasteText}
                pasteDisplayName={pasteDisplayName}
                onPasteDisplayName={setPasteDisplayName}
                onPasteAdd={handlePasteAdd}
                uploadError={uploadError}
              />
              {comparisonUsers.length === 0 ? (
                <div className="text-sm text-slate-600">
                  {t("permissions.upload.empty")}
                </div>
              ) : (
                <div className="flex flex-col gap-3">
                  <div className="flex flex-wrap items-end gap-3">
                    <label className="flex min-w-[12rem] flex-1 flex-col gap-1.5 text-sm text-slate-700">
                      {t("permissions.compare.searchLabel")}
                      <input
                        id="permissions-compare-group-search"
                        type="search"
                        className="h-9 rounded-md border border-slate-200 px-3 text-sm"
                        placeholder={t("permissions.compare.searchPlaceholder")}
                        value={compareGroupSearch}
                        onChange={(event) => setCompareGroupSearch(event.target.value)}
                        autoComplete="off"
                      />
                    </label>
                    <label className="flex max-w-md cursor-pointer items-center gap-2 text-sm text-slate-700">
                      <input
                        type="checkbox"
                        className="h-4 w-4 rounded border-slate-300"
                        checked={compareOnlyUtilized}
                        onChange={(event) => {
                          const next = event.target.checked;
                          setCompareOnlyUtilized(next);
                          if (!next) pendingAutoUtilAfterUploadRef.current = false;
                        }}
                      />
                      {t("permissions.compare.utilizedOnly")}
                    </label>
                  </div>
                  {hasOtherProjectMemberships ? (
                    <div className="flex max-w-2xl flex-col gap-1 rounded-md border border-violet-200 bg-violet-50/60 px-3 py-2">
                      <label className="flex cursor-pointer items-start gap-2 text-sm text-slate-800">
                        <input
                          type="checkbox"
                          className="mt-0.5 h-4 w-4 shrink-0 rounded border-slate-300"
                          checked={compareIncludeOtherProjects}
                          onChange={(event) => setCompareIncludeOtherProjects(event.target.checked)}
                        />
                        <span>{t("permissions.compare.includeOtherProjects")}</span>
                      </label>
                    </div>
                  ) : null}
                  {compareTableModel.truncated ? (
                    <div className="flex flex-wrap items-center gap-2 rounded-md border border-amber-200 bg-amber-50/80 px-3 py-2 text-sm text-amber-950">
                      <span>
                        {t("permissions.compare.truncatedSummary", {
                          shown: compareTableModel.rows.length,
                          total: compareFilteredRows.length,
                          pinned: compareTableModel.pinnedCount,
                          hidden: compareTableModel.hiddenCount,
                        })}
                      </span>
                      <button
                        type="button"
                        className="cursor-pointer rounded-md border border-amber-300 bg-white px-3 py-1 text-sm font-medium text-amber-950 hover:bg-amber-100"
                        onClick={() => setCompareShowAllGroups(true)}
                      >
                        {t("permissions.compare.showAll", { total: compareFilteredRows.length })}
                      </button>
                    </div>
                  ) : null}
                  {compareShowAllGroups && groups.length > 100 && !compareGroupSearch.trim() ? (
                    <div>
                      <button
                        type="button"
                        className="cursor-pointer rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50"
                        onClick={() => setCompareShowAllGroups(false)}
                      >
                        {t("permissions.compare.collapseList")}
                      </button>
                    </div>
                  ) : null}
                  {compareFilteredRows.length === 0 ? (
                    <div className="text-sm text-slate-600">{t("permissions.compare.noMatches")}</div>
                  ) : (
                    <div className="space-y-2">
                      {remoteOtherGroupLabelsStatus === "error" ? (
                        <div className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-950">
                          {t("permissions.compare.otherProjectNameError")}
                        </div>
                      ) : null}
                      {remoteOtherGroupForbiddenProjects.length > 0 ? (
                        <div className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-950">
                          {t("permissions.compare.otherProjectGroupsForbiddenSummary", {
                            projects: remoteOtherGroupForbiddenProjects.join(", "),
                          })}
                        </div>
                      ) : null}
                    <div className="overflow-auto rounded-md border border-slate-200">
                      <table className="w-full border-collapse text-left text-sm">
                        <thead className="bg-slate-50 text-slate-600">
                          <tr>
                            <th className="min-w-[200px] px-3 py-2 font-medium">
                              {t("permissions.table.group")}
                            </th>
                            {comparisonUsers.map((user) => (
                              <th key={user.id} className="px-3 py-2 font-medium">
                                {user.label || user.data.subject}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-100">
                          {compareTableModel.rows.map((row) => (
                            <tr key={compareTableRowKey(row)}>
                              <td className={`px-3 py-2 text-slate-700${pc}`}>
                                {row.scope === "current" ? (
                                  <button
                                    type="button"
                                    className={`cursor-pointer text-left font-medium text-slate-900 hover:text-blue-800 hover:underline${pc}`}
                                    onClick={() =>
                                      openGroupDefinitionJson(
                                        groupNameMap.get(row.group.id) ??
                                          t("permissions.group.fallback", { id: row.group.id }),
                                        row.group
                                      )
                                    }
                                  >
                                    {groupNameMap.get(row.group.id)}
                                  </button>
                                ) : (
                                  (() => {
                                    const lk = otherProjectGroupLabelKey(
                                      row.projectUrlName,
                                      row.groupId
                                    );
                                    const resolved = remoteOtherGroupLabels[lk];
                                    const fallbackLabel = t("permissions.group.fallback", {
                                      id: row.groupId,
                                    });
                                    const forbiddenProject = remoteOtherGroupForbiddenProjects.includes(
                                      row.projectUrlName
                                    );
                                    const projectTag = (
                                      <span
                                        className={`inline-flex max-w-[14rem] shrink-0 items-center truncate rounded-md px-2 py-0.5 text-[11px] font-medium${
                                          forbiddenProject
                                            ? ` bg-amber-100 text-amber-950 ring-1 ring-amber-300/90${pc}`
                                            : ` bg-violet-100 text-violet-900${pc}`
                                        }`}
                                        title={row.projectUrlName}
                                      >
                                        {row.projectUrlName}
                                      </span>
                                    );
                                    return (
                                      <div className="flex flex-col gap-1">
                                        <div className="flex flex-wrap items-center gap-2">
                                          {remoteOtherGroupLabelsStatus === "loading" && !resolved ? (
                                            <span className="text-sm text-slate-500">
                                              {t("permissions.compare.otherProjectLoading")}
                                            </span>
                                          ) : (
                                            <button
                                              type="button"
                                              className="cursor-pointer text-left font-medium text-slate-900 hover:text-blue-800 hover:underline"
                                              onClick={() =>
                                                openGroupDefinitionJson(
                                                  resolved ?? fallbackLabel,
                                                  remoteOtherGroupPayloads[lk] ?? {
                                                    id: row.groupId,
                                                    projectUrlName: row.projectUrlName,
                                                  }
                                                )
                                              }
                                            >
                                              {resolved ?? fallbackLabel}
                                            </button>
                                          )}
                                          <button
                                            type="button"
                                            className={`cursor-pointer rounded-md border border-transparent hover:border-violet-300${pc}`}
                                            title={row.projectUrlName}
                                            onClick={() =>
                                              openGroupDefinitionJson(
                                                row.projectUrlName,
                                                remoteOtherGroupPayloads[lk] ?? {
                                                  id: row.groupId,
                                                  projectUrlName: row.projectUrlName,
                                                }
                                              )
                                            }
                                          >
                                            {projectTag}
                                          </button>
                                        </div>
                                      </div>
                                    );
                                  })()
                                )}
                              </td>
                              {comparisonUsers.map((user) => {
                                const isMember =
                                  row.scope === "current"
                                    ? (user.data.projects
                                        .find((proj) => proj.projectUrlName === sdk.project)
                                        ?.groups?.includes(row.group.id) ?? false)
                                    : userMemberOfGroupInProject(
                                        user,
                                        row.projectUrlName,
                                        row.groupId
                                      );
                                return (
                                  <td
                                    key={`${compareTableRowKey(row)}-${user.id}`}
                                    className="px-3 py-2 text-center"
                                  >
                                    {isMember ? "X" : ""}
                                  </td>
                                );
                              })}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      ) : null}
      {permissionsTab === "crossProject" ? (
        <Card>
          <CardHeader>
            <CardTitle>{t("permissions.crossProject.title")}</CardTitle>
            <CardDescription>{t("permissions.crossProject.description")}</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="rounded-md border border-slate-200 bg-slate-50/70">
              <button
                type="button"
                className="flex w-full items-start justify-between gap-3 px-3 py-2 text-left text-sm text-slate-800 hover:bg-slate-100/80"
                aria-expanded={crossProjectAccessExpanded}
                onClick={() => setCrossProjectAccessExpanded((open) => !open)}
              >
                <span className="min-w-0 flex-1 space-y-0.5">
                  <span className="block truncate">
                    <span className="text-slate-500">{t("permissions.crossProject.viewerCollapsedPrefix")}</span>{" "}
                    <span className={`font-medium text-slate-900${pc}`}>{crossProjectViewerSummary}</span>
                  </span>
                  <span className="block text-xs font-normal text-slate-500">
                    {users.length > 0
                      ? t("permissions.crossProject.accessCollapsedUsers", { n: users.length })
                      : t("permissions.crossProject.accessCollapsedHint")}
                  </span>
                </span>
                <span className="shrink-0 pt-0.5 text-xs font-medium text-slate-600">
                  {crossProjectAccessExpanded
                    ? t("permissions.crossProject.accessBlockCollapse")
                    : t("permissions.crossProject.accessBlockExpand")}
                </span>
              </button>
              {crossProjectAccessExpanded ? (
                <div className="space-y-4 border-t border-slate-200 bg-white px-3 py-3">
                  <PermissionsAccessInfoInput
                    idSuffix="crossProject"
                    uploading={uploading}
                    onFiles={handleUserUpload}
                    pasteText={pasteText}
                    onPasteText={setPasteText}
                    pasteDisplayName={pasteDisplayName}
                    onPasteDisplayName={setPasteDisplayName}
                    onPasteAdd={handlePasteAdd}
                    uploadError={uploadError}
                  />
                  <div className="space-y-2">
                    <label htmlFor="cross-project-viewer" className="text-xs font-medium text-slate-600">
                      {t("permissions.crossProject.viewAs")}
                    </label>
                    <select
                      id="cross-project-viewer"
                      className="w-full max-w-xl rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-800 shadow-sm"
                      value={crossProjectViewerId}
                      onChange={(event) => {
                        setCrossProjectViewerId(event.target.value);
                        setCrossProjectAccessExpanded(false);
                      }}
                    >
                      <option value={CROSS_PROJECT_VIEW_TOKEN}>
                        {t("permissions.crossProject.viewerCurrentUser")}
                      </option>
                      {users.map((u) => (
                        <option key={u.id} value={u.id}>
                          {u.label || u.data.subject}
                        </option>
                      ))}
                    </select>
                    <p className="text-xs text-slate-500">{t("permissions.crossProject.viewerHint")}</p>
                  </div>
                </div>
              ) : null}
            </div>
            <PermissionsCrossProject state={crossProjectState} privateMaskClass={pc} />
          </CardContent>
        </Card>
      ) : null}
      {permissionsTab === "spaces" ? (
        <Card>
          <CardHeader>
            <CardTitle>{t("permissions.scopes.space.title")}</CardTitle>
            <CardDescription>{t("permissions.scopes.space.description")}</CardDescription>
          </CardHeader>
          <CardContent>
            {spaceRows.length === 0 ? (
              <div className="text-sm text-slate-600">{t("permissions.spaces.none")}</div>
            ) : (
              <div className="space-y-3">
                <div className="overflow-auto rounded-md border border-slate-200">
                  <table className="w-full border-collapse text-left text-sm">
                    <thead className="bg-slate-50 text-slate-600">
                      <tr>
                        <th className="px-3 py-2 font-medium">{t("permissions.table.space")}</th>
                        <th className="px-3 py-2 font-medium">{t("permissions.table.name")}</th>
                        <th className="px-3 py-2 font-medium">{t("permissions.table.groups")}</th>
                      </tr>
                    </thead>
                    <tbody className={`divide-y divide-slate-100${pc}`}>
                      {spaceRows.map((space) => (
                        <tr key={space.space}>
                          <td className="px-3 py-2">{space.space}</td>
                          <td className="px-3 py-2">
                            {space.name ?? t("permissions.space.unnamed")}
                          </td>
                          <td className="px-3 py-2">
                            <div className="flex flex-wrap gap-2">
                              {(spaceAccess[space.space] ?? []).map((group) => (
                                <span
                                  key={`${space.space}-${group}`}
                                  className="rounded-md bg-slate-100 px-2 py-1 text-xs text-slate-700"
                                >
                                  {group}
                                </span>
                              ))}
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="flex flex-wrap items-center gap-2 text-[11px] text-slate-600">
                  <span className="font-medium text-slate-700">{t("permissions.legend.label")}</span>
                  <span className="flex items-center gap-2 rounded-md border border-slate-200 bg-white px-2 py-1">
                    <span className="h-3 w-3 rounded-sm bg-slate-100" />
                    {t("permissions.legend.space")}
                  </span>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      ) : null}
      {permissionsTab === "datasets" ? (
        <Card>
          <CardHeader>
            <CardTitle>{t("permissions.scopes.dataset.title")}</CardTitle>
            <CardDescription>{t("permissions.scopes.dataset.description")}</CardDescription>
          </CardHeader>
          <CardContent>
            {datasetRows.length === 0 ? (
              <div className="text-sm text-slate-600">{t("permissions.datasets.none")}</div>
            ) : (
              <div className="space-y-3">
                <div className="overflow-auto rounded-md border border-slate-200">
                  <table className="w-full border-collapse text-left text-sm">
                    <thead className="bg-slate-50 text-slate-600">
                      <tr>
                        <th className="px-3 py-2 font-medium">{t("permissions.table.name")}</th>
                        <th className="px-3 py-2 font-medium">{t("permissions.table.id")}</th>
                        <th className="px-3 py-2 font-medium">{t("permissions.table.groups")}</th>
                      </tr>
                    </thead>
                    <tbody className={`divide-y divide-slate-100${pc}`}>
                      {datasetRows.map((dataset) => (
                        <tr key={dataset.id}>
                          <td className="px-3 py-2">
                            {dataset.name ?? t("permissions.dataset.unnamed")}
                          </td>
                          <td className="px-3 py-2">{dataset.id}</td>
                          <td className="px-3 py-2">
                            <div className="flex flex-wrap gap-2">
                              {(dataSetAccess[dataset.id] ?? []).map((group) => (
                                <span
                                  key={`${dataset.id}-${group}`}
                                  className="rounded-md bg-slate-100 px-2 py-1 text-xs text-slate-700"
                                >
                                  {group}
                                </span>
                              ))}
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="flex flex-wrap items-center gap-2 text-[11px] text-slate-600">
                  <span className="font-medium text-slate-700">{t("permissions.legend.label")}</span>
                  <span className="flex items-center gap-2 rounded-md border border-slate-200 bg-white px-2 py-1">
                    <span className="h-3 w-3 rounded-sm bg-slate-100" />
                    {t("permissions.legend.dataset")}
                  </span>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      ) : null}
      </section>
      <PermissionsHelpModal open={showHelp} onClose={() => setShowHelp(false)} />
      <PermissionsCompareHelpModal
        open={showCompareHelp}
        onClose={() => setShowCompareHelp(false)}
      />
      <PermissionsGroupJsonModal
        open={groupJsonModal != null}
        title={groupJsonModal?.title ?? ""}
        json={groupJsonModal?.json ?? "{}"}
        onClose={() => setGroupJsonModal(null)}
      />
      <Loader open={showLoader} onClose={() => setShowLoader(false)} />
    </>
  );
}
