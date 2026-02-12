import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { useAppSdk } from "@/shared/auth";
import { useI18n } from "@/shared/i18n";
import {
  getActionDisplay,
  getCapability,
  getScopeDisplay,
  normalizeCapability,
} from "@/shared/permissions-utils";
import { useEffect, useMemo, useState } from "react";
import { PermissionsHelpModal } from "./PermissionsHelpModal";
import type { UploadedUser } from "./types";
import { usePermissionsData } from "./usePermissionsData";

const permissionUsersStorageKey = "qualitizer.permissionUsers.v1";

export function Permissions() {
  const { sdk, isLoading: isDuneLoading } = useAppSdk();
  const { t } = useI18n();
  const {
    status,
    errorMessage,
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
  const [showHelp, setShowHelp] = useState(false);

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
            projects: [
              {
                projectUrlName: sdk.project,
                groups: projectEntry?.groups ?? [],
              },
            ],
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
  }, [isDuneLoading, sdk]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const loadCachedUsers = () => {
      const stored = window.localStorage.getItem(permissionUsersStorageKey);
      if (!stored) {
        setUsers([]);
        return;
      }
      try {
        const parsed = JSON.parse(stored) as UploadedUser[];
        setUsers(parsed);
      } catch {
        setUsers([]);
      }
    };

    loadCachedUsers();
    const handler = () => loadCachedUsers();
    window.addEventListener("permissions-users-update", handler);
    return () => window.removeEventListener("permissions-users-update", handler);
  }, []);

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

  const handleUserUpload = async (files: FileList | null) => {
    if (!files || files.length === 0) return;
    setUploading(true);
    setUploadError(null);
    const loaded: UploadedUser[] = [];
    for (const file of Array.from(files)) {
      try {
        const text = await file.text();
        const parsed = JSON.parse(text) as UploadedUser["data"];
        if (!parsed || !parsed.subject || !Array.isArray(parsed.projects)) {
          throw new Error(t("permissions.upload.invalid", { fileName: file.name }));
        }
        const label = file.name.replace(/\.[^/.]+$/, "");
        loaded.push({
          id: `${parsed.subject}-${label}`,
          label,
          data: parsed,
        });
      } catch (error) {
        setUploadError(error instanceof Error ? error.message : t("permissions.compare.error"));
      }
    }
    if (loaded.length > 0) {
      const nextUsers = [...users, ...loaded];
      setUsers(nextUsers);
      if (typeof window !== "undefined") {
        window.localStorage.setItem(permissionUsersStorageKey, JSON.stringify(nextUsers));
        window.dispatchEvent(new Event("permissions-users-update"));
      }
    }
    setUploading(false);
  };

  if (status === "loading") {
    return <div className="text-sm text-slate-600">{t("permissions.loading")}</div>;
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
                        <td className="px-2 py-2 text-sm font-medium text-slate-800">
                          <div>
                            {group.name ?? t("permissions.group.fallback", { id: group.id })}
                          </div>
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
      <Card>
        <CardHeader>
          <CardTitle>{t("permissions.compare.title")}</CardTitle>
          <CardDescription>{t("permissions.compare.membership")}</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col gap-3">
            <div className="flex flex-wrap items-center gap-3">
              <label className="text-sm font-medium text-slate-700">
                {t("permissions.upload.label")}
              </label>
              <input
                type="file"
                accept="application/json"
                multiple
                disabled={uploading}
                className="rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm hover:border-slate-400"
                onChange={(event) => handleUserUpload(event.target.files)}
              />
              {uploading ? (
                <span className="text-xs text-slate-500">{t("permissions.upload.uploading")}</span>
              ) : null}
            </div>
            {uploadError ? (
              <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
                {uploadError}
              </div>
            ) : null}
            {comparisonUsers.length === 0 ? (
              <div className="text-sm text-slate-600">
                {t("permissions.upload.empty")}
              </div>
            ) : (
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
                    {comparisonRows.map((group) => (
                      <tr key={group.id}>
                        <td className="px-3 py-2 font-medium text-slate-700">
                          {groupNameMap.get(group.id)}
                        </td>
                        {comparisonUsers.map((user) => {
                          const project = user.data.projects.find(
                            (proj) => proj.projectUrlName === sdk.project
                          );
                          const isMember = project?.groups?.includes(group.id) ?? false;
                          return (
                            <td key={`${group.id}-${user.id}`} className="px-3 py-2 text-center">
                              {isMember ? "X" : ""}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
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
                  <tbody className="divide-y divide-slate-100">
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
                  <tbody className="divide-y divide-slate-100">
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
      </section>
      <PermissionsHelpModal open={showHelp} onClose={() => setShowHelp(false)} />
    </>
  );
}
