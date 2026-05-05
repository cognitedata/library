import type { CogniteClient } from "@cognite/sdk";
import { CogniteError, HttpError } from "@cognite/sdk";
import { useEffect, useState } from "react";
import { isForbiddenError } from "@/shared/cdf-errors";
import {
  driftIsReadWriteTierOnly,
  mergeCapabilitiesWidestForSameName,
  normalizeCapability,
  stableCapabilitySignature,
} from "@/shared/permissions-utils";
import { cachedSecurityGroupsList, type SecurityGroupListItem } from "@/shared/security-groups-cache";
import type { GroupSummary, NormalizedCapability } from "./types";

export type CrossProjectMatrixMetric = "status" | "name" | "sourceId" | "id";

export type CrossProjectMembershipSource =
  | { kind: "token" }
  | {
      kind: "projects";
      signature: string;
      projects: Array<{ projectUrlName: string; groups: number[] }>;
    };

export type CrossProjectCell = {
  member: boolean;
  groupId?: number;
  name?: string;
  sourceId?: string;
};

export type CrossProjectRow = {
  canonicalKey: string;
  rowLabel: string;
  idOnlyMatch: boolean;
  cells: Record<string, CrossProjectCell>;
};

export type CrossProjectCapabilityCell = {
  present: boolean;
  gap: boolean;
  showScopeDrift: boolean;
  /** Pretty-printed JSON for this project’s canonical capability shape (smallest signature in the cell). */
  driftLeftJson?: string;
  /** First differing capability JSON (another group in this project, or another project). */
  driftRightJson?: string;
  /** Short label for the right column (project name or “other group”). */
  driftCompareLabel?: string;
  /** Drift is only read vs write tier (R/R+ vs W/W+) at identical scope — show tier badges instead of orange dot. */
  driftReadWriteTierOnly?: boolean;
  /** Group list returned 403 — cannot resolve capabilities for this project column. */
  definitionsUnavailable?: boolean;
};

export type CrossProjectCapabilityRow = {
  capabilityName: string;
  cells: Record<string, CrossProjectCapabilityCell>;
};

function computeCapabilityDriftPreview(
  projectP: string,
  perProject: Record<string, { present: boolean; sigs: Set<string> }>,
  uniqueProjects: string[],
  globalSigsSorted: string[]
): {
  driftLeftJson: string;
  driftRightJson: string;
  driftCompareLabel: string;
  driftReadWriteTierOnly: boolean;
} | null {
  const sigsP = perProject[projectP]?.sigs;
  if (!sigsP || sigsP.size === 0) return null;
  const sortedInP = [...sigsP].sort();
  const leftSig = sortedInP[0]!;

  const format = (sig: string) => JSON.stringify(JSON.parse(sig), null, 2);

  for (const s of sortedInP) {
    if (s !== leftSig) {
      try {
        return {
          driftLeftJson: format(leftSig),
          driftRightJson: format(s),
          driftCompareLabel: "Other member group (same project)",
          driftReadWriteTierOnly: driftIsReadWriteTierOnly(leftSig, s),
        };
      } catch {
        return null;
      }
    }
  }

  const rightSig = globalSigsSorted.find((g) => g !== leftSig);
  if (!rightSig) return null;
  try {
    const otherProject = [...uniqueProjects]
      .sort((a, b) => a.localeCompare(b))
      .find((q) => q !== projectP && perProject[q]?.sigs.has(rightSig));
    const label =
      otherProject ??
      [...uniqueProjects].sort((a, b) => a.localeCompare(b)).find((q) => perProject[q]?.sigs.has(rightSig)) ??
      "Other";
    return {
      driftLeftJson: format(leftSig),
      driftRightJson: format(rightSig),
      driftCompareLabel: label,
      driftReadWriteTierOnly: driftIsReadWriteTierOnly(leftSig, rightSig),
    };
  } catch {
    return null;
  }
}

function signaturesForMemberGroups(
  defs: Map<number, SecurityGroupListItem>,
  memberIds: number[],
  capabilityName: string
): Set<string> {
  const norms: NormalizedCapability[] = [];
  for (const gid of memberIds) {
    const g = defs.get(gid) as GroupSummary | undefined;
    for (const raw of g?.capabilities ?? []) {
      try {
        const n = normalizeCapability(raw as Record<string, unknown>);
        if (n.name === capabilityName) norms.push(n);
      } catch {
        // skip malformed capability entries
      }
    }
  }
  const merged = mergeCapabilitiesWidestForSameName(norms);
  if (!merged) return new Set();
  return new Set([stableCapabilitySignature(merged)]);
}

function buildCapabilityRows(
  uniqueProjects: string[],
  membershipByProject: Map<string, number[]>,
  definitionsByProject: Map<string, Map<number, SecurityGroupListItem>>,
  groupDefinitionsAccessDenied: Set<string>
): CrossProjectCapabilityRow[] {
  const allNames = new Set<string>();
  for (const projectUrlName of uniqueProjects) {
    if (groupDefinitionsAccessDenied.has(projectUrlName)) continue;
    const defs = definitionsByProject.get(projectUrlName)!;
    const memberIds = membershipByProject.get(projectUrlName) ?? [];
    for (const gid of memberIds) {
      const g = defs.get(gid) as GroupSummary | undefined;
      for (const raw of g?.capabilities ?? []) {
        try {
          allNames.add(normalizeCapability(raw as Record<string, unknown>).name);
        } catch {
          // ignore
        }
      }
    }
  }
  const sortedNames = [...allNames].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
  const rows: CrossProjectCapabilityRow[] = [];

  for (const capabilityName of sortedNames) {
    const perProject: Record<
      string,
      { present: boolean; sigs: Set<string>; internalMulti: boolean; rep: string | null }
    > = {};
    for (const projectUrlName of uniqueProjects) {
      const defs = definitionsByProject.get(projectUrlName)!;
      const memberIds = membershipByProject.get(projectUrlName) ?? [];
      const sigs = signaturesForMemberGroups(defs, memberIds, capabilityName);
      const present = sigs.size > 0;
      const internalMulti = sigs.size > 1;
      const rep = present ? [...sigs].sort()[0]! : null;
      perProject[projectUrlName] = { present, sigs, internalMulti, rep };
    }

    const anyPresent = uniqueProjects.some((p) => perProject[p]!.present);
    const repsAcross = uniqueProjects
      .map((p) => perProject[p]!.rep)
      .filter((r): r is string => r != null);
    const crossScopeDrift = new Set(repsAcross).size > 1;

    const globalSigsSorted = [
      ...new Set(uniqueProjects.flatMap((p) => [...perProject[p].sigs])),
    ].sort();

    const cells: Record<string, CrossProjectCapabilityCell> = {};
    for (const projectUrlName of uniqueProjects) {
      if (groupDefinitionsAccessDenied.has(projectUrlName)) {
        cells[projectUrlName] = {
          present: false,
          gap: false,
          definitionsUnavailable: true,
          showScopeDrift: false,
        };
        continue;
      }
      const { present, internalMulti } = perProject[projectUrlName]!;
      const gap = anyPresent && !present;
      const showScopeDrift = present && (internalMulti || crossScopeDrift);
      const drift =
        showScopeDrift
          ? computeCapabilityDriftPreview(projectUrlName, perProject, uniqueProjects, globalSigsSorted)
          : null;
      cells[projectUrlName] = drift
        ? { present, gap, showScopeDrift, ...drift }
        : { present, gap, showScopeDrift };
    }
    rows.push({ capabilityName, cells });
  }
  return rows;
}

export type CrossProjectMembershipErrorState = {
  status: "error";
  message: string;
  api?: string;
  detailPayload?: unknown;
};

function formatCrossProjectError(e: unknown, api: string): Omit<CrossProjectMembershipErrorState, "status"> {
  if (e instanceof CogniteError) {
    const parts: string[] = [];
    if (e.errorMessage) parts.push(e.errorMessage);
    if (e.status) parts.push(`HTTP ${e.status}`);
    if (e.requestId) parts.push(`X-Request-ID: ${e.requestId}`);
    const message = parts.length > 0 ? parts.join(" | ") : e.message;
    const detailPayload = {
      status: e.status,
      requestId: e.requestId,
      errorMessage: e.errorMessage,
      missing: e.missing,
      duplicated: e.duplicated,
      extra: e.extra,
    };
    return { message, api, detailPayload };
  }
  if (e instanceof HttpError) {
    const apiMsg = e.data?.error?.message ?? e.message;
    const message = [apiMsg, `HTTP ${e.status}`].filter(Boolean).join(" | ");
    const detailPayload = {
      status: e.status,
      response: e.data,
    };
    return { message, api, detailPayload };
  }
  return {
    message: e instanceof Error ? e.message : "Failed to load cross-project memberships.",
    api,
  };
}

export type CrossProjectMembershipState =
  | { status: "idle" }
  | { status: "loading" }
  | CrossProjectMembershipErrorState
  | {
      status: "ready";
      projects: string[];
      rows: CrossProjectRow[];
      allProjectsMatch: boolean;
      /** Raw length of `groups` in token or access-info for that project (numeric IDs). */
      projectTokenGroupIdCounts: Record<string, number>;
      /** Rows where this project has a checkmark — distinct logical groups after merging same sourceId/name. */
      projectLogicalMemberCounts: Record<string, number>;
      capabilityRows: CrossProjectCapabilityRow[];
      /** Projects where `groups` list returned HTTP 403 (viewer cannot read definitions). */
      groupListAccessDeniedProjects: string[];
    };

function canonicalKeyForGroup(
  g: SecurityGroupListItem,
  projectUrlName: string
): { key: string; idOnly: boolean } {
  const sid = g.sourceId?.trim();
  if (sid) return { key: `sid:${sid}`, idOnly: false };
  const nm = g.name?.trim();
  if (nm) return { key: `name:${nm.toLowerCase()}`, idOnly: false };
  return { key: `id:${projectUrlName}:${g.id}`, idOnly: true };
}

function rowLabelForGroup(g: SecurityGroupListItem, idOnly: boolean): string {
  if (idOnly) return g.name?.trim() || `ID ${g.id}`;
  return g.name?.trim() || g.sourceId?.trim() || `ID ${g.id}`;
}

function buildMembershipFromProjects(
  projectRows: Array<{ projectUrlName: string; groups: number[] }>
): { uniqueProjects: string[]; membershipByProject: Map<string, number[]> } {
  const relevantRows = projectRows.filter((row) => (row.groups?.length ?? 0) > 0);
  const names = relevantRows.map((p) => p.projectUrlName).filter(Boolean);
  const uniqueProjects = Array.from(new Set(names)).sort((a, b) => a.localeCompare(b));
  const membershipByProject = new Map<string, number[]>();
  for (const name of uniqueProjects) {
    const merged: number[] = [];
    for (const row of relevantRows) {
      if (row.projectUrlName !== name) continue;
      merged.push(...(row.groups ?? []));
    }
    membershipByProject.set(name, [...new Set(merged)].sort((a, b) => a - b));
  }
  return { uniqueProjects, membershipByProject };
}

export function useCrossProjectMembershipCheck(
  active: boolean,
  sdk: CogniteClient,
  getSdk: (project: string) => CogniteClient,
  isDuneLoading: boolean,
  source: CrossProjectMembershipSource
): CrossProjectMembershipState {
  const [state, setState] = useState<CrossProjectMembershipState>({ status: "idle" });

  const sourceKey = source.kind === "token" ? "token" : source.signature;

  useEffect(() => {
    if (!active || isDuneLoading) {
      setState({ status: "idle" });
      return;
    }
    let cancelled = false;
    setState({ status: "loading" });

    (async () => {
      try {
        let uniqueProjects: string[];
        let membershipByProject: Map<string, number[]>;

        if (source.kind === "token") {
          let tokenInspect: { data?: { projects?: Array<{ projectUrlName?: string; groups?: number[] }> } };
          try {
            tokenInspect = await sdk.get<{
              projects?: Array<{ projectUrlName?: string; groups?: number[] }>;
            }>("/api/v1/token/inspect");
          } catch (e) {
            if (cancelled) return;
            setState({
              status: "error",
              ...formatCrossProjectError(e, "GET /api/v1/token/inspect"),
            });
            return;
          }
          if (cancelled) return;

          const tokenProjects = tokenInspect.data?.projects ?? [];
          const projectsWithMembership = tokenProjects.filter(
            (p): p is { projectUrlName: string; groups?: number[] } =>
              Boolean(p.projectUrlName && typeof p.projectUrlName === "string") &&
              (p.groups?.length ?? 0) > 0
          );
          const projectNames = projectsWithMembership.map((p) => p.projectUrlName);
          uniqueProjects = Array.from(new Set(projectNames)).sort((a, b) => a.localeCompare(b));
          membershipByProject = new Map<string, number[]>();
          for (const name of uniqueProjects) {
            const merged: number[] = [];
            for (const p of tokenProjects) {
              if (p.projectUrlName !== name) continue;
              merged.push(...(p.groups ?? []));
            }
            membershipByProject.set(name, [...new Set(merged)].sort((a, b) => a - b));
          }
        } else {
          const built = buildMembershipFromProjects(source.projects);
          uniqueProjects = built.uniqueProjects;
          membershipByProject = built.membershipByProject;
        }

        if (cancelled) return;

        if (uniqueProjects.length === 0) {
          setState({
            status: "ready",
            projects: [],
            rows: [],
            allProjectsMatch: true,
            projectTokenGroupIdCounts: {},
            projectLogicalMemberCounts: {},
            capabilityRows: [],
            groupListAccessDeniedProjects: [],
          });
          return;
        }

        const definitionsByProject = new Map<string, Map<number, SecurityGroupListItem>>();
        const groupListAccessDeniedProjects: string[] = [];
        const groupListSettled = await Promise.allSettled(
          uniqueProjects.map(async (projectUrlName) => {
            const client = getSdk(projectUrlName);
            const list = await cachedSecurityGroupsList(client, projectUrlName);
            return { projectUrlName, list } as const;
          })
        );
        if (cancelled) return;
        for (let i = 0; i < groupListSettled.length; i++) {
          const entry = groupListSettled[i];
          const projectUrlName = uniqueProjects[i];
          if (entry.status === "rejected") {
            if (isForbiddenError(entry.reason)) {
              definitionsByProject.set(projectUrlName, new Map());
              groupListAccessDeniedProjects.push(projectUrlName);
              continue;
            }
            setState({
              status: "error",
              ...formatCrossProjectError(
                entry.reason,
                `GET /api/v1/projects/${encodeURIComponent(projectUrlName)}/groups`
              ),
            });
            return;
          }
          definitionsByProject.set(
            entry.value.projectUrlName,
            new Map(entry.value.list.map((g) => [g.id, g]))
          );
        }
        groupListAccessDeniedProjects.sort((a, b) => a.localeCompare(b));

        const canonicalMeta = new Map<
          string,
          { idOnly: boolean; label: string; sourceId?: string; name?: string }
        >();

        for (const projectUrlName of uniqueProjects) {
          const defs = definitionsByProject.get(projectUrlName)!;
          const memberIds = membershipByProject.get(projectUrlName) ?? [];
          for (const id of memberIds) {
            const g = defs.get(id) ?? { id };
            const { key, idOnly } = canonicalKeyForGroup(g, projectUrlName);
            const label = rowLabelForGroup(g, idOnly);
            const existing = canonicalMeta.get(key);
            if (!existing || (!existing.label && label)) {
              canonicalMeta.set(key, {
                idOnly,
                label: label || existing?.label || key,
                sourceId: g.sourceId?.trim() || existing?.sourceId,
                name: g.name?.trim() || existing?.name,
              });
            }
          }
        }

        const rows: CrossProjectRow[] = [];
        for (const [canonicalKey, meta] of canonicalMeta) {
          const cells: Record<string, CrossProjectCell> = {};
          for (const projectUrlName of uniqueProjects) {
            const defs = definitionsByProject.get(projectUrlName)!;
            const memberIds = membershipByProject.get(projectUrlName) ?? [];
            let hit: CrossProjectCell = { member: false };
            for (const gid of memberIds) {
              const g = defs.get(gid) ?? { id: gid };
              const { key } = canonicalKeyForGroup(g, projectUrlName);
              if (key === canonicalKey) {
                hit = {
                  member: true,
                  groupId: g.id,
                  name: g.name,
                  sourceId: g.sourceId,
                };
                break;
              }
            }
            cells[projectUrlName] = hit;
          }
          rows.push({
            canonicalKey,
            rowLabel: meta.label,
            idOnlyMatch: meta.idOnly,
            cells,
          });
        }

        rows.sort((a, b) => a.rowLabel.localeCompare(b.rowLabel, undefined, { sensitivity: "base" }));

        const canonicalSetForProject = (projectUrlName: string) => {
          const keys = new Set<string>();
          for (const row of rows) {
            if (row.cells[projectUrlName]?.member) keys.add(row.canonicalKey);
          }
          return keys;
        };

        const ref = canonicalSetForProject(uniqueProjects[0]);
        const allProjectsMatch = uniqueProjects.every((p) => {
          const s = canonicalSetForProject(p);
          if (s.size !== ref.size) return false;
          for (const k of s) {
            if (!ref.has(k)) return false;
          }
          return true;
        });

        const projectTokenGroupIdCounts: Record<string, number> = {};
        const projectLogicalMemberCounts: Record<string, number> = {};
        for (const p of uniqueProjects) {
          projectTokenGroupIdCounts[p] = membershipByProject.get(p)?.length ?? 0;
          projectLogicalMemberCounts[p] = rows.filter((r) => r.cells[p]?.member).length;
        }

        const deniedSet = new Set(groupListAccessDeniedProjects);
        const capabilityRows = buildCapabilityRows(
          uniqueProjects,
          membershipByProject,
          definitionsByProject,
          deniedSet
        );

        setState({
          status: "ready",
          projects: uniqueProjects,
          rows,
          allProjectsMatch,
          projectTokenGroupIdCounts,
          projectLogicalMemberCounts,
          capabilityRows,
          groupListAccessDeniedProjects,
        });
      } catch (e: unknown) {
        if (cancelled) return;
        setState({
          status: "error",
          ...formatCrossProjectError(e, "Cross-project membership"),
        });
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [active, getSdk, isDuneLoading, sdk, sourceKey]);

  return state;
}
