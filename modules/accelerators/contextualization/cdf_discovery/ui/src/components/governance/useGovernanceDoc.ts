import { useCallback, useEffect, useRef, useState } from "react";
import {
  fetchGovernanceModel,
  mirrorGovernanceModel,
  saveGovernanceModel,
} from "../../api/governanceDeclared";
import type { GovernanceDocument } from "../../types/governanceConfig";
import {
  DEFAULT_GROUP_DISPLAY_NAME_TEMPLATE,
  DEFAULT_GROUP_NAME_TEMPLATE,
  DEFAULT_INSTANCE_SPACE_ID_TEMPLATE,
  DEFAULT_SPACE_NAME_TEMPLATE,
  emptyGovernanceDocument,
} from "../../types/governanceConfig";

const GROUP_VARIABLE_PREFIX = "gp_";
const TOOLKIT_MODULE_PATH = ["cdf_discovery"] as const;

function isObjectRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function readPathRecord(root: Record<string, unknown>, path: readonly string[]): Record<string, unknown> | null {
  let cur: Record<string, unknown> = root;
  for (const part of path) {
    const nxt = cur[part];
    if (!isObjectRecord(nxt)) return null;
    cur = nxt;
  }
  return cur;
}

function ensurePathRecord(root: Record<string, unknown>, path: readonly string[]): Record<string, unknown> {
  let cur: Record<string, unknown> = root;
  for (const part of path) {
    const nxt = cur[part];
    if (isObjectRecord(nxt)) {
      cur = nxt;
      continue;
    }
    const created: Record<string, unknown> = {};
    cur[part] = created;
    cur = created;
  }
  return cur;
}

function sourceIdsFromConfigVariables(data: GovernanceDocument): Record<string, string> | null {
  if (!isObjectRecord(data.environment) || !isObjectRecord(data.variables)) {
    return null;
  }
  const moduleVars = readPathRecord(data.variables, ["modules", ...TOOLKIT_MODULE_PATH]);
  if (!moduleVars) {
    return null;
  }
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(moduleVars)) {
    if (!k.startsWith(GROUP_VARIABLE_PREFIX)) continue;
    out[k] = typeof v === "string" ? v : "";
  }
  return out;
}

function mergeGroupSourceIdsIntoVariables(data: GovernanceDocument): GovernanceDocument {
  if (!isObjectRecord(data.environment)) {
    return data;
  }
  const next: GovernanceDocument = { ...data };
  const variables: Record<string, unknown> = isObjectRecord(next.variables) ? { ...next.variables } : {};
  const moduleVars = ensurePathRecord(variables, ["modules", ...TOOLKIT_MODULE_PATH]);
  const sourceIds = next.groups?.global?.source_ids ?? {};
  for (const [name, sourceId] of Object.entries(sourceIds)) {
    if (!name.startsWith(GROUP_VARIABLE_PREFIX)) continue;
    moduleVars[name] = typeof sourceId === "string" ? sourceId : "";
  }
  next.variables = variables;
  return next;
}

export function useGovernanceDoc() {
  const [doc, setDoc] = useState<GovernanceDocument>(() => emptyGovernanceDocument());
  const [savedSnapshot, setSavedSnapshot] = useState("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const loadGen = useRef(0);

  const dirty = savedSnapshot !== JSON.stringify(doc);

  const load = useCallback(async () => {
    const gen = ++loadGen.current;
    setLoading(true);
    setError(null);
    try {
      const data = await fetchGovernanceModel();
      if (gen !== loadGen.current) return;
      const defaults = emptyGovernanceDocument();
      const groupSourceIdsFromVariables = sourceIdsFromConfigVariables(data);
      const merged: GovernanceDocument = {
        ...defaults,
        ...data,
        scope_hierarchy: data.scope_hierarchy ?? defaults.scope_hierarchy,
        dimensions: data.dimensions ?? {},
        spaces: {
          ...defaults.spaces,
          ...data.spaces,
          nodes: data.spaces?.nodes ?? defaults.spaces?.nodes ?? "leaves",
          instance_space_id_template:
            data.spaces?.instance_space_id_template ??
            defaults.spaces?.instance_space_id_template ??
            DEFAULT_INSTANCE_SPACE_ID_TEMPLATE,
          name_template:
            data.spaces?.name_template ??
            defaults.spaces?.name_template ??
            DEFAULT_SPACE_NAME_TEMPLATE,
        },
        groups: {
          ...defaults.groups,
          ...data.groups,
          global: {
            ...(defaults.groups?.global ?? {}),
            ...(data.groups?.global ?? {}),
            source_ids: groupSourceIdsFromVariables ?? data.groups?.global?.source_ids ?? {},
          },
          nodes: data.groups?.nodes ?? defaults.groups?.nodes ?? "leaves",
          name_template:
            data.groups?.name_template ??
            defaults.groups?.name_template ??
            DEFAULT_GROUP_NAME_TEMPLATE,
          display_name_template:
            data.groups?.display_name_template ??
            defaults.groups?.display_name_template ??
            DEFAULT_GROUP_DISPLAY_NAME_TEMPLATE,
        },
      };
      setDoc(merged);
      setSavedSnapshot(JSON.stringify(merged));
    } catch (e) {
      if (gen !== loadGen.current) return;
      setError(String(e));
    } finally {
      if (gen === loadGen.current) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const save = useCallback(async () => {
    setSaving(true);
    setError(null);
    try {
      const next = mergeGroupSourceIdsIntoVariables(doc);
      await saveGovernanceModel(next);
      setDoc(next);
      setSavedSnapshot(JSON.stringify(next));
    } catch (e) {
      setError(String(e));
      throw e;
    } finally {
      setSaving(false);
    }
  }, [doc]);

  const mirror = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      await mirrorGovernanceModel({
        scope_hierarchy: doc.scope_hierarchy,
        dimensions: doc.dimensions,
        spaces: doc.spaces,
        groups: doc.groups,
      });
      await load();
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [doc, load]);

  return {
    doc,
    setDoc,
    dirty,
    loading,
    saving,
    error,
    load,
    save,
    mirror,
  };
}
