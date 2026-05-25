import { useCallback, useEffect, useRef, useState } from "react";
import {
  fetchTransformScopeHierarchy,
  saveTransformScopeHierarchy,
} from "../../api";
import type { HierarchyDimension } from "../../types/governanceConfig";

const DEFAULT_SCOPE: HierarchyDimension = {
  type: "hierarchy",
  levels: ["site", "unit", "area", "system"],
  locations: [],
};

export function useTransformScopeDoc() {
  const [scopeHierarchy, setScopeHierarchy] = useState<HierarchyDimension>(DEFAULT_SCOPE);
  const [savedSnapshot, setSavedSnapshot] = useState("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const loadGen = useRef(0);

  const dirty = savedSnapshot !== JSON.stringify(scopeHierarchy);

  const load = useCallback(async () => {
    const gen = ++loadGen.current;
    setLoading(true);
    setError(null);
    try {
      const data = await fetchTransformScopeHierarchy();
      if (gen !== loadGen.current) return;
      const block: HierarchyDimension = {
        ...DEFAULT_SCOPE,
        ...(data.scope_hierarchy ?? {}),
        type: "hierarchy",
      };
      setScopeHierarchy(block);
      setSavedSnapshot(JSON.stringify(block));
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
      await saveTransformScopeHierarchy(scopeHierarchy);
      setSavedSnapshot(JSON.stringify(scopeHierarchy));
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  }, [scopeHierarchy]);

  const saveScopeHierarchy = useCallback(async (block: Record<string, unknown>) => {
    const next: HierarchyDimension = {
      ...DEFAULT_SCOPE,
      ...block,
      type: "hierarchy",
    };
    setScopeHierarchy(next);
    await saveTransformScopeHierarchy(next);
    setSavedSnapshot(JSON.stringify(next));
  }, []);

  return {
    scopeHierarchy,
    setScopeHierarchy,
    dirty,
    loading,
    saving,
    error,
    load,
    save,
    saveScopeHierarchy,
  };
}
