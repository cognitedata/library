import { useMemo } from "react";
import yaml from "js-yaml";
import { useAppSettings } from "../../context/AppSettingsContext";
import { AdvancedYamlPanel } from "../governance/AdvancedYamlPanel";
import { GovernanceToolbar } from "../governance/GovernanceToolbar";
import { ScopeHierarchyEditor } from "../governance/ScopeHierarchyEditor";
import { useTransformScopeDoc } from "./useTransformScopeDoc";

export function TransformScopePane() {
  const { t } = useAppSettings();
  const scope = useTransformScopeDoc();

  const rawYaml = useMemo(
    () =>
      yaml.dump(
        { scope_hierarchy: scope.scopeHierarchy },
        { lineWidth: -1, noRefs: true }
      ),
    [scope.scopeHierarchy]
  );

  const onSaveRaw = async (content: string) => {
    const parsed = yaml.load(content) as { scope_hierarchy?: unknown };
    if (!parsed || typeof parsed !== "object" || !parsed.scope_hierarchy) {
      throw new Error(t("dimensions.jsonRootObject"));
    }
    await scope.saveScopeHierarchy(parsed.scope_hierarchy as Record<string, unknown>);
  };

  return (
    <div className="disc-gov-pane">
      <header className="disc-gov-pane-header">
        <div className="disc-gov-pane-header__row">
          <p className="disc-gov-pane-header__hint">{t("transform.scope.hint")}</p>
          <GovernanceToolbar
            dirty={scope.dirty}
            loading={scope.loading}
            saving={scope.saving}
            error={scope.error}
            onReload={() => void scope.load()}
            onSave={() => void scope.save()}
          />
        </div>
      </header>
      <div className="disc-gov-pane-body gov-stack gov-stack--lg">
        {scope.loading ? (
          <p className="disc-empty-hint">{t("tree.loading")}</p>
        ) : scope.error ? (
          <p className="disc-banner--error" role="alert">
            {scope.error}
          </p>
        ) : (
          <>
            <ScopeHierarchyEditor
              doc={{ scope_hierarchy: scope.scopeHierarchy }}
              onChange={(next) => {
                if (next.scope_hierarchy) {
                  scope.setScopeHierarchy({
                    ...scope.scopeHierarchy,
                    ...next.scope_hierarchy,
                    type: "hierarchy",
                  });
                }
              }}
            />
            <AdvancedYamlPanel
              initialContent={rawYaml}
              onSaveRaw={onSaveRaw}
              onAfterSave={scope.load}
            />
          </>
        )}
      </div>
    </div>
  );
}
