import { useCallback, useEffect, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import {
  listGovernanceArtifacts,
  readGovernanceFile,
  writeGovernanceFile,
  fetchSourceIdHint,
} from "../../api/governanceDeclared";
import { groupNameFromYaml, literalSourceIdFromGroupYaml } from "../../utils/groupYamlSourceId";
import type { GovernanceDocument } from "../../types/governanceConfig";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import { GovernanceArtifactTree } from "./GovernanceArtifactTree";

type Props = {
  kind: "spaces" | "groups";
  doc: GovernanceDocument;
  setDoc: (next: GovernanceDocument) => void;
  initialRel?: string | null;
  /** Bump after a successful build to reload the file list. */
  refreshToken?: number;
};

export function GovernanceArtifactsSection({
  kind,
  doc,
  setDoc,
  initialRel,
  refreshToken = 0,
}: Props) {
  const { t } = useAppSettings();
  const [paths, setPaths] = useState<string[]>([]);
  const [rel, setRel] = useState<string | null>(initialRel ?? null);
  const [yaml, setYaml] = useState("");
  const [entraDraft, setEntraDraft] = useState("");
  const [hint, setHint] = useState("");
  const [loadError, setLoadError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoadError(null);
    try {
      const list = await listGovernanceArtifacts(kind);
      setPaths(list);
    } catch (e) {
      setLoadError(String(e));
      setPaths([]);
    }
  }, [kind]);

  useEffect(() => {
    void refresh();
  }, [refresh, refreshToken]);

  useEffect(() => {
    if (initialRel) {
      setRel(initialRel);
    }
  }, [initialRel]);

  useEffect(() => {
    if (!rel) return;
    void (async () => {
      const content = await readGovernanceFile(rel);
      setYaml(content);
      if (kind === "groups") {
        const name = groupNameFromYaml(content);
        const sid = name ? doc.groups?.global?.source_ids?.[name] : undefined;
        setEntraDraft(sid ?? literalSourceIdFromGroupYaml(content) ?? "");
      }
    })();
  }, [rel, kind, doc.groups?.global?.source_ids]);

  const saveFile = async () => {
    if (!rel) return;
    await writeGovernanceFile(rel, yaml);
    await refresh();
  };

  const saveEntraToConfig = () => {
    if (!rel?.includes(".Group.yaml")) return;
    const name = groupNameFromYaml(yaml);
    if (!name) return;
    setDoc({
      ...doc,
      groups: {
        ...doc.groups,
        global: {
          ...doc.groups?.global,
          source_ids: {
            ...(doc.groups?.global?.source_ids ?? {}),
            [name]: entraDraft.trim(),
          },
        },
      },
    });
    setHint(t("governance.artifacts.entraSaved"));
  };

  return (
    <div className="disc-gov-artifacts">
      <div className="disc-gov-artifacts-layout">
        <div className="disc-gov-artifact-list">
          {loadError && <p className="disc-gov-hint disc-gov-hint--error">{loadError}</p>}
          <GovernanceArtifactTree paths={paths} selectedPath={rel} onSelectFile={setRel} />
        </div>
        <div className="disc-gov-artifact-editor">
          {rel ? (
            <>
              <div className="disc-gov-toolbar">
                <span className="disc-gov-file-label">{rel}</span>
                <button type="button" className="disc-btn" onClick={() => void saveFile()}>
                  {t("governance.artifacts.saveFile")}
                </button>
              </div>
              <textarea
                className="disc-gov-yaml"
                value={yaml}
                onChange={(e) => setYaml(e.target.value)}
                spellCheck={false}
              />
              {kind === "groups" && rel.endsWith(".Group.yaml") && (
                <div className="disc-gov-entra">
                  <label className="disc-gov-label">
                    {t("governance.artifacts.entraId")}
                    <DeferredCommitInput
                      className="disc-input"
                      committedValue={entraDraft}
                      onCommit={setEntraDraft}
                    />
                  </label>
                  <button
                    type="button"
                    className="disc-btn"
                    onClick={() =>
                      void fetchSourceIdHint(entraDraft).then((r) => {
                        if (r.empty) setHint(t("governance.sourceId.empty"));
                        else if (r.valid) setHint(t("governance.sourceId.valid"));
                        else setHint(t("governance.sourceId.invalid"));
                      })
                    }
                  >
                    {t("governance.artifacts.validateId")}
                  </button>
                  <button type="button" className="disc-btn" onClick={saveEntraToConfig}>
                    {t("governance.artifacts.saveEntraConfig")}
                  </button>
                  {hint && <p className="disc-gov-hint">{hint}</p>}
                </div>
              )}
            </>
          ) : (
            <p className="disc-empty-hint">{t("governance.artifacts.selectFile")}</p>
          )}
        </div>
      </div>
    </div>
  );
}
