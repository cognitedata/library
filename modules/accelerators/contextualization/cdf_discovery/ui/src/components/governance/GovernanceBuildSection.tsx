import { useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { runGovernanceBuild, type BuildTarget } from "../../api/governanceDeclared";

type BuildResult = { ok: boolean; dryRun: boolean; exitCode: number };

type Props = {
  target: Extract<BuildTarget, "spaces" | "groups">;
  onBuildComplete?: (result: BuildResult) => void;
};

export function GovernanceBuildSection({ target, onBuildComplete }: Props) {
  const { t } = useAppSettings();
  const [log, setLog] = useState("");
  const [busy, setBusy] = useState(false);

  const run = async (force: boolean, dryRun: boolean) => {
    if (force && !dryRun && !window.confirm(t("governance.build.confirmForce"))) return;
    setBusy(true);
    setLog(`${t("governance.build.running")}\n`);
    try {
      const body = await runGovernanceBuild({ target, force, dryRun });
      setLog(
        `${t("governance.build.logExitCode", { code: String(body.exit_code) })}${t("governance.build.logStdout")}${body.stdout}${t("governance.build.logStderr")}${body.stderr}`
      );
      onBuildComplete?.({
        ok: body.ok,
        dryRun,
        exitCode: body.exit_code,
      });
    } catch (e) {
      setLog(String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="disc-gov-build">
      <div className="disc-gov-toolbar">
        <button type="button" className="disc-btn" disabled={busy} onClick={() => void run(false, false)}>
          {t("governance.build.run")}
        </button>
        <button type="button" className="disc-btn" disabled={busy} onClick={() => void run(false, true)}>
          {t("governance.build.dryRun")}
        </button>
        <button type="button" className="disc-btn" disabled={busy} onClick={() => void run(true, false)}>
          {t("governance.build.force")}
        </button>
      </div>
      <pre className="disc-gov-log">{log || t("governance.build.hint")}</pre>
    </div>
  );
}
