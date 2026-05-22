import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import { SourceViewFiltersSection } from "./SourceViewFiltersSection";
import { readFilters } from "../utils/filtersConfigModel";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  cfg: Record<string, unknown>;
  /** Update local draft only (text fields while typing). */
  onChange: (next: Record<string, unknown>) => void;
  /** Write draft to the canvas / graph (blur, operator change, add/remove). */
  onPersist: (next: Record<string, unknown>) => void;
  t: TFn;
  allowEmpty?: boolean;
  fieldKey?: string;
};

/** Filter-node config: description + same CDF filter DSL as view query nodes. */
export function FilterNodeStageConfigFields({
  cfg,
  onChange,
  onPersist,
  t: _t,
  allowEmpty = false,
  fieldKey = "instance_filter",
}: Props) {
  const filters = readFilters(cfg);

  const setFilters = (next: JsonObject[], persistNow = true) => {
    const merged = { ...cfg };
    if (!next.length && allowEmpty) {
      delete merged.filters;
      onChange(merged);
      if (persistNow) onPersist(merged);
      return;
    }
    merged.filters = next;
    onChange(merged);
    if (persistNow) onPersist(merged);
  };

  return (
    <>
      <label className="discovery-label discovery-label--block">
        {_t("filters.description")}
        <DeferredCommitInput
          className="discovery-input"
          style={{ marginTop: "0.35rem" }}
          committedValue={cfg.description != null ? String(cfg.description) : ""}
          syncKey={`${fieldKey}-desc`}
          onCommit={(v) => {
            const next = { ...cfg, description: v };
            onChange(next);
            onPersist(next);
          }}
        />
      </label>
      <SourceViewFiltersSection
        filters={filters}
        onFiltersChange={(next) => setFilters(next, true)}
        fieldKey={fieldKey}
      />
    </>
  );
}
