import type { MessageKey } from "../i18n/types";
import type { BuildIndexHandlerId } from "../components/transform/etlBuildIndexHandlerRegistry";
import { isBuildIndexHandlerId } from "../components/transform/etlBuildIndexHandlerRegistry";

const BUILD_INDEX_DEFAULTS: Record<BuildIndexHandlerId, Record<string, unknown>> = {
  property_token_index: {
    lookup_key_normalization: "strip_casefold",
    token_initial_confidence: 1.0,
    row_key_template: "{index_kind}:{lookup_key}",
    query_source: "build_index",
    default_view_version: "v1",
    index_kinds: {},
  },
};

const BUILD_INDEX_DOC: Record<BuildIndexHandlerId, MessageKey> = {
  property_token_index: "buildIndex.handlerDoc.property_token_index",
};

export function defaultBuildIndexHandlerBlock(handler: BuildIndexHandlerId): Record<string, unknown> {
  return JSON.parse(JSON.stringify(BUILD_INDEX_DEFAULTS[handler])) as Record<string, unknown>;
}

export function defaultBuildIndexNodeConfig(
  handler: BuildIndexHandlerId = "property_token_index"
): Record<string, unknown> {
  return {
    description: "Build inverted index",
    handler_id: handler,
    index_kinds: { metadata: ["indexKey"] },
    [handler]: defaultBuildIndexHandlerBlock(handler),
  };
}

export function readBuildIndexHandlerId(cfg: Record<string, unknown>): string {
  return String(cfg.handler_id ?? cfg.handler ?? "property_token_index").trim() || "property_token_index";
}

export function readBuildIndexHandlerBlock(
  cfg: Record<string, unknown>,
  handlerId: string
): Record<string, unknown> {
  const block = cfg[handlerId];
  if (block && typeof block === "object" && !Array.isArray(block)) {
    return block as Record<string, unknown>;
  }
  if (isBuildIndexHandlerId(handlerId)) {
    return defaultBuildIndexHandlerBlock(handlerId);
  }
  return {};
}

export function patchBuildIndexHandlerBlock(
  cfg: Record<string, unknown>,
  handlerId: BuildIndexHandlerId,
  block: Record<string, unknown>
): Record<string, unknown> {
  return { ...cfg, [handlerId]: block };
}

export function buildIndexHandlerDocKey(handler: string): MessageKey {
  if (isBuildIndexHandlerId(handler)) return BUILD_INDEX_DOC[handler];
  return "buildIndex.handlerDoc.generic";
}

export const LOOKUP_KEY_NORMALIZATION_OPTIONS = ["strip_casefold", "strip", "none"] as const;
export type LookupKeyNormalization = (typeof LOOKUP_KEY_NORMALIZATION_OPTIONS)[number];

export function isLookupKeyNormalization(v: string): v is LookupKeyNormalization {
  return (LOOKUP_KEY_NORMALIZATION_OPTIONS as readonly string[]).includes(v);
}
