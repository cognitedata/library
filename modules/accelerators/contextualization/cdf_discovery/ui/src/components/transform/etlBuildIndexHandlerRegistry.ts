import type { MessageKey } from "../../i18n/types";

export const BUILD_INDEX_HANDLER_IDS = ["property_token_index", "annotation_vertex_index"] as const;
export type BuildIndexHandlerId = (typeof BUILD_INDEX_HANDLER_IDS)[number];

export type BuildIndexHandlerDefinition = {
  id: BuildIndexHandlerId;
  nameKey: MessageKey;
};

export const BUILD_INDEX_HANDLER_DEFINITIONS: readonly BuildIndexHandlerDefinition[] = [
  { id: "property_token_index", nameKey: "buildIndex.handlerName.property_token_index" },
  { id: "annotation_vertex_index", nameKey: "buildIndex.handlerName.annotation_vertex_index" },
] as const;

export function isBuildIndexHandlerId(h: string): h is BuildIndexHandlerId {
  return (BUILD_INDEX_HANDLER_IDS as readonly string[]).includes(h);
}

export function buildIndexHandlerDisplayName(
  id: BuildIndexHandlerId,
  t: (key: MessageKey, vars?: Record<string, string | number>) => string
): string {
  const def = BUILD_INDEX_HANDLER_DEFINITIONS.find((d) => d.id === id);
  return def ? t(def.nameKey) : id;
}
