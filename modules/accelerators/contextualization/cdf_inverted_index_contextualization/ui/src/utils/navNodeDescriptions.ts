import type { MessageKey } from "../i18n";

const NAV_DESC_KEYS: Partial<Record<MessageKey, MessageKey>> = {
  "nav.indexing": "nav.desc.indexing",
  "nav.overview": "nav.desc.overview",
  "nav.ops": "nav.desc.ops",
  "nav.buildMetadata": "nav.desc.buildMetadata",
  "nav.buildAnnotations": "nav.desc.buildAnnotations",
  "nav.query": "nav.desc.query",
  "nav.fileContext": "nav.desc.fileContext",
  "nav.targetDriven": "nav.desc.targetDriven",
  "nav.tagReuse": "nav.desc.tagReuse",
};

export function navNodeDescription(
  labelKey: MessageKey,
  hasChildren: boolean,
  t: (key: MessageKey) => string
): string {
  const descKey = NAV_DESC_KEYS[labelKey];
  if (descKey) return t(descKey);
  if (hasChildren) return t("nav.desc.folder");
  return t("nav.desc.default");
}
