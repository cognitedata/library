export type DocLookupDataCategory = "standard" | "custom" | "legacy";

const STANDARD_VIEW_SPACES = new Set(["cdf_cdm", "cdf_idm", "cdf_infield"]);
const LEGACY_VIEW_SPACES = new Set(["cdf_apm"]);

export type PropertyColorClasses = {
  border: string;
  bg: string;
  label: string;
};

export type ViewSpaceTheme = {
  panelBorder: string;
  panelBorderDrift: string;
  headerBg: string;
  headerBorder: string;
  headerText: string;
  spaceLabel: string;
  versionPanelBg: string;
  versionPanelBorder: string;
};

function hashString(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) >>> 0;
  }
  return hash;
}

const STANDARD_PROPERTY_PALETTE: PropertyColorClasses[] = [
  { border: "border-sky-200", bg: "bg-sky-50", label: "text-sky-700" },
  { border: "border-violet-200", bg: "bg-violet-50", label: "text-violet-700" },
  { border: "border-emerald-200", bg: "bg-emerald-50", label: "text-emerald-700" },
  { border: "border-rose-200", bg: "bg-rose-50", label: "text-rose-700" },
  { border: "border-amber-200", bg: "bg-amber-50", label: "text-amber-700" },
  { border: "border-teal-200", bg: "bg-teal-50", label: "text-teal-700" },
  { border: "border-indigo-200", bg: "bg-indigo-50", label: "text-indigo-700" },
  { border: "border-pink-200", bg: "bg-pink-50", label: "text-pink-700" },
  { border: "border-lime-200", bg: "bg-lime-50", label: "text-lime-700" },
  { border: "border-cyan-200", bg: "bg-cyan-50", label: "text-cyan-700" },
];

const CUSTOM_PROPERTY_PALETTE: PropertyColorClasses[] = [
  { border: "border-sky-400", bg: "bg-sky-200", label: "text-sky-950" },
  { border: "border-violet-400", bg: "bg-violet-200", label: "text-violet-950" },
  { border: "border-emerald-400", bg: "bg-emerald-200", label: "text-emerald-950" },
  { border: "border-rose-400", bg: "bg-rose-200", label: "text-rose-950" },
  { border: "border-amber-400", bg: "bg-amber-200", label: "text-amber-950" },
  { border: "border-teal-400", bg: "bg-teal-200", label: "text-teal-950" },
  { border: "border-indigo-400", bg: "bg-indigo-200", label: "text-indigo-950" },
  { border: "border-fuchsia-400", bg: "bg-fuchsia-200", label: "text-fuchsia-950" },
  { border: "border-lime-500", bg: "bg-lime-300", label: "text-lime-950" },
  { border: "border-cyan-500", bg: "bg-cyan-300", label: "text-cyan-950" },
];

const LEGACY_PROPERTY_PALETTE: PropertyColorClasses[] = [
  { border: "border-stone-300", bg: "bg-stone-100", label: "text-stone-600" },
  { border: "border-stone-400", bg: "bg-stone-200/70", label: "text-stone-700" },
  { border: "border-amber-300", bg: "bg-amber-100/80", label: "text-amber-900/80" },
  { border: "border-orange-300", bg: "bg-orange-100/70", label: "text-orange-900/70" },
  { border: "border-neutral-300", bg: "bg-neutral-100", label: "text-neutral-600" },
  { border: "border-yellow-300", bg: "bg-yellow-100/60", label: "text-yellow-900/70" },
  { border: "border-zinc-300", bg: "bg-zinc-100", label: "text-zinc-600" },
  { border: "border-stone-500/40", bg: "bg-stone-200/50", label: "text-stone-700" },
];

const PALETTES: Record<DocLookupDataCategory, PropertyColorClasses[]> = {
  standard: STANDARD_PROPERTY_PALETTE,
  custom: CUSTOM_PROPERTY_PALETTE,
  legacy: LEGACY_PROPERTY_PALETTE,
};

export function getViewSpaceDataCategory(viewSpace: string): DocLookupDataCategory {
  if (LEGACY_VIEW_SPACES.has(viewSpace)) return "legacy";
  if (STANDARD_VIEW_SPACES.has(viewSpace)) return "standard";
  return "custom";
}

export function getPropertyColorClasses(
  propertyName: string,
  category: DocLookupDataCategory
): PropertyColorClasses {
  const palette = PALETTES[category];
  const index = hashString(propertyName.toLowerCase()) % palette.length;
  return palette[index];
}

export function getViewSpaceTheme(category: DocLookupDataCategory): ViewSpaceTheme {
  if (category === "standard") {
    return {
      panelBorder: "border-slate-200",
      panelBorderDrift: "border-amber-300",
      headerBg: "bg-slate-50",
      headerBorder: "border-slate-200",
      headerText: "text-slate-900",
      spaceLabel: "text-slate-600",
      versionPanelBg: "bg-white",
      versionPanelBorder: "border-slate-200",
    };
  }

  if (category === "legacy") {
    return {
      panelBorder: "border-stone-300",
      panelBorderDrift: "border-amber-400/60",
      headerBg: "bg-stone-100",
      headerBorder: "border-stone-300",
      headerText: "text-stone-800",
      spaceLabel: "text-stone-600",
      versionPanelBg: "bg-stone-50",
      versionPanelBorder: "border-stone-300",
    };
  }

  return {
    panelBorder: "border-indigo-300",
    panelBorderDrift: "border-amber-400",
    headerBg: "bg-indigo-100",
    headerBorder: "border-indigo-300",
    headerText: "text-indigo-950",
    spaceLabel: "text-indigo-800",
    versionPanelBg: "bg-indigo-50/40",
    versionPanelBorder: "border-indigo-200",
  };
}

export const DATA_CATEGORY_SWATCHES: Record<DocLookupDataCategory, string> = {
  standard: "bg-sky-100 border-sky-200",
  custom: "bg-indigo-200 border-indigo-400",
  legacy: "bg-stone-200 border-stone-300",
};

export function getDataCategoryLabels(
  t: (key: string) => string
): Record<DocLookupDataCategory, { label: string; description: string; swatch: string }> {
  return {
    standard: {
      label: t("dataCatalog.docLookup.category.standard.label"),
      description: t("dataCatalog.docLookup.category.standard.description"),
      swatch: DATA_CATEGORY_SWATCHES.standard,
    },
    custom: {
      label: t("dataCatalog.docLookup.category.custom.label"),
      description: t("dataCatalog.docLookup.category.custom.description"),
      swatch: DATA_CATEGORY_SWATCHES.custom,
    },
    legacy: {
      label: t("dataCatalog.docLookup.category.legacy.label"),
      description: t("dataCatalog.docLookup.category.legacy.description"),
      swatch: DATA_CATEGORY_SWATCHES.legacy,
    },
  };
}
