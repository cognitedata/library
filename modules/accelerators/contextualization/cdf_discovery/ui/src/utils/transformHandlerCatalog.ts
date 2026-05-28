import catalogJson from "../generated/transformHandlerCatalog.json";

type HandlerCatalogBucket = Record<string, string>;

type TransformHandlerCatalogFile = {
  transform: HandlerCatalogBucket;
  build_index: HandlerCatalogBucket;
};

const catalog = catalogJson as TransformHandlerCatalogFile;

export function transformHandlerDescription(handlerId: string): string {
  const desc = catalog.transform[handlerId]?.trim();
  return desc || "Transform";
}

export function buildIndexHandlerDescription(handlerId: string): string {
  const desc = catalog.build_index[handlerId]?.trim();
  return desc || "Build inverted index";
}

/** Palette / connect-menu tooltip text from exported Python handler descriptions. */
export function handlerPaletteTooltip(stage: "transform" | "build_index", handlerId: string): string {
  if (stage === "build_index") return buildIndexHandlerDescription(handlerId);
  return transformHandlerDescription(handlerId);
}
