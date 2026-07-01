export const ANNOTATION_PREVIEW_DEFAULTS = {
  page: 1,
  zoom: 0.5,
  showActual: true,
  showPotential: true,
  activeCategory: "all" as const,
  viewportHeightPx: 600,
} as const;

export const ANNOTATION_PREVIEW_ZOOM = {
  min: 0.25,
  max: 4,
  buttonStep: 0.25,
  wheelDelta: 0.1,
  focusTarget: 3.25,
} as const;

export const ANNOTATION_PREVIEW_FIT = {
  horizontalPaddingPx: 20,
  containerHeightPx: 560,
} as const;

export const ANNOTATION_NAVIGATOR_BEHAVIOR = {
  scrollDelayFrames: 2,
} as const;
