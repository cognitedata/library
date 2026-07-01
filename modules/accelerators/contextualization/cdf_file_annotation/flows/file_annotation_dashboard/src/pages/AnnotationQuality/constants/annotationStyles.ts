export const ANNOTATION_COLORS = {
  actual: {
    stroke: "#22c55e",
    fill: "rgba(34, 197, 94, 0.15)",
    text: "#166534",
  },
  potential: {
    stroke: "#f59e0b",
    fill: "rgba(245, 158, 11, 0.15)",
    text: "#92400e",
  },
  hover: {
    stroke: "#3b82f6",
    fill: "rgba(59, 130, 246, 0.25)",
  },
  activeMatch: {
    stroke: "#3b82f6",
  },
} as const;

export const ANNOTATION_STYLE = {
  borderWidth: 2,
  borderWidthHover: 3,
  fontSize: 11,
  labelPadding: 4,
  labelHeight: 18,
} as const;
