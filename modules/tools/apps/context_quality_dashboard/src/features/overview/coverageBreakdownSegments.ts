export type BreakdownSegmentId = 'contextualised' | 'remaining' | 'excluded';

export type BreakdownSegment = {
  id: BreakdownSegmentId;
  value: number;
  fillClassName: string;
  strokeClassName: string;
  legendDotClassName: string;
};

/** Sky palette aligned with the coverage badge (`variant="sky"`). */
export function buildBreakdownSegments(input: {
  contextualised: number;
  remaining: number;
  excluded: number;
}): BreakdownSegment[] {
  const segments: BreakdownSegment[] = [
    {
      id: 'contextualised',
      value: input.contextualised,
      fillClassName: 'fill-sky-600',
      strokeClassName: 'stroke-sky-600',
      legendDotClassName: 'bg-sky-600',
    },
    {
      id: 'remaining',
      value: input.remaining,
      fillClassName: 'fill-sky-400',
      strokeClassName: 'stroke-sky-400',
      legendDotClassName: 'bg-sky-400',
    },
    {
      id: 'excluded',
      value: input.excluded,
      fillClassName: 'fill-sky-200',
      strokeClassName: 'stroke-sky-200',
      legendDotClassName: 'bg-sky-200',
    },
  ];

  return segments.filter((segment) => segment.value > 0);
}

const IN_SCOPE_SEGMENT_DEFS: readonly Omit<BreakdownSegment, 'value'>[] = [
  {
    id: 'contextualised',
    fillClassName: 'fill-sky-600',
    strokeClassName: 'stroke-sky-600',
    legendDotClassName: 'bg-sky-600',
  },
  {
    id: 'remaining',
    fillClassName: 'fill-sky-400',
    strokeClassName: 'stroke-sky-400',
    legendDotClassName: 'bg-sky-400',
  },
];

/** In-scope hero breakdown: contextualised vs remaining only (no excluded segment). */
export function buildInScopeBreakdownSegments(
  input: {
    contextualised: number;
    remaining: number;
  },
  options?: { includeZeroSegments?: boolean }
): BreakdownSegment[] {
  const segments = IN_SCOPE_SEGMENT_DEFS.map((definition) => ({
    ...definition,
    value: definition.id === 'contextualised' ? input.contextualised : input.remaining,
  }));
  if (options?.includeZeroSegments === true) {
    return segments;
  }
  return segments.filter((segment) => segment.value > 0);
}

export function polarToCartesian(cx: number, cy: number, radius: number, angleDegrees: number) {
  const radians = ((angleDegrees - 90) * Math.PI) / 180;
  return {
    x: cx + radius * Math.cos(radians),
    y: cy + radius * Math.sin(radians),
  };
}

export function describePieSlice(
  cx: number,
  cy: number,
  radius: number,
  startAngle: number,
  endAngle: number
): string {
  const start = polarToCartesian(cx, cy, radius, endAngle);
  const end = polarToCartesian(cx, cy, radius, startAngle);
  const largeArc = endAngle - startAngle <= 180 ? 0 : 1;
  return `M ${cx} ${cy} L ${start.x} ${start.y} A ${radius} ${radius} 0 ${largeArc} 0 ${end.x} ${end.y} Z`;
}
