import React, { useState } from 'react';
import type { DocumentAnnotation, AnnotationResourceType, BoundingRect } from './types';

// ============================================================================
// Annotation colours (matches cogs.js-v10 design tokens)
// ============================================================================

const ANNOTATION_COLORS: Record<
  AnnotationResourceType,
  { stroke: string; hoverFill: string }
> = {
  asset: {
    stroke: 'rgb(212, 106, 226)',
    hoverFill: 'rgba(212, 106, 226, 0.15)',
  },
  file: {
    stroke: 'rgb(255, 135, 70)',
    hoverFill: 'rgba(255, 135, 70, 0.15)',
  },
  timeSeries: {
    stroke: 'rgb(164, 178, 252)',
    hoverFill: 'rgba(164, 178, 252, 0.15)',
  },
  sequence: {
    stroke: 'rgb(255, 220, 127)',
    hoverFill: 'rgba(255, 220, 127, 0.15)',
  },
  event: {
    stroke: 'rgb(253, 81, 144)',
    hoverFill: 'rgba(253, 81, 144, 0.15)',
  },
  diagram: {
    stroke: 'rgb(76, 175, 80)',
    hoverFill: 'rgba(76, 175, 80, 0.15)',
  },
  unknown: {
    stroke: 'rgb(89, 89, 89)',
    hoverFill: 'rgba(89, 89, 89, 0.15)',
  },
};

// ============================================================================
// Types
// ============================================================================

export interface DocumentAnnotationOverlayProps {
  /** Annotations to render (coordinates are normalised 0-1) */
  annotations: DocumentAnnotation[];
  /** Rendered page width in CSS pixels */
  containerWidth: number;
  /** Rendered page height in CSS pixels */
  containerHeight: number;
  /** Document rotation in degrees (0, 90, 180, 270) */
  rotation?: number;
  /** Called when a user clicks an annotation */
  onAnnotationClick?: (annotation: DocumentAnnotation) => void;
  /** Called when a user hovers over / leaves an annotation */
  onAnnotationHover?: (annotation: DocumentAnnotation | null) => void;
  /** Render a custom tooltip for hovered annotations. Receives the annotation and its pixel-space bounding rect. */
  renderAnnotationTooltip?: (
    annotation: DocumentAnnotation,
    rect: BoundingRect,
  ) => React.ReactNode;
}

// ============================================================================
// Helpers
// ============================================================================

function getStyle(
  resourceType: AnnotationResourceType,
  isHovered: boolean,
) {
  const colors = ANNOTATION_COLORS[resourceType] ?? ANNOTATION_COLORS.unknown;
  return {
    stroke: colors.stroke,
    fill: isHovered ? colors.hoverFill : 'none',
    strokeWidth: isHovered ? 2 : 1.5,
  };
}

function transformAnnotation(
  annotation: DocumentAnnotation,
  w: number,
  h: number,
  rotation: number,
) {
  const { x, y, width, height } = annotation;
  switch (rotation) {
    case 90:
      return {
        x: (1 - y - height) * w,
        y: x * h,
        width: height * w,
        height: width * h,
      };
    case 180:
      return {
        x: (1 - x - width) * w,
        y: (1 - y - height) * h,
        width: width * w,
        height: height * h,
      };
    case 270:
      return {
        x: y * w,
        y: (1 - x - width) * h,
        width: height * w,
        height: width * h,
      };
    default:
      return {
        x: x * w,
        y: y * h,
        width: width * w,
        height: height * h,
      };
  }
}

// ============================================================================
// Component
// ============================================================================

export const DocumentAnnotationOverlay: React.FC<
  DocumentAnnotationOverlayProps
> = ({
  annotations,
  containerWidth,
  containerHeight,
  rotation = 0,
  onAnnotationClick,
  onAnnotationHover,
  renderAnnotationTooltip,
}) => {
  const [hoveredId, setHoveredId] = useState<string | null>(null);

  if (annotations.length === 0 || containerWidth === 0 || containerHeight === 0) {
    return null;
  }

  const hoveredAnnotation = hoveredId
    ? annotations.find((a) => a.id === hoveredId)
    : null;
  const hoveredRect = hoveredAnnotation
    ? transformAnnotation(hoveredAnnotation, containerWidth, containerHeight, rotation)
    : null;

  return (
    <>
      <svg
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          width: '100%',
          height: '100%',
          pointerEvents: 'none',
          overflow: 'visible',
          zIndex: 10,
        }}
        viewBox={`0 0 ${containerWidth} ${containerHeight}`}
      >
        {annotations.map((annotation) => {
          const isHovered = hoveredId === annotation.id;
          const style = getStyle(annotation.resourceType, isHovered);
          const rect = transformAnnotation(
            annotation,
            containerWidth,
            containerHeight,
            rotation,
          );

          return (
            <rect
              key={annotation.id}
              x={rect.x}
              y={rect.y}
              width={rect.width}
              height={rect.height}
              fill={style.fill}
              stroke={style.stroke}
              strokeWidth={style.strokeWidth}
              rx={1}
              ry={1}
              style={{ pointerEvents: 'auto', cursor: 'pointer' }}
              onMouseEnter={() => {
                setHoveredId(annotation.id);
                onAnnotationHover?.(annotation);
              }}
              onMouseLeave={() => {
                setHoveredId(null);
                onAnnotationHover?.(null);
              }}
              onClick={(e) => {
                e.stopPropagation();
                onAnnotationClick?.(annotation);
              }}
            >
              {!renderAnnotationTooltip && annotation.text && (
                <title>{annotation.text}</title>
              )}
            </rect>
          );
        })}
      </svg>
      {renderAnnotationTooltip && hoveredAnnotation && hoveredRect && (
        renderAnnotationTooltip(hoveredAnnotation, hoveredRect)
      )}
    </>
  );
};

// ============================================================================
// Utilities
// ============================================================================

export function getAnnotationColor(
  resourceType: AnnotationResourceType,
): { stroke: string; hoverFill: string } {
  return ANNOTATION_COLORS[resourceType] ?? ANNOTATION_COLORS.unknown;
}

export function getAllAnnotationColors(): Record<
  AnnotationResourceType,
  { stroke: string; hoverFill: string }
> {
  return ANNOTATION_COLORS;
}
