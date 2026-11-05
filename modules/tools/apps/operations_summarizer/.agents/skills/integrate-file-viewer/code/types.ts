import type React from 'react';
import type { CogniteClient, FileInfo } from '@cognite/sdk';

// ============================================================================
// File Source (discriminated union)
// ============================================================================

export type FileSource =
  | { type: 'instanceId'; space: string; externalId: string }
  | { type: 'url'; url: string; mimeType?: string }
  | { type: 'internalId'; id: number };

// ============================================================================
// Viewer Types
// ============================================================================

export type FileViewerType = 'pdf' | 'image' | 'text' | 'unsupported';

// ============================================================================
// Annotations
// ============================================================================

export type AnnotationResourceType =
  | 'asset'
  | 'file'
  | 'timeSeries'
  | 'sequence'
  | 'event'
  | 'diagram'
  | 'unknown';

export interface DocumentAnnotation {
  id: string;
  /** Normalized bounding box (0-1 range relative to page) */
  x: number;
  y: number;
  width: number;
  height: number;
  /** 1-indexed page number */
  page: number;
  resourceType: AnnotationResourceType;
  linkedResource?: { space: string; externalId: string };
  /** Text content (e.g. tag name) */
  text?: string;
  annotationType: string;
}

// ============================================================================
// Resolved File
// ============================================================================

export interface ResolvedFile {
  url: string;
  mimeType: string;
  fileInfo?: FileInfo;
  instanceId?: { space: string; externalId: string };
}

// ============================================================================
// Hook Results
// ============================================================================

export interface UseFileResolverResult extends Partial<ResolvedFile> {
  isLoading: boolean;
  error: Error | null;
}

export interface UseDocumentAnnotationsResult {
  annotations: DocumentAnnotation[];
  isLoading: boolean;
  error: Error | null;
}

// ============================================================================
// Geometry
// ============================================================================

export interface BoundingRect {
  x: number;
  y: number;
  width: number;
  height: number;
}

// ============================================================================
// Overlay
// ============================================================================

export interface OverlayRenderInfo {
  /** Rendered page width in CSS pixels (after zoom/scale) */
  width: number;
  /** Rendered page height in CSS pixels (after zoom/scale) */
  height: number;
  /** Original page width before zoom (PDF points or image natural pixels) */
  originalWidth: number;
  /** Original page height before zoom (PDF points or image natural pixels) */
  originalHeight: number;
  /** Current page number (1-indexed) */
  pageNumber: number;
  /** Current rotation in degrees */
  rotation: 0 | 90 | 180 | 270;
}

// ============================================================================
// Component Props
// ============================================================================

export interface CogniteFileViewerProps {
  /** File source — instance ID, direct URL, or CDF internal ID */
  source: FileSource;
  /** CogniteClient instance (required for instanceId and internalId sources) */
  client?: CogniteClient;

  // -- Annotations --
  /** Show diagram annotations overlay on PDFs (default: true) */
  showAnnotations?: boolean;
  /** Called when a user clicks an annotation */
  onAnnotationClick?: (annotation: DocumentAnnotation) => void;
  /** Called when a user hovers over / leaves an annotation */
  onAnnotationHover?: (annotation: DocumentAnnotation | null) => void;
  /** Render a custom tooltip when hovering an annotation. Receives the annotation and its pixel-space bounding rect. */
  renderAnnotationTooltip?: (
    annotation: DocumentAnnotation,
    rect: BoundingRect,
  ) => React.ReactNode;

  // -- PDF controls --
  /** Current page (1-indexed). Uncontrolled if omitted. */
  page?: number;
  /** Called when the displayed page changes */
  onPageChange?: (page: number) => void;
  /** Called once the PDF document is loaded */
  onDocumentLoad?: (info: { numPages: number }) => void;
  /** Desired page width in pixels */
  width?: number;
  /** Page rotation in degrees */
  rotation?: 0 | 90 | 180 | 270;

  // -- Zoom & Pan --
  /** Current zoom level (1 = 100%). Supports controlled + uncontrolled. */
  zoom?: number;
  /** Called when zoom changes (Ctrl/Cmd+wheel or pinch) */
  onZoomChange?: (zoom: number) => void;
  /** Minimum zoom level (default: 0.25) */
  minZoom?: number;
  /** Maximum zoom level (default: 5) */
  maxZoom?: number;
  /** Pan offset in pixels. Supports controlled + uncontrolled. Resets on page change. */
  panOffset?: { x: number; y: number };
  /** Called when pan changes (drag when zoomed in) */
  onPanChange?: (offset: { x: number; y: number }) => void;

  // -- Fit & Progress --
  /** Auto-fit mode: 'width' fits page to container width, 'page' fits entire page in container */
  fitMode?: 'width' | 'page';
  /** Called during PDF download with progress info */
  onLoadProgress?: (progress: { loaded: number; total: number }) => void;

  // -- Custom overlay --
  /**
   * Render custom content (e.g. SVG paths, highlights, drawings) on top of the page.
   * The overlay is absolutely positioned over the rendered page.
   *
   * Provides both rendered dimensions and original (unscaled) page dimensions,
   * so consumers can set up an SVG `viewBox` in the original coordinate space:
   * ```tsx
   * renderOverlay={({ width, height, originalWidth, originalHeight }) => (
   *   <svg width={width} height={height}
   *        viewBox={`0 0 ${originalWidth} ${originalHeight}`}
   *        preserveAspectRatio="none"
   *        style={{ pointerEvents: 'all' }}>
   *     <path d="..." />
   *   </svg>
   * )}
   * ```
   */
  renderOverlay?: (info: OverlayRenderInfo) => React.ReactNode;

  // -- Customisation --
  /** Override the default loading indicator */
  renderLoading?: () => React.ReactNode;
  /** Override the default error view */
  renderError?: (error: Error) => React.ReactNode;
  /** Override the default "unsupported file" view */
  renderUnsupported?: (mimeType: string | undefined) => React.ReactNode;

  className?: string;
  style?: React.CSSProperties;
}
