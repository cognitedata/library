import { useRef, useEffect, useState, useMemo, useCallback } from "react";
import type { CogniteClient } from "@cognite/sdk";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Button } from "@/shared/components/ui/button";
import { Badge } from "@/shared/components/ui/badge";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/shared/components/ui/tooltip";
import {
  ChevronLeft,
  ChevronRight,
  ZoomIn,
  ZoomOut,
  RotateCcw,
  Image,
  Loader2,
  AlertCircle,
  Eye,
  EyeOff,
  CheckCircle2,
  Sparkles,
  Move,
  Maximize2,
  X,
} from "lucide-react";
import { useFileCdfId, useFilePreview, useFilePageCount } from "@/pages/AnnotationQuality/hooks/useFilePreview";
import type { AnnotationRecord } from "@/shared/utils/types";

interface AnnotationOverlay {
  id: string;
  text: string;
  boundingBox: {
    xMin: number;
    yMin: number;
    xMax: number;
    yMax: number;
  };
  isActual: boolean;
  resourceType?: string;
  confidence?: number;
}

interface FilePreviewCanvasProps {
  sdk: CogniteClient | null;
  fileExternalId: string;
  fileSourceId?: string;
  fileName?: string;
  actualAnnotations: AnnotationRecord[];
  potentialAnnotations: AnnotationRecord[];
  onAnnotationClick?: (annotation: AnnotationRecord, isActual: boolean) => void;
  selectedFileId?: string | null;
  setSelectedFileId?: (id: string | null) => void;
}

const ANNOTATION_COLORS = {
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
};

// Fixed sizes for annotations (in CSS pixels, won't scale with zoom)
const ANNOTATION_STYLE = {
  borderWidth: 2,
  borderWidthHover: 3,
  fontSize: 11,
  labelPadding: 4,
  labelHeight: 18,
};

export function FilePreviewCanvas({
  sdk,
  fileExternalId,
  fileSourceId,
  fileName,
  actualAnnotations,
  potentialAnnotations,
  onAnnotationClick,
  selectedFileId,
  setSelectedFileId,
}: FilePreviewCanvasProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewportRef = useRef<HTMLDivElement>(null);

  const [currentPage, setCurrentPage] = useState(1);
  const [zoom, setZoom] = useState(0.5); // Start at 50% to fit large diagrams
  const [showActual, setShowActual] = useState(true);
  const [showPotential, setShowPotential] = useState(true);
  const [hoveredAnnotation, setHoveredAnnotation] = useState<string | null>(null);
  const [imageLoaded, setImageLoaded] = useState(false);
  const [imageDimensions, setImageDimensions] = useState({ width: 0, height: 0 });

  // Pan state
  const [isPanning, setIsPanning] = useState(false);
  const [panStart, setPanStart] = useState({ x: 0, y: 0 });
  const [panOffset, setPanOffset] = useState({ x: 0, y: 0 });

  // Fetch file info (CDF ID) - pass fileName to skip DMS lookup
  const fileLookupId = fileSourceId || fileExternalId;
  const { data: fileInfo, isLoading: isLoadingId, error: idError } = useFileCdfId(
    sdk,
    fileLookupId,
    fileName
  );

  // Fetch page count
  const { data: pageCount = 1 } = useFilePageCount(sdk, fileInfo?.id ?? null);

  // Fetch preview
  const { data: previewData, isLoading: isLoadingPreview, error: previewError } = useFilePreview(
    sdk,
    fileInfo?.id ?? null,
    currentPage,
    fileInfo?.mimeType
  );

  const previewUrl = previewData?.url ?? null;
  const previewType = previewData?.type ?? "image";

  // Filter annotations for current page
  const currentPageAnnotations = useMemo(() => {
    const overlays: AnnotationOverlay[] = [];

    const matchesCurrentPage = (annPage: number | undefined) => {
      const normalizedPage = annPage != null && annPage > 0 ? annPage : 1;
      return normalizedPage === currentPage;
    };

    if (showActual) {
      for (const ann of actualAnnotations) {
        if (matchesCurrentPage(ann.page) && ann.boundingBox) {
          overlays.push({
            id: ann.externalId || `actual-${ann.startNodeText}-${ann.page ?? 1}`,
            text: ann.startNodeText || "Unknown",
            boundingBox: ann.boundingBox,
            isActual: true,
            resourceType: ann.endNodeResourceType,
            confidence: ann.confidence,
          });
        }
      }
    }

    if (showPotential) {
      for (const ann of potentialAnnotations) {
        if (matchesCurrentPage(ann.page) && ann.boundingBox) {
          overlays.push({
            id: ann.externalId || `potential-${ann.startNodeText}-${ann.page ?? 1}`,
            text: ann.startNodeText || "Unknown",
            boundingBox: ann.boundingBox,
            isActual: false,
            resourceType: ann.endNodeResourceType,
            confidence: ann.confidence,
          });
        }
      }
    }

    return overlays;
  }, [actualAnnotations, potentialAnnotations, currentPage, showActual, showPotential]);

  // Count annotations per type for current page
  const annotationCounts = useMemo(() => {
    const matchesCurrentPage = (annPage: number | undefined) => {
      const normalizedPage = annPage != null && annPage > 0 ? annPage : 1;
      return normalizedPage === currentPage;
    };
    const actual = actualAnnotations.filter((a) => matchesCurrentPage(a.page) && a.boundingBox).length;
    const potential = potentialAnnotations.filter((a) => matchesCurrentPage(a.page) && a.boundingBox).length;
    return { actual, potential };
  }, [actualAnnotations, potentialAnnotations, currentPage]);

  // Reset pan when page changes
  useEffect(() => {
    setPanOffset({ x: 0, y: 0 });
  }, [currentPage]);

  // Handle mouse down for panning
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    // Only pan with middle mouse button or when holding space
    if (e.button === 1 || e.altKey) {
      e.preventDefault();
      setIsPanning(true);
      setPanStart({ x: e.clientX - panOffset.x, y: e.clientY - panOffset.y });
    }
  }, [panOffset]);

  // Handle mouse move for panning
  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (isPanning) {
      setPanOffset({
        x: e.clientX - panStart.x,
        y: e.clientY - panStart.y,
      });
    }
  }, [isPanning, panStart]);

  // Handle mouse up
  const handleMouseUp = useCallback(() => {
    setIsPanning(false);
  }, []);

  // Handle wheel for zoom
  const handleWheel = useCallback((e: React.WheelEvent) => {
    if (e.shiftKey) {
      e.preventDefault();
      const delta = e.deltaY > 0 ? -0.1 : 0.1;
      setZoom((z) => Math.max(0.25, Math.min(4, z + delta)));
    }
  }, []);

  // Fit to viewport
  const handleFitToView = useCallback(() => {
    if (!containerRef.current || !imageDimensions.width) return;
    const containerWidth = containerRef.current.clientWidth - 20;
    const containerHeight = 560; // Account for padding
    const scaleX = containerWidth / imageDimensions.width;
    const scaleY = containerHeight / imageDimensions.height;
    const newZoom = Math.min(scaleX, scaleY, 1);
    setZoom(newZoom);
    setPanOffset({ x: 0, y: 0 });
  }, [imageDimensions]);

  // Handle annotation hover
  const handleAnnotationHover = useCallback((id: string | null) => {
    setHoveredAnnotation(id);
  }, []);

  // Handle annotation click
  const handleAnnotationClick = useCallback(
    (annotation: AnnotationOverlay) => {
      if (!onAnnotationClick) return;

      const actualAnn = actualAnnotations.find(
        (a) => (a.externalId || `actual-${a.startNodeText}-${a.page ?? 1}`) === annotation.id
      );
      if (actualAnn) {
        onAnnotationClick(actualAnn, true);
        return;
      }

      const potentialAnn = potentialAnnotations.find(
        (a) => (a.externalId || `potential-${a.startNodeText}-${a.page ?? 1}`) === annotation.id
      );
      if (potentialAnn) {
        onAnnotationClick(potentialAnn, false);
      }
    },
    [actualAnnotations, potentialAnnotations, onAnnotationClick]
  );

  const isLoading = isLoadingId || isLoadingPreview;
  const hasError = idError || previewError;

  // Render annotation box as a DOM element (doesn't scale with zoom)
  const renderAnnotationBox = (annotation: AnnotationOverlay) => {
    const colors = annotation.isActual ? ANNOTATION_COLORS.actual : ANNOTATION_COLORS.potential;
    const isHovered = hoveredAnnotation === annotation.id;

    // Calculate position in image coordinates (will be scaled by CSS transform)
    const left = annotation.boundingBox.xMin * 100;
    const top = annotation.boundingBox.yMin * 100;
    const width = (annotation.boundingBox.xMax - annotation.boundingBox.xMin) * 100;
    const height = (annotation.boundingBox.yMax - annotation.boundingBox.yMin) * 100;

    // Calculate inverse scale for fixed-size elements
    const inverseScale = 1 / zoom;

    return (
      <div
        key={annotation.id}
        className="absolute cursor-pointer transition-colors"
        style={{
          left: `${left}%`,
          top: `${top}%`,
          width: `${width}%`,
          height: `${height}%`,
          backgroundColor: isHovered ? ANNOTATION_COLORS.hover.fill : colors.fill,
          border: `${ANNOTATION_STYLE.borderWidth * inverseScale}px solid ${isHovered ? ANNOTATION_COLORS.hover.stroke : colors.stroke}`,
          boxSizing: "border-box",
        }}
        onMouseEnter={() => handleAnnotationHover(annotation.id)}
        onMouseLeave={() => handleAnnotationHover(null)}
        onClick={(e) => {
          e.stopPropagation();
          handleAnnotationClick(annotation);
        }}
      >
        {/* Label - only shown on hover */}
        {isHovered && (
          <div
            className="absolute whitespace-nowrap overflow-hidden text-ellipsis pointer-events-none z-50"
            style={{
              bottom: "100%",
              left: 0,
              transform: `scale(${inverseScale})`,
              transformOrigin: "bottom left",
              backgroundColor: ANNOTATION_COLORS.hover.stroke,
              color: "#ffffff",
              fontSize: `${ANNOTATION_STYLE.fontSize}px`,
              padding: `2px ${ANNOTATION_STYLE.labelPadding}px`,
              borderRadius: "2px 2px 0 0",
              maxWidth: "300px",
              boxShadow: "0 2px 8px rgba(0,0,0,0.2)",
            }}
          >
            {annotation.text}
            {annotation.resourceType && (
              <span className="ml-1 opacity-70">({annotation.resourceType})</span>
            )}
          </div>
        )}
      </div>
    );
  };

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <CardTitle className="text-sm flex items-center gap-2">
              <Image className="h-4 w-4 text-muted-foreground" />
              File Preview
            </CardTitle>
            {setSelectedFileId && (
              <Button
                size="xs"
                variant={selectedFileId === fileExternalId ? "secondary" : "outline"}
                className="ml-2"
                onClick={() => {
                  if (selectedFileId === fileExternalId) {
                    setSelectedFileId(null);
                  } else {
                    setSelectedFileId(fileExternalId);
                  }
                }}
              >
                {selectedFileId === fileExternalId ? (
                  <span className="flex items-center gap-1">
                    <span>Unselect file in table</span>
                    <span className="ml-0.5">
                      <X className="h-3 w-3" />
                    </span>
                  </span>
                ) : (
                  "Select file in table"
                )}
              </Button>
            )}
          </div>
          <div className="flex items-center gap-2">
            {annotationCounts.actual > 0 && (
              <Badge variant="success" className="text-[10px]">
                <CheckCircle2 className="h-3 w-3" />
                {annotationCounts.actual} actual
              </Badge>
            )}
            {annotationCounts.potential > 0 && (
              <Badge variant="warning" className="text-[10px]">
                <Sparkles className="h-3 w-3" />
                {annotationCounts.potential} potential
              </Badge>
            )}
          </div>
        </div>
        {fileName && (
          <p className="text-xs text-muted-foreground truncate">{fileName}</p>
        )}
      </CardHeader>
      <CardContent className="space-y-3">
        {/* Controls */}
        <div className="flex flex-wrap items-center gap-2">
          {/* Page navigation */}
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="icon"
              className="h-7 w-7"
              disabled={currentPage <= 1}
              onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
            >
              <ChevronLeft className="h-3.5 w-3.5" />
            </Button>
            <span className="text-xs font-medium px-2">
              Page {currentPage} / {pageCount}
            </span>
            <Button
              variant="outline"
              size="icon"
              className="h-7 w-7"
              disabled={currentPage >= pageCount}
              onClick={() => setCurrentPage((p) => Math.min(pageCount, p + 1))}
            >
              <ChevronRight className="h-3.5 w-3.5" />
            </Button>
          </div>

          <div className="h-4 w-px bg-border" />

          {/* Zoom controls */}
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="icon"
              className="h-7 w-7"
              disabled={zoom <= 0.25}
              onClick={() => setZoom((z) => Math.max(0.25, z - 0.25))}
            >
              <ZoomOut className="h-3.5 w-3.5" />
            </Button>
            <span className="text-xs font-medium w-12 text-center">
              {Math.round(zoom * 100)}%
            </span>
            <Button
              variant="outline"
              size="icon"
              className="h-7 w-7"
              disabled={zoom >= 4}
              onClick={() => setZoom((z) => Math.min(4, z + 0.25))}
            >
              <ZoomIn className="h-3.5 w-3.5" />
            </Button>
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="outline"
                    size="icon"
                    className="h-7 w-7"
                    onClick={handleFitToView}
                  >
                    <Maximize2 className="h-3.5 w-3.5" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  <p>Fit to view</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="outline"
                    size="icon"
                    className="h-7 w-7"
                    onClick={() => {
                      setZoom(1);
                      setPanOffset({ x: 0, y: 0 });
                    }}
                  >
                    <RotateCcw className="h-3.5 w-3.5" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  <p>Reset to 100%</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          </div>

          <div className="h-4 w-px bg-border" />

          {/* Visibility toggles */}
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant={showActual ? "default" : "outline"}
                  size="sm"
                  className="h-7 text-xs gap-1"
                  onClick={() => setShowActual(!showActual)}
                >
                  {showActual ? <Eye className="h-3 w-3" /> : <EyeOff className="h-3 w-3" />}
                  Actual
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                <p>Toggle actual annotations visibility</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>

          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant={showPotential ? "secondary" : "outline"}
                  size="sm"
                  className="h-7 text-xs gap-1"
                  onClick={() => setShowPotential(!showPotential)}
                >
                  {showPotential ? <Eye className="h-3 w-3" /> : <EyeOff className="h-3 w-3" />}
                  Potential
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                <p>Toggle potential annotations visibility</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>

          <div className="h-4 w-px bg-border" />

          {/* Pan hint */}
          <div className="flex items-center gap-1 text-[10px] text-muted-foreground">
            <Move className="h-3 w-3" />
            <span>Alt+Drag to pan • Shift+Scroll to zoom</span>
          </div>
        </div>

        {/* Viewport container */}
        <div
          ref={containerRef}
          className="relative"
          style={{ height: "600px" }}
          onMouseDown={handleMouseDown}
          onMouseMove={handleMouseMove}
          onMouseUp={handleMouseUp}
          onMouseLeave={handleMouseUp}
          onWheel={handleWheel}
        >
          {isLoading && (
            <div className="absolute inset-0 flex items-center justify-center bg-background/50 z-10">
              <div className="flex flex-col items-center gap-2">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                <p className="text-xs text-muted-foreground truncate">Loading preview...</p>
              </div>
            </div>
          )}

          {hasError && !isLoading && (
            <div className="flex flex-col items-center gap-2">
              <AlertCircle className="h-10 w-10 mb-3 opacity-30" />
              <p className="text-sm">Failed to load preview</p>
              <p className="text-xs mt-1">
                Preview may not be available for this file type
              </p>
            </div>
          )}

          {!hasError && previewUrl && previewType === "pdf" && (
            <div className="relative">
              <object
                data={previewUrl}
                type="application/pdf"
                className="w-full h-full rounded"
              >
                <iframe
                  src={previewUrl}
                  title={`PDF preview: ${fileName || fileExternalId}`}
                  className="w-full h-full border-0 rounded"
                  sandbox="allow-same-origin allow-scripts"
                />
              </object>
              {currentPageAnnotations.length > 0 && (
                <div className="absolute bottom-2 left-2 bg-background/90 rounded px-2 py-1 text-[10px] text-muted-foreground">
                  Note: Annotation overlays are not shown for PDF embed view
                </div>
              )}
            </div>
          )}

          {!hasError && previewUrl && previewType === "image" && (
            <div
              ref={viewportRef}
              className="absolute inset-0 overflow-hidden"
              style={{ cursor: isPanning ? "grabbing" : "default" }}
            >
              {/* Transformable content wrapper */}
              <div
                className="relative"
                style={{
                  transform: `translate(${panOffset.x}px, ${panOffset.y}px) scale(${zoom})`,
                  transformOrigin: "top left",
                  willChange: "transform",
                }}
              >
                {/* Image */}
                <img
                  src={previewUrl}
                  alt={`Page ${currentPage} preview`}
                  className="max-w-none"
                  draggable={false}
                  style={{
                    display: imageLoaded ? "block" : "none",
                  }}
                  onLoad={(e) => {
                    const img = e.target as HTMLImageElement;
                    setImageDimensions({
                      width: img.naturalWidth,
                      height: img.naturalHeight,
                    });
                    setImageLoaded(true);
                  }}
                />

                {/* Annotation overlays - rendered as DOM elements */}
                {imageLoaded && (
                  <div
                    className="absolute inset-0"
                    style={{
                      width: imageDimensions.width,
                      height: imageDimensions.height,
                    }}
                  >
                    {currentPageAnnotations.map(renderAnnotationBox)}
                  </div>
                )}
              </div>
            </div>
          )}

          {!hasError && previewUrl && previewType === "download" && (
            <div className="flex flex-col items-center gap-2">
              <Image className="h-10 w-10 mb-3 opacity-30" />
              <p className="text-sm">Preview not available for this file type</p>
              <a
                href={previewUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-primary underline mt-2"
              >
                Download file to view
              </a>
            </div>
          )}

          {!hasError && !previewUrl && !isLoading && (
            <div className="flex flex-col items-center gap-2">
              <Image className="h-10 w-10 mb-3 opacity-30" />
              <p className="text-sm">No preview available</p>
            </div>
          )}
        </div>

        {/* Legend */}
        <div className="flex items-center gap-4 text-[10px] text-muted-foreground">
          <div className="flex items-center gap-1.5">
            <div
              className="w-3 h-3 rounded border-2"
              style={{
                borderColor: ANNOTATION_COLORS.actual.stroke,
                backgroundColor: ANNOTATION_COLORS.actual.fill,
              }}
            />
            <span>Actual Annotation</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div
              className="w-3 h-3 rounded border-2"
              style={{
                borderColor: ANNOTATION_COLORS.potential.stroke,
                backgroundColor: ANNOTATION_COLORS.potential.fill,
              }}
            />
            <span>Potential Annotation</span>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
