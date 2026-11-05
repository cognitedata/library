import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Document, Page, pdfjs } from 'react-pdf';
import type { PageCallback } from 'react-pdf/dist/shared/types.js';
import 'react-pdf/dist/Page/TextLayer.css';
import 'react-pdf/dist/Page/AnnotationLayer.css';

pdfjs.GlobalWorkerOptions.workerSrc = new URL(
  'pdfjs-dist/build/pdf.worker.min.mjs',
  import.meta.url,
).toString();
import type { CogniteFileViewerProps } from './types';
import { getViewerType } from './mimeTypes';
import { useFileResolver } from './useFileResolver';
import { useDocumentAnnotations } from './useDocumentAnnotations';
import { DocumentAnnotationOverlay } from './DocumentAnnotationOverlay';
import { useViewport, computeBaseWidth } from './useViewport';

// ============================================================================
// Sub-renderers
// ============================================================================

function DefaultLoading() {
  return <div style={{ padding: 16, color: '#666' }}>Loading file...</div>;
}

function DefaultError({ error }: { error: Error }) {
  return (
    <div style={{ padding: 16, color: '#c00' }}>
      Failed to load file: {error.message}
    </div>
  );
}

function DefaultUnsupported({ mimeType }: { mimeType: string | undefined }) {
  return (
    <div style={{ padding: 16, color: '#666' }}>
      Unsupported file type{mimeType ? `: ${mimeType}` : ''}
    </div>
  );
}

// ---------- Shared blob fetch hook ----------

function useBlobUrl(url: string) {
  const [blobUrl, setBlobUrl] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const objectUrlRef = useRef<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    // Reset state for new URL
    setBlobUrl(null);
    setError(null);

    // Revoke previous blob URL
    if (objectUrlRef.current) {
      URL.revokeObjectURL(objectUrlRef.current);
      objectUrlRef.current = null;
    }

    fetch(url)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.blob();
      })
      .then((blob) => {
        if (cancelled) return;
        const newUrl = URL.createObjectURL(blob);
        objectUrlRef.current = newUrl;
        setBlobUrl(newUrl);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err : new Error(String(err)));
      });

    return () => {
      cancelled = true;
      if (objectUrlRef.current) {
        URL.revokeObjectURL(objectUrlRef.current);
        objectUrlRef.current = null;
      }
    };
  }, [url]);

  return { blobUrl, error };
}

// ---------- Image ----------

interface ImageRendererProps
  extends Omit<CogniteFileViewerProps, 'source' | 'client' | 'className' | 'style'> {
  url: string;
}

function ImageRenderer(props: ImageRendererProps) {
  const { url, rotation = 0, fitMode, width: explicitWidth, renderLoading, renderError, renderOverlay } = props;
  const { currentZoom, effectivePan, containerDims, viewportRef, cursor, handleMouseDown } =
    useViewport(props);

  const { blobUrl, error } = useBlobUrl(url);
  const [naturalSize, setNaturalSize] = useState<{ width: number; height: number } | null>(null);

  // Reset natural size when URL changes
  const prevUrlRef = useRef(url);
  if (prevUrlRef.current !== url) {
    prevUrlRef.current = url;
    setNaturalSize(null);
  }

  const handleLoad = useCallback((e: React.SyntheticEvent<HTMLImageElement>) => {
    setNaturalSize({ width: e.currentTarget.naturalWidth, height: e.currentTarget.naturalHeight });
  }, []);

  if (error) return renderError ? renderError(error) : <DefaultError error={error} />;
  if (!blobUrl) return renderLoading ? renderLoading() : <DefaultLoading />;

  const baseWidth = computeBaseWidth(fitMode, explicitWidth, containerDims, naturalSize);
  const imgWidth = baseWidth ?? naturalSize?.width;

  // Until we know image dimensions, render hidden to measure
  if (!imgWidth || !naturalSize) {
    return (
      <div ref={viewportRef} style={{ overflow: 'hidden' }}>
        {renderLoading ? renderLoading() : <DefaultLoading />}
        <img
          src={blobUrl}
          alt=""
          style={{ position: 'absolute', visibility: 'hidden', pointerEvents: 'none' }}
          onLoad={handleLoad}
        />
      </div>
    );
  }

  const imgHeight = imgWidth * (naturalSize.height / naturalSize.width);
  const isSwapped = rotation === 90 || rotation === 270;
  const visualW = (isSwapped ? imgHeight : imgWidth) * currentZoom;
  const visualH = (isSwapped ? imgWidth : imgHeight) * currentZoom;

  return (
    <div ref={viewportRef} style={{ overflow: currentZoom > 1 ? 'hidden' : 'auto', cursor }} onMouseDown={handleMouseDown}>
      <div
        style={{
          display: 'inline-block',
          transform:
            effectivePan.x !== 0 || effectivePan.y !== 0
              ? `translate(${effectivePan.x}px, ${effectivePan.y}px)`
              : undefined,
        }}
      >
        <div style={{ width: visualW, height: visualH, position: 'relative' }}>
          <img
            src={blobUrl}
            alt=""
            style={{
              position: 'absolute',
              width: imgWidth * currentZoom,
              top: '50%',
              left: '50%',
              transform: `translate(-50%, -50%) rotate(${rotation}deg)`,
            }}
            onLoad={handleLoad}
          />
          {renderOverlay && naturalSize && (
            renderOverlay({
              width: visualW,
              height: visualH,
              originalWidth: naturalSize.width,
              originalHeight: naturalSize.height,
              pageNumber: 1,
              rotation,
            })
          )}
        </div>
      </div>
    </div>
  );
}

// ---------- Text ----------

function TextRenderer({ url }: { url: string }) {
  const [content, setContent] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetch(url)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.text();
      })
      .then((text) => {
        if (!cancelled) setContent(text);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err : new Error(String(err)));
      });
    return () => {
      cancelled = true;
    };
  }, [url]);

  if (error) return <DefaultError error={error} />;
  if (content === null) return <DefaultLoading />;

  return (
    <pre
      style={{
        margin: 0,
        padding: 16,
        overflow: 'auto',
        whiteSpace: 'pre-wrap',
        wordBreak: 'break-word',
        fontSize: 13,
        lineHeight: 1.5,
        fontFamily: 'monospace',
      }}
    >
      {content}
    </pre>
  );
}

// ============================================================================
// PDF Renderer (with annotation overlay)
// ============================================================================

const PDF_LOAD_ERROR = new Error('Failed to load PDF');

interface PdfRendererProps
  extends Omit<CogniteFileViewerProps, 'source' | 'className' | 'style' | 'renderUnsupported'> {
  url: string;
  instanceId?: { space: string; externalId: string };
}

function PdfRenderer(props: PdfRendererProps) {
  const {
    url,
    instanceId,
    client,
    showAnnotations = true,
    onAnnotationClick,
    onAnnotationHover,
    renderAnnotationTooltip,
    page: controlledPage,
    onPageChange,
    onDocumentLoad,
    width,
    rotation = 0,
    fitMode,
    onLoadProgress,
    renderLoading,
    renderError,
    renderOverlay,
  } = props;

  // -- Viewport (zoom, pan, wheel, drag) --
  const {
    currentZoom,
    effectivePan,
    containerDims,
    viewportRef,
    cursor,
    handleMouseDown,
    handlePanChange,
  } = useViewport(props);

  // -- Page state (controlled + uncontrolled) --
  const [internalPage, setInternalPage] = useState(1);
  const currentPage = controlledPage ?? internalPage;

  const handlePageChange = useCallback(
    (newPage: number) => {
      setInternalPage(newPage);
      onPageChange?.(newPage);
    },
    [onPageChange],
  );

  // Reset pan on page change
  const handlePanChangeRef = useRef(handlePanChange);
  handlePanChangeRef.current = handlePanChange;
  useEffect(() => {
    handlePanChangeRef.current({ x: 0, y: 0 });
  }, [currentPage]);

  // -- Page dimensions (for annotation overlay) --
  const [pageDims, setPageDims] = useState({ width: 0, height: 0 });
  const pageObserverRef = useRef<ResizeObserver | null>(null);

  const pageWrapperRef = useCallback((node: HTMLDivElement | null) => {
    if (pageObserverRef.current) {
      pageObserverRef.current.disconnect();
      pageObserverRef.current = null;
    }
    if (node) {
      const measure = () => {
        const w = node.clientWidth;
        const h = node.clientHeight;
        setPageDims((prev) => (prev.width === w && prev.height === h ? prev : { width: w, height: h }));
      };
      const observer = new ResizeObserver(measure);
      observer.observe(node);
      measure();
      pageObserverRef.current = observer;
    }
  }, []);

  useEffect(() => {
    return () => {
      pageObserverRef.current?.disconnect();
      pageObserverRef.current = null;
    };
  }, []);

  // -- Page natural dimensions (for fitMode='page') --
  const [pageNaturalSize, setPageNaturalSize] = useState<{ width: number; height: number } | null>(null);

  const handlePageLoadSuccess = useCallback((page: PageCallback) => {
    const { originalWidth: w, originalHeight: h } = page;
    if (w && h) setPageNaturalSize({ width: w, height: h });
  }, []);

  // -- Compute base width from fitMode --
  const baseWidth = computeBaseWidth(fitMode, width, containerDims, pageNaturalSize);

  // -- Annotations --
  const annotationsEnabled = showAnnotations && instanceId !== undefined;

  const { annotations } = useDocumentAnnotations(
    client,
    instanceId,
    currentPage,
    { enabled: annotationsEnabled },
  );

  // -- PDF Document callbacks --
  const currentPageRef = useRef(currentPage);
  currentPageRef.current = currentPage;

  const handleLoadSuccess = useCallback(
    ({ numPages }: { numPages: number }) => {
      onDocumentLoad?.({ numPages });
      if (currentPageRef.current > numPages) handlePageChange(1);
    },
    [onDocumentLoad, handlePageChange],
  );

  return (
    <div
      ref={viewportRef}
      style={{ overflow: currentZoom > 1 ? 'hidden' : 'auto', cursor, height: '100%' }}
      onMouseDown={handleMouseDown}
    >
      <Document
        file={url}
        onLoadSuccess={handleLoadSuccess}
        onLoadProgress={onLoadProgress}
        loading={renderLoading ? renderLoading() : <DefaultLoading />}
        error={
          renderError ? (
            renderError(PDF_LOAD_ERROR)
          ) : (
            <DefaultError error={PDF_LOAD_ERROR} />
          )
        }
      >
        <div
          ref={pageWrapperRef}
          style={{
            position: 'relative',
            display: 'inline-block',
            transform: effectivePan.x !== 0 || effectivePan.y !== 0
              ? `translate(${effectivePan.x}px, ${effectivePan.y}px)`
              : undefined,
          }}
        >
          <Page
            pageNumber={currentPage}
            width={baseWidth}
            scale={currentZoom}
            rotate={rotation}
            onLoadSuccess={handlePageLoadSuccess}
          />
          {annotationsEnabled && pageDims.width > 0 && annotations.length > 0 && (
            <DocumentAnnotationOverlay
              annotations={annotations}
              containerWidth={pageDims.width}
              containerHeight={pageDims.height}
              rotation={rotation}
              onAnnotationClick={onAnnotationClick}
              onAnnotationHover={onAnnotationHover}
              renderAnnotationTooltip={renderAnnotationTooltip}
            />
          )}
          {renderOverlay && pageDims.width > 0 && pageDims.height > 0 && pageNaturalSize && (
            renderOverlay({
              width: pageDims.width,
              height: pageDims.height,
              originalWidth: pageNaturalSize.width,
              originalHeight: pageNaturalSize.height,
              pageNumber: currentPage,
              rotation,
            })
          )}
        </div>
      </Document>
    </div>
  );
}

// ============================================================================
// Main Component
// ============================================================================

export const CogniteFileViewer: React.FC<CogniteFileViewerProps> = (props) => {
  const {
    source,
    client,
    renderLoading,
    renderError,
    renderUnsupported,
    className,
    style,
  } = props;

  const {
    url,
    mimeType,
    instanceId,
    isLoading,
    error,
  } = useFileResolver(source, client);

  const viewerType = getViewerType(mimeType);
  const rotation = props.rotation ?? 0;

  // -- Loading --
  if (isLoading) {
    return (
      <div className={className} style={style}>
        {renderLoading ? renderLoading() : <DefaultLoading />}
      </div>
    );
  }

  // -- Error --
  if (error || !url) {
    return (
      <div className={className} style={style}>
        {renderError
          ? renderError(error ?? new Error('No URL resolved'))
          : <DefaultError error={error ?? new Error('No URL resolved')} />}
      </div>
    );
  }

  // -- Render by type --
  const renderContent = () => {
    switch (viewerType) {
      case 'pdf':
        return <PdfRenderer {...props} url={url} instanceId={instanceId} rotation={rotation} />;
      case 'image':
        return <ImageRenderer {...props} url={url} rotation={rotation} />;
      case 'text':
        return <TextRenderer url={url} />;
      default:
        return renderUnsupported ? renderUnsupported(mimeType) : <DefaultUnsupported mimeType={mimeType} />;
    }
  };

  return (
    <div className={className} style={style}>
      {renderContent()}
    </div>
  );
};
