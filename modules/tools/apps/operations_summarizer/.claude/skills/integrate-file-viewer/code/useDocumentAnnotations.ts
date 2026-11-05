import { useState, useEffect, useRef, useMemo } from 'react';
import type { CogniteClient, EdgeDefinition } from '@cognite/sdk';
import type {
  DocumentAnnotation,
  AnnotationResourceType,
  UseDocumentAnnotationsResult,
} from './types';

// ============================================================================
// CDM constants
// ============================================================================

const CDM_SPACE = 'cdf_cdm';
const CDM_VERSION = 'v1';
const DIAGRAM_ANNOTATION_VIEW = 'CogniteDiagramAnnotation';
const QUERY_LIMIT = 10_000;

// ============================================================================
// Cache — stores ALL annotations for a file, filtered by page at read time
// ============================================================================

const STALE_TIME = 5 * 60 * 1000; // 5 minutes
const MAX_CACHE_SIZE = 50;

interface CacheEntry {
  data: DocumentAnnotation[];
  timestamp: number;
}

const annotationCache = new Map<string, CacheEntry>();

/** Cache key scoped by project + file instance. */
function fileCacheKey(project: string, space: string, externalId: string): string {
  return JSON.stringify([project, space, externalId]);
}

function evictStaleAnnotations(): void {
  const now = Date.now();
  for (const [key, entry] of annotationCache) {
    if (now - entry.timestamp > STALE_TIME) annotationCache.delete(key);
  }
  if (annotationCache.size > MAX_CACHE_SIZE) {
    for (const key of Array.from(annotationCache.keys()).slice(0, annotationCache.size - MAX_CACHE_SIZE)) {
      annotationCache.delete(key);
    }
  }
}

export function clearAnnotationCache(): void {
  annotationCache.clear();
}

// ============================================================================
// Helpers
// ============================================================================

interface CdmAnnotationProps {
  status?: string;
  startNodeText?: string;
  startNodeYMax?: number;
  startNodeYMin?: number;
  startNodeXMax?: number;
  startNodeXMin?: number;
  startNodePageNumber?: number;
}

function getResourceType(annotationType: string): AnnotationResourceType {
  const lower = annotationType.toLowerCase();
  if (lower.includes('asset')) return 'asset';
  if (lower.includes('file')) return 'file';
  if (lower.includes('timeseries') || lower.includes('time_series'))
    return 'timeSeries';
  if (lower.includes('sequence')) return 'sequence';
  if (lower.includes('event')) return 'event';
  if (lower.includes('diagram')) return 'diagram';
  return 'unknown';
}

// ============================================================================
// Fetcher — fetches ALL annotations for a file (not per-page)
// ============================================================================

async function fetchAllAnnotations(
  client: CogniteClient,
  space: string,
  externalId: string,
): Promise<DocumentAnnotation[]> {
  const containerId = `${space}:${externalId}`;
  const propPath = `${DIAGRAM_ANNOTATION_VIEW}/${CDM_VERSION}`;

  const allEdges: EdgeDefinition[] = [];
  let cursor: string | undefined;

  do {
    const response = await client.instances.query({
      with: {
        files: {
          nodes: {
            filter: {
              and: [
                {
                  equals: {
                    property: ['node', 'externalId'],
                    value: externalId,
                  },
                },
                {
                  equals: {
                    property: ['node', 'space'],
                    value: space,
                  },
                },
              ],
            },
          },
        },
        annotations: {
          edges: {
            from: 'files',
            direction: 'outwards',
          },
          limit: QUERY_LIMIT,
        },
      },
      select: {
        annotations: {
          sources: [
            {
              source: {
                externalId: DIAGRAM_ANNOTATION_VIEW,
                space: CDM_SPACE,
                type: 'view' as const,
                version: CDM_VERSION,
              },
              properties: [
                'status',
                'startNodeText',
                'startNodeYMax',
                'startNodeYMin',
                'startNodeXMax',
                'startNodeXMin',
                'startNodePageNumber',
              ],
            },
          ],
          limit: QUERY_LIMIT,
        },
      },
      cursors: cursor ? { annotations: cursor } : undefined,
    });

    const edges = (response.items?.annotations ?? []).filter(
      (a) => a.instanceType === 'edge',
    );
    allEdges.push(...edges);

    cursor =
      edges.length < QUERY_LIMIT
        ? undefined
        : response.nextCursor?.annotations;
  } while (cursor);

  return allEdges.flatMap((edge) => {
    const props: CdmAnnotationProps | undefined =
      edge.properties?.[CDM_SPACE]?.[propPath];
    if (!props) return [];
    if (props.status === 'Rejected') return [];

    const xMin = Number(props.startNodeXMin ?? 0);
    const xMax = Number(props.startNodeXMax ?? 0);
    const yMin = Number(props.startNodeYMin ?? 0);
    const yMax = Number(props.startNodeYMax ?? 0);

    const annotationType =
      edge.type?.externalId ?? 'diagrams.AssetLink';

    const annotation: DocumentAnnotation = {
      id: `${containerId}-${edge.space}-${edge.externalId}`,
      x: Math.min(xMin, xMax),
      y: Math.min(yMin, yMax),
      width: Math.abs(xMax - xMin),
      height: Math.abs(yMax - yMin),
      page: Number(props.startNodePageNumber ?? 1),
      resourceType: getResourceType(annotationType),
      linkedResource: edge.endNode
        ? { space: edge.endNode.space, externalId: edge.endNode.externalId }
        : undefined,
      text: props.startNodeText ?? undefined,
      annotationType,
    };
    return [annotation];
  });
}

// ============================================================================
// Hook
// ============================================================================

interface AnnotationState {
  allAnnotations: DocumentAnnotation[];
  isLoading: boolean;
  error: Error | null;
}

const INITIAL_STATE: AnnotationState = {
  allAnnotations: [],
  isLoading: false,
  error: null,
};

export function useDocumentAnnotations(
  client: CogniteClient | undefined,
  instanceId: { space: string; externalId: string } | undefined,
  currentPage: number = 1,
  options?: { enabled?: boolean },
): UseDocumentAnnotationsResult {
  const enabled = options?.enabled ?? true;
  const [state, setState] = useState<AnnotationState>(INITIAL_STATE);
  const cancelRef = useRef(0);

  const space = instanceId?.space;
  const extId = instanceId?.externalId;
  const project = client?.project;

  // Fetch all annotations for the file (not per-page)
  useEffect(() => {
    if (!enabled || !client || !space || !extId || !project) {
      setState(INITIAL_STATE);
      return;
    }

    const id = ++cancelRef.current;
    const cancelled = () => id !== cancelRef.current;

    const key = fileCacheKey(project, space, extId);
    const cached = annotationCache.get(key);
    if (cached && Date.now() - cached.timestamp < STALE_TIME) {
      setState({ allAnnotations: cached.data, isLoading: false, error: null });
      return;
    }

    setState((prev) => ({ ...prev, isLoading: true, error: null }));

    fetchAllAnnotations(client, space, extId)
      .then((data) => {
        if (cancelled()) return;
        annotationCache.set(key, { data, timestamp: Date.now() });
        evictStaleAnnotations();
        setState({ allAnnotations: data, isLoading: false, error: null });
      })
      .catch((err) => {
        if (cancelled()) return;
        setState({
          allAnnotations: [],
          isLoading: false,
          error: err instanceof Error ? err : new Error(String(err)),
        });
      });
  }, [client, project, space, extId, enabled]);

  // Filter by current page (cheap client-side filter on cached data)
  const annotations = useMemo(
    () => state.allAnnotations.filter((a) => a.page === currentPage),
    [state.allAnnotations, currentPage],
  );

  return { annotations, isLoading: state.isLoading, error: state.error };
}
