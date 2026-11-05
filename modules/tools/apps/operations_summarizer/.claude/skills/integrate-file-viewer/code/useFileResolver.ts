import { useState, useEffect, useRef } from 'react';
import type { CogniteClient } from '@cognite/sdk';
import type { FileSource, UseFileResolverResult } from './types';
import { inferMimeTypeFromUrl } from './mimeTypes';
import { resolveFileDownloadConfig } from './fileResolution';

// ============================================================================
// Helpers
// ============================================================================

function getSourceKey(source: FileSource): string {
  switch (source.type) {
    case 'instanceId':
      return `inst:${source.space}/${source.externalId}`;
    case 'internalId':
      return `id:${source.id}`;
    case 'url':
      return `url:${source.url}\0${source.mimeType ?? ''}`;
  }
}

const INITIAL: UseFileResolverResult = {
  isLoading: true,
  error: null,
};

// ============================================================================
// Hook
// ============================================================================

/**
 * Resolves a {@link FileSource} to a download URL and MIME type.
 *
 * - `url` sources are returned directly (no client needed).
 * - `internalId` and `instanceId` sources use the CogniteClient to fetch
 *   metadata and resolve a download URL (with caching).
 */
export function useFileResolver(
  source: FileSource,
  client?: CogniteClient,
): UseFileResolverResult {
  const [result, setResult] = useState<UseFileResolverResult>(INITIAL);
  const sourceKey = getSourceKey(source);
  const cancelRef = useRef(0);

  useEffect(() => {
    const id = ++cancelRef.current;
    const cancelled = () => id !== cancelRef.current;

    async function resolve() {
      setResult(INITIAL);

      try {
        // ----- URL source: no client needed -----
        if (source.type === 'url') {
          const mimeType = source.mimeType ?? inferMimeTypeFromUrl(source.url);
          setResult({
            url: source.url,
            mimeType: mimeType ?? '',
            isLoading: false,
            error: null,
          });
          return;
        }

        // ----- CDF sources: client is required -----
        if (!client) {
          throw new Error(
            'CogniteClient is required for instanceId and internalId sources',
          );
        }

        // Build the lookup identifier the SDK expects
        const idParam =
          source.type === 'internalId'
            ? { id: source.id }
            : {
                instanceId: {
                  space: source.space,
                  externalId: source.externalId,
                },
              };

        const [fileInfo] = await client.files.retrieve([idParam]);
        if (cancelled()) return;

        const resolved = await resolveFileDownloadConfig(client, fileInfo);
        if (cancelled()) return;

        // Derive instanceId — prefer the one returned by the API,
        // fall back to what the caller passed for instanceId sources.
        const instanceId = fileInfo.instanceId
          ? {
              space: fileInfo.instanceId.space,
              externalId: fileInfo.instanceId.externalId,
            }
          : source.type === 'instanceId'
            ? { space: source.space, externalId: source.externalId }
            : undefined;

        setResult({
          url: resolved.url,
          mimeType: resolved.mimeType,
          fileInfo,
          instanceId,
          isLoading: false,
          error: null,
        });
      } catch (err) {
        if (cancelled()) return;
        setResult({
          isLoading: false,
          error: err instanceof Error ? err : new Error(String(err)),
        });
      }
    }

    resolve();
  }, [sourceKey, client]);

  return result;
}
