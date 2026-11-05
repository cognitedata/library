/**
 * usePyodideRuntime — React hook for managing PyodideRuntime lifecycle.
 *
 * Separate entry point so the core atlas-agent bundle stays Pyodide-free.
 */

import { useState, useRef, useEffect } from 'react';
import type { CogniteClient } from '@cognite/sdk';
import { getGlobalPyodideRuntime } from './pyodide-runtime';
import type { PyodideRuntimeConfig } from './pyodide-runtime';
import type { PythonRuntime } from './types';

// ============================================================================
// Types
// ============================================================================

export interface PyodideProgress {
  stage: string;
  percent: number;
}

export interface UsePyodideRuntimeOptions {
  /** The `loadPyodide` function from the `pyodide` package. */
  loadPyodide: PyodideRuntimeConfig['loadPyodide'];
  /** CogniteClient for SDK credential injection. `null` disables initialization. */
  client: CogniteClient | null;
  /** Additional Python packages to install via micropip. */
  requirements?: string[];
  /** CDN URL for Pyodide files. */
  cdnUrl?: string;
}

export interface UsePyodideRuntimeReturn {
  /** The initialized runtime, or undefined if not yet ready. */
  runtime: PythonRuntime | undefined;
  /** True while Pyodide is loading / initializing. */
  loading: boolean;
  /** Error message if initialization failed. */
  error: string | null;
  /** Current initialization progress. */
  progress: PyodideProgress;
  /** Convenience: true when runtime is ready to use. */
  isReady: boolean;
}

// ============================================================================
// Hook
// ============================================================================

const DEFAULT_BASE_URL = 'https://api.cognitedata.com';

/**
 * Manages PyodideRuntime initialization lifecycle.
 *
 * Loads Pyodide, installs packages, injects Cognite SDK credentials,
 * and returns a ready-to-use `PythonRuntime` with loading/error state.
 *
 * ```tsx
 * import { loadPyodide } from 'pyodide';
 *
 * const { runtime, loading, progress, isReady } = usePyodideRuntime({
 *   loadPyodide,
 *   client: sdk,
 *   requirements: ['pandas', 'numpy'],
 * });
 * ```
 */
export function usePyodideRuntime(
  options: UsePyodideRuntimeOptions,
): UsePyodideRuntimeReturn {
  const { client } = options;

  const [runtime, setRuntime] = useState<PythonRuntime>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<PyodideProgress>({ stage: '', percent: 0 });

  // Refs for values that shouldn't trigger re-initialization
  const loadPyodideRef = useRef(options.loadPyodide);
  const requirementsRef = useRef(options.requirements);
  const cdnUrlRef = useRef(options.cdnUrl);
  loadPyodideRef.current = options.loadPyodide;
  requirementsRef.current = options.requirements;
  cdnUrlRef.current = options.cdnUrl;

  useEffect(() => {
    if (!client) {
      setLoading(false);
      return;
    }

    let mounted = true;

    (async () => {
      try {
        setLoading(true);
        setError(null);

        const instance = getGlobalPyodideRuntime({
          loadPyodide: loadPyodideRef.current,
          requirements: requirementsRef.current,
          cdnUrl: cdnUrlRef.current,
          onProgress: (stage, percent) => {
            if (mounted) setProgress({ stage, percent });
          },
        });

        if (!instance.isInitialized) {
          if (mounted) setProgress({ stage: 'Initializing...', percent: 0 });

          const headers = client.getDefaultRequestHeaders();
          const token = headers.Authorization?.split(' ')[1] ?? '';

          await instance.initialize({
            project: client.project,
            baseUrl: client.getBaseUrl?.() ?? DEFAULT_BASE_URL,
            token,
          });
        }

        if (mounted) {
          setRuntime(instance);
          setProgress({ stage: 'Ready', percent: 100 });
        }
      } catch (err) {
        if (mounted) {
          setError(err instanceof Error ? err.message : String(err));
        }
      } finally {
        if (mounted) setLoading(false);
      }
    })();

    return () => {
      mounted = false;
    };
  }, [client]);

  return {
    runtime,
    loading,
    error,
    progress,
    isReady: !loading && !error && runtime !== undefined,
  };
}
