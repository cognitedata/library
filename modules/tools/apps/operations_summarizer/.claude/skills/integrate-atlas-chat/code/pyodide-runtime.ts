/**
 * PyodideRuntime — browser-based Python execution via Pyodide.
 *
 * Wraps Pyodide loading, package installation, Cognite SDK setup,
 * and localStorage caching into a clean PythonRuntime implementation.
 *
 * The consumer owns the 'pyodide' npm package — they pass `loadPyodide`
 * as a config parameter so this module has no hard dependency on it.
 */

import type { PythonRuntime } from './types';

// ============================================================================
// Minimal Pyodide Interfaces (avoids 'pyodide' package dependency)
// ============================================================================

interface PyodideGlobals {
  get(name: string): unknown;
  set(name: string, value: unknown): void;
}

/** Subset of PyodideInterface that this module uses. */
export interface PyodideInstance {
  loadPackage(packages: string[]): Promise<void>;
  runPython(code: string): unknown;
  runPythonAsync(code: string): Promise<unknown>;
  globals: PyodideGlobals;
  pyimport(name: string): unknown;
}

interface Micropip {
  install(packages: string | string[]): Promise<void>;
}

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_CDN_URL = 'https://cdn.jsdelivr.net/pyodide/v0.29.3/full/';
const CACHE_KEY = 'dune_pyodide_initialized';
const CACHE_VERSION = 'v1';

// ============================================================================
// Config Types
// ============================================================================

export interface PyodideRuntimeConfig {
  /** The `loadPyodide` function from the `pyodide` package. */
  loadPyodide: (options: { indexURL: string }) => Promise<PyodideInstance>;
  /** CDN URL for Pyodide files. Defaults to jsDelivr v0.29.3. */
  cdnUrl?: string;
  /** Additional Python packages to install via micropip. */
  requirements?: string[];
  /** Progress callback for initialization stages. */
  onProgress?: (stage: string, percent: number) => void;
}

export interface PyodideSDKConfig {
  project: string;
  baseUrl: string;
  token: string;
}

// ============================================================================
// Python Utility Code (injected at init)
// ============================================================================

const PYTHON_UTILS = `
import json

def _serialize_cognite_object(obj, depth=0):
    if depth > 10: return str(obj)
    for attr in ('dump', 'as_dict'):
        fn = getattr(obj, attr, None)
        if fn:
            try: return fn()
            except: pass
    if isinstance(obj, dict):
        return {k: _serialize_cognite_object(v, depth+1) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize_cognite_object(i, depth+1) for i in obj]
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    d = getattr(obj, '__dict__', None)
    if d is not None:
        try: return _serialize_cognite_object(d, depth+1)
        except: pass
    return str(obj)

def as_json_string(value):
    return json.dumps(_serialize_cognite_object(value))
`;

// ============================================================================
// Cache Helpers
// ============================================================================

function isCacheValid(): boolean {
  try {
    return localStorage.getItem(CACHE_KEY) === CACHE_VERSION;
  } catch {
    return false;
  }
}

function markCacheValid(): void {
  try {
    localStorage.setItem(CACHE_KEY, CACHE_VERSION);
  } catch {
    /* localStorage unavailable */
  }
}

/** Clear the Pyodide package cache — forces re-download on next init. */
export function clearPyodideCache(): void {
  try {
    localStorage.removeItem(CACHE_KEY);
  } catch {
    /* localStorage unavailable */
  }
}

// ============================================================================
// PyProxy Detection (structural — avoids importing from 'pyodide')
// ============================================================================

function isPyProxy(value: unknown): value is { destroy(): void } {
  return (
    value != null &&
    typeof value === 'object' &&
    'destroy' in value &&
    typeof (value as Record<string, unknown>).destroy === 'function'
  );
}

function isMicropip(value: unknown): value is Micropip {
  return (
    typeof value === 'object' &&
    value !== null &&
    'install' in value &&
    typeof (value as Record<string, unknown>).install === 'function'
  );
}

function destroyIfPyProxy(value: unknown): void {
  if (isPyProxy(value)) {
    value.destroy();
  }
}

// ============================================================================
// PyodideRuntime Class
// ============================================================================

/**
 * PythonRuntime backed by Pyodide. Handles loading, package installation,
 * Cognite SDK credential injection, caching, and PyProxy conversion.
 */
export class PyodideRuntime implements PythonRuntime {
  private pyodide?: PyodideInstance;
  private micropip?: Micropip;
  private _initialized = false;
  private _initPromise?: Promise<void>;
  private readonly config: PyodideRuntimeConfig;

  constructor(config: PyodideRuntimeConfig) {
    this.config = config;
  }

  get isInitialized(): boolean {
    return this._initialized;
  }

  /**
   * Load Pyodide, install packages, and set up the Cognite SDK.
   * Safe to call multiple times — concurrent callers await the same init.
   */
  async initialize(sdk: PyodideSDKConfig): Promise<void> {
    if (this._initialized) return;
    // Memoize the in-flight initialization so concurrent callers await the same
    // load instead of each triggering a full Pyodide setup. Running init /
    // loadPackage / micropip concurrently corrupts package state and surfaces as
    // intermittent "No module named 'micropip'" errors.
    if (this._initPromise) return this._initPromise;

    this._initPromise = this.doInitialize(sdk).catch((error) => {
      this._initPromise = undefined;
      throw error;
    });

    return this._initPromise;
  }

  private async doInitialize(sdk: PyodideSDKConfig): Promise<void> {
    const report = this.config.onProgress ?? (() => {});
    const cdnUrl = this.config.cdnUrl ?? DEFAULT_CDN_URL;

    // 1. Load Pyodide — keep a local reference until packages are fully installed.
    report('Loading Pyodide...', 10);
    const pyodide = await this.config.loadPyodide({ indexURL: cdnUrl });
    report('Pyodide loaded', 30);

    // 2. Core packages (micropip + HTTP patching)
    report('Loading core packages...', 40);
    await pyodide.loadPackage(['micropip', 'pyodide-http']);
    await pyodide.runPythonAsync(`
try:
    import pyodide_http
    pyodide_http.patch_all()
except Exception:
    pass
`);
    const micropip = pyodide.pyimport('micropip');
    if (!isMicropip(micropip)) {
      throw new Error(
        'micropip did not load correctly — pyimport returned an unexpected shape',
      );
    }

    // 3. Cognite SDK
    const verb = isCacheValid() ? 'Loading' : 'Downloading';
    report(`${verb} cognite-sdk...`, 60);
    await micropip.install('cognite-sdk');
    if (!isCacheValid()) markCacheValid();
    report('cognite-sdk ready', 80);

    // 4. Additional packages
    const reqs = this.config.requirements ?? [];
    if (reqs.length > 0) {
      report('Installing packages...', 85);
      await micropip.install(reqs);
    }

    // 5. Utility functions + Cognite client
    report('Setting up environment...', 90);
    pyodide.runPython(PYTHON_UTILS);

    report('Initializing Cognite client...', 95);
    pyodide.runPython(`
import os
os.environ["COGNITE_PROJECT"] = "${sdk.project}"
os.environ["COGNITE_BASE_URL"] = "${sdk.baseUrl}"
os.environ["COGNITE_TOKEN"] = "${sdk.token}"
os.environ["COGNITE_FUSION_NOTEBOOK"] = "1"
os.environ["MPLBACKEND"] = "AGG"
from cognite.client import CogniteClient
client = CogniteClient()
`);

    this.pyodide = pyodide;
    this.micropip = micropip;
    this._initialized = true;
    report('Ready', 100);
  }

  /** Execute Python code asynchronously. PyProxy results are converted to JSON-safe values. */
  async runCodeAsync(code: string): Promise<unknown> {
    const pyodide = this.requirePyodide();
    const raw = await pyodide.runPythonAsync(code);
    return this.toJsonSafe(raw);
  }

  /** Refresh the Cognite SDK token (e.g. after token rotation). */
  refreshToken(token: string): void {
    this.requirePyodide().runPython(
      `import os; os.environ["COGNITE_TOKEN"] = "${token}"`,
    );
  }

  private requirePyodide(): PyodideInstance {
    if (!this._initialized || !this.pyodide) {
      throw new Error(
        'PyodideRuntime not initialized — call initialize() first',
      );
    }
    return this.pyodide;
  }

  /** Convert a Pyodide result to a JSON-safe JS value. */
  private toJsonSafe(value: unknown): unknown {
    if (value == null) return undefined;
    if (!isPyProxy(value)) return value;

    const pyodide = this.requirePyodide();
    const converter = pyodide.globals.get('as_json_string') as
      | ((obj: unknown) => string)
      | undefined;

    if (!converter) {
      throw new Error(
        'as_json_string not available — was initialize() called?',
      );
    }

    try {
      return JSON.parse((converter as (obj: unknown) => string)(value));
    } finally {
      destroyIfPyProxy(converter);
      destroyIfPyProxy(value);
    }
  }
}

// ============================================================================
// Singleton
// ============================================================================

let globalInstance: PyodideRuntime | undefined;

/**
 * Get or create the global PyodideRuntime singleton.
 * Config is only used on first call — subsequent calls return the existing instance.
 */
export function getGlobalPyodideRuntime(
  config: PyodideRuntimeConfig,
): PyodideRuntime {
  if (!globalInstance) {
    globalInstance = new PyodideRuntime(config);
  }
  return globalInstance;
}

/** Reset the global runtime (e.g. on logout). */
export function resetGlobalPyodideRuntime(): void {
  globalInstance = undefined;
}
