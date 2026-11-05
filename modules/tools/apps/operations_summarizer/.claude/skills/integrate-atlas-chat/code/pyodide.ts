/**
 * Pyodide entry point — browser-based Python execution for Atlas agents.
 *
 * Import path: @cognite/dune-utils/atlas-agent/pyodide
 */

// Runtime
export {
  PyodideRuntime,
  getGlobalPyodideRuntime,
  resetGlobalPyodideRuntime,
  clearPyodideCache,
} from './pyodide-runtime';
export type { PyodideRuntimeConfig, PyodideSDKConfig, PyodideInstance } from './pyodide-runtime';

// React hook
export { usePyodideRuntime } from './pyodide-react';
export type {
  PyodideProgress,
  UsePyodideRuntimeOptions,
  UsePyodideRuntimeReturn,
} from './pyodide-react';
