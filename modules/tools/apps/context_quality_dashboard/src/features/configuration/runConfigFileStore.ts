export type StoredRunConfigFile = {
  fileName: string;
  /** The file as uploaded. It is parsed every time it is read, so a stored file is never trusted as valid. */
  text: string;
};

/** What the store needs of the browser's storage. */
export type RunConfigStorage = Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;

/** A React external store (`useSyncExternalStore`) holding the uploaded config file. */
export interface RunConfigFileStore {
  getSnapshot(): StoredRunConfigFile | null;
  subscribe(listener: () => void): () => void;
  save(file: StoredRunConfigFile): void;
  clear(): void;
}

const STORAGE_KEY = 'context-quality.run-config-file';

function isStoredFile(value: unknown): value is StoredRunConfigFile {
  return (
    typeof value === 'object' &&
    value !== null &&
    'fileName' in value &&
    'text' in value &&
    typeof value.fileName === 'string' &&
    typeof value.text === 'string'
  );
}

function readStored(storage: RunConfigStorage | null): StoredRunConfigFile | null {
  try {
    const raw = storage?.getItem(STORAGE_KEY) ?? null;
    const value: unknown = raw === null ? null : JSON.parse(raw);
    return isStoredFile(value) ? { fileName: value.fileName, text: value.text } : null;
  } catch {
    return null;
  }
}

export function createRunConfigFileStore(storage: RunConfigStorage | null): RunConfigFileStore {
  let current = readStored(storage);
  const listeners = new Set<() => void>();

  const change = (next: StoredRunConfigFile | null, write: () => void): void => {
    current = next;
    try {
      write();
    } catch {
      // the browser would not store it (full or blocked): the file still works for this session
    }
    listeners.forEach((listener) => listener());
  };

  return {
    getSnapshot: () => current,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    save: (file) => change({ ...file }, () => storage?.setItem(STORAGE_KEY, JSON.stringify(file))),
    clear: () => change(null, () => storage?.removeItem(STORAGE_KEY)),
  };
}

function browserStorage(): RunConfigStorage | null {
  try {
    return typeof window === 'undefined' ? null : window.localStorage;
  } catch {
    return null;
  }
}

/** The store the app uses. Tests make their own with `createRunConfigFileStore`. */
export const runConfigFileStore: RunConfigFileStore = createRunConfigFileStore(browserStorage());
