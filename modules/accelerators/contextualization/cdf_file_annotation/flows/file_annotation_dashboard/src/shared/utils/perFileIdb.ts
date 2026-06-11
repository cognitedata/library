import { deleteDB, openDB } from "idb";
import type { DBSchema, IDBPDatabase } from "idb";
import type { AnnotationRecord, FileAggregation } from "./types";

const DB_NAME = "file-annotation-perfile";
const DB_VERSION = 2;
const SESSION_KEY = "perFileSessionId";
let activeDb: IDBPDatabase<PerFileDbSchema> | null = null;
let openingDb: Promise<IDBPDatabase<PerFileDbSchema>> | null = null;

type AnnotationKind = "actual" | "potential";

export interface StoredAnnotationRecord extends AnnotationRecord {
  id?: number;
  pipelineId: string;
  fileExternalId: string;
  kind: AnnotationKind;
}

export interface StoredFileAggregation extends FileAggregation {
  pipelineId: string;
}

export interface StoredPerFileAnnotations {
  pipelineId: string;
  fileExternalId: string;
  actual: AnnotationRecord[];
  potential: AnnotationRecord[];
}

interface PerFileDbSchema extends DBSchema {
  annotations: {
    key: number;
    value: StoredAnnotationRecord;
    indexes: {
      byPipeline: string;
      byFile: [string, string];
      byFileKind: [string, string, AnnotationKind];
    };
  };
  fileAggregations: {
    key: [string, string];
    value: StoredFileAggregation;
    indexes: {
      byPipeline: string;
    };
  };
  perFileAnnotations: {
    key: [string, string];
    value: StoredPerFileAnnotations;
    indexes: {
      byPipeline: string;
    };
  };
  meta: {
    key: string;
    value: string;
  };
}

function getSessionId(): string {
  if (typeof window === "undefined") return "server";
  const existing = window.sessionStorage.getItem(SESSION_KEY);
  if (existing) return existing;
  const next = typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `session-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  window.sessionStorage.setItem(SESSION_KEY, next);
  return next;
}

export async function openPerFileDb(): Promise<IDBPDatabase<PerFileDbSchema>> {
  if (activeDb) return activeDb;
  if (openingDb) return openingDb;
  openingDb = openDB<PerFileDbSchema>(DB_NAME, DB_VERSION, {
    upgrade(db) {
      if (!db.objectStoreNames.contains("annotations")) {
        const store = db.createObjectStore("annotations", {
          keyPath: "id",
          autoIncrement: true,
        });
        store.createIndex("byPipeline", "pipelineId");
        store.createIndex("byFile", ["pipelineId", "fileExternalId"]);
        store.createIndex("byFileKind", ["pipelineId", "fileExternalId", "kind"]);
      }
      if (!db.objectStoreNames.contains("fileAggregations")) {
        const store = db.createObjectStore("fileAggregations", {
          keyPath: ["pipelineId", "fileExternalId"],
        });
        store.createIndex("byPipeline", "pipelineId");
      }
      if (!db.objectStoreNames.contains("perFileAnnotations")) {
        const store = db.createObjectStore("perFileAnnotations", {
          keyPath: ["pipelineId", "fileExternalId"],
        });
        store.createIndex("byPipeline", "pipelineId");
      }
      if (!db.objectStoreNames.contains("meta")) {
        db.createObjectStore("meta");
      }
    },
  });
  const db = await openingDb;
  activeDb = db;
  openingDb = null;
  return db;
}

export function closePerFileDb() {
  if (activeDb) {
    try {
      activeDb.close();
    } catch {
      // Ignore close errors.
    }
    activeDb = null;
  }
  openingDb = null;
}

export async function clearPerFileDb(timeoutMs = 5000): Promise<{
  cleared: boolean;
  blocked: boolean;
  timedOut: boolean;
}> {
  if (typeof indexedDB === "undefined") {
    closePerFileDb();
    await deleteDB(DB_NAME);
    return { cleared: true, blocked: false, timedOut: false };
  }

  closePerFileDb();

  return new Promise((resolve) => {
    let settled = false;
    const finish = (result: { cleared: boolean; blocked: boolean; timedOut: boolean }) => {
      if (settled) return;
      settled = true;
      resolve(result);
    };

    const request = indexedDB.deleteDatabase(DB_NAME);
    request.onsuccess = () => finish({ cleared: true, blocked: false, timedOut: false });
    request.onerror = () => finish({ cleared: false, blocked: false, timedOut: false });
    request.onblocked = () => finish({ cleared: false, blocked: true, timedOut: false });

    setTimeout(() => {
      finish({ cleared: false, blocked: false, timedOut: true });
    }, timeoutMs);
  });
}

export async function ensurePerFileDbSession(): Promise<IDBPDatabase<PerFileDbSchema>> {
  const sessionId = getSessionId();
  let db = await openPerFileDb();
  const storedSession = await db.get("meta", "sessionId");

  if (storedSession && storedSession !== sessionId) {
    db.close();
    await clearPerFileDb();
    db = await openPerFileDb();
  }

  await db.put("meta", sessionId, "sessionId");
  return db;
}

export async function setPerFilePipelineId(
  db: IDBPDatabase<PerFileDbSchema>,
  pipelineId: string
) {
  await db.put("meta", pipelineId, "pipelineId");
}

export async function getPerFilePipelineId(
  db: IDBPDatabase<PerFileDbSchema>
): Promise<string | undefined> {
  return db.get("meta", "pipelineId");
}

export async function putAnnotations(
  db: IDBPDatabase<PerFileDbSchema>,
  rows: StoredAnnotationRecord[]
) {
  if (rows.length === 0) return;
  const tx = db.transaction("annotations", "readwrite");
  for (const row of rows) {
    tx.store.add(row);
  }
  await tx.done;
}

export async function putAnnotationsBatch(
  db: IDBPDatabase<PerFileDbSchema>,
  rows: StoredAnnotationRecord[]
) {
  if (rows.length === 0) return;
  const tx = db.transaction("annotations", "readwrite");
  for (const row of rows) {
    tx.store.put(row);
  }
  await tx.done;
}

export async function getAnnotationsByFile(
  db: IDBPDatabase<PerFileDbSchema>,
  pipelineId: string,
  fileExternalId: string
): Promise<{ actual: AnnotationRecord[]; potential: AnnotationRecord[] }> {
  const tx = db.transaction("perFileAnnotations", "readonly");
  const record = await tx.store.get([pipelineId, fileExternalId]);
  await tx.done;
  return {
    actual: record?.actual ?? [],
    potential: record?.potential ?? [],
  };
}

export async function putPerFileAnnotationsBatch(
  db: IDBPDatabase<PerFileDbSchema>,
  pipelineId: string,
  rows: Array<{ fileExternalId: string; actual: AnnotationRecord[]; potential: AnnotationRecord[] }>
) {
  if (rows.length === 0) return;
  const tx = db.transaction("perFileAnnotations", "readwrite");
  for (const row of rows) {
    tx.store.put({
      pipelineId,
      fileExternalId: row.fileExternalId,
      actual: row.actual,
      potential: row.potential,
    });
  }
  await tx.done;
}

export async function putFileAggregations(
  db: IDBPDatabase<PerFileDbSchema>,
  pipelineId: string,
  rows: FileAggregation[]
) {
  const tx = db.transaction("fileAggregations", "readwrite");
  for (const row of rows) {
    tx.store.put({ ...row, pipelineId });
  }
  await tx.done;
}

export async function getFileAggregations(
  db: IDBPDatabase<PerFileDbSchema>,
  pipelineId: string
): Promise<FileAggregation[]> {
  const tx = db.transaction("fileAggregations", "readonly");
  const index = tx.store.index("byPipeline");
  const rows = await index.getAll(pipelineId);
  await tx.done;
  return rows.map(({ pipelineId: _pipelineId, ...rest }) => rest);
}
