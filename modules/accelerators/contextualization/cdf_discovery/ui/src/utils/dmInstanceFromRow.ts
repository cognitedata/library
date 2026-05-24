import type { DmInstanceKind, OpenTarget } from "../types/discoveryNodes";

export type { DmInstanceKind };

type Row = Record<string, unknown>;

function field(row: Row, keys: string[]): unknown {
  for (const key of keys) {
    if (key in row && row[key] != null && row[key] !== "") {
      return row[key];
    }
  }
  return undefined;
}

function asString(value: unknown): string {
  if (value == null) return "";
  return String(value).trim();
}

export function dmInstanceKindFromOpenTarget(target: OpenTarget): DmInstanceKind | null {
  if (target.type === "dm_instances") return target.instance_kind;
  if (target.type === "fusion_dm_all") {
    return target.entity === "edges" ? "edge" : target.entity === "nodes" ? "node" : null;
  }
  return null;
}

export function parseDmInstanceRefFromRow(
  row: Row,
  kind: DmInstanceKind
): { space: string; externalId: string } | null {
  const space = asString(
    field(row, ["space", "instanceSpace", "instance_space", "nodeSpace", "node_space"])
  );
  const externalId = asString(
    field(row, [
      "externalId",
      "external_id",
      "nodeExternalId",
      "node_external_id",
      "edgeExternalId",
      "edge_external_id",
      kind === "edge" ? "edgeExternalId" : "nodeExternalId",
    ])
  );
  if (!space || !externalId) return null;
  return { space, externalId };
}

export function containerRefFromNodeMeta(meta: Record<string, unknown> | undefined): {
  space: string;
  externalId: string;
} | null {
  if (!meta) return null;
  const space = asString(field(meta, ["space"]));
  const externalId = asString(field(meta, ["external_id", "externalId"]));
  if (!space || !externalId) return null;
  return { space, externalId };
}

export function containerRefFromNodeId(nodeId: string): { space: string; externalId: string } | null {
  const prefix = "fusion:dm:space:";
  const marker = ":container:";
  if (!nodeId.startsWith(prefix) || !nodeId.includes(marker)) return null;
  const body = nodeId.slice(prefix.length);
  const idx = body.indexOf(marker);
  if (idx < 0) return null;
  try {
    const space = decodeURIComponent(body.slice(0, idx));
    const externalId = decodeURIComponent(body.slice(idx + marker.length));
    if (!space || !externalId) return null;
    return { space, externalId };
  } catch {
    return null;
  }
}
