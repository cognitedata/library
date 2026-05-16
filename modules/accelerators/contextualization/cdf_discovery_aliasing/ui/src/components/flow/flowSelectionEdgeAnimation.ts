/** React Flow–only: edge ids to animate for upstream ∪ downstream of selected nodes. */

export function upstreamDownstreamAnimatedEdgeIds(
  edges: readonly { id: string; source: string; target: string }[],
  selectedNodeIds: readonly string[]
): Set<string> {
  if (selectedNodeIds.length === 0) return new Set();

  const incoming = new Map<string, { edgeId: string; source: string }[]>();
  const outgoing = new Map<string, { edgeId: string; target: string }[]>();
  for (const e of edges) {
    if (!incoming.has(e.target)) incoming.set(e.target, []);
    incoming.get(e.target)!.push({ edgeId: e.id, source: e.source });
    if (!outgoing.has(e.source)) outgoing.set(e.source, []);
    outgoing.get(e.source)!.push({ edgeId: e.id, target: e.target });
  }

  const animated = new Set<string>();

  const stackB = [...selectedNodeIds];
  const seenB = new Set<string>(selectedNodeIds);
  while (stackB.length) {
    const v = stackB.pop()!;
    const inc = incoming.get(v);
    if (!inc) continue;
    for (const { edgeId, source } of inc) {
      animated.add(edgeId);
      if (!seenB.has(source)) {
        seenB.add(source);
        stackB.push(source);
      }
    }
  }

  const stackF = [...selectedNodeIds];
  const seenF = new Set<string>(selectedNodeIds);
  while (stackF.length) {
    const v = stackF.pop()!;
    const out = outgoing.get(v);
    if (!out) continue;
    for (const { edgeId, target } of out) {
      animated.add(edgeId);
      if (!seenF.has(target)) {
        seenF.add(target);
        stackF.push(target);
      }
    }
  }

  return animated;
}
