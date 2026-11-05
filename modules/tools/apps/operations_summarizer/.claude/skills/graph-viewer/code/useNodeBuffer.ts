import { useEffect, useMemo, useState } from "react";
import { type CDFEdge, type CDFNode, createInstanceId } from "./types";

type BufferedNode = {
  node: CDFNode;
  lastAccessed: number;
};

type BufferState = {
  nodes: Map<string, BufferedNode>;
  connections: CDFEdge[];
};

function pruneConnections(connections: CDFEdge[], nodes: Map<string, BufferedNode>) {
  const validNodeIds = new Set(Array.from(nodes.keys()));
  return connections.filter((connection) => {
    const startId = createInstanceId(connection.startNode.space, connection.startNode.externalId);
    const endId = createInstanceId(connection.endNode.space, connection.endNode.externalId);
    return validNodeIds.has(startId) && validNodeIds.has(endId);
  });
}

function evictIfNeeded(state: BufferState, maxSize: number): BufferState {
  if (state.nodes.size <= maxSize) {
    return {
      nodes: state.nodes,
      connections: pruneConnections(state.connections, state.nodes),
    };
  }

  const entries = Array.from(state.nodes.entries());
  entries.sort((a, b) => a[1].lastAccessed - b[1].lastAccessed);

  const toRemove = entries.length - maxSize;
  for (let i = 0; i < toRemove; i++) {
    state.nodes.delete(entries[i][0]);
  }

  return {
    nodes: state.nodes,
    connections: pruneConnections(state.connections, state.nodes),
  };
}

export function useNodeBuffer(initialMaxSize = 1000) {
  const [maxSize, setMaxSize] = useState(initialMaxSize);
  const [state, setState] = useState<BufferState>({
    nodes: new Map<string, BufferedNode>(),
    connections: [],
  });

  useEffect(() => {
    setState((prev) => evictIfNeeded(prev, maxSize));
  }, [maxSize]);

  const addNodes = (nodes: CDFNode[]) => {
    const now = Date.now();
    setState((prev) => {
      const nextNodes = new Map(prev.nodes);
      nodes.forEach((node) => {
        const key = createInstanceId(node.space, node.externalId);
        nextNodes.set(key, { node, lastAccessed: now });
      });
      return evictIfNeeded({ nodes: nextNodes, connections: prev.connections }, maxSize);
    });
  };

  const addEdges = (connections: CDFEdge[]) => {
    setState((prev) => {
      const existingIds = new Set(
        prev.connections.map((c) => createInstanceId(c.space, c.externalId))
      );
      const merged = [...prev.connections];
      connections.forEach((connection) => {
        const id = createInstanceId(connection.space, connection.externalId);
        if (!existingIds.has(id)) {
          merged.push(connection);
        }
      });
      return {
        nodes: prev.nodes,
        connections: pruneConnections(merged, prev.nodes),
      };
    });
  };

  const touchNode = (nodeId: string) => {
    setState((prev) => {
      const nextNodes = new Map(prev.nodes);
      const buffered = nextNodes.get(nodeId);
      if (buffered) {
        nextNodes.set(nodeId, { ...buffered, lastAccessed: Date.now() });
      }
      return { nodes: nextNodes, connections: prev.connections };
    });
  };

  const clear = () => {
    setState({
      nodes: new Map(),
      connections: [],
    });
  };

  const setBuffer = (nodes: CDFNode[], connections: CDFEdge[]) => {
    const now = Date.now();
    const nodesMap = new Map<string, BufferedNode>();
    nodes.forEach((node) => {
      const key = createInstanceId(node.space, node.externalId);
      nodesMap.set(key, { node, lastAccessed: now });
    });
    const pruned = pruneConnections(connections, nodesMap);
    setState(evictIfNeeded({ nodes: nodesMap, connections: pruned }, maxSize));
  };

  const bufferedNodes = useMemo(
    () => Array.from(state.nodes.values()).map((entry) => entry.node),
    [state.nodes]
  );

  return {
    nodes: bufferedNodes,
    edges: state.connections,
    addNodes,
    addEdges,
    touchNode,
    clear,
    setBuffer,
    maxSize,
    setMaxSize,
  };
}
