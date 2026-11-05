import { useDune } from "@cognite/dune";
import { useCallback, useEffect, useRef, useState } from "react";
import { fetchConnectedNodes, fetchNodeDetails } from "./graph-service";
import type {
  CDFEdge,
  CDFNode,
  DataModelInfo,
  ReverseRelationQuery,
  ViewPriorityConfig,
} from "./types";
import { createInstanceId } from "./types";

interface UseSeedNodeConfig {
  dataModel: DataModelInfo | null;
  initialInstance?: { space: string; externalId: string };
  addNodes: (nodes: CDFNode[]) => void;
  addEdges: (edges: CDFEdge[]) => void;
  clearBuffer: () => void;
  whitelistedRelationProps?: Set<string>;
  coreReverseQueries?: ReverseRelationQuery[];
  viewPriorityConfig?: ViewPriorityConfig;
  initialConnectionLimit: number;
}

interface UseSeedNodeReturn {
  isLoading: boolean;
  error: string | null;
  loadInstance: (space: string, externalId: string) => Promise<void>;
}

export function useSeedNode({
  dataModel,
  initialInstance,
  addNodes,
  addEdges,
  clearBuffer,
  whitelistedRelationProps,
  coreReverseQueries,
  viewPriorityConfig,
  initialConnectionLimit,
}: UseSeedNodeConfig): UseSeedNodeReturn {
  const { sdk } = useDune();
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const loadedRef = useRef(false);

  const loadInstance = useCallback(
    async (space: string, externalId: string) => {
      if (!sdk || !dataModel) {
        setError("SDK or data model not ready");
        return;
      }

      try {
        setIsLoading(true);
        setError(null);
        clearBuffer();

        const node = await fetchNodeDetails(sdk, space, externalId);
        if (!node) {
          throw new Error(`Node not found: ${space}/${externalId}`);
        }

        addNodes([node]);

        const seedId = createInstanceId(space, externalId);
        const existingIds = new Set([seedId]);

        const result = await fetchConnectedNodes(
          sdk,
          space,
          externalId,
          existingIds,
          dataModel,
          initialConnectionLimit,
          whitelistedRelationProps,
          coreReverseQueries,
          viewPriorityConfig
        );

        addNodes(result.newNodes);
        addEdges(result.newEdges);
      } catch (err) {
        setError(
          err instanceof Error
            ? err.message
            : "Failed to load instance"
        );
      } finally {
        setIsLoading(false);
      }
    },
    [
      sdk,
      dataModel,
      addNodes,
      addEdges,
      clearBuffer,
      whitelistedRelationProps,
      coreReverseQueries,
      viewPriorityConfig,
      initialConnectionLimit,
    ]
  );

  useEffect(() => {
    if (loadedRef.current || !dataModel || !initialInstance || !sdk) return;
    loadedRef.current = true;
    loadInstance(initialInstance.space, initialInstance.externalId);
  }, [dataModel, initialInstance, sdk, loadInstance]);

  return { isLoading, error, loadInstance };
}
