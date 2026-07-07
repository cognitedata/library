import { useCallback, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";

type QueryPrefixesByScope<TScope extends string> = Record<TScope, string[]>;

function isQueryPrefixMatch(queryKey: readonly unknown[], prefixes: string[]) {
  const firstKey = queryKey[0];
  return typeof firstKey === "string" && prefixes.includes(firstKey);
}

function createInitialResetCounts<TScope extends string>(
  prefixesByScope: QueryPrefixesByScope<TScope>
): Record<TScope, number> {
  const entries = (Object.keys(prefixesByScope) as TScope[]).map((scope) => [scope, 0] as const);
  return Object.fromEntries(entries) as Record<TScope, number>;
}

export function useScopedCacheReset<TScope extends string>(
  prefixesByScope: QueryPrefixesByScope<TScope>
) {
  const queryClient = useQueryClient();
  const [isResetting, setIsResetting] = useState(false);
  const [resetCounts, setResetCounts] = useState<Record<TScope, number>>(
    createInitialResetCounts(prefixesByScope)
  );

  const resetScope = useCallback(
    async (scope: TScope) => {
      const prefixes = prefixesByScope[scope] || [];
      setIsResetting(true);
      try {
        await queryClient.cancelQueries({
          predicate: (query) => isQueryPrefixMatch(query.queryKey, prefixes),
        });
        queryClient.removeQueries({
          predicate: (query) => isQueryPrefixMatch(query.queryKey, prefixes),
        });
        setResetCounts((prev) => ({
          ...prev,
          [scope]: (prev[scope] ?? 0) + 1,
        }));
      } finally {
        setIsResetting(false);
      }
    },
    [prefixesByScope, queryClient]
  );

  return {
    isResetting,
    resetCounts,
    resetScope,
  };
}
