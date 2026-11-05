import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { CdfClientProvider } from "@/contexts/CdfClientContext";
import { NavigationProvider } from "@/contexts/NavigationContext";
import { OperationsProvider } from "@/contexts/OperationsSummarizerContext";

export function createTestQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });
}

/** Query + CdfClientProvider (mock `useCogniteSdk` via vi.mock). */
export function createQueryWithCdfWrapper() {
  const queryClient = createTestQueryClient();
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <CdfClientProvider>{children}</CdfClientProvider>
      </QueryClientProvider>
    );
  }
  return { queryClient, Wrapper };
}

export function createOperationsWithQueryWrapper() {
  const queryClient = createTestQueryClient();
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <CdfClientProvider>
          <OperationsProvider>{children}</OperationsProvider>
        </CdfClientProvider>
      </QueryClientProvider>
    );
  }
  return { queryClient, Wrapper };
}

/** Navigation + Query + CDF + Operations (mock `useCogniteSdk` in test file). */
export function createFullAppWrapper() {
  const queryClient = createTestQueryClient();
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <CdfClientProvider>
          <NavigationProvider>
            <OperationsProvider>{children}</OperationsProvider>
          </NavigationProvider>
        </CdfClientProvider>
      </QueryClientProvider>
    );
  }
  return { queryClient, Wrapper };
}
