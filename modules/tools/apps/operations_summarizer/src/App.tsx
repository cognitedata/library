/**
 * Main Application Component
 * Provides navigation between Operations Summarizer pages.
 */

import { CogniteSdkProvider } from "@cognite/app-sdk/react";
import { Loader } from "@cognite/aura/components";
import { AlertTriangle } from "lucide-react";
import { lazy, Suspense } from "react";
import { ErrorBoundary, type FallbackProps } from "react-error-boundary";
import { Navigation } from "@/components/layout/Navigation";
import { Button } from "@/components/ui/button";
import {
  CdfClientProvider,
  NavigationProvider,
  OperationsProvider,
  useNavigation,
} from "@/contexts";
import { AppConfigProvider } from "@/contexts/AppConfigContext";
import type { NavigationPage } from "@/contexts/NavigationContext";

// Pages are lazy-loaded so they don't inflate the initial bundle.
// react-pdf, html2canvas, and other heavy deps only load when the page is visited.
const SummariesPage = lazy(() =>
  import("@/pages/SummariesPage").then((m) => ({ default: m.SummariesPage })),
);
const TestsPage = lazy(() =>
  import("@/pages/TestsPage").then((m) => ({ default: m.TestsPage })),
);

const PAGE_ROUTES: Record<NavigationPage, React.ComponentType> = {
  summaries: SummariesPage,
  tests: TestsPage,
};

function ErrorFallback({ resetErrorBoundary }: FallbackProps) {
  return (
    <div
      role="alert"
      className="flex flex-col items-center justify-center h-full p-8 text-center gap-4"
    >
      <AlertTriangle className="w-12 h-12 text-destructive" />
      <div>
        <p className="text-lg font-semibold text-foreground">
          Something went wrong
        </p>
        <p className="mt-2 text-sm text-muted-foreground max-w-lg">
          An unexpected error occurred. Please refresh the page and try again.
        </p>
        <Button type="button" className="mt-4" onClick={resetErrorBoundary}>
          Try again
        </Button>
      </div>
    </div>
  );
}

function AppContent() {
  const { currentPage } = useNavigation();
  const PageComponent = PAGE_ROUTES[currentPage];

  return (
    <div className="h-screen flex flex-col">
      <Navigation />
      <main className="flex-1 min-h-0 overflow-y-auto">
        <ErrorBoundary
          FallbackComponent={ErrorFallback}
          resetKeys={[currentPage]}
        >
          <Suspense
            fallback={
              <div
                role="status"
                className="flex items-center justify-center h-full"
                aria-live="polite"
                aria-label="Loading page"
              >
                <Loader size={20} className="text-muted-foreground" />
              </div>
            }
          >
            <PageComponent />
          </Suspense>
        </ErrorBoundary>
      </main>
    </div>
  );
}

function App() {
  return (
    <CogniteSdkProvider
      loadingFallback={
        <div
          role="status"
          className="flex h-screen items-center justify-center"
          aria-live="polite"
          aria-label="Loading"
        >
          <Loader size={20} className="text-muted-foreground" />
        </div>
      }
      errorFallback={
        <div
          role="alert"
          className="flex h-screen items-center justify-center p-8 text-center text-sm text-muted-foreground"
        >
          Failed to connect to Fusion
        </div>
      }
    >
      <CdfClientProvider>
        <AppConfigProvider>
          <NavigationProvider>
            <OperationsProvider>
              <AppContent />
            </OperationsProvider>
          </NavigationProvider>
        </AppConfigProvider>
      </CdfClientProvider>
    </CogniteSdkProvider>
  );
}

export default App;
