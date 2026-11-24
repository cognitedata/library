import { Component, type ErrorInfo, type ReactNode } from 'react';

type AppErrorBoundaryProps = {
  children: ReactNode;
};

type AppErrorBoundaryState = {
  failed: boolean;
};

/**
 * Catches a render error anywhere below it, so the host iframe shows a message and a reload button
 * instead of a blank page. It sits outside the language provider, so its text is not translated.
 */
export class AppErrorBoundary extends Component<AppErrorBoundaryProps, AppErrorBoundaryState> {
  state: AppErrorBoundaryState = { failed: false };

  static getDerivedStateFromError(): AppErrorBoundaryState {
    return { failed: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('The dashboard stopped rendering', error, info.componentStack);
  }

  render() {
    if (!this.state.failed) {
      return this.props.children;
    }
    return (
      <div role="alert" className="flex h-screen flex-col items-center justify-center gap-3 p-6 text-center" data-testid="app-error-boundary">
        <p className="text-sm font-semibold text-foreground">Something went wrong and the dashboard stopped.</p>
        <button type="button" className="rounded-md border border-border px-3 py-1 text-sm" onClick={() => window.location.reload()}>
          Reload
        </button>
      </div>
    );
  }
}
