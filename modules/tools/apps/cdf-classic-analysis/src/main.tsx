import React, { Component, Suspense } from "react";
import ReactDOM from "react-dom/client";
import { AuthBridge } from "./auth";
import App from "./App";
import "./index.css";

const rootEl = document.getElementById("root");
if (!rootEl) throw new Error("Missing #root element");

/** Catches render errors and shows them on screen instead of a blank page */
class ErrorBoundary extends Component<
  { children: React.ReactNode },
  { error: Error | null; componentStack: string | null }
> {
  state = { error: null as Error | null, componentStack: null as string | null };

  static getDerivedStateFromError(error: Error) {
    return { error, componentStack: null };
  }

  componentDidCatch(_error: Error, { componentStack }: React.ErrorInfo) {
    this.setState((s) => ({ ...s, componentStack: componentStack ?? null }));
  }

  render() {
    if (this.state.error) {
      const err = this.state.error;
      return (
        <div
          style={{
            padding: "1.5rem",
            background: "#1e293b",
            color: "#f87171",
            borderRadius: 8,
            marginTop: "1rem",
            fontFamily: "system-ui, sans-serif",
            fontSize: "0.875rem",
          }}
        >
          <strong>App error</strong>
          <pre style={{ marginTop: "0.5rem", whiteSpace: "pre-wrap", overflow: "auto" }}>
            {err.message}
          </pre>
          {this.state.componentStack && (
            <pre style={{ marginTop: "0.5rem", whiteSpace: "pre-wrap", overflow: "auto", fontSize: "0.75rem", color: "#94a3b8" }}>
              {this.state.componentStack}
            </pre>
          )}
        </div>
      );
    }
    return this.props.children;
  }
}

function LoadingFallback() {
  return (
    <div style={{ padding: "1.5rem", color: "#94a3b8" }}>
      Loading…
    </div>
  );
}

try {
  ReactDOM.createRoot(rootEl).render(
    <React.StrictMode>
      <ErrorBoundary>
        <Suspense fallback={<LoadingFallback />}>
          <AuthBridge>
            <App />
          </AuthBridge>
        </Suspense>
      </ErrorBoundary>
    </React.StrictMode>
  );
} catch (err) {
  rootEl.innerHTML = `
    <div style="padding:1.5rem;background:#1e293b;color:#f87171;border-radius:8px;font-family:system-ui,sans-serif;">
      <strong>Failed to start app</strong>
      <pre style="margin-top:0.5rem;white-space:pre-wrap;">${(err as Error).message}</pre>
    </div>
  `;
}
