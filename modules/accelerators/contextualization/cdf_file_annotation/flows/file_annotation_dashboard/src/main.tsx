import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App.tsx";
import { isLocalCdfMode, isLocalMockMode } from "@/runtime/authMode";
import { FlowsSdkProvider, LocalCdfSdkProvider, LocalMockSdkProvider } from "@/providers/AppSdkProvider";

import "./styles.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000, // 5 minutes
      gcTime: 10 * 60 * 1000, // 10 minutes
    },
  },
});

const root = ReactDOM.createRoot(document.getElementById("root")!);

root.render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      {isLocalMockMode ? (
        <LocalMockSdkProvider>
          <App />
        </LocalMockSdkProvider>
      ) : isLocalCdfMode ? (
        <LocalCdfSdkProvider>
          <App />
        </LocalCdfSdkProvider>
      ) : (
        <FlowsSdkProvider>
          <App />
        </FlowsSdkProvider>
      )}
    </QueryClientProvider>
  </React.StrictMode>
);

