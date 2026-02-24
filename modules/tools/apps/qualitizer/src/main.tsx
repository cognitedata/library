import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import mixpanel from "mixpanel-browser";
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App.tsx";
import { AppAuthProvider } from "./shared/auth";

import "./styles.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000, // 5 minutes
      gcTime: 10 * 60 * 1000, // 10 minutes
    },
  },
});

/** Bump manually when major changes happen; included in Mixpanel tracking. */
const APP_VERSION = 1;

mixpanel.init("8f28374a6614237dd49877a0d27daa78", {
  autocapture: true,
  record_sessions_percent: 100,
  api_host: "https://api-eu.mixpanel.com",
});
mixpanel.register({ version: APP_VERSION });

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <AppAuthProvider>
        <App />
      </AppAuthProvider>
    </QueryClientProvider>
  </React.StrictMode>
);

