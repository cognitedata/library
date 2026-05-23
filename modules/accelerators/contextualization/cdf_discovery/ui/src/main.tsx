import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./App";
import { AppSettingsProvider } from "./context/AppSettingsContext";
import { DiscoveryConfigProvider } from "./context/DiscoveryConfigContext";
import "@xyflow/react/dist/style.css";
import "./discovery-styles.css";
import "./governance-editor.css";
import "./discovery-layout.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AppSettingsProvider>
      <DiscoveryConfigProvider>
        <App />
      </DiscoveryConfigProvider>
    </AppSettingsProvider>
  </React.StrictMode>
);
