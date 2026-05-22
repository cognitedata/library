import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./App";
import { AppSettingsProvider } from "./context/AppSettingsContext";
import { ExplorerConfigProvider } from "./context/ExplorerConfigContext";
import "@xyflow/react/dist/style.css";
import "./explorer-styles.css";
import "./explorer-layout.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AppSettingsProvider>
      <ExplorerConfigProvider>
        <App />
      </ExplorerConfigProvider>
    </AppSettingsProvider>
  </React.StrictMode>
);
