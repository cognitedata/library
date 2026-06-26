import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./App";
import { AppSettingsProvider } from "./context/AppSettingsContext";
import { IndexWorkspaceProvider } from "./context/IndexWorkspaceContext";
import "./idx-styles.css";
import "./idx-layout.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AppSettingsProvider>
      <IndexWorkspaceProvider>
        <App />
      </IndexWorkspaceProvider>
    </AppSettingsProvider>
  </React.StrictMode>
);
