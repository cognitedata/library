import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./App";
import { AppSettingsProvider } from "./context/AppSettingsContext";
import "./index.css";
import "./gov-extensions.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AppSettingsProvider>
      <App />
    </AppSettingsProvider>
  </React.StrictMode>
);
