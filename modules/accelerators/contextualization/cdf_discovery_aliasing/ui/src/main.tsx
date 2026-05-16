import React from "react";
import ReactDOM from "react-dom/client";
import { AppSettingsProvider } from "./context/AppSettingsContext";
import App from "./App";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AppSettingsProvider>
      <App />
    </AppSettingsProvider>
  </React.StrictMode>
);
