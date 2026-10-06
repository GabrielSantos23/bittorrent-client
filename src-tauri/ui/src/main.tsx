import React from "react";
import ReactDOM from "react-dom/client";
import { installTauriDevStub } from "./dev-tauri-stub";

// Fakes the Tauri IPC when the UI runs in a plain browser; no-op in the app.
installTauriDevStub();

import App from "./App";
import "@fontsource-variable/inter";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
