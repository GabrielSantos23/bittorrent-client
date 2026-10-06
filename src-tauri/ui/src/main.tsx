import React from "react";
import ReactDOM from "react-dom/client";
import { installTauriDevStub } from "./dev-tauri-stub";

// Fakes the Tauri IPC when the UI runs in a plain browser; no-op in the app.
installTauriDevStub();

// Suppress the WebView's default right-click menu in the installed app; the
// torrent table provides its own context menu. The plain-browser dev preview
// keeps the native menu so Inspect stays reachable.
if (import.meta.env.PROD) {
  window.addEventListener("contextmenu", (event) => event.preventDefault());
}

import App from "./App";
import "@fontsource-variable/inter";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
