import React from "react";
import ReactDOM from "react-dom/client";

import "@fontsource-variable/inter";
import "../index.css";
import { RemoteApp } from "./RemoteApp";

// The phone page mirrors the desktop app: same dark theme tokens, same
// font, same components (see RemoteApp.tsx).
ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <RemoteApp />
  </React.StrictMode>,
);
