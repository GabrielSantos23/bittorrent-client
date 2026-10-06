import { fileURLToPath, URL } from "node:url";

import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import { viteSingleFile } from "vite-plugin-singlefile";

// Builds the phone page served by the desktop app's remote server
// (see src-tauri/src/remote.rs) as one self-contained HTML file, reusing the
// desktop UI's components and theme. `bun run build:remote` regenerates it.
export default defineConfig({
  plugins: [react(), tailwindcss(), viteSingleFile()],
  clearScreen: false,
  resolve: {
    alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) },
  },
  server: {
    port: 5184,
    strictPort: true,
    // Live-reload the page against a running desktop app's remote server.
    proxy: { "/api": "http://localhost:8420" },
  },
  build: {
    outDir: "dist-remote",
    target: "es2021",
    rollupOptions: {
      input: fileURLToPath(new URL("./remote.html", import.meta.url)),
    },
  },
});
