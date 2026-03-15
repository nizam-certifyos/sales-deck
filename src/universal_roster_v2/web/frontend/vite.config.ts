import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../static/dist",
    emptyOutDir: false,
    manifest: true,
  },
  server: {
    port: 5173,
    strictPort: true,
  },
});
