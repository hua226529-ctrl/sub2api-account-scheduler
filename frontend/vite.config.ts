import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import { fileURLToPath, URL } from "node:url";

export default defineConfig({
  base: "./",
  plugins: [vue()],
  build: {
    outDir: fileURLToPath(new URL("../internal/webui/dist", import.meta.url)),
    emptyOutDir: true,
    sourcemap: false
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8323",
      "/healthz": "http://127.0.0.1:8323",
      "/readyz": "http://127.0.0.1:8323"
    }
  }
});
