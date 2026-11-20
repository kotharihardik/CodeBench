import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// On macOS, port 5000 is taken by the AirPlay Receiver (ControlCenter). Run the backend with
// PORT=5050 and start Vite with VITE_PROXY_TARGET=http://localhost:5050 in that case.
const proxyTarget = process.env.VITE_PROXY_TARGET || "http://localhost:5000";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: proxyTarget,
        changeOrigin: true
      },
      "/health": {
        target: proxyTarget,
        changeOrigin: true
      }
    }
  }
});
