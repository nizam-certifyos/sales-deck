import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  use: {
    baseURL: "http://127.0.0.1:8000",
    headless: true,
  },
  webServer: {
    command: "python -m universal_roster_v2.web.server",
    url: "http://127.0.0.1:8000/health",
    reuseExistingServer: true,
  },
});
