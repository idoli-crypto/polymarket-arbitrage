import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { DashboardView } from "../src/App.jsx";

const apiBaseUrl = process.env.VERIFICATION_API_BASE_URL || "http://127.0.0.1:8000";

async function request(path, { allowNotFound = false } = {}) {
  const response = await fetch(`${apiBaseUrl}${path}`);
  if (allowNotFound && response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`Request failed for ${path}: ${response.status}`);
  }
  return response.json();
}

async function main() {
  const [recommendationStatus, recommendations, kpi, kpiRun, systemStatus] = await Promise.all([
    request("/recommendations/status"),
    request("/recommendations?limit=5&sort=score"),
    request("/kpi/latest", { allowNotFound: true }),
    request("/kpi/runs/latest", { allowNotFound: true }),
    request("/system/status"),
  ]);

  const html = renderToStaticMarkup(
    React.createElement(DashboardView, {
      routeName: "overview",
      overviewData: {
        recommendationStatus,
        topRecommendations: recommendations,
        kpi,
        kpiRun,
        systemStatus,
      },
      loading: false,
      error: null,
    }),
  );

  console.log(html);
}

main().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exitCode = 1;
});
