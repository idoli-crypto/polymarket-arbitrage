const API_BASE_URL = import.meta.env?.VITE_API_BASE_URL || "/api";

async function request(path, { allowNotFound = false } = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`);
  if (allowNotFound && response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`Request failed for ${path}: ${response.status}`);
  }
  return response.json();
}

export function fetchDashboardData() {
  return Promise.all([
    request("/opportunities"),
    request("/simulations"),
    request("/kpi/latest", { allowNotFound: true }),
    request("/system/status"),
  ]).then(([opportunities, simulations, kpi, systemStatus]) => ({
    opportunities,
    simulations,
    kpi,
    systemStatus,
  }));
}
