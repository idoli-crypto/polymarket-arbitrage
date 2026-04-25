const API_BASE_URL = import.meta.env?.VITE_API_BASE_URL || "/api";

async function request(path, { allowNotFound = false } = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`);
  if (allowNotFound && response.status === 404) {
    return { data: null, headers: response.headers };
  }
  if (!response.ok) {
    throw new Error(`Request failed for ${path}: ${response.status}`);
  }

  return {
    data: await response.json(),
    headers: response.headers,
  };
}

function parsePagination(headers) {
  return {
    totalCount: Number(headers.get("X-Total-Count") || 0),
    limit: Number(headers.get("X-Limit") || 0),
    offset: Number(headers.get("X-Offset") || 0),
  };
}

export const dashboardApi = {
  async fetchOverview() {
    const [recommendationStatus, queueResponse, kpiResponse, kpiRunResponse, systemResponse] =
      await Promise.all([
        request("/recommendations/status"),
        request("/recommendations?limit=5&sort=score"),
        request("/kpi/latest", { allowNotFound: true }),
        request("/kpi/runs/latest", { allowNotFound: true }),
        request("/system/status"),
      ]);

    return {
      recommendationStatus: recommendationStatus.data,
      topRecommendations: queueResponse.data,
      topRecommendationsPage: parsePagination(queueResponse.headers),
      kpi: kpiResponse.data,
      kpiRun: kpiRunResponse.data,
      systemStatus: systemResponse.data,
    };
  },

  async fetchQueue({ tier = "", family = "", sort = "score", limit = 25, offset = 0 } = {}) {
    const params = new URLSearchParams();
    if (tier) {
      params.set("tier", tier);
    }
    if (family) {
      params.set("family", family);
    }
    params.set("sort", sort);
    params.set("limit", String(limit));
    params.set("offset", String(offset));

    const [queueResponse, recommendationStatus] = await Promise.all([
      request(`/recommendations?${params.toString()}`),
      request("/recommendations/status"),
    ]);

    return {
      rows: queueResponse.data,
      page: parsePagination(queueResponse.headers),
      recommendationStatus: recommendationStatus.data,
    };
  },

  async fetchRecommendationDetail(opportunityId) {
    const [detailResponse, recommendationStatus] = await Promise.all([
      request(`/recommendations/${opportunityId}`),
      request("/recommendations/status"),
    ]);

    return {
      detail: detailResponse.data,
      recommendationStatus: recommendationStatus.data,
    };
  },

  async fetchKpiAnalytics() {
    const [kpiResponse, kpiRunResponse, recommendationStatus] = await Promise.all([
      request("/kpi/latest", { allowNotFound: true }),
      request("/kpi/runs/latest", { allowNotFound: true }),
      request("/recommendations/status"),
    ]);

    return {
      kpi: kpiResponse.data,
      kpiRun: kpiRunResponse.data,
      recommendationStatus: recommendationStatus.data,
    };
  },

  async fetchSystemStatus() {
    const [recommendationStatus, systemResponse, kpiRunResponse] = await Promise.all([
      request("/recommendations/status"),
      request("/system/status"),
      request("/kpi/runs/latest", { allowNotFound: true }),
    ]);

    return {
      recommendationStatus: recommendationStatus.data,
      systemStatus: systemResponse.data,
      kpiRun: kpiRunResponse.data,
    };
  },
};
