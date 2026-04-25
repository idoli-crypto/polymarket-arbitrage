import React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";

import { DashboardApp, parseRoute } from "./App";

function createApi(overrides = {}) {
  return {
    fetchOverview: vi.fn().mockResolvedValue({
      recommendationStatus: {
        freshness_status: "stale",
        stale_reasons: ["recommendation_scoring_not_run"],
        latest_scoring_run_timestamp: "2026-04-25T12:00:00Z",
        high_conviction_last_run: 2,
        review_last_run: 1,
        blocked_last_run: 1,
        latest_validation_time: "2026-04-25T11:45:00Z",
        latest_kpi_time: "2026-04-25T11:40:00Z",
      },
      topRecommendations: [
        {
          opportunity_id: 11,
          event_id: "event-overview-1",
          tier: "high_conviction",
          score: "93.0000",
          reason_summary: "clear edge with strong validation",
          fill_completion_ratio: "0.8100",
          freshness_status: "stale",
          stale_reasons: ["recommendation_scoring_not_run"],
          confidence_tier: "high",
          executable_edge: { fee_adjusted_edge: "0.2200" },
        },
      ],
      topRecommendationsPage: {
        totalCount: 4,
        limit: 5,
        offset: 0,
      },
      kpi: {
        avg_real_edge: "0.1200",
        avg_fill_ratio: "0.7200",
        false_positive_rate: "0.0800",
        total_opportunities: 12,
        valid_opportunities: 5,
      },
      kpiRun: null,
      systemStatus: {
        last_detection_time: "2026-04-25T11:50:00Z",
        last_simulation_time: "2026-04-25T11:55:00Z",
      },
    }),
    fetchQueue: vi.fn().mockResolvedValue({
      rows: [
        {
          opportunity_id: 101,
          ranking_position: 1,
          event_id: "event-alpha",
          detected_at: "2026-04-25T11:30:00Z",
          family: "neg_risk_conversion",
          tier: "high_conviction",
          score: "91.0000",
          validation_status: "valid",
          simulation_status: "executable",
          executable_edge: { fee_adjusted_edge: "0.1800" },
          fill_completion_ratio: "0.8500",
          capital_lock_estimate: "0.0200",
          confidence_tier: "high",
          warning_summary: null,
          recommendation_block_reason: null,
          reason_summary: "all critical validations passed",
          freshness_status: "fresh",
          stale_reasons: [],
          manual_review_required: false,
        },
        {
          opportunity_id: 102,
          ranking_position: 2,
          event_id: "event-beta",
          detected_at: "2026-04-25T11:10:00Z",
          family: "cross_market_logic",
          tier: "review",
          score: "68.0000",
          validation_status: "risky",
          simulation_status: "review",
          executable_edge: { fee_adjusted_edge: "0.0900" },
          fill_completion_ratio: "0.5500",
          capital_lock_estimate: "0.0300",
          confidence_tier: "medium",
          warning_summary: "weak persistence",
          recommendation_block_reason: "weak_persistence",
          reason_summary: "manual review required",
          freshness_status: "stale",
          stale_reasons: ["validation_feed_lag"],
          manual_review_required: true,
        },
      ],
      page: {
        totalCount: 2,
        limit: 25,
        offset: 0,
      },
      recommendationStatus: {
        freshness_status: "stale",
        stale_reasons: ["recommendation_scoring_not_run"],
        latest_scoring_run_timestamp: "2026-04-25T12:00:00Z",
      },
    }),
    fetchRecommendationDetail: vi.fn().mockResolvedValue({
      recommendationStatus: {
        freshness_status: "stale",
        stale_reasons: ["validation_feed_lag"],
      },
      detail: {
        summary: {
          opportunity_id: 101,
          event_id: "event-alpha",
          detected_at: "2026-04-25T11:30:00Z",
          family: "neg_risk_conversion",
          confidence_tier: "high",
          recommendation_eligibility: true,
          recommendation_block_reason: null,
          tier: "high_conviction",
          score: "91.0000",
          reason_summary: "all critical validations passed",
          warning_summary: "monitor freshness lag",
          freshness_status: "stale",
          stale_reasons: ["validation_feed_lag"],
        },
        validation_evidence: {
          rule_validation: {
            status: "valid",
            summary: "rule check passed",
            score: "1.0000",
            validator_version: "rule_v1",
            details_json: { checks: [] },
          },
          semantic_validation: {
            status: "valid",
            summary: "semantic check passed",
            score: "1.0000",
            validator_version: "semantic_v1",
            details_json: { checks: [] },
          },
          resolution_validation: {
            status: "risky",
            summary: "resolution review required",
            score: "0.4000",
            validator_version: "resolution_v1",
            details_json: { checks: [] },
          },
        },
        executable_edge: {
          top_of_book_edge: "0.2800",
          depth_weighted_edge: "0.2400",
          fee_adjusted_edge: "0.1800",
          min_executable_size: "100.0000",
          fill_completion_ratio: "0.8500",
          capital_lock_estimate_hours: "0.0200",
        },
        latest_execution_simulation: {
          simulation_status: "executable",
          real_edge: "23.5000",
          executable_size: "100.0000",
          simulation_reason: "executable",
        },
        simulation_results: [{ id: 1 }],
        kpi_snapshot: {
          final_status: "accepted",
          validation_stage_reached: "simulation",
          decay_status: "current",
          fill_completion_ratio: "0.8500",
          capital_lock_estimate_hours: "0.0200",
        },
        audit: {
          scores: [{ id: 1, scoring_version: "score_v1", created_at: "2026-04-25T11:40:00Z" }],
          involved_markets: [{ id: 1 }, { id: 2 }],
          raw_context: { a: 1 },
        },
      },
    }),
    fetchKpiAnalytics: vi.fn().mockResolvedValue({
      recommendationStatus: {
        freshness_status: "fresh",
        stale_reasons: [],
      },
      kpi: {
        avg_real_edge: "0.1200",
        avg_fill_ratio: "0.7200",
        false_positive_rate: "0.0800",
      },
      kpiRun: {
        valid_after_rule: 10,
        valid_after_semantic: 8,
        valid_after_resolution: 6,
        valid_after_executable: 4,
        valid_after_simulation: 3,
        avg_executable_edge: "0.1200",
        avg_fill_ratio: "0.7200",
        avg_capital_lock: "0.0400",
        family_distribution: { neg_risk_conversion: 2 },
      },
    }),
    fetchSystemStatus: vi.fn().mockResolvedValue({
      recommendationStatus: {
        freshness_status: "missing",
        stale_reasons: ["recommendation_scoring_not_run"],
        scoring_worker_status: "missing",
        latest_scoring_run_timestamp: null,
        latest_validation_time: null,
        latest_kpi_time: null,
      },
      systemStatus: {
        last_snapshot_time: null,
        last_detection_time: null,
        last_simulation_time: null,
      },
      kpiRun: null,
    }),
    ...overrides,
  };
}

describe("dashboard routing and decision support UX", () => {
  beforeEach(() => {
    window.location.hash = "#/overview";
  });

  afterEach(() => {
    cleanup();
  });

  it("renders overview with immediate freshness and warning context", async () => {
    const api = createApi();

    render(<DashboardApp api={api} />);

    expect(await screen.findByRole("heading", { name: "Overview" })).toBeInTheDocument();
    expect(screen.getByText(/freshness warning/i)).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Top recommendation" })).toBeInTheDocument();
    expect(screen.getByText(/warnings and review pressure/i)).toBeInTheDocument();
    expect(screen.getAllByText(/recommendation scoring not run/i).length).toBeGreaterThan(0);
  });

  it("renders queue as a tiered decision surface with visible warnings", async () => {
    const api = createApi();
    window.location.hash = "#/recommendations";

    render(<DashboardApp api={api} />);

    expect(await screen.findByRole("heading", { name: "Recommendation Queue" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "High conviction recommendations" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Review recommendations" })).toBeInTheDocument();
    expect(screen.getByText("event-alpha")).toBeInTheDocument();
    expect(screen.getByText("event-beta")).toBeInTheDocument();
    expect(screen.getAllByText(/weak persistence/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/freshness stale/i).length).toBeGreaterThan(0);
    expect(screen.getAllByRole("link", { name: "Open detail" }).length).toBeGreaterThan(0);
  });

  it("updates queue filters and sort controls through the API", async () => {
    const api = createApi();
    window.location.hash = "#/recommendations";

    render(<DashboardApp api={api} />);
    await screen.findByText("event-alpha");

    fireEvent.change(screen.getByLabelText("Tier filter"), {
      target: { value: "review" },
    });
    fireEvent.change(screen.getByLabelText("Sort recommendations"), {
      target: { value: "edge" },
    });

    await waitFor(() => {
      expect(api.fetchQueue).toHaveBeenLastCalledWith(
        expect.objectContaining({ tier: "review", sort: "edge" }),
      );
    });
  });

  it("clicks through from queue to a structured decision detail screen", async () => {
    const api = createApi();
    window.location.hash = "#/recommendations";

    render(<DashboardApp api={api} />);
    fireEvent.click(await screen.findByRole("link", { name: "event-alpha" }));

    await waitFor(() => {
      expect(window.location.hash).toBe("#/recommendations/101");
    });

    expect(await screen.findByRole("heading", { name: "Recommendation Detail" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Decision summary" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Trust and warning summary" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Primary evidence" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Logic validation" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Semantic validation" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Resolution validation" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Executable edge" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Simulation" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Supporting data" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "KPI" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Audit" })).toBeInTheDocument();
    expect(screen.getByText(/monitor freshness lag/i)).toBeInTheDocument();
  });

  it("shows a clear empty state when the queue has no recommendations", async () => {
    const api = createApi({
      fetchQueue: vi.fn().mockResolvedValue({
        rows: [],
        page: {
          totalCount: 0,
          limit: 25,
          offset: 0,
        },
        recommendationStatus: {
          freshness_status: "fresh",
          stale_reasons: [],
          latest_scoring_run_timestamp: "2026-04-25T12:00:00Z",
        },
      }),
    });
    window.location.hash = "#/recommendations";

    render(<DashboardApp api={api} />);

    expect(await screen.findByText("No recommendations match the current filters.")).toBeInTheDocument();
  });

  it("shows a visible API failure state on the queue screen", async () => {
    const api = createApi({
      fetchQueue: vi.fn().mockRejectedValue(new Error("boom")),
    });
    window.location.hash = "#/recommendations";

    render(<DashboardApp api={api} />);

    expect(await screen.findByText("API load error: boom")).toBeInTheDocument();
  });

  it("shows a visible loading state while queue data is pending", () => {
    const api = createApi({
      fetchQueue: vi.fn(() => new Promise(() => {})),
    });
    window.location.hash = "#/recommendations";

    render(<DashboardApp api={api} />);

    expect(screen.getByText("Loading recommendation queue…")).toBeInTheDocument();
  });

  it("shows a visible loading state while recommendation detail is pending", async () => {
    const api = createApi({
      fetchRecommendationDetail: vi.fn(() => new Promise(() => {})),
    });
    window.location.hash = "#/recommendations/101";

    render(<DashboardApp api={api} />);

    expect(await screen.findByText("Loading recommendation detail…")).toBeInTheDocument();
    expect(screen.getByText(/pulling validation evidence, execution context, and audit records/i)).toBeInTheDocument();
  });

  it("shows stale freshness warning on system status without hiding the screen", async () => {
    const api = createApi();
    window.location.hash = "#/system-status";

    render(<DashboardApp api={api} />);

    expect(await screen.findByRole("heading", { name: "System Status" })).toBeInTheDocument();
    expect(screen.getAllByText(/freshness warning/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/recommendation scoring not run/i)).toHaveLength(2);
    expect(screen.getByText(/scoring worker missing/i)).toBeInTheDocument();
  });

  it("keeps trust labels and queue controls consistent", async () => {
    const api = createApi();
    window.location.hash = "#/recommendations";

    render(<DashboardApp api={api} />);

    expect(await screen.findByRole("heading", { name: "Recommendation Queue" })).toBeInTheDocument();
    expect(screen.getByText("Rows per page")).toBeInTheDocument();
    expect(screen.getByText("Freshness")).toBeInTheDocument();
    expect(screen.getByText("Recommendation freshness")).toBeInTheDocument();
  });

  it("shows analytics placeholder messaging when KPI data is unavailable", async () => {
    const api = createApi({
      fetchKpiAnalytics: vi.fn().mockResolvedValue({
        recommendationStatus: {
          freshness_status: "fresh",
          stale_reasons: [],
          latest_kpi_time: null,
        },
        kpi: null,
        kpiRun: null,
      }),
    });
    window.location.hash = "#/analytics";

    render(<DashboardApp api={api} />);

    expect(await screen.findByRole("heading", { name: "KPI Analytics" })).toBeInTheDocument();
    expect(screen.getByText(/validation funnel data will appear after a kpi run completes/i)).toBeInTheDocument();
    expect(screen.getByText(/family distribution will appear when the latest kpi run publishes family-level slices/i)).toBeInTheDocument();
  });

  it("does not expose trade or wallet buttons in the main dashboard surfaces", async () => {
    const api = createApi();
    window.location.hash = "#/recommendations";

    render(<DashboardApp api={api} />);
    await screen.findByText("event-alpha");

    expect(screen.queryByRole("button", { name: /trade/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /buy/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /sell/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /wallet/i })).not.toBeInTheDocument();
  });

  it("parses configured routes", () => {
    expect(parseRoute("#/overview")).toEqual({ name: "overview", path: "/overview" });
    expect(parseRoute("#/recommendations/55")).toEqual({
      name: "detail",
      path: "/recommendations",
      opportunityId: "55",
    });
  });
});
