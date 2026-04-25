import React, { useEffect, useMemo, useState } from "react";

import { dashboardApi } from "./api";

const NAV_ITEMS = [
  { label: "Overview", path: "/overview" },
  { label: "Recommendation Queue", path: "/recommendations" },
  { label: "KPI Analytics", path: "/analytics" },
  { label: "System Status", path: "/system-status" },
];

const DEFAULT_QUEUE_STATE = {
  tier: "",
  family: "",
  sort: "score",
  limit: 25,
  offset: 0,
};

const QUEUE_TIERS = ["high_conviction", "review", "blocked", "unscored"];

export default function App() {
  return <DashboardApp api={dashboardApi} />;
}

export function DashboardApp({ api }) {
  const route = useHashRoute();

  return (
    <div className="app-shell">
      <DashboardLayout route={route}>
        {route.name === "overview" ? <OverviewPage api={api} /> : null}
        {route.name === "queue" ? <RecommendationQueuePage api={api} /> : null}
        {route.name === "detail" ? (
          <RecommendationDetailPage api={api} opportunityId={route.opportunityId} />
        ) : null}
        {route.name === "analytics" ? <KpiAnalyticsPage api={api} /> : null}
        {route.name === "system" ? <SystemStatusPage api={api} /> : null}
        {route.name === "notFound" ? <NotFoundPage /> : null}
      </DashboardLayout>
    </div>
  );
}

export function DashboardView({
  routeName = "overview",
  overviewData = null,
  loading = false,
  error = null,
}) {
  const route = routeName === "overview" ? { name: "overview", path: "/overview" } : parseRoute("");

  return (
    <div className="app-shell">
      <DashboardLayout route={route}>
        {route.name === "overview" ? (
          <OverviewContent loading={loading} error={error} data={overviewData} />
        ) : (
          <NotFoundPage />
        )}
      </DashboardLayout>
    </div>
  );
}

function DashboardLayout({ route, children }) {
  return (
    <div className="layout">
      <aside className="sidebar" aria-label="Primary">
        <div className="brand-block">
          <p className="eyebrow">Decision Surface</p>
          <h1>Polymarket Research Dashboard</h1>
          <p className="sidebar-copy">
            Read-only recommendation review for arbitrage research. No trade execution, no wallet
            actions, and no hidden freshness risk.
          </p>
        </div>
        <nav className="nav-list">
          {NAV_ITEMS.map((item) => (
            <a
              key={item.path}
              href={`#${item.path}`}
              aria-current={item.path === route.path ? "page" : undefined}
              className={item.path === route.path ? "nav-link active" : "nav-link"}
            >
              {item.label}
            </a>
          ))}
        </nav>
      </aside>
      <main className="main-content">{children}</main>
    </div>
  );
}

function OverviewPage({ api }) {
  const state = useAsyncData(() => api.fetchOverview(), [api]);
  return <OverviewContent {...state} />;
}

function OverviewContent({ data, loading, error }) {
  const recommendationStatus = data?.recommendationStatus ?? null;
  const topRecommendations = data?.topRecommendations ?? [];
  const topRecommendationsPage = data?.topRecommendationsPage ?? null;
  const kpi = data?.kpi ?? null;
  const kpiRun = data?.kpiRun ?? null;
  const systemStatus = data?.systemStatus ?? null;
  const primaryRecommendation = topRecommendations[0] ?? null;

  const tierCounts = {
    high_conviction: recommendationStatus?.high_conviction_last_run ?? 0,
    review: recommendationStatus?.review_last_run ?? 0,
    blocked: recommendationStatus?.blocked_last_run ?? 0,
  };

  const attentionItems = buildOverviewAttentionItems({
    recommendationStatus,
    topRecommendations,
    totalCount: topRecommendationsPage?.totalCount ?? topRecommendations.length,
  });

  return (
    <PageFrame
      title="Overview"
      subtitle="Quick read on recommendation quality, freshness risk, and what deserves immediate analyst attention."
    >
      <FreshnessBanner status={recommendationStatus} />

      <div className="decision-strip">
        <MetricCard
          label="High conviction"
          value={String(tierCounts.high_conviction)}
          detail="Ready for first review"
          emphasis="strong"
        />
        <MetricCard
          label="Review"
          value={String(tierCounts.review)}
          detail="Needs caution or added evidence"
        />
        <MetricCard
          label="Blocked"
          value={String(tierCounts.blocked)}
          detail="Do not rely without resolution"
        />
        <MetricCard
          label="Freshness"
          value={readableFreshness(recommendationStatus?.freshness_status)}
          detail={formatDateTime(recommendationStatus?.latest_scoring_run_timestamp)}
          tone={recommendationStatus?.freshness_status === "fresh" ? "ok" : "warning"}
        />
      </div>

      <div className="two-column">
        <Panel
          title="Top recommendation"
          description="The first item a user should evaluate if a decision must be made quickly."
        >
          {loading ? (
            <LoadingState
              label="Loading top recommendation"
              detail="Checking the highest-ranked recommendation and its trust context."
            />
          ) : error ? (
            <ErrorState title="Could not load the top recommendation" message={error} />
          ) : primaryRecommendation ? (
            <RecommendationOverviewHero row={primaryRecommendation} />
          ) : (
            <EmptyState
              message="No recommendations available right now."
              detail="Wait for the next scoring run before relying on the overview."
            />
          )}
        </Panel>

        <Panel
          title="Attention summary"
          description="Warnings stay visible here so stale or risky data cannot be missed."
        >
          {loading ? (
            <LoadingState label="Loading overview attention summary" detail="Reading the current warning pressure and freshness risk." />
          ) : error ? (
            <ErrorState title="Could not load the overview warning summary" message={error} />
          ) : attentionItems.length ? (
            <WarningCallout title="Warnings and review pressure" items={attentionItems} />
          ) : (
            <EmptyState
              message="No active warnings surfaced in the overview."
              detail="Freshness and review pressure currently look stable."
            />
          )}
        </Panel>
      </div>

      <div className="two-column">
        <Panel
          title="Top recommendations"
          description="Overview first. Open detail only when the reason and warning summary justify it."
        >
          {loading ? (
            <LoadingState label="Loading top recommendations" detail="Pulling the latest ranked recommendations for quick review." />
          ) : error ? (
            <ErrorState title="Could not load top recommendations" message={error} />
          ) : topRecommendations.length ? (
            <RecommendationPreviewList rows={topRecommendations} />
          ) : (
            <EmptyState
              message="No ranked recommendations available yet."
              detail="This usually means scoring has not produced review-ready output yet."
            />
          )}
        </Panel>

        <Panel
          title="System summary"
          description="Freshness-adjacent evidence stays in view beside the recommendation snapshot."
        >
          {loading ? (
            <LoadingState label="Loading system summary" detail="Checking the timestamps that support recommendation trust." />
          ) : error ? (
            <ErrorState title="Could not load the system summary" message={error} />
          ) : (
            <DefinitionList
              rows={[
                ["Recommendations in queue", String(topRecommendationsPage?.totalCount ?? topRecommendations.length)],
                ["Latest scoring run", formatDateTime(recommendationStatus?.latest_scoring_run_timestamp)],
                ["Latest validation time", formatDateTime(recommendationStatus?.latest_validation_time)],
                ["Latest KPI time", formatDateTime(recommendationStatus?.latest_kpi_time)],
                ["Last detection time", formatDateTime(systemStatus?.last_detection_time)],
                ["Last simulation time", formatDateTime(systemStatus?.last_simulation_time)],
              ]}
            />
          )}
        </Panel>
      </div>

      <Panel
        title="Validation summary"
        description="KPI context only. No decorative charting, only evidence useful to the review process."
      >
        {loading ? (
          <LoadingState label="Loading KPI summary" detail="Checking the latest KPI snapshot and validation funnel counts." />
        ) : error ? (
          <ErrorState title="Could not load KPI summary" message={error} />
        ) : kpiRun ? (
          <DefinitionList
            rows={[
              ["After logic validation", String(kpiRun.valid_after_rule)],
              ["After semantic validation", String(kpiRun.valid_after_semantic)],
              ["After resolution validation", String(kpiRun.valid_after_resolution)],
              ["After executable edge validation", String(kpiRun.valid_after_executable)],
              ["After simulation validation", String(kpiRun.valid_after_simulation)],
            ]}
          />
        ) : kpi ? (
          <DefinitionList
            rows={[
              ["Valid opportunities", String(kpi.valid_opportunities)],
              ["Total opportunities", String(kpi.total_opportunities)],
              ["Average executable edge", formatEdge(kpi.avg_real_edge)],
              ["Average fill ratio", formatPercent(kpi.avg_fill_ratio)],
            ]}
          />
        ) : (
          <EmptyState
            message="KPI summary is not available yet."
            detail="Recommendation review still works, but KPI context has not been published."
          />
        )}
      </Panel>
    </PageFrame>
  );
}

function RecommendationQueuePage({ api }) {
  const [query, setQuery] = useState(DEFAULT_QUEUE_STATE);
  const state = useAsyncData(
    () => api.fetchQueue(query),
    [api, query.tier, query.family, query.sort, query.limit, query.offset],
  );

  const rows = state.data?.rows ?? [];
  const page = state.data?.page ?? { totalCount: 0, limit: query.limit, offset: query.offset };
  const recommendationStatus = state.data?.recommendationStatus ?? null;

  const familyOptions = useMemo(() => {
    const values = Array.from(new Set(rows.map((row) => row.family).filter(Boolean)));
    return values.sort();
  }, [rows]);

  const groupedRows = useMemo(() => groupRowsByTier(rows), [rows]);
  const queueCounts = useMemo(() => countBy(rows, (row) => row.tier || "unscored"), [rows]);
  const topScoreId = useMemo(() => selectBestRowId(rows, (row) => row.score), [rows]);
  const topEdgeId = useMemo(
    () => selectBestRowId(rows, (row) => row.executable_edge?.fee_adjusted_edge ?? row.fee_adjusted_edge),
    [rows],
  );
  const warningCount = rows.filter((row) => getRecommendationWarnings(row).length > 0).length;

  return (
    <PageFrame
      title="Recommendation Queue"
      subtitle="Review queue built for fast triage. Tier, score, executable edge, warnings, and freshness stay visible at the same time."
      actions={
        <span className="muted-label">
          {page.totalCount
            ? `${page.offset + 1}-${Math.min(page.offset + rows.length, page.totalCount)}`
            : "0"}{" "}
          of {page.totalCount}
        </span>
      }
    >
      <FreshnessBanner status={recommendationStatus} />

      <div className="decision-strip">
        <MetricCard
          label="Visible recommendations"
          value={String(rows.length)}
          detail={page.totalCount ? `${page.totalCount} total in current query` : "No results"}
          emphasis="strong"
        />
        <MetricCard
          label="High conviction"
          value={String(queueCounts.high_conviction || 0)}
          detail="Fastest review candidates"
        />
        <MetricCard
          label="Warnings"
          value={String(warningCount)}
          detail="Rows carrying warning or freshness risk"
          tone={warningCount ? "warning" : "ok"}
        />
        <MetricCard
          label="Freshness"
          value={readableFreshness(recommendationStatus?.freshness_status)}
          detail={formatDateTime(recommendationStatus?.latest_scoring_run_timestamp)}
          tone={freshnessTone(recommendationStatus?.freshness_status)}
        />
      </div>

      <Panel
        title="Queue controls"
        description="Filtering and sorting stay explicit so the queue never feels ambiguous."
      >
        <div className="filter-row">
          <label className="control">
            <span>Tier</span>
            <select
              aria-label="Tier filter"
              value={query.tier}
              onChange={(event) => setQuery((current) => ({ ...current, tier: event.target.value, offset: 0 }))}
            >
              <option value="">All tiers</option>
              <option value="high_conviction">High conviction</option>
              <option value="review">Review</option>
              <option value="blocked">Blocked</option>
            </select>
          </label>
          <label className="control">
            <span>Family</span>
            <select
              aria-label="Family filter"
              value={query.family}
              onChange={(event) => setQuery((current) => ({ ...current, family: event.target.value, offset: 0 }))}
            >
              <option value="">All families</option>
              {familyOptions.map((family) => (
                <option key={family} value={family}>
                  {humanizeToken(family)}
                </option>
              ))}
            </select>
          </label>
          <label className="control">
            <span>Sort</span>
            <select
              aria-label="Sort recommendations"
              value={query.sort}
              onChange={(event) => setQuery((current) => ({ ...current, sort: event.target.value, offset: 0 }))}
            >
              <option value="score">Score</option>
              <option value="edge">Executable edge</option>
              <option value="recency">Recency</option>
            </select>
          </label>
          <label className="control">
            <span>Rows per page</span>
            <select
              aria-label="Rows per page"
              value={query.limit}
              onChange={(event) =>
                setQuery((current) => ({ ...current, limit: Number(event.target.value), offset: 0 }))
              }
            >
              <option value={10}>10</option>
              <option value={25}>25</option>
              <option value={50}>50</option>
            </select>
          </label>
        </div>
      </Panel>

      <Panel
        title="Recommendation review surface"
        description="Recommendations are grouped by tier so priority, caution, and blocked items do not blend together."
      >
        {state.loading ? (
          <LoadingState
            label="Loading recommendation queue"
            detail="Pulling the latest ranked recommendations and trust signals."
          />
        ) : state.error ? (
          <ErrorState
            title="Could not load the recommendation queue"
            message={state.error}
            detail="The queue stays read-only. Retry after the API recovers."
          />
        ) : rows.length === 0 ? (
          <EmptyState
            message="No recommendations match the current filters."
            detail="Change the filters or wait for the next scoring run."
          />
        ) : (
          <div className="tier-section-list">
            {QUEUE_TIERS.filter((tier) => groupedRows[tier]?.length).map((tier) => (
              <QueueTierSection
                key={tier}
                tier={tier}
                rows={groupedRows[tier]}
                topScoreId={topScoreId}
                topEdgeId={topEdgeId}
              />
            ))}
          </div>
        )}

        <div className="pagination-row">
          <button
            type="button"
            className="secondary-button"
            onClick={() =>
              setQuery((current) => ({
                ...current,
                offset: Math.max(0, current.offset - current.limit),
              }))
            }
            disabled={query.offset === 0}
          >
            Previous
          </button>
          <button
            type="button"
            className="secondary-button"
            onClick={() =>
              setQuery((current) => ({
                ...current,
                offset: current.offset + current.limit,
              }))
            }
            disabled={query.offset + rows.length >= page.totalCount}
          >
            Next
          </button>
        </div>
      </Panel>
    </PageFrame>
  );
}

function RecommendationDetailPage({ api, opportunityId }) {
  const state = useAsyncData(() => api.fetchRecommendationDetail(opportunityId), [api, opportunityId]);
  const detail = state.data?.detail ?? null;
  const recommendationStatus = state.data?.recommendationStatus ?? null;

  return (
    <PageFrame
      title="Recommendation Detail"
      subtitle="Decision screen for one recommendation. Reason, risk, validation, edge, simulation, and audit evidence stay structured."
      actions={
        <a href="#/recommendations" className="back-link">
          Back to queue
        </a>
      }
    >
      <FreshnessBanner status={recommendationStatus || detail?.summary} />
      {state.loading ? (
        <LoadingState
          label="Loading recommendation detail"
          detail="Pulling validation evidence, execution context, and audit records."
        />
      ) : null}
      {state.error ? (
        <ErrorState
          title="Could not load recommendation detail"
          message={state.error}
          detail="Return to the queue if the API stays unavailable."
        />
      ) : null}
      {!state.loading && !state.error && !detail ? (
        <EmptyState
          message="Recommendation detail not found."
          detail="The recommendation may have expired or the URL may be out of date."
        />
      ) : null}
      {!state.loading && !state.error && detail ? (
        <RecommendationDetailContent detail={detail} />
      ) : null}
    </PageFrame>
  );
}

function RecommendationDetailContent({ detail }) {
  const summary = detail.summary ?? {};
  const warnings = getRecommendationWarnings(summary);
  const decisionHeadline = describeDecisionPosture(summary);
  const logicLayer = detail.validation_evidence?.rule_validation ?? null;
  const semanticLayer = detail.validation_evidence?.semantic_validation ?? null;
  const resolutionLayer = detail.validation_evidence?.resolution_validation ?? null;
  const auditScores = detail.audit?.scores?.length || 0;
  const involvedMarkets = detail.audit?.involved_markets?.length || 0;

  return (
    <>
      <section className={`decision-hero tier-${summary.tier || "unscored"}`}>
        <div className="decision-hero-header">
          <div>
            <p className="eyebrow">Recommendation</p>
            <h3 className="decision-hero-title">{summary.event_id}</h3>
            <p className="page-subtitle">{decisionHeadline}</p>
          </div>
          <div className="badge-row">
            <StatusBadge tone={toneForTier(summary.tier)}>
              Tier {humanizeToken(summary.tier || "unscored")}
            </StatusBadge>
            <StatusBadge tone={summary.recommendation_eligibility ? "ok" : "danger"}>
              {summary.recommendation_eligibility ? "Eligible for review" : "Blocked"}
            </StatusBadge>
            <FreshnessPill status={summary.freshness_status} />
          </div>
        </div>

        <div className="decision-stat-grid">
          <DecisionStat label="Score" value={formatScore(summary.score)} emphasis="strong" />
          <DecisionStat
            label="Executable edge"
            value={formatEdge(detail.executable_edge?.fee_adjusted_edge)}
            emphasis="strong"
          />
          <DecisionStat label="Confidence" value={humanizeToken(summary.confidence_tier || "unknown")} />
          <DecisionStat label="Fill ratio" value={formatPercent(detail.executable_edge?.fill_completion_ratio)} />
        </div>

        <div className="two-column">
          <Panel
            title="Decision summary"
            description="Keep the recommendation posture and primary reason readable before opening raw evidence."
          >
            <p className="support-copy detail-summary-copy">
              {summary.reason_summary || "No reason summary available."}
            </p>
            <DefinitionList
              rows={[
                ["Decision posture", decisionHeadline],
                ["Detected", formatDateTime(summary.detected_at)],
                ["Family", humanizeToken(summary.family)],
              ]}
            />
          </Panel>

          <Panel
            title="Trust and warning summary"
            description="Freshness, review status, and blocking context stay next to the recommendation reason."
          >
            {warnings.length ? (
              <WarningCallout title="Review carefully" items={warnings} compact />
            ) : (
              <p className="support-copy detail-summary-copy">No active warning summary surfaced.</p>
            )}
            <DefinitionList
              rows={[
                ["Freshness", readableFreshness(summary.freshness_status)],
                [
                  "Review status",
                  summary.recommendation_eligibility ? "Eligible for review" : "Blocked pending review",
                ],
                ["Block reason", summary.recommendation_block_reason ? humanizeToken(summary.recommendation_block_reason) : "None"],
                ["Active warnings", String(warnings.length)],
              ]}
            />
          </Panel>
        </div>
      </section>

      <SectionIntro
        title="Primary evidence"
        description="Strong evidence stays above supporting raw data so the detail view remains calm and reviewable."
      />
      <div className="detail-section-grid">
        <ValidationPanel label="Logic validation" layer={logicLayer} />
        <ValidationPanel label="Semantic validation" layer={semanticLayer} />
        <ValidationPanel label="Resolution validation" layer={resolutionLayer} />
      </div>

      <div className="two-column">
        <Panel
          title="Executable edge"
          description="Primary execution evidence. Keep this near the score when judging recommendation quality."
        >
          <DefinitionList
            rows={[
              ["Top of book edge", formatEdge(detail.executable_edge?.top_of_book_edge)],
              ["Depth weighted edge", formatEdge(detail.executable_edge?.depth_weighted_edge)],
              ["Fee adjusted edge", formatEdge(detail.executable_edge?.fee_adjusted_edge)],
              ["Minimum executable size", formatCurrency(detail.executable_edge?.min_executable_size)],
              ["Fill ratio", formatPercent(detail.executable_edge?.fill_completion_ratio)],
              ["Capital lock estimate", formatHours(detail.executable_edge?.capital_lock_estimate_hours)],
            ]}
          />
        </Panel>

        <Panel
          title="Simulation"
          description="Simulation should explain whether the observed edge survives execution assumptions."
        >
          <DefinitionList
            rows={[
              [
                "Latest status",
                humanizeToken(detail.latest_execution_simulation?.simulation_status || "missing"),
              ],
              ["Real edge", formatCurrency(detail.latest_execution_simulation?.real_edge)],
              ["Executable size", formatCurrency(detail.latest_execution_simulation?.executable_size)],
              [
                "Simulation reason",
                detail.latest_execution_simulation?.simulation_reason || "Unavailable",
              ],
              ["Simulation records", String(detail.simulation_results?.length || 0)],
            ]}
          />
          <DataReveal
            label="Simulation raw data"
            value={detail.latest_execution_simulation || detail.simulation_results}
          />
        </Panel>
      </div>

      <SectionIntro
        title="Supporting data"
        description="Audit and KPI context remain available for verification without overwhelming the main decision flow."
      />
      <div className="two-column">
        <Panel
          title="KPI"
          description="Outcome-level KPI context remains secondary, but still available for auditability."
        >
          {detail.kpi_snapshot ? (
            <>
              <DefinitionList
                rows={[
                  ["Final status", humanizeToken(detail.kpi_snapshot.final_status)],
                  [
                    "Validation stage reached",
                    humanizeToken(detail.kpi_snapshot.validation_stage_reached),
                  ],
                  ["Decay status", humanizeToken(detail.kpi_snapshot.decay_status)],
                  ["Fill ratio", formatPercent(detail.kpi_snapshot.fill_completion_ratio)],
                  [
                    "Capital lock estimate",
                    formatHours(detail.kpi_snapshot.capital_lock_estimate_hours),
                  ],
                ]}
              />
              <DataReveal label="KPI raw data" value={detail.kpi_snapshot} />
            </>
          ) : (
            <EmptyState message="No KPI snapshot available for this recommendation." />
          )}
        </Panel>

        <Panel
          title="Audit"
          description="Audit evidence stays visible at a glance, while raw context remains collapsed until needed."
        >
          <DefinitionList
            rows={[
              ["Score records", String(auditScores)],
              ["Involved markets", String(involvedMarkets)],
              ["Scoring version", detail.audit?.scores?.[0]?.scoring_version || "Unavailable"],
              ["Latest score created", formatDateTime(detail.audit?.scores?.[0]?.created_at)],
            ]}
          />
          <DataReveal label="Audit raw context" value={detail.audit?.raw_context} />
        </Panel>
      </div>
    </>
  );
}

function KpiAnalyticsPage({ api }) {
  const state = useAsyncData(() => api.fetchKpiAnalytics(), [api]);
  const kpi = state.data?.kpi ?? null;
  const kpiRun = state.data?.kpiRun ?? null;
  const recommendationStatus = state.data?.recommendationStatus ?? null;

  return (
    <PageFrame
      title="KPI Analytics"
      subtitle="Research performance context. Only metrics with a clear decision-support purpose are shown."
    >
      <FreshnessBanner status={recommendationStatus} />
      <div className="decision-strip">
        <MetricCard
          label="Freshness"
          value={readableFreshness(recommendationStatus?.freshness_status)}
          detail={formatDateTime(recommendationStatus?.latest_kpi_time)}
          tone={freshnessTone(recommendationStatus?.freshness_status)}
        />
        <MetricCard
          label="Validation funnel"
          value={kpiRun ? "Available" : "Pending"}
          detail={kpiRun ? "Latest KPI run is present" : "Waiting for KPI run output"}
          tone={kpiRun ? "ok" : "neutral"}
        />
        <MetricCard
          label="Family coverage"
          value={kpiRun?.family_distribution ? "Available" : "Pending"}
          detail={
            kpiRun?.family_distribution
              ? `${Object.keys(kpiRun.family_distribution).length} families in the latest run`
              : "Family breakdown has not been produced yet"
          }
        />
        <MetricCard
          label="False positive rate"
          value={formatPercent(kpi?.false_positive_rate)}
          detail={kpi ? "Latest KPI snapshot" : "No KPI snapshot available"}
        />
      </div>
      {state.loading ? (
        <LoadingState label="Loading KPI analytics" detail="Collecting the latest KPI snapshots and run metadata." />
      ) : null}
      {state.error ? (
        <ErrorState
          title="Could not load KPI analytics"
          message={state.error}
          detail="Recommendation review remains available even while KPI analytics are unavailable."
        />
      ) : null}
      {!state.loading && !state.error ? (
        <div className="analytics-grid">
          <AnalyticsSection
            title="Validation funnel"
            description="Stage counts show how much of the recommendation set survives each validation layer."
            rows={
              kpiRun
                ? [
                    ["Logic", String(kpiRun.valid_after_rule)],
                    ["Semantic", String(kpiRun.valid_after_semantic)],
                    ["Resolution", String(kpiRun.valid_after_resolution)],
                    ["Executable edge", String(kpiRun.valid_after_executable)],
                    ["Simulation", String(kpiRun.valid_after_simulation)],
                  ]
                : null
            }
            fallback="Validation funnel data will appear after a KPI run completes."
          />
          <AnalyticsSection
            title="False positive attribution"
            description="Keep downside context visible without turning this screen into a chart wall."
            rows={kpi ? [["False positive rate", formatPercent(kpi.false_positive_rate)]] : null}
            fallback="False positive attribution is not available in the latest KPI snapshot."
          />
          <AnalyticsSection
            title="Family distribution"
            description="Availability is explicit so missing slices do not look like healthy zeros."
            rows={
              kpiRun?.family_distribution
                ? Object.entries(kpiRun.family_distribution).map(([key, value]) => [
                    humanizeToken(key),
                    String(value),
                  ])
                : null
            }
            fallback="Family distribution will appear when the latest KPI run publishes family-level slices."
          />
          <AnalyticsSection
            title="Average executable edge"
            description="Only the latest trusted snapshot is shown."
            rows={
              kpiRun
                ? [["Average executable edge", formatEdge(kpiRun.avg_executable_edge)]]
                : kpi
                  ? [["Average executable edge", formatEdge(kpi.avg_real_edge)]]
                  : null
            }
            fallback="Average executable edge is not available in the latest KPI output."
          />
          <AnalyticsSection
            title="Average fill ratio"
            description="Review this beside executable edge, not in isolation."
            rows={
              kpiRun
                ? [["Average fill ratio", formatPercent(kpiRun.avg_fill_ratio)]]
                : kpi
                  ? [["Average fill ratio", formatPercent(kpi.avg_fill_ratio)]]
                  : null
            }
            fallback="Average fill ratio is not available in the latest KPI output."
          />
          <AnalyticsSection
            title="Capital lock estimate"
            description="Capital efficiency remains visible when the KPI run provides it."
            rows={kpiRun ? [["Average capital lock", formatHours(kpiRun.avg_capital_lock)]] : null}
            fallback="Capital lock estimate data is not available in the latest KPI run."
          />
        </div>
      ) : null}
    </PageFrame>
  );
}

function SystemStatusPage({ api }) {
  const state = useAsyncData(() => api.fetchSystemStatus(), [api]);
  const recommendationStatus = state.data?.recommendationStatus ?? null;
  const systemStatus = state.data?.systemStatus ?? null;
  const kpiRun = state.data?.kpiRun ?? null;

  return (
    <PageFrame
      title="System Status"
      subtitle="Operational trust view. Stale reasons remain explicit even when recommendation screens still render."
    >
      <FreshnessBanner status={recommendationStatus} />
      <div className="decision-strip">
        <MetricCard
          label="Recommendation freshness"
          value={readableFreshness(recommendationStatus?.freshness_status)}
          detail={formatDateTime(recommendationStatus?.latest_scoring_run_timestamp)}
          tone={freshnessTone(recommendationStatus?.freshness_status)}
        />
        <MetricCard
          label="Scoring worker"
          value={humanizeToken(recommendationStatus?.scoring_worker_status || "unknown")}
          detail="Worker health is separate from queue rendering"
          tone={toneForStatus(recommendationStatus?.scoring_worker_status)}
        />
        <MetricCard
          label="Active warnings"
          value={String(recommendationStatus?.stale_reasons?.length || 0)}
          detail="Freshness warnings stay explicit"
          tone={recommendationStatus?.stale_reasons?.length ? "warning" : "ok"}
        />
        <MetricCard
          label="Latest KPI run"
          value={kpiRun ? "Available" : "Missing"}
          detail={kpiRun ? formatDateTime(kpiRun.run_completed_at || kpiRun.created_at) : "No KPI run metadata"}
          tone={kpiRun ? "ok" : "warning"}
        />
      </div>
      <div className="two-column">
        <Panel
          title="Recommendation freshness"
          description="Latest scoring run and stale reasons, kept separate from the recommendation queue."
        >
          {state.loading ? (
            <LoadingState label="Loading recommendation freshness" detail="Checking the latest scoring metadata." />
          ) : state.error ? (
            <ErrorState title="Could not load recommendation freshness" message={state.error} />
          ) : (
            <DefinitionList
              rows={[
                ["Freshness", readableFreshness(recommendationStatus?.freshness_status)],
                [
                  "Scoring worker status",
                  humanizeToken(recommendationStatus?.scoring_worker_status || "unknown"),
                ],
                ["Latest scoring run", formatDateTime(recommendationStatus?.latest_scoring_run_timestamp)],
                ["Run reason", recommendationStatus?.run_reason || "Unavailable"],
              ]}
            />
          )}
        </Panel>
        <Panel
          title="Latest timestamps"
          description="Timestamp visibility prevents false confidence in stale system state."
        >
          {state.loading ? (
            <LoadingState label="Loading system timestamps" detail="Checking the latest ingest, validation, and simulation times." />
          ) : state.error ? (
            <ErrorState title="Could not load system timestamps" message={state.error} />
          ) : (
            <DefinitionList
              rows={[
                ["Latest validation", formatDateTime(recommendationStatus?.latest_validation_time)],
                ["Latest KPI", formatDateTime(recommendationStatus?.latest_kpi_time)],
                ["Last market snapshot", formatDateTime(systemStatus?.last_snapshot_time)],
                ["Last detection", formatDateTime(systemStatus?.last_detection_time)],
                ["Last simulation", formatDateTime(systemStatus?.last_simulation_time)],
              ]}
            />
          )}
        </Panel>
      </div>

      <Panel title="Warnings" description="Warnings are listed directly rather than hidden behind secondary tabs.">
        {state.loading ? (
          <LoadingState label="Loading stale reasons" detail="Reading the current trust warnings from the API." />
        ) : state.error ? (
          <ErrorState title="Could not load freshness warnings" message={state.error} />
        ) : recommendationStatus?.stale_reasons?.length ? (
          <WarningCallout
            title="Active freshness warnings"
            items={recommendationStatus.stale_reasons.map((reason) => humanizeToken(reason))}
          />
        ) : (
          <EmptyState
            message="No stale reasons reported."
            detail="System freshness looks current based on the latest recommendation status."
          />
        )}
      </Panel>

      <Panel title="Latest KPI run" description="Read-only KPI metadata to help assess dashboard trust.">
        {state.loading ? (
          <LoadingState label="Loading KPI run status" detail="Checking whether KPI analytics have a recent supporting run." />
        ) : state.error ? (
          <ErrorState title="Could not load KPI run status" message={state.error} />
        ) : kpiRun ? (
          <DefinitionList
            rows={[
              ["KPI version", kpiRun.kpi_version],
              ["Created at", formatDateTime(kpiRun.created_at)],
              ["Run completed at", formatDateTime(kpiRun.run_completed_at)],
              ["Total opportunities", String(kpiRun.total_opportunities)],
            ]}
          />
        ) : (
          <EmptyState
            message="No KPI run status available."
            detail="Recommendation review can still render, but KPI trust context is incomplete."
          />
        )}
      </Panel>
    </PageFrame>
  );
}

function NotFoundPage() {
  return (
    <PageFrame title="Route not found" subtitle="Use dashboard navigation to return to a supported screen.">
      <Panel title="Available screens">
        <ul className="warning-list">
          {NAV_ITEMS.map((item) => (
            <li key={item.path}>
              <a href={`#${item.path}`} className="table-link">
                {item.label}
              </a>
            </li>
          ))}
        </ul>
      </Panel>
    </PageFrame>
  );
}

function PageFrame({ title, subtitle, actions = null, children }) {
  return (
    <section className="page-frame">
      <header className="page-header">
        <div>
          <p className="eyebrow">Human Decision Support</p>
          <h2>{title}</h2>
          <p className="page-subtitle">{subtitle}</p>
        </div>
        {actions ? <div className="page-actions">{actions}</div> : null}
      </header>
      {children}
    </section>
  );
}

function Panel({ title, description, children }) {
  return (
    <section className="panel">
      {title ? (
        <div className="panel-heading">
          <div>
            <h3>{title}</h3>
            {description ? <p className="panel-copy">{description}</p> : null}
          </div>
        </div>
      ) : null}
      {children}
    </section>
  );
}

function SectionIntro({ title, description }) {
  return (
    <div className="section-intro">
      <p className="eyebrow">Evidence framing</p>
      <h3>{title}</h3>
      <p className="panel-copy">{description}</p>
    </div>
  );
}

function MetricCard({ label, value, detail, emphasis = "normal", tone = "neutral" }) {
  return (
    <article className={`metric-card ${emphasis} ${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </article>
  );
}

function DecisionStat({ label, value, emphasis = "normal" }) {
  return (
    <article className={`decision-stat ${emphasis}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </article>
  );
}

function QueueTierSection({ tier, rows, topScoreId, topEdgeId }) {
  return (
    <section className={`tier-section tier-${tier}`}>
      <div className="tier-section-heading">
        <div>
          <h3>{tierHeading(tier)}</h3>
          <p className="panel-copy">{tierDescription(tier)}</p>
        </div>
        <StatusBadge tone={toneForTier(tier)}>{rows.length} recommendations</StatusBadge>
      </div>

      <div className="queue-card-list">
        {rows.map((row, index) => (
          <RecommendationQueueCard
            key={row.opportunity_id}
            row={row}
            markers={{
              reviewFirst: index === 0,
              topScore: row.opportunity_id === topScoreId,
              topEdge: row.opportunity_id === topEdgeId,
            }}
          />
        ))}
      </div>
    </section>
  );
}

function RecommendationQueueCard({ row, markers }) {
  const warnings = getRecommendationWarnings(row);
  const primaryWarning = warnings[0] ?? null;
  const highlights = [];

  if (markers.reviewFirst) {
    highlights.push("Review first");
  }
  if (markers.topScore) {
    highlights.push("Top score");
  }
  if (markers.topEdge) {
    highlights.push("Top edge");
  }
  if (row.manual_review_required) {
    highlights.push("Manual review");
  }
  if (warnings.length) {
    highlights.push("Warnings");
  }

  return (
    <article className={`queue-card tier-${row.tier || "unscored"}`}>
      <div className="queue-card-header">
        <div className="queue-card-title">
          <div className="queue-rank">#{row.ranking_position}</div>
          <div>
            <a href={`#/recommendations/${row.opportunity_id}`} className="queue-link">
              {row.event_id}
            </a>
            <p className="support-copy queue-reason">
              {row.reason_summary || row.recommendation_block_reason || "Reason pending"}
            </p>
            <p className="secondary-text queue-inline-summary">
              {primaryWarning
                ? `Primary warning: ${primaryWarning}`
                : "No active warning summary surfaced."}
            </p>
          </div>
        </div>
        <div className="badge-row">
          <StatusBadge tone={toneForTier(row.tier)}>{humanizeToken(row.tier || "unscored")}</StatusBadge>
          <FreshnessPill status={row.freshness_status} />
        </div>
      </div>

      {highlights.length ? (
        <div className="badge-row highlight-row">
          {highlights.map((highlight) => (
            <StatusBadge
              key={highlight}
              tone={highlight === "Warnings" || highlight === "Manual review" ? "warning" : "neutral"}
            >
              {highlight}
            </StatusBadge>
          ))}
        </div>
      ) : null}

      {warnings.length ? <WarningCallout title="Warnings" items={warnings} compact /> : null}

      <div className="decision-stat-grid queue-stat-grid">
        <DecisionStat label="Score" value={formatScore(row.score)} emphasis="strong" />
        <DecisionStat
          label="Executable edge"
          value={formatEdge(row.executable_edge?.fee_adjusted_edge ?? row.fee_adjusted_edge)}
          emphasis="strong"
        />
        <DecisionStat label="Fill ratio" value={formatPercent(row.fill_completion_ratio)} />
        <DecisionStat label="Confidence" value={humanizeToken(row.confidence_tier || "unknown")} />
      </div>

      <DefinitionList
        rows={[
          ["Family", humanizeToken(row.family)],
          ["Validation", humanizeToken(row.validation_status || "unknown")],
          ["Simulation", humanizeToken(row.simulation_status || "unknown")],
          ["Capital lock", formatHours(row.capital_lock_estimate ?? row.capital_lock_estimate_hours)],
          ["Detected", formatDateTime(row.detected_at)],
          ["Next step", row.tier === "blocked" ? "Resolve warnings before relying on it" : "Open detail for evidence"],
        ]}
        compact
      />

      <div className="queue-card-footer">
        <a href={`#/recommendations/${row.opportunity_id}`} className="table-link">
          Open detail
        </a>
      </div>
    </article>
  );
}

function RecommendationOverviewHero({ row }) {
  const warnings = getRecommendationWarnings(row);

  return (
    <a href={`#/recommendations/${row.opportunity_id}`} className={`preview-card hero-card tier-${row.tier || "unscored"}`}>
      <div className="section-heading">
        <div>
          <h3>{row.event_id}</h3>
          <p className="support-copy">{row.reason_summary || "Reason pending"}</p>
        </div>
        <div className="badge-row">
          <StatusBadge tone={toneForTier(row.tier)}>{humanizeToken(row.tier || "unscored")}</StatusBadge>
          <FreshnessPill status={row.freshness_status} />
        </div>
      </div>

      <div className="decision-stat-grid">
        <DecisionStat label="Score" value={formatScore(row.score)} emphasis="strong" />
        <DecisionStat
          label="Executable edge"
          value={formatEdge(row.executable_edge?.fee_adjusted_edge ?? row.fee_adjusted_edge)}
          emphasis="strong"
        />
        <DecisionStat label="Fill ratio" value={formatPercent(row.fill_completion_ratio)} />
        <DecisionStat label="Confidence" value={humanizeToken(row.confidence_tier || "unknown")} />
      </div>

      {warnings.length ? <WarningCallout title="Warnings" items={warnings} compact /> : null}
    </a>
  );
}

function RecommendationPreviewList({ rows }) {
  return (
    <div className="preview-list">
      {rows.map((row) => {
        const warnings = getRecommendationWarnings(row);

        return (
          <a
            key={row.opportunity_id}
            href={`#/recommendations/${row.opportunity_id}`}
            className={`preview-card tier-${row.tier || "unscored"}`}
          >
            <div className="section-heading">
              <div>
                <h3>{row.event_id}</h3>
                <p className="support-copy">{row.reason_summary || "Reason pending"}</p>
              </div>
              <div className="badge-row">
                <StatusBadge tone={toneForTier(row.tier)}>
                  {humanizeToken(row.tier || "unscored")}
                </StatusBadge>
                <FreshnessPill status={row.freshness_status} />
              </div>
            </div>
            <div className="inline-meta">
              <span>Score {formatScore(row.score)}</span>
              <span>Edge {formatEdge(row.executable_edge?.fee_adjusted_edge ?? row.fee_adjusted_edge)}</span>
              <span>Confidence {humanizeToken(row.confidence_tier || "unknown")}</span>
            </div>
            {warnings.length ? (
              <p className="secondary-text">Warning: {warnings[0]}</p>
            ) : (
              <p className="secondary-text">No warning summary surfaced.</p>
            )}
          </a>
        );
      })}
    </div>
  );
}

function ValidationPanel({ label, layer }) {
  return (
    <Panel
      title={label}
      description="Direct validator output kept concise, with raw evidence available on demand."
    >
      <div className="section-heading">
        <StatusBadge tone={toneForStatus(layer?.status)}>{humanizeToken(layer?.status || "missing")}</StatusBadge>
        <span className="muted-label">Validator score {formatScore(layer?.score)}</span>
      </div>
      <p className="support-copy">{layer?.summary || "No validation summary available."}</p>
      <DefinitionList
        rows={[
          ["Validator version", layer?.validator_version || "Unavailable"],
          ["Evidence summary", summarizeObject(layer?.details_json || layer?.raw_context)],
        ]}
        compact
      />
      <DataReveal label={`${label} raw data`} value={layer?.details_json || layer?.raw_context} />
    </Panel>
  );
}

function WarningCallout({ title, items, compact = false }) {
  const safeItems = items.filter(Boolean);

  if (!safeItems.length) {
    return null;
  }

  return (
    <section className={compact ? "warning-callout compact" : "warning-callout"} aria-label={title}>
      <strong>{title}</strong>
      <ul className="warning-list">
        {safeItems.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </section>
  );
}

function DataReveal({ label, value }) {
  if (!value) {
    return null;
  }

  return (
    <details className="data-reveal">
      <summary>
        {label} <span className="secondary-text">({summarizeObject(value)})</span>
      </summary>
      <pre>{JSON.stringify(value, null, 2)}</pre>
    </details>
  );
}

function AnalyticsSection({ title, description, rows, fallback }) {
  return (
    <Panel title={title} description={description}>
      {rows?.length ? <DefinitionList rows={rows} /> : <EmptyState message={fallback} />}
    </Panel>
  );
}

function DefinitionList({ rows, compact = false }) {
  return (
    <dl className={compact ? "definition-list compact" : "definition-list"}>
      {rows.map(([label, value]) => (
        <React.Fragment key={label}>
          <dt>{label}</dt>
          <dd>{value ?? "Unavailable"}</dd>
        </React.Fragment>
      ))}
    </dl>
  );
}

function FreshnessBanner({ status }) {
  if (!status) {
    return null;
  }

  const reasons = status.stale_reasons || [];
  const tone = freshnessTone(status.freshness_status);
  const details = [
    ["Recommendation freshness", readableFreshness(status.freshness_status)],
    ["Latest scoring run", formatDateTime(status.latest_scoring_run_timestamp)],
    ["Latest validation", formatDateTime(status.latest_validation_time)],
    ["Latest KPI", formatDateTime(status.latest_kpi_time)],
  ].filter(([, value]) => value !== "Unavailable");
  const scoringWorkerStatus = status.scoring_worker_status
    ? humanizeToken(status.scoring_worker_status)
    : null;

  return (
    <section className={`freshness-banner ${tone}`} aria-live="polite">
      <div className="freshness-banner-copy">
        <div className="badge-row">
          <StatusBadge tone={tone}>Freshness {readableFreshness(status.freshness_status)}</StatusBadge>
          {scoringWorkerStatus ? (
            <StatusBadge tone={toneForStatus(status.scoring_worker_status)}>
              Scoring worker {scoringWorkerStatus}
            </StatusBadge>
          ) : null}
        </div>
        <strong>{freshnessBannerTitle(status.freshness_status)}</strong>
        <p>{freshnessBannerDescription(status)}</p>
      </div>
      {details.length ? <DefinitionList rows={details} compact /> : null}
      {reasons.length ? <WarningCallout title="Active freshness reasons" items={reasons.map((reason) => humanizeToken(reason))} compact /> : null}
    </section>
  );
}

function FreshnessPill({ status }) {
  return (
    <StatusBadge tone={freshnessTone(status)}>
      Freshness {readableFreshness(status)}
    </StatusBadge>
  );
}

function StatusBadge({ tone, children }) {
  return <span className={`status-badge ${tone}`}>{children}</span>;
}

function LoadingState({ label, detail = "Waiting for the latest API response." }) {
  return <StateBlock tone="loading" title={`${label}…`} detail={detail} />;
}

function ErrorState({ title = "Could not load this view", message, detail = "Refresh after the API recovers." }) {
  return <StateBlock tone="error" title={`API load error: ${message}`} detail={`${title}. ${detail}`} />;
}

function EmptyState({ message, detail = "This state is expected when the API has nothing to show yet." }) {
  return <StateBlock tone="empty" title={message} detail={detail} />;
}

function StateBlock({ tone, title, detail }) {
  return (
    <div className={`state-block ${tone}`}>
      <strong>{title}</strong>
      {detail ? <p>{detail}</p> : null}
    </div>
  );
}

function useAsyncData(loader, deps) {
  const [state, setState] = useState({
    data: null,
    loading: true,
    error: null,
  });

  useEffect(() => {
    let cancelled = false;

    async function run() {
      try {
        setState((current) => ({ ...current, loading: true, error: null }));
        const data = await loader();
        if (!cancelled) {
          setState({ data, loading: false, error: null });
        }
      } catch (error) {
        if (!cancelled) {
          setState({
            data: null,
            loading: false,
            error: error.message || String(error),
          });
        }
      }
    }

    run();

    return () => {
      cancelled = true;
    };
  }, deps);

  return state;
}

function useHashRoute() {
  const [route, setRoute] = useState(() => parseRoute(window.location.hash));

  useEffect(() => {
    const handleChange = () => {
      setRoute(parseRoute(window.location.hash));
    };

    window.addEventListener("hashchange", handleChange);
    return () => window.removeEventListener("hashchange", handleChange);
  }, []);

  return route;
}

export function parseRoute(hash) {
  const raw = hash?.startsWith("#") ? hash.slice(1) : hash || "/overview";
  const path = raw || "/overview";

  if (path === "/" || path === "/overview") {
    return { name: "overview", path: "/overview" };
  }
  if (path === "/recommendations") {
    return { name: "queue", path: "/recommendations" };
  }
  if (path.startsWith("/recommendations/")) {
    const opportunityId = path.split("/")[2];
    return { name: "detail", path: "/recommendations", opportunityId };
  }
  if (path === "/analytics") {
    return { name: "analytics", path: "/analytics" };
  }
  if (path === "/system-status") {
    return { name: "system", path: "/system-status" };
  }
  return { name: "notFound", path };
}

function buildOverviewAttentionItems({ recommendationStatus, topRecommendations, totalCount }) {
  const items = [];
  const staleReasons = recommendationStatus?.stale_reasons ?? [];
  const warnedRecommendations = topRecommendations.filter((row) => getRecommendationWarnings(row).length > 0);

  staleReasons.forEach((reason) => items.push(humanizeToken(reason)));

  if (warnedRecommendations.length) {
    items.push(
      `${warnedRecommendations.length} of ${topRecommendations.length} visible top recommendations carry warnings or stale freshness`,
    );
  }

  if (totalCount === 0) {
    items.push("No recommendations available in the current queue");
  }

  return dedupe(items);
}

function groupRowsByTier(rows) {
  return rows.reduce((groups, row) => {
    const key = row.tier || "unscored";
    if (!groups[key]) {
      groups[key] = [];
    }
    groups[key].push(row);
    return groups;
  }, {});
}

function selectBestRowId(rows, getValue) {
  if (!rows.length) {
    return null;
  }

  return rows.reduce(
    (best, row) => {
      const value = Number(getValue(row));
      if (Number.isNaN(value) || value <= best.value) {
        return best;
      }
      return { id: row.opportunity_id, value };
    },
    { id: null, value: Number.NEGATIVE_INFINITY },
  ).id;
}

function getRecommendationWarnings(subject) {
  if (!subject) {
    return [];
  }

  const warnings = [];

  if (subject.warning_summary) {
    warnings.push(subject.warning_summary);
  }
  if (subject.recommendation_block_reason) {
    warnings.push(`Block reason: ${humanizeToken(subject.recommendation_block_reason)}`);
  }
  if (subject.freshness_status && subject.freshness_status !== "fresh") {
    warnings.push(`Freshness ${readableFreshness(subject.freshness_status)}`);
  }
  if (Array.isArray(subject.stale_reasons)) {
    subject.stale_reasons.forEach((reason) => warnings.push(humanizeToken(reason)));
  }

  return dedupe(warnings);
}

function describeDecisionPosture(summary) {
  const warningCount = getRecommendationWarnings(summary).length;

  if (!summary.recommendation_eligibility || summary.tier === "blocked") {
    return "Blocked recommendation. Resolve the warning summary before relying on this decision.";
  }
  if (summary.tier === "high_conviction" && warningCount === 0 && summary.freshness_status === "fresh") {
    return "Priority recommendation. The score and edge are strong, and no active warning is surfaced.";
  }
  if (summary.tier === "high_conviction") {
    return "High-conviction recommendation with active caution flags. Review the warning summary before trusting it.";
  }
  if (summary.tier === "review") {
    return "Review recommendation. Evidence is promising, but risk or uncertainty still needs analyst judgment.";
  }
  return "Recommendation requires caution. Use the validation, edge, and audit sections before making a decision.";
}

function tierHeading(value) {
  if (value === "high_conviction") {
    return "High conviction recommendations";
  }
  if (value === "review") {
    return "Review recommendations";
  }
  if (value === "blocked") {
    return "Blocked recommendations";
  }
  return "Unscored recommendations";
}

function tierDescription(value) {
  if (value === "high_conviction") {
    return "Highest priority items. Review score, executable edge, and warnings first.";
  }
  if (value === "review") {
    return "Promising ideas with caution. Keep evidence and warning context close together.";
  }
  if (value === "blocked") {
    return "These items should not be relied on until the blocking reason is addressed.";
  }
  return "Recommendations missing a published tier.";
}

function countBy(rows, keyFn) {
  return rows.reduce((counts, row) => {
    const key = keyFn(row);
    counts[key] = (counts[key] || 0) + 1;
    return counts;
  }, {});
}

function formatScore(value) {
  if (value === null || value === undefined || value === "") {
    return "Unavailable";
  }
  const number = Number(value);
  if (Number.isNaN(number)) {
    return String(value);
  }
  return number.toFixed(1);
}

function formatNumber(value) {
  if (value === null || value === undefined || value === "") {
    return "Unavailable";
  }
  const number = Number(value);
  if (Number.isNaN(number)) {
    return String(value);
  }
  return number.toFixed(4);
}

function formatEdge(value) {
  return formatPercent(value);
}

function formatPercent(value) {
  if (value === null || value === undefined || value === "") {
    return "Unavailable";
  }
  const number = Number(value);
  if (Number.isNaN(number)) {
    return String(value);
  }
  return `${(number * 100).toFixed(1)}%`;
}

function formatCurrency(value) {
  if (value === null || value === undefined || value === "") {
    return "Unavailable";
  }
  const number = Number(value);
  if (Number.isNaN(number)) {
    return String(value);
  }
  return `$${number.toFixed(2)}`;
}

function formatHours(value) {
  if (value === null || value === undefined || value === "") {
    return "Unavailable";
  }
  const number = Number(value);
  if (Number.isNaN(number)) {
    return String(value);
  }
  return `${number.toFixed(4)}h`;
}

function formatDateTime(value) {
  if (!value) {
    return "Unavailable";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "short",
  });
}

function freshnessTone(value) {
  return value === "fresh" ? "ok" : "warning";
}

function readableFreshness(value) {
  if (!value) {
    return "Unavailable";
  }
  return humanizeToken(value);
}

function freshnessBannerTitle(value) {
  if (value === "fresh") {
    return "Freshness current";
  }
  if (value === "missing") {
    return "Freshness unavailable";
  }
  if (value === "failed") {
    return "Freshness degraded";
  }
  return "Freshness warning";
}

function freshnessBannerDescription(status) {
  const freshness = status?.freshness_status;

  if (freshness === "fresh") {
    return "Recommendation scoring is current enough for normal review.";
  }
  if (freshness === "missing") {
    return "Recommendation scoring metadata is missing. Treat recommendation views as incomplete until a fresh run lands.";
  }
  if (freshness === "failed") {
    return "The latest scoring output is degraded or incomplete. Review timestamps and warnings before trusting the dashboard.";
  }
  return "Recommendation data is stale or delayed. Review the freshness reasons before relying on the queue.";
}

function humanizeToken(value) {
  if (!value) {
    return "Unavailable";
  }
  return String(value).replaceAll("_", " ");
}

function summarizeObject(value) {
  if (!value) {
    return "Unavailable";
  }
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value)) {
    return `${value.length} items`;
  }
  if (typeof value === "object") {
    return `${Object.keys(value).length} fields`;
  }
  return String(value);
}

function dedupe(values) {
  return Array.from(new Set(values.filter(Boolean)));
}

function toneForTier(value) {
  if (value === "high_conviction") {
    return "ok";
  }
  if (value === "review") {
    return "warning";
  }
  if (value === "blocked") {
    return "danger";
  }
  return "neutral";
}

function toneForStatus(value) {
  if (value === "valid" || value === "fresh" || value === "accepted" || value === "current") {
    return "ok";
  }
  if (value === "risky" || value === "stale" || value === "missing" || value === "review") {
    return "warning";
  }
  if (value === "failed" || value === "blocked" || value === "rejected" || value === "invalid") {
    return "danger";
  }
  return "neutral";
}
