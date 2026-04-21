import React, { useEffect, useState } from "react";

import { fetchDashboardData } from "./api";

const SORT_OPTIONS = {
  newest: {
    label: "Newest",
    compare: (left, right) => right.opportunity_id - left.opportunity_id,
  },
  edge_desc: {
    label: "Edge high to low",
    compare: (left, right) => compareNullableNumber(right.real_edge, left.real_edge),
  },
  edge_asc: {
    label: "Edge low to high",
    compare: (left, right) => compareNullableNumber(left.real_edge, right.real_edge),
  },
  fill_desc: {
    label: "Fill high to low",
    compare: (left, right) => compareNullableNumber(right.fill_ratio, left.fill_ratio),
  },
  fill_asc: {
    label: "Fill low to high",
    compare: (left, right) => compareNullableNumber(left.fill_ratio, right.fill_ratio),
  },
  event_asc: {
    label: "Event A-Z",
    compare: (left, right) => left.event_id.localeCompare(right.event_id),
  },
};

export default function App() {
  const [dashboard, setDashboard] = useState({
    opportunities: [],
    simulations: [],
    kpi: null,
    systemStatus: null,
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [filterText, setFilterText] = useState("");
  const [sortKey, setSortKey] = useState("newest");

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setLoading(true);
        const nextDashboard = await fetchDashboardData();
        if (!cancelled) {
          setDashboard(nextDashboard);
          setError(null);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError.message);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    load();

    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <DashboardView
      dashboard={dashboard}
      loading={loading}
      error={error}
      filterText={filterText}
      sortKey={sortKey}
      onFilterTextChange={setFilterText}
      onSortKeyChange={setSortKey}
    />
  );
}

export function DashboardView({
  dashboard,
  loading,
  error,
  filterText,
  sortKey,
  onFilterTextChange,
  onSortKeyChange,
}) {
  const opportunities = dashboard.opportunities.filter((opportunity) => {
    const haystack = [
      opportunity.event_id,
      opportunity.validation_status,
      opportunity.simulation_status || "unsimulated",
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(filterText.trim().toLowerCase());
  });
  const sortedOpportunities = [...opportunities].sort(SORT_OPTIONS[sortKey].compare);
  const overview = {
    totalValidated: dashboard.opportunities.length,
    executableCount: dashboard.opportunities.filter(
      (opportunity) => opportunity.simulation_status === "executable",
    ).length,
    rejectedCount: dashboard.opportunities.filter(
      (opportunity) => opportunity.simulation_status === "rejected",
    ).length,
    avgRealEdge: dashboard.kpi?.avg_real_edge || null,
    avgFillRatio: dashboard.kpi?.avg_fill_ratio || null,
  };

  return (
    <div className="shell">
      <header className="hero">
        <div>
          <p className="eyebrow">Research Monitoring</p>
          <h1>Polymarket V1 Dashboard</h1>
          <p className="subtitle">
            Validated opportunities, execution outcomes, KPI context, and pipeline timestamps from
            persisted research data only.
          </p>
        </div>
        <div className="status-chip">{loading ? "Loading" : "Read-only live view"}</div>
      </header>

      {error ? <section className="panel error-panel">API load error: {error}</section> : null}

      <section className="overview-grid">
        <MetricCard label="Validated Opportunities" value={overview.totalValidated} />
        <MetricCard
          label="Executable vs Rejected"
          value={`${overview.executableCount} / ${overview.rejectedCount}`}
        />
        <MetricCard label="Avg Real Edge" value={formatMetric(overview.avgRealEdge)} />
        <MetricCard label="Avg Fill Ratio" value={formatMetric(overview.avgFillRatio)} />
      </section>

      <section className="content-grid">
        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Opportunities</h2>
              <p>Validated opportunities only. Simulation fields stay empty until persisted.</p>
            </div>
            <div className="toolbar">
              <input
                aria-label="Filter opportunities"
                className="input"
                placeholder="Filter event or status"
                value={filterText}
                onChange={(event) => onFilterTextChange(event.target.value)}
              />
              <select
                aria-label="Sort opportunities"
                className="select"
                value={sortKey}
                onChange={(event) => onSortKeyChange(event.target.value)}
              >
                {Object.entries(SORT_OPTIONS).map(([key, option]) => (
                  <option key={key} value={key}>
                    {option.label}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {loading ? (
            <EmptyState message="Loading opportunities…" />
          ) : sortedOpportunities.length === 0 ? (
            <EmptyState message="No validated opportunities found." />
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>event</th>
                    <th>edge</th>
                    <th>fill_ratio</th>
                    <th>status</th>
                    <th>simulation_status</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedOpportunities.map((opportunity) => (
                    <tr key={opportunity.opportunity_id}>
                      <td>{opportunity.event_id}</td>
                      <td>{formatMetric(opportunity.real_edge)}</td>
                      <td>{formatMetric(opportunity.fill_ratio)}</td>
                      <td>{opportunity.validation_status}</td>
                      <td>{opportunity.simulation_status || "pending"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <section className="panel">
          <h2>KPI Snapshot</h2>
          <p>Latest persisted KPI snapshot with sample-size context.</p>
          {loading ? (
            <EmptyState message="Loading KPI snapshot…" />
          ) : dashboard.kpi ? (
            <dl className="stats-list">
              <StatRow label="avg_real_edge" value={formatMetric(dashboard.kpi.avg_real_edge)} />
              <StatRow label="avg_fill_ratio" value={formatMetric(dashboard.kpi.avg_fill_ratio)} />
              <StatRow
                label="false_positive_rate"
                value={formatMetric(dashboard.kpi.false_positive_rate)}
              />
              <StatRow
                label="total_intended_capital"
                value={formatMetric(dashboard.kpi.total_intended_capital)}
              />
              <StatRow
                label="total_executable_capital"
                value={formatMetric(dashboard.kpi.total_executable_capital)}
              />
              <StatRow label="total_opportunities" value={String(dashboard.kpi.total_opportunities)} />
              <StatRow label="valid_opportunities" value={String(dashboard.kpi.valid_opportunities)} />
            </dl>
          ) : (
            <EmptyState message="No KPI snapshot has been persisted yet." />
          )}
        </section>
      </section>

      <section className="content-grid">
        <section className="panel">
          <h2>System Health</h2>
          <p>Stage presence only. No freshness thresholds or inferred health rules.</p>
          {loading ? (
            <EmptyState message="Loading system status…" />
          ) : (
            <dl className="health-list">
              <HealthRow
                label="snapshot"
                timestamp={dashboard.systemStatus?.last_snapshot_time || null}
              />
              <HealthRow
                label="detection"
                timestamp={dashboard.systemStatus?.last_detection_time || null}
              />
              <HealthRow
                label="simulation"
                timestamp={dashboard.systemStatus?.last_simulation_time || null}
              />
              <HealthRow label="kpi" timestamp={dashboard.systemStatus?.last_kpi_time || null} />
            </dl>
          )}
        </section>

        <section className="panel">
          <h2>Recent Simulations</h2>
          <p>Newest persisted execution simulation rows from the API.</p>
          {loading ? (
            <EmptyState message="Loading simulations…" />
          ) : dashboard.simulations.length === 0 ? (
            <EmptyState message="No simulations have been persisted yet." />
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>opportunity_id</th>
                    <th>status</th>
                    <th>net_edge</th>
                    <th>fill_ratio</th>
                    <th>reason</th>
                  </tr>
                </thead>
                <tbody>
                  {dashboard.simulations.slice(0, 6).map((simulation) => (
                    <tr key={`${simulation.opportunity_id}-${simulation.simulation_status}-${simulation.reason}`}>
                      <td>{simulation.opportunity_id}</td>
                      <td>{simulation.simulation_status}</td>
                      <td>{formatMetric(simulation.net_edge)}</td>
                      <td>{formatMetric(simulation.fill_ratio)}</td>
                      <td>{simulation.reason || "n/a"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </section>
    </div>
  );
}

function MetricCard({ label, value }) {
  return (
    <article className="metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </article>
  );
}

function StatRow({ label, value }) {
  return (
    <>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </>
  );
}

function HealthRow({ label, timestamp }) {
  return (
    <div className="health-row">
      <div>
        <dt>{label}</dt>
        <dd>{timestamp || "missing"}</dd>
      </div>
      <span className={`badge ${timestamp ? "ok" : "missing"}`}>{timestamp ? "present" : "missing"}</span>
    </div>
  );
}

function EmptyState({ message }) {
  return <div className="empty-state">{message}</div>;
}

function compareNullableNumber(left, right) {
  const leftValue = left === null ? Number.NEGATIVE_INFINITY : Number(left);
  const rightValue = right === null ? Number.NEGATIVE_INFINITY : Number(right);
  return leftValue - rightValue;
}

function formatMetric(value) {
  if (value === null || value === undefined) {
    return "—";
  }
  return String(value);
}
