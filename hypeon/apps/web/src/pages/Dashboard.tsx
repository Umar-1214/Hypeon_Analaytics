import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Legend,
} from 'recharts'
import { api, apiV1, subscribePipelineEvents } from '../api'

const defaultEnd = new Date()
const defaultStart = new Date('2025-01-01')

function formatDate(s: string) {
  return new Date(s).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: '2-digit' })
}

/* Enterprise chart palette: muted, professional */
const CHART_COLORS = {
  meta: '#2563eb',
  google: '#7c3aed',
  bing: '#c2410c',
  pinterest: '#be185d',
  revenue: '#0d9488',
}
const getChannelColor = (ch: string) => CHART_COLORS[ch as keyof typeof CHART_COLORS] ?? '#64748b'

export default function Dashboard() {
  const [startDate, setStartDate] = useState(defaultStart.toISOString().slice(0, 10))
  const [endDate, setEndDate] = useState(defaultEnd.toISOString().slice(0, 10))
  const [metrics, setMetrics] = useState<Awaited<ReturnType<typeof api.metrics>> | null>(null)
  const [decisions, setDecisions] = useState<Awaited<ReturnType<typeof api.decisions>> | null>(null)
  const [mmm, setMmm] = useState<Awaited<ReturnType<typeof api.mmmStatus>> | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [runStatus, setRunStatus] = useState<string | null>(null)
  const [modelInfo, setModelInfo] = useState<Awaited<ReturnType<typeof apiV1.modelInfo>> | null>(null)

  const fetchData = () => {
    setError(null)
    Promise.all([
      api.metrics({ start_date: startDate, end_date: endDate }),
      api.decisions(),
      api.mmmStatus(),
      apiV1.modelInfo().catch(() => null),
    ])
      .then(([m, d, mm, info]) => {
        setMetrics(m)
        setDecisions(d)
        setMmm(mm)
        setModelInfo(info ?? null)
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    setLoading(true)
    fetchData()
  }, [startDate, endDate])

  useEffect(() => {
    const unsubscribe = subscribePipelineEvents(() => {
      fetchData()
    })
    return unsubscribe
  }, [])

  const handleRunPipeline = () => {
    setRunStatus('Running pipeline…')
    apiV1.engineRun()
      .then((r) => {
        setRunStatus('Pipeline completed. Refreshing…')
        fetchData()
        setRunStatus(`Done: ${r.run_id}`)
        setTimeout(() => setRunStatus(null), 4000)
      })
      .catch((e) => {
        setRunStatus(`Failed: ${e.message}`)
        setTimeout(() => setRunStatus(null), 8000)
      })
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[50vh]">
        <div className="flex flex-col items-center gap-3">
          <div className="h-8 w-8 animate-spin rounded-full border-2 border-surface-200 border-t-brand-600" />
          <p className="text-body-md text-surface-500">Loading dashboard…</p>
        </div>
      </div>
    )
  }
  if (error) {
    return (
      <div className="p-6 max-w-4xl mx-auto">
        <div className="card border-error-200 bg-error-50 p-5 text-error-800">
          <p className="font-medium">Could not load data</p>
          <p className="text-body-md mt-1">
            Is the API running at {import.meta.env.VITE_API_URL || '/api'}? {error}
          </p>
        </div>
      </div>
    )
  }

  const allMetrics = metrics?.metrics || []
  const channels = [...new Set(allMetrics.map((m) => m.channel))].sort()
  const byDate = allMetrics.reduce<
    Record<string, { date: string; revenue: number; [channel: string]: number | string }>
  >((acc, r) => {
    if (!acc[r.date]) {
      acc[r.date] = { date: r.date, revenue: 0, ...Object.fromEntries(channels.map((c) => [c, 0])) }
    }
    const row = acc[r.date] as Record<string, number>
    row[r.channel] = (row[r.channel] ?? 0) + r.spend
    row.revenue = (row.revenue ?? 0) + r.attributed_revenue
    return acc
  }, {})
  const sortedByDate = Object.values(byDate).sort((a, b) => a.date.localeCompare(b.date))
  const chartData = sortedByDate.slice(-21)
  const totalSpend = allMetrics.reduce((s, r) => s + r.spend, 0)
  const totalRev = allMetrics.reduce((s, r) => s + r.attributed_revenue, 0)
  const roas = totalSpend ? (totalRev / totalSpend).toFixed(2) : '—'
  const rangeLabel = metrics?.start_date && metrics?.end_date
    ? `${metrics.start_date} to ${metrics.end_date}`
    : 'Summary'

  return (
    <div className="p-6 lg:p-8 max-w-7xl mx-auto">
      {/* Engine / model info bar */}
      {modelInfo && (modelInfo.run_id || modelInfo.timestamp) && (
        <div className="mb-6 card flex flex-wrap items-center gap-4 px-4 py-3 text-body-sm bg-surface-50/80 border-surface-200">
          <span className="kpi-label text-surface-600">Engine</span>
          {modelInfo.run_id && (
            <span className="text-surface-700 font-mono text-caption">Run: {modelInfo.run_id}</span>
          )}
          {modelInfo.timestamp && (
            <span className="text-surface-500 text-caption">
              Updated: {new Date(modelInfo.timestamp).toLocaleString()}
            </span>
          )}
          {modelInfo.mta_version && (
            <span className="text-surface-500 text-caption">MTA {modelInfo.mta_version}</span>
          )}
          {modelInfo.mmm_version && (
            <span className="text-surface-500 text-caption">MMM {modelInfo.mmm_version}</span>
          )}
        </div>
      )}

      {/* Page header + filters */}
      <div className="flex flex-wrap items-center justify-between gap-6 mb-8">
        <div>
          <h1 className="page-title">Data & Model Health</h1>
          <p className="page-subtitle">{rangeLabel}</p>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <label className="flex items-center gap-2 text-body-sm">
            <span className="text-surface-500">From</span>
            <input
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="input-base w-auto min-w-[140px] py-2"
            />
          </label>
          <label className="flex items-center gap-2 text-body-sm">
            <span className="text-surface-500">To</span>
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="input-base w-auto min-w-[140px] py-2"
            />
          </label>
          <button
            type="button"
            onClick={handleRunPipeline}
            disabled={!!runStatus && runStatus.startsWith('Running')}
            className="btn-primary"
          >
            {runStatus?.startsWith('Running') ? (
              <span className="flex items-center gap-2">
                <span className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-white border-t-transparent" />
                Running…
              </span>
            ) : (
              'Run pipeline'
            )}
          </button>
          {runStatus && !runStatus.startsWith('Running') && (
            <span className="text-body-sm text-surface-600">{runStatus}</span>
          )}
        </div>
      </div>

      {allMetrics.length === 0 && (
        <div className="mb-8 card border-warning-200 bg-warning-50 p-5 text-warning-800 text-body-md">
          No metrics in this date range. Click <strong>Run pipeline</strong> to ingest data and compute
          metrics (uses sample data in <code className="bg-warning-100 px-1 rounded-badge">data/raw/</code>).
          Set dates that include your data range (e.g. 2025-01-01 to 2025-03-31 for the 90-day sample).
        </div>
      )}

      {/* KPI cards */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-5 mb-8">
        <div className="card p-5">
          <p className="kpi-label">Total spend</p>
          <p className="kpi-value">
            ${totalSpend.toLocaleString(undefined, { minimumFractionDigits: 2 })}
          </p>
        </div>
        <div className="card p-5">
          <p className="kpi-label">Attributed revenue</p>
          <p className="kpi-value">
            ${totalRev.toLocaleString(undefined, { minimumFractionDigits: 2 })}
          </p>
        </div>
        <div className="card p-5">
          <p className="kpi-label">ROAS</p>
          <p className="kpi-value">{roas}</p>
        </div>
      </div>

      {/* Chart */}
      <div className="card p-5 mb-8">
        <h2 className="font-display font-semibold text-display-sm text-surface-900 mb-4">
          Spend & revenue by day
        </h2>
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData} margin={{ top: 12, right: 12, left: 12, bottom: 12 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
              <XAxis
                dataKey="date"
                tickFormatter={formatDate}
                fontSize={11}
                tick={{ fill: '#64748b' }}
                axisLine={{ stroke: '#e2e8f0' }}
              />
              <YAxis
                fontSize={11}
                tickFormatter={(v) => `$${v}`}
                tick={{ fill: '#64748b' }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip
                formatter={(v: number) =>
                  `$${Number(v).toLocaleString(undefined, { minimumFractionDigits: 2 })}`
                }
                labelFormatter={formatDate}
                contentStyle={{
                  borderRadius: '6px',
                  border: '1px solid #e2e8f0',
                  boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.07)',
                }}
              />
              <Legend wrapperStyle={{ fontSize: '12px' }} />
              {channels.map((ch) => (
                <Bar
                  key={ch}
                  dataKey={ch}
                  name={`${ch} spend`}
                  fill={getChannelColor(ch)}
                  radius={[4, 4, 0, 0]}
                />
              ))}
              <Bar dataKey="revenue" name="Revenue" fill={CHART_COLORS.revenue} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Quick links */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="card card-hover p-5">
          <div className="flex items-center justify-between mb-3">
            <h2 className="font-display font-semibold text-display-sm text-surface-900">Decisions</h2>
            <Link
              to="/decisions"
              className="text-body-sm font-medium text-brand-600 hover:text-brand-700 focus-visible:underline"
            >
              View all →
            </Link>
          </div>
          <p className="text-body-md text-surface-600">
            {decisions?.total ?? 0} total ({decisions?.decisions.filter((d) => d.status === 'pending').length ?? 0}{' '}
            pending)
          </p>
          {mmm && (
            <p className="text-caption text-surface-500 mt-2">
              MMM: {mmm.status === 'completed' ? mmm.last_run_id : mmm.status}
            </p>
          )}
        </div>
        <div className="card card-hover p-5 border-brand-200 bg-brand-50/50">
          <div className="flex items-center justify-between mb-3">
            <h2 className="font-display font-semibold text-display-sm text-brand-900">Ask in plain language</h2>
            <Link
              to="/copilot"
              className="text-body-sm font-medium text-brand-700 hover:text-brand-800 focus-visible:underline"
            >
              Open Copilot →
            </Link>
          </div>
          <p className="text-body-md text-brand-800">
            Get answers like “How are we doing?” or “Where should we spend?” without opening spreadsheets.
          </p>
        </div>
      </div>
    </div>
  )
}
