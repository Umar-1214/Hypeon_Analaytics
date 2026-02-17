import { useEffect, useState } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Legend,
} from 'recharts'
import { api, UnifiedMetricRow } from '../api'

const defaultEnd = new Date()
const defaultStart = new Date(defaultEnd)
defaultStart.setDate(defaultStart.getDate() - 400)

function formatDate(s: string) {
  return new Date(s).toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
}

const CHART_COLORS = {
  metaSpend: '#2563eb',
  googleSpend: '#7c3aed',
  metaRevenue: '#0d9488',
  googleRevenue: '#c2410c',
}

export default function Metrics() {
  const [metrics, setMetrics] = useState<UnifiedMetricRow[]>([])
  const [start, setStart] = useState(defaultStart.toISOString().slice(0, 10))
  const [end, setEnd] = useState(defaultEnd.toISOString().slice(0, 10))
  const [channel, setChannel] = useState<string>('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    api
      .metrics({ start_date: start, end_date: end, channel: channel || undefined })
      .then((r) => setMetrics(r.metrics))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [start, end, channel])

  const byDate = metrics.reduce<Record<string, Record<string, UnifiedMetricRow>>>((acc, r) => {
    if (!acc[r.date]) acc[r.date] = {}
    acc[r.date][r.channel] = r
    return acc
  }, {})
  const chartData = Object.entries(byDate)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, ch]) => ({
      date,
      metaSpend: ch.meta?.spend ?? 0,
      googleSpend: ch.google?.spend ?? 0,
      metaRevenue: ch.meta?.attributed_revenue ?? 0,
      googleRevenue: ch.google?.attributed_revenue ?? 0,
    }))

  return (
    <div className="p-6 lg:p-8 max-w-7xl mx-auto">
      <h1 className="page-title">Metrics</h1>
      <p className="page-subtitle mb-6">Unified daily spend, revenue, and ROAS by channel</p>

      <div className="flex flex-wrap gap-4 mb-6">
        <label className="flex items-center gap-2">
          <span className="text-body-sm text-surface-600">From</span>
          <input
            type="date"
            value={start}
            onChange={(e) => setStart(e.target.value)}
            className="input-base w-auto min-w-[140px] py-2"
          />
        </label>
        <label className="flex items-center gap-2">
          <span className="text-body-sm text-surface-600">To</span>
          <input
            type="date"
            value={end}
            onChange={(e) => setEnd(e.target.value)}
            className="input-base w-auto min-w-[140px] py-2"
          />
        </label>
        <label className="flex items-center gap-2">
          <span className="text-body-sm text-surface-600">Channel</span>
          <select
            value={channel}
            onChange={(e) => setChannel(e.target.value)}
            className="input-base w-auto min-w-[120px] py-2"
          >
            <option value="">All</option>
            <option value="meta">Meta</option>
            <option value="google">Google</option>
          </select>
        </label>
      </div>

      {error && (
        <div className="mb-6 card border-error-200 bg-error-50 p-5 text-error-800 text-body-md">
          {error}
        </div>
      )}

      {loading ? (
        <div className="flex items-center justify-center py-16">
          <div className="flex flex-col items-center gap-3">
            <div className="h-8 w-8 animate-spin rounded-full border-2 border-surface-200 border-t-brand-600" />
            <p className="text-body-md text-surface-500">Loading…</p>
          </div>
        </div>
      ) : (
        <>
          <div className="card p-5 mb-6">
            <h2 className="font-display font-semibold text-display-sm text-surface-900 mb-4">
              Spend & revenue over time
            </h2>
            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData} margin={{ top: 12, right: 12, left: 12, bottom: 12 }}>
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
                  <Line
                    type="monotone"
                    dataKey="metaSpend"
                    name="Meta spend"
                    stroke={CHART_COLORS.metaSpend}
                    dot={false}
                    strokeWidth={2}
                  />
                  <Line
                    type="monotone"
                    dataKey="googleSpend"
                    name="Google spend"
                    stroke={CHART_COLORS.googleSpend}
                    dot={false}
                    strokeWidth={2}
                  />
                  <Line
                    type="monotone"
                    dataKey="metaRevenue"
                    name="Meta revenue"
                    stroke={CHART_COLORS.metaRevenue}
                    dot={false}
                    strokeWidth={2}
                    strokeDasharray="4 4"
                  />
                  <Line
                    type="monotone"
                    dataKey="googleRevenue"
                    name="Google revenue"
                    stroke={CHART_COLORS.googleRevenue}
                    dot={false}
                    strokeWidth={2}
                    strokeDasharray="4 4"
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card overflow-hidden">
            <div className="px-5 py-4 border-b border-surface-200">
              <h2 className="font-display font-semibold text-display-sm text-surface-900">Table</h2>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-body-md">
                <thead>
                  <tr className="table-header">
                    <th className="py-3 px-4">Date</th>
                    <th className="py-3 px-4">Channel</th>
                    <th className="py-3 px-4 text-right">Spend</th>
                    <th className="py-3 px-4 text-right">Revenue</th>
                    <th className="py-3 px-4 text-right">ROAS</th>
                  </tr>
                </thead>
                <tbody>
                  {metrics.slice(0, 100).map((r) => (
                    <tr key={`${r.date}-${r.channel}`} className="table-row-hover">
                      <td className="table-cell">{formatDate(r.date)}</td>
                      <td className="table-cell capitalize">{r.channel}</td>
                      <td className="table-cell text-right tabular-nums">
                        ${r.spend.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                      </td>
                      <td className="table-cell text-right tabular-nums">
                        ${r.attributed_revenue.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                      </td>
                      <td className="table-cell text-right tabular-nums">
                        {r.roas != null ? r.roas.toFixed(2) : '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {metrics.length > 100 && (
              <p className="px-5 py-3 text-body-sm text-surface-500 border-t border-surface-100">
                Showing first 100 rows
              </p>
            )}
          </div>
        </>
      )}
    </div>
  )
}
