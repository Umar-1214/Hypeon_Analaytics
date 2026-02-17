import { useEffect, useState } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts'
import { api } from '../api'

const defaultStart = '2025-01-01'
const defaultEnd = '2025-01-31'

const CHART_COLORS = {
  attribution: '#2563eb',
  mmm: '#7c3aed',
}

export default function Report() {
  const [report, setReport] = useState<Awaited<ReturnType<typeof api.reportAttributionMmm>> | null>(null)
  const [start, setStart] = useState(defaultStart)
  const [end, setEnd] = useState(defaultEnd)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    api
      .reportAttributionMmm({ start_date: start, end_date: end })
      .then(setReport)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [start, end])

  const chartData =
    report?.channels.map((ch) => ({
      channel: ch,
      attribution: (report.attribution_share[ch] ?? 0) * 100,
      mmm: (report.mmm_share[ch] ?? 0) * 100,
    })) ?? []

  return (
    <div className="p-6 lg:p-8 max-w-7xl mx-auto">
      <h1 className="page-title">Attribution vs MMM</h1>
      <p className="page-subtitle mb-6">
        Compare MTA attribution share vs MMM contribution share; instability when they disagree.
      </p>

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
      ) : report ? (
        <>
          {report.channels.length === 0 ||
          report.channels.every(
            (ch) => ((report.attribution_share[ch] ?? 0) + (report.mmm_share[ch] ?? 0)) === 0
          ) ? (
            <div className="mb-6 card border-warning-200 bg-warning-50 p-5 text-warning-800 text-body-md">
              <strong>No data in this date range.</strong> Sample data covers 2025-01-01 to 2025-03-31.
              Run the pipeline from the Overview, then use a range within 2025-01-01–2025-03-31.
            </div>
          ) : null}
          <div className="flex flex-wrap gap-4 mb-6">
            <div className="card px-4 py-2.5 text-body-md text-surface-800">
              Disagreement score: <strong className="tabular-nums">{report.disagreement_score.toFixed(3)}</strong>
            </div>
            {report.instability_flagged && (
              <div className="px-4 py-2.5 rounded-input bg-warning-100 text-warning-800 text-body-md font-medium">
                Instability flagged
              </div>
            )}
          </div>

          <div className="card p-5 mb-6">
            <h2 className="font-display font-semibold text-display-sm text-surface-900 mb-4">
              Share by channel (%)
            </h2>
            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData} margin={{ top: 12, right: 12, left: 12, bottom: 12 }}>
                  <XAxis
                    dataKey="channel"
                    fontSize={12}
                    tick={{ fill: '#64748b' }}
                    axisLine={{ stroke: '#e2e8f0' }}
                  />
                  <YAxis
                    fontSize={11}
                    tickFormatter={(v) => `${v}%`}
                    tick={{ fill: '#64748b' }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <Tooltip
                    formatter={(v: number) => [`${v.toFixed(1)}%`, '']}
                    contentStyle={{
                      borderRadius: '6px',
                      border: '1px solid #e2e8f0',
                      boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.07)',
                    }}
                  />
                  <Legend wrapperStyle={{ fontSize: '12px' }} />
                  <Bar
                    dataKey="attribution"
                    name="Attribution share"
                    fill={CHART_COLORS.attribution}
                    radius={[4, 4, 0, 0]}
                  />
                  <Bar dataKey="mmm" name="MMM share" fill={CHART_COLORS.mmm} radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-body-md">
                <thead>
                  <tr className="table-header">
                    <th className="py-3 px-4">Channel</th>
                    <th className="py-3 px-4 text-right">Attribution %</th>
                    <th className="py-3 px-4 text-right">MMM %</th>
                  </tr>
                </thead>
                <tbody>
                  {report.channels.map((ch) => (
                    <tr key={ch} className="table-row-hover">
                      <td className="table-cell capitalize">{ch}</td>
                      <td className="table-cell text-right tabular-nums">
                        {((report.attribution_share[ch] ?? 0) * 100).toFixed(1)}%
                      </td>
                      <td className="table-cell text-right tabular-nums">
                        {((report.mmm_share[ch] ?? 0) * 100).toFixed(1)}%
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      ) : null}
    </div>
  )
}
