import { useState, useEffect, useCallback } from 'react'
import Plot from 'react-plotly.js'
import { getKPIs, getCharts, getFilters, type KPIData, type ChartsData, type FilterOptions } from '../api/client'
import { format } from 'date-fns'

const KPI_CONFIG = [
  { key: 'leads', label: 'Total Leads', gradient: 'linear-gradient(135deg, #20c997 0%, #1fb9a6 100%)', subKey: null as null | string, subFmt: null as null | ((value: number) => string) },
  { key: 'prospects', label: 'Valid Prospects', gradient: 'linear-gradient(135deg, #1d5db5 0%, #2f7ef7 100%)', subKey: 'conversion_rate', subFmt: (value: number) => `${value.toFixed(1)}% conversion` },
  { key: 'invalid', label: 'Invalid Leads', gradient: 'linear-gradient(135deg, #6956d6 0%, #8d72ff 100%)', subKey: null, subFmt: null },
  { key: 'sent', label: 'Emails Sent', gradient: 'linear-gradient(135deg, #f2648b 0%, #ff7f72 100%)', subKey: null, subFmt: null },
  { key: 'opened', label: 'Opened', gradient: 'linear-gradient(135deg, #8b5cf6 0%, #b06df4 100%)', subKey: null, subFmt: null },
  { key: 'clicked', label: 'Clicked', gradient: 'linear-gradient(135deg, #1bbf9d 0%, #26d07c 100%)', subKey: null, subFmt: null },
  { key: 'unsubscribed', label: 'Unsubscribed', gradient: 'linear-gradient(135deg, #f59e0b 0%, #f97316 100%)', subKey: null, subFmt: null },
] as const

const CHART_META = {
  funnel: { title: 'Leads to Prospect Conversion' },
  email: { title: 'Engagement Overview' },
  conversion: { title: 'AI Conversion Signals' },
  segments: { title: 'Prospect Clusters' },
} as const

function formatCompact(value: number): string {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1).replace(/\.0$/, '')}K`
  return value.toLocaleString()
}

function chartValueLabel(value: number): string {
  if (value === 0) return '0'
  if (value >= 1000) return formatCompact(value)
  return value.toLocaleString()
}

function KPISkeleton() {
  return (
    <div className="analytics-kpi-grid">
      {Array.from({ length: 7 }).map((_, index) => (
        <div key={index} className="skeleton" style={{ minHeight: 92 }} />
      ))}
    </div>
  )
}

function ChartPanel({
  title,
  json,
}: {
  title: string
  json: string | null
}) {
  if (!json) {
    return (
      <section className="analytics-chart-panel">
        <div className="analytics-chart-head">
          <div className="analytics-chart-title">{title}</div>
        </div>
        <div className="analytics-chart-empty">No data available for the current filter selection.</div>
      </section>
    )
  }

  const parsed = JSON.parse(json) as { data: Plotly.Data[]; layout: Partial<Plotly.Layout> }
  const normalizedData = parsed.data.map(trace => {
    if (trace.type === 'bar') {
      return {
        ...trace,
        textposition: 'outside',
        cliponaxis: false,
        hoverlabel: { namelength: -1 },
      }
    }

    if (trace.type === 'pie') {
      return {
        ...trace,
        textposition: 'outside',
        textinfo: 'label+percent',
        automargin: true,
        sort: false,
        direction: 'clockwise',
        pull: 0,
      }
    }

    return trace
  }) as Plotly.Data[]

  const layout: Partial<Plotly.Layout> = {
    ...parsed.layout,
    title: undefined,
    autosize: true,
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    margin: { l: 72, r: 40, t: 34, b: 76 },
    font: { family: 'Inter, Arial, sans-serif', size: 11, color: '#445066' },
    legend: {
      ...parsed.layout.legend,
      orientation: 'h',
      x: 0,
      xanchor: 'left',
      y: -0.16,
      yanchor: 'top',
      font: { size: 10, color: '#445066' },
    },
    xaxis: {
      ...parsed.layout.xaxis,
      automargin: true,
      ticklabelposition: 'outside',
      ticks: 'outside',
      ticklen: 4,
      tickfont: { size: 10, color: '#607087' },
      title: parsed.layout.xaxis?.title,
    },
    yaxis: {
      ...parsed.layout.yaxis,
      automargin: true,
      ticklabelposition: 'outside',
      ticks: 'outside',
      ticklen: 4,
      tickfont: { size: 10, color: '#607087' },
      title: parsed.layout.yaxis?.title,
    },
  }

  return (
    <section className="analytics-chart-panel">
      <div className="analytics-chart-head">
        <div className="analytics-chart-title">{title}</div>
      </div>
      <Plot
        data={normalizedData}
        layout={layout}
        config={{ displayModeBar: false, responsive: true }}
        style={{ width: '100%', minHeight: 272 }}
        useResizeHandler
      />
    </section>
  )
}

export default function Analytics() {
  const today = format(new Date(), 'yyyy-MM-dd')
  const defaultStart = format(new Date(2026, 0, 1), 'yyyy-MM-dd')

  const [startDate, setStartDate] = useState(defaultStart)
  const [endDate, setEndDate] = useState(today)
  const [channel, setChannel] = useState('All')
  const [journey, setJourney] = useState('All')
  const [filters, setFilters] = useState<FilterOptions | null>(null)
  const [kpis, setKpis] = useState<KPIData | null>(null)
  const [charts, setCharts] = useState<ChartsData | null>(null)
  const [loadingKPIs, setLoadingKPIs] = useState(false)
  const [loadingCharts, setLoadingCharts] = useState(false)
  const [lastLoaded, setLastLoaded] = useState('')
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getFilters().then(setFilters).catch(() => {})
  }, [])

  const loadData = useCallback(async () => {
    setError(null)
    setLoadingKPIs(true)
    setLoadingCharts(true)

    try {
      const [nextKPIs, nextCharts] = await Promise.all([
        getKPIs(startDate, endDate, channel, journey),
        getCharts(startDate, endDate, channel, journey),
      ])
      setKpis(nextKPIs)
      setCharts(nextCharts)
      setLastLoaded(format(new Date(), 'hh:mm a'))
    } catch {
      setError('Could not load dashboard data. Please check the Snowflake connection.')
    } finally {
      setLoadingKPIs(false)
      setLoadingCharts(false)
    }
  }, [startDate, endDate, channel, journey])

  useEffect(() => {
    loadData()
  }, [loadData])

  const kpiMap = kpis as unknown as Record<string, number>

  return (
    <div className="analytics-page">
      <div className="analytics-shell">
        <section className="analytics-hero">
          <div className="analytics-controls">
            <div className="analytics-title" style={{ fontSize: '0.86rem', marginBottom: 12, letterSpacing: '0.12em', textTransform: 'uppercase' }}>
              Filters
            </div>
            <div className="control-grid">
              <div className="control-field">
                <label className="control-label" style={{ color: 'rgba(207,223,248,0.72)' }}>Start date</label>
                <input className="control-input" type="date" value={startDate} onChange={event => setStartDate(event.target.value)} />
              </div>
              <div className="control-field">
                <label className="control-label" style={{ color: 'rgba(207,223,248,0.72)' }}>End date</label>
                <input className="control-input" type="date" value={endDate} onChange={event => setEndDate(event.target.value)} />
              </div>
              <div className="control-field">
                <label className="control-label" style={{ color: 'rgba(207,223,248,0.72)' }}>Channel</label>
                <select className="filter-select" value={channel} onChange={event => setChannel(event.target.value)}>
                  {(filters?.channels ?? ['All']).map(option => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              </div>
              <div className="control-field">
                <label className="control-label" style={{ color: 'rgba(207,223,248,0.72)' }}>SFMC journey</label>
                <select className="filter-select" value={journey} onChange={event => setJourney(event.target.value)}>
                  {(filters?.journeys ?? ['All']).map(option => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              </div>
            </div>
            <div style={{ marginTop: 10, display: 'flex', justifyContent: 'flex-end' }}>
              <button className="btn-primary" onClick={loadData} disabled={loadingKPIs}>
                {loadingKPIs ? <span className="spinner" style={{ width: 14, height: 14, borderWidth: 2, borderTopColor: '#fff', borderColor: 'rgba(255,255,255,0.3)' }} /> : 'Refresh'}
              </button>
            </div>
          </div>
        </section>

        {error && (
          <div style={{ padding: '12px 16px', borderRadius: 16, background: '#fff1f2', border: '1px solid #fecdd3', color: '#be123c', fontSize: '0.84rem' }}>
            {error}
          </div>
        )}

        {loadingKPIs || !kpis ? (
          <KPISkeleton />
        ) : (
          <section className="analytics-kpi-grid fade-up">
            {KPI_CONFIG.map(item => {
              const value = kpiMap[item.key] ?? 0
              const sub = item.subKey ? item.subFmt?.(kpiMap[item.subKey] ?? 0) ?? null : null
              return (
                <article key={item.key} className="analytics-kpi-card" style={{ background: item.gradient }}>
                  <div className="analytics-kpi-label">{item.label}</div>
                  <div className="analytics-kpi-value">{formatCompact(value)}</div>
                  <div className="analytics-kpi-sub">{sub ?? chartValueLabel(value)}</div>
                </article>
              )
            })}
          </section>
        )}

        <section className="analytics-board">
          {loadingCharts ? (
            <>
              <div className="skeleton" style={{ minHeight: 300 }} />
              <div className="skeleton" style={{ minHeight: 300 }} />
              <div className="skeleton" style={{ minHeight: 300 }} />
              <div className="skeleton" style={{ minHeight: 300 }} />
            </>
          ) : (
            <>
              <ChartPanel {...CHART_META.funnel} json={charts?.funnel ?? null} />
              <ChartPanel {...CHART_META.email} json={charts?.email ?? null} />
              <ChartPanel {...CHART_META.conversion} json={charts?.conversion ?? null} />
              <ChartPanel {...CHART_META.segments} json={charts?.segments ?? null} />
            </>
          )}
        </section>
      </div>
    </div>
  )
}
