import { useState, useRef, useEffect, useCallback } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import Plot from 'react-plotly.js'
import { sendFREL, resetFREL, checkEmail } from '../api/client'

interface FRELMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  charts?: string[]
  loading?: boolean
  email_sent?: boolean
  email_meta?: { to: string; subject: string; sent_at: string; charts_attached: number } | null
}

const EXAMPLES = [
  ['Send me the funnel report for January 2026',         'Queries Snowflake → emails full report'],
  ['Email me the rejection analysis with a chart',       'Queries + chart → emails with embedded image'],
  ['Send the SFMC journey performance report',           'Queries all journeys → emails table'],
  ['Give me the conversion analysis and email it',       'Shows in UI + emails'],
]

function TypingDots() {
  return (
    <div style={{ display: 'flex', gap: '5px', alignItems: 'center', padding: '8px 4px' }}>
      {[0, 1, 2].map(i => <div key={i} className="typing-dot" style={{ animationDelay: `${i * 0.2}s` }} />)}
    </div>
  )
}

function EmailBadge({ meta }: { meta: NonNullable<FRELMessage['email_meta']> }) {
  return (
    <div style={{
      background: 'linear-gradient(135deg, #052e16, #166534)',
      border: '1px solid #22c55e', borderRadius: '12px',
      padding: '14px 18px', marginTop: '10px',
      boxShadow: '0 4px 16px rgba(22,101,52,0.22)',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '7px', marginBottom: '8px' }}>
        <span style={{ fontSize: '1rem' }}>✅</span>
        <span style={{ color: '#4ade80', fontSize: '0.78rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.8px' }}>
          Email Sent Successfully
        </span>
      </div>
      <div style={{ color: '#bbf7d0', fontSize: '0.84rem', lineHeight: 1.7 }}>
        <b style={{ color: '#86efac' }}>To:</b> {meta.to}<br />
        <b style={{ color: '#86efac' }}>Subject:</b> {meta.subject}<br />
        <b style={{ color: '#86efac' }}>Sent at:</b> {meta.sent_at}
        {meta.charts_attached > 0 && ` · ${meta.charts_attached} chart(s) embedded`}
      </div>
    </div>
  )
}

export default function FREL() {
  const [messages, setMessages]   = useState<FRELMessage[]>([])
  const [input, setInput]         = useState('')
  const [sessionId]               = useState(() => crypto.randomUUID())
  const [loading, setLoading]     = useState(false)
  const [emailInfo, setEmailInfo] = useState<{ configured: boolean; to_address: string } | null>(null)
  const [showExamples, setShowExamples] = useState(false)
  const bottomRef                 = useRef<HTMLDivElement>(null)
  const textareaRef               = useRef<HTMLTextAreaElement>(null)

  useEffect(() => { checkEmail().then(setEmailInfo).catch(() => {}) }, [])
  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth' }) }, [messages])

  const submit = useCallback(async (text: string) => {
    if (!text.trim() || loading) return
    setInput('')

    const userMsg: FRELMessage = { id: crypto.randomUUID(), role: 'user', content: text }
    const placeholder: FRELMessage = { id: crypto.randomUUID(), role: 'assistant', content: '', loading: true }
    setMessages(prev => [...prev, userMsg, placeholder])
    setLoading(true)

    try {
      const res = await sendFREL(sessionId, text)
      setMessages(prev => prev.map(m =>
        m.id === placeholder.id
          ? { ...m, content: res.response, charts: res.charts, loading: false, email_sent: res.email_sent, email_meta: res.email_meta }
          : m
      ))
    } catch {
      setMessages(prev => prev.map(m =>
        m.id === placeholder.id
          ? { ...m, content: 'Error contacting server. Check backend connection.', loading: false }
          : m
      ))
    } finally {
      setLoading(false)
    }
  }, [sessionId, loading])

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); submit(input) }
  }

  const handleClear = async () => {
    await resetFREL(sessionId)
    setMessages([])
  }

  const hasMessages = messages.length > 0

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', background: 'var(--bg)' }}>
      {/* Header */}
      <div style={{
        background: 'linear-gradient(135deg, #0f0520 0%, #2a0a5e 52%, #3d1a9e 100%)',
        padding: '14px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        boxShadow: '0 2px 12px rgba(15,5,32,0.30)', flexShrink: 0,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <div style={{ width: '36px', height: '36px', borderRadius: '50%', background: 'rgba(255,255,255,0.10)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '1.1rem' }}>📧</div>
          <div>
            <div style={{ fontSize: '0.95rem', fontWeight: 800, color: '#fff' }}>FREL Agent</div>
            <div style={{ fontSize: '0.7rem', color: '#8a6ac0', marginTop: '2px' }}>
              Full data intelligence + one-click email delivery
              {emailInfo?.to_address && <> · <b style={{ color: '#c4a8f8' }}>{emailInfo.to_address}</b></>}
            </div>
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          {emailInfo && (
            <span className={`badge ${emailInfo.configured ? 'badge-green' : 'badge-amber'}`}>
              {emailInfo.configured ? '✓ Email ready' : '⚠ Email not configured'}
            </span>
          )}
          {hasMessages && (
            <button onClick={handleClear} className="btn-ghost" style={{ color: '#8a6ac0', borderColor: 'rgba(255,255,255,0.15)', background: 'rgba(255,255,255,0.06)' }}>
              ↺ Clear
            </button>
          )}
        </div>
      </div>

      {/* Example requests bar */}
      <div style={{ padding: '10px 24px', background: 'white', borderBottom: '1px solid var(--border)', flexShrink: 0 }}>
        <button
          onClick={() => setShowExamples(!showExamples)}
          style={{ fontSize: '0.76rem', color: 'var(--blue)', fontWeight: 600, background: 'none', border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '5px' }}
        >
          <span style={{ transform: showExamples ? 'rotate(180deg)' : 'none', display: 'inline-block', transition: 'transform 0.2s' }}>▾</span>
          Example requests
        </button>
        {showExamples && (
          <div className="fade-up" style={{ marginTop: '10px', display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
            {EXAMPLES.map(([req, desc]) => (
              <div
                key={req}
                onClick={() => { submit(req); setShowExamples(false) }}
                style={{
                  padding: '10px 14px', borderRadius: '10px', border: '1px solid var(--border)',
                  cursor: 'pointer', transition: 'all 0.15s', background: '#fafbfd',
                }}
                onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = '#eff6ff'; (e.currentTarget as HTMLElement).style.borderColor = '#bfdbfe' }}
                onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = '#fafbfd'; (e.currentTarget as HTMLElement).style.borderColor = 'var(--border)' }}
              >
                <div style={{ fontSize: '0.78rem', fontWeight: 600, color: 'var(--navy)', marginBottom: '3px' }}>{req}</div>
                <div style={{ fontSize: '0.7rem', color: 'var(--muted)' }}>{desc}</div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Messages */}
      <div style={{ flex: 1, overflowY: 'auto', padding: hasMessages ? '20px 10%' : '0' }}>
        {!hasMessages ? (
          <div style={{ height: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', color: 'var(--muted)', textAlign: 'center', gap: '16px', padding: '40px 20px' }}>
            <div style={{ width: '70px', height: '70px', borderRadius: '50%', background: 'linear-gradient(135deg, #f5f0ff, #e8d8ff)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '1.8rem', boxShadow: '0 4px 20px rgba(124,58,237,0.12)' }}>📬</div>
            <div>
              <div style={{ fontSize: '1rem', fontWeight: 700, color: '#475569', marginBottom: '8px' }}>Ready to assist</div>
              <div style={{ fontSize: '0.84rem', lineHeight: 1.7, maxWidth: '420px' }}>
                Ask any question about your prospect data.<br />
                Say <b style={{ color: 'var(--purple)' }}>"send it over email"</b> and the report will be delivered instantly.
              </div>
            </div>
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '18px', paddingBottom: '20px' }}>
            {messages.map(msg => (
              <div key={msg.id} className="fade-up" style={{ display: 'flex', gap: '10px', flexDirection: msg.role === 'user' ? 'row-reverse' : 'row', alignItems: 'flex-start' }}>
                <div style={{
                  width: '32px', height: '32px', borderRadius: '50%', flexShrink: 0,
                  display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.8rem', fontWeight: 700,
                  background: msg.role === 'user'
                    ? 'linear-gradient(135deg, #2a0a5e, #3d1a9e)'
                    : 'linear-gradient(135deg, #f5f0ff, #e8d8ff)',
                  color: msg.role === 'user' ? '#fff' : '#7c3aed',
                  boxShadow: '0 1px 6px rgba(124,58,237,0.15)',
                }}>
                  {msg.role === 'user' ? 'U' : 'F'}
                </div>
                <div style={{ maxWidth: '82%', display: 'flex', flexDirection: 'column', gap: '8px' }}>
                  {msg.loading
                    ? <div className="bubble-ai"><TypingDots /></div>
                    : msg.role === 'user'
                      ? <div className="bubble-user" style={{ background: 'linear-gradient(135deg, #2a0a5e, #3d1a9e)' }}>{msg.content}</div>
                      : <div className="bubble-ai" style={{ borderLeftColor: '#7c3aed' }}>
                          <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.content}</ReactMarkdown>
                        </div>
                  }
                  {msg.email_sent && msg.email_meta && <EmailBadge meta={msg.email_meta} />}
                  {msg.charts?.map((c, i) => {
                    const { data, layout } = JSON.parse(c)
                    return (
                      <div key={i} style={{ borderRadius: '12px', overflow: 'hidden', border: '1px solid var(--border)' }}>
                        <Plot data={data} layout={{ ...layout, autosize: true, paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', margin: { l: 10, r: 10, t: 44, b: 10 } }} config={{ displayModeBar: false, responsive: true }} style={{ width: '100%', minHeight: '240px' }} useResizeHandler />
                      </div>
                    )
                  })}
                </div>
              </div>
            ))}
            <div ref={bottomRef} />
          </div>
        )}
      </div>

      {/* Input */}
      <div style={{ padding: '14px 10%', flexShrink: 0, background: 'var(--bg)', borderTop: '1px solid var(--border)' }}>
        <div className="chat-input-bar" style={{ borderColor: '#ddd0ff' }}>
          <textarea
            ref={textareaRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask a question or say 'send me the funnel report over email'…"
            rows={1}
            disabled={loading}
            style={{ minHeight: '24px' }}
            onInput={e => {
              const el = e.currentTarget
              el.style.height = 'auto'
              el.style.height = Math.min(el.scrollHeight, 120) + 'px'
            }}
          />
          <button
            className="btn-send"
            onClick={() => submit(input)}
            disabled={!input.trim() || loading}
            style={{ background: 'linear-gradient(135deg, #2a0a5e, #7c3aed)' }}
          >
            {loading
              ? <span className="spinner" style={{ width: 16, height: 16, borderWidth: 2 }} />
              : <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
            }
          </button>
        </div>
        <div style={{ textAlign: 'center', marginTop: '8px', fontSize: '0.68rem', color: '#b0bdd0' }}>
          FREL Agent · 18 tools · Shift+Enter for new line
        </div>
      </div>
    </div>
  )
}
