import { useState, useRef, useEffect, useCallback } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import Plot from 'react-plotly.js'
import { sendChat, resetChat } from '../api/client'
import fipsarLogo from '../../../FIPSAR_LOGO.png'

interface Message {
  id: string
  role: 'user' | 'assistant'
  content: string
  charts?: string[]
  loading?: boolean
}

interface SampleCategory {
  category: string
  questions: string[]
}

const SAMPLE_QUESTIONS: SampleCategory[] = [
  {
    category: 'Funnel & Drop Analysis',
    questions: [
      'Give me a full funnel summary - leads to prospects to engagement.',
      'Show me the funnel chart for all time.',
      'Why is there a volume drop? What are the top rejection reasons?',
      'Show me the lead-to-prospect conversion rate.',
    ],
  },
  {
    category: 'Rejections & DQ',
    questions: [
      'Who got dropped and why? List all rejection reasons.',
      'Show me a chart of rejection reasons.',
      'How many NULL_EMAIL rejections are there?',
      'What are the SUPPRESSED and FATAL_ERROR patterns?',
    ],
  },
  {
    category: 'SFMC Journey & Events',
    questions: [
      'What are the SFMC event counts broken down by journey?',
      'Show me an SFMC engagement chart.',
      'How is the Welcome journey performing?',
      'Which journey stage has the highest bounce rate?',
    ],
  },
  {
    category: 'Prospect Trace',
    questions: [
      'Trace prospect with email john.doe@example.com through the pipeline.',
      'Show me the journey history for MASTER_PATIENT_ID P001.',
    ],
  },
  {
    category: 'AI & Scores',
    questions: [
      'What is the conversion and drop-off probability for active prospects?',
      'Show me a conversion segment chart.',
      'Which prospects are at risk of dropping off?',
    ],
  },
  {
    category: 'Trends',
    questions: [
      'Show me the monthly intake trend for 2026.',
      'Plot weekly lead and prospect volume for January 2026.',
    ],
  },
  {
    category: 'Observability',
    questions: [
      'Show pipeline run health for the last 30 days.',
      'Are there any data quality issues I should know about?',
    ],
  },
]

const QUICK_CHIPS = [
  'Give me a full funnel summary',
  'Show me rejection reasons',
  'SFMC engagement by journey',
  'Conversion probability chart',
  'Monthly intake trend 2026',
  'Data quality issues',
]

function useIsMobile(breakpoint = 960) {
  const getMatch = () => (typeof window !== 'undefined' ? window.innerWidth <= breakpoint : false)
  const [isMobile, setIsMobile] = useState(getMatch)

  useEffect(() => {
    const onResize = () => setIsMobile(getMatch())
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [breakpoint])

  return isMobile
}

function TypingDots() {
  return (
    <div style={{ display: 'flex', gap: 5, alignItems: 'center', padding: '8px 4px' }}>
      {[0, 1, 2].map(index => (
        <div key={index} className="typing-dot" />
      ))}
    </div>
  )
}

function ChartEmbed({ json }: { json: string }) {
  const { data, layout } = JSON.parse(json)

  return (
    <div className="chat-plot">
      <Plot
        data={data}
        layout={{
          ...layout,
          autosize: true,
          paper_bgcolor: 'rgba(0,0,0,0)',
          plot_bgcolor: 'rgba(0,0,0,0)',
          margin: { l: 12, r: 12, t: 40, b: 18 },
          font: { family: 'Inter, Arial, sans-serif', size: 11, color: '#445066' },
        }}
        config={{ displayModeBar: false, responsive: true }}
        style={{ width: '100%', minHeight: 250 }}
        useResizeHandler
      />
    </div>
  )
}

function MessageBubble({ msg }: { msg: Message }) {
  const isUser = msg.role === 'user'

  return (
    <div className={`chat-row fade-up${isUser ? ' user' : ''}`}>
      <div className={`chat-avatar ${isUser ? 'user' : 'assistant'}`}>{isUser ? 'U' : 'F'}</div>
      <div className="chat-bubble-stack">
        {msg.loading ? (
          <div className="bubble-ai">
            <TypingDots />
          </div>
        ) : isUser ? (
          <div className="bubble-user">{msg.content}</div>
        ) : (
          <div className="bubble-ai">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.content}</ReactMarkdown>
          </div>
        )}
        {msg.charts?.map((chart, index) => (
          <ChartEmbed key={index} json={chart} />
        ))}
      </div>
    </div>
  )
}

function SampleRail({
  open,
  isMobile,
  expanded,
  onToggleCategory,
  onQuestion,
  onClose,
}: {
  open: boolean
  isMobile: boolean
  expanded: string
  onToggleCategory: (category: string) => void
  onQuestion: (question: string) => void
  onClose: () => void
}) {
  if (!open && isMobile) return null

  return (
    <>
      {isMobile && open && <div className="chat-rail-overlay" onClick={onClose} />}
      <aside className={`chat-rail ${isMobile ? 'mobile' : open ? '' : 'desktop-hidden'}`}>
        <div className="chat-rail-header">
          <div className="chat-rail-title">FAQ</div>
          {isMobile && (
            <button className="chat-rail-close" onClick={onClose} aria-label="Close sample questions">
              x
            </button>
          )}
        </div>

        <div className="chat-rail-scroll">
          {SAMPLE_QUESTIONS.map(category => {
            const isOpen = expanded === category.category
            return (
              <section key={category.category} className="chat-category">
                <button
                  className={`chat-category-trigger${isOpen ? ' active' : ''}`}
                  onClick={() => onToggleCategory(category.category)}
                >
                  <span className={`chat-category-arrow${isOpen ? ' open' : ''}`}>{'>'}</span>
                  <span className="chat-category-copy">
                    <span className="chat-category-title">{category.category}</span>
                  </span>
                </button>

                {isOpen && (
                  <div className="chat-question-list">
                    {category.questions.map(question => (
                      <button
                        key={question}
                        className="chat-question-button"
                        onClick={() => onQuestion(question)}
                      >
                        {question}
                      </button>
                    ))}
                  </div>
                )}
              </section>
            )
          })}
        </div>
      </aside>
    </>
  )
}

function EmptyState({ onQuestion }: { onQuestion: (question: string) => void }) {
  return (
    <div className="chat-empty">
      <div className="chat-empty-panel fade-up">
        <img
          src={fipsarLogo}
          alt="FIPSAR"
          className="chat-empty-orb"
          onError={event => ((event.target as HTMLImageElement).style.display = 'none')}
        />
        <div className="chat-empty-title">FIPSAR Intelligence</div>
        <div className="chat-empty-chips">
          {QUICK_CHIPS.map(chip => (
            <button key={chip} className="chat-chip" onClick={() => onQuestion(chip)}>
              {chip}
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}

export default function Chat() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [sessionId] = useState(() => crypto.randomUUID())
  const [loading, setLoading] = useState(false)
  const [sidebarOpen, setSidebarOpen] = useState(true)
  const [expandedCategory, setExpandedCategory] = useState('Funnel & Drop Analysis')
  const bottomRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const isMobile = useIsMobile()

  useEffect(() => {
    if (isMobile) {
      setSidebarOpen(false)
    } else {
      setSidebarOpen(true)
    }
  }, [isMobile])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const submit = useCallback(async (text: string) => {
    if (!text.trim() || loading) return

    setInput('')
    if (textareaRef.current) textareaRef.current.style.height = 'auto'

    const userMsg: Message = { id: crypto.randomUUID(), role: 'user', content: text }
    const placeholder: Message = { id: crypto.randomUUID(), role: 'assistant', content: '', loading: true }
    setMessages(prev => [...prev, userMsg, placeholder])
    setLoading(true)

    try {
      const res = await sendChat(sessionId, text)
      setMessages(prev =>
        prev.map(message =>
          message.id === placeholder.id
            ? { ...message, content: res.response, charts: res.charts, loading: false }
            : message,
        ),
      )
    } catch {
      setMessages(prev =>
        prev.map(message =>
          message.id === placeholder.id
            ? { ...message, content: 'Error contacting the server. Please ensure the backend is running.', loading: false }
            : message,
        ),
      )
    } finally {
      setLoading(false)
    }
  }, [loading, sessionId])

  const handleQuestion = useCallback((question: string) => {
    submit(question)
    if (isMobile) setSidebarOpen(false)
  }, [isMobile, submit])

  const handleKeyDown = (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault()
      submit(input)
    }
  }

  const handleClear = async () => {
    await resetChat(sessionId)
    setMessages([])
  }

  return (
    <div className="chat-page">
      <SampleRail
        open={sidebarOpen}
        isMobile={isMobile}
        expanded={expandedCategory}
        onToggleCategory={category => setExpandedCategory(current => (current === category ? '' : category))}
        onQuestion={handleQuestion}
        onClose={() => setSidebarOpen(false)}
      />

      <section className="chat-main">
        <header className="chat-header">
          <div className="chat-header-main">
            <button className="chat-toggle" onClick={() => setSidebarOpen(value => !value)} aria-label="Toggle sample questions">
              {sidebarOpen && !isMobile ? '<' : '>'}
            </button>
            <img
              src={fipsarLogo}
              alt="FIPSAR"
              className="chat-brand-logo"
              onError={event => ((event.target as HTMLImageElement).style.display = 'none')}
            />
            <div>
              <div className="chat-header-title">FIPSAR INTELLIGENCE</div>
            </div>
          </div>

          {messages.length > 0 && (
            <button className="btn-ghost" onClick={handleClear}>
              Clear chat
            </button>
          )}
        </header>

        <div className="chat-scroll">
          {messages.length === 0 ? (
            <EmptyState onQuestion={handleQuestion} />
          ) : (
            <div className="chat-thread">
              {messages.map(message => (
                <MessageBubble key={message.id} msg={message} />
              ))}
              <div ref={bottomRef} />
            </div>
          )}
        </div>

        <div className="chat-composer-wrap">
          <div className="chat-composer">
            <div className="chat-input-bar">
              <textarea
                ref={textareaRef}
                value={input}
                onChange={event => setInput(event.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Ask about leads, journeys, engagement, or data quality..."
                rows={1}
                disabled={loading}
                style={{ minHeight: 24 }}
                onInput={event => {
                  const element = event.currentTarget
                  element.style.height = 'auto'
                  element.style.height = `${Math.min(element.scrollHeight, 120)}px`
                }}
              />
              <button className="btn-send" onClick={() => submit(input)} disabled={!input.trim() || loading}>
                {loading ? (
                  <span className="spinner" style={{ width: 16, height: 16, borderWidth: 2 }} />
                ) : (
                  <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
                    <line x1="22" y1="2" x2="11" y2="13" />
                    <polygon points="22 2 15 22 11 13 2 9 22 2" />
                  </svg>
                )}
              </button>
            </div>
            <div className="chat-footnote">FIPSAR Intelligence · Shift+Enter for a new line</div>
          </div>
        </div>
      </section>
    </div>
  )
}
