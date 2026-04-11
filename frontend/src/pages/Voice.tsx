import { useState, useRef, useEffect } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import Plot from 'react-plotly.js'
import { transcribeAudio, voiceChat } from '../api/client'

interface VoiceMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  audio_b64?: string | null
  charts?: string[]
}

const VOICE_OPTIONS = ['alloy', 'echo', 'fable', 'onyx', 'nova', 'shimmer']

export default function Voice() {
  const [messages, setMessages]     = useState<VoiceMessage[]>([])
  const [recording, setRecording]   = useState(false)
  const [processing, setProcessing] = useState(false)
  const [status, setStatus]         = useState<string>('')
  const [voice, setVoice]           = useState('alloy')
  const [speed, setSpeed]           = useState(1.0)
  const [sessionId]                 = useState(() => crypto.randomUUID())
  const mediaRef                    = useRef<MediaRecorder | null>(null)
  const chunksRef                   = useRef<Blob[]>([])
  const bottomRef                   = useRef<HTMLDivElement>(null)

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth' }) }, [messages])

  const startRecording = async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
      const mr = new MediaRecorder(stream)
      chunksRef.current = []
      mr.ondataavailable = e => chunksRef.current.push(e.data)
      mr.onstop = async () => {
        stream.getTracks().forEach(t => t.stop())
        const blob = new Blob(chunksRef.current, { type: 'audio/webm' })
        await processAudio(blob)
      }
      mr.start()
      mediaRef.current = mr
      setRecording(true)
      setStatus('Recording… click again to stop')
    } catch {
      setStatus('Microphone access denied.')
    }
  }

  const stopRecording = () => {
    mediaRef.current?.stop()
    setRecording(false)
    setStatus('')
  }

  const processAudio = async (blob: Blob) => {
    setProcessing(true)
    setStatus('Transcribing…')
    try {
      const transcript = await transcribeAudio(blob)
      if (!transcript) { setStatus('Could not transcribe — please try again.'); return }

      setMessages(prev => [...prev, { id: crypto.randomUUID(), role: 'user', content: transcript }])
      setStatus('Thinking…')

      const res = await voiceChat(sessionId, transcript, voice, speed)
      const msg: VoiceMessage = {
        id: crypto.randomUUID(),
        role: 'assistant',
        content: res.response,
        audio_b64: res.audio_b64,
        charts: res.charts,
      }
      setMessages(prev => [...prev, msg])
      setStatus('')
    } catch {
      setStatus('Error processing audio. Check backend connection.')
    } finally {
      setProcessing(false)
    }
  }

  const handleRecord = () => recording ? stopRecording() : startRecording()

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', background: 'var(--bg)' }}>
      {/* Header */}
      <div style={{
        background: 'linear-gradient(135deg, #0a0f1e 0%, #0d2a5e 52%, #1a4a9e 100%)',
        padding: '14px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        flexShrink: 0, boxShadow: '0 2px 12px rgba(7,15,34,0.20)',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <div style={{ width: '36px', height: '36px', borderRadius: '50%', background: 'rgba(255,255,255,0.10)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '1.1rem' }}>🎤</div>
          <div>
            <div style={{ fontSize: '0.95rem', fontWeight: 800, color: '#fff' }}>Voice Assistant</div>
            <div style={{ fontSize: '0.7rem', color: '#5a82c0' }}>Whisper transcription · LangGraph agent · TTS response</div>
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <select value={voice} onChange={e => setVoice(e.target.value)} className="filter-select" style={{ background: 'rgba(255,255,255,0.08)', color: '#a8c8ff', borderColor: 'rgba(255,255,255,0.15)' }}>
            {VOICE_OPTIONS.map(v => <option key={v}>{v}</option>)}
          </select>
          {messages.length > 0 && (
            <button onClick={() => setMessages([])} className="btn-ghost" style={{ color: '#6a90c0', borderColor: 'rgba(255,255,255,0.15)', background: 'rgba(255,255,255,0.06)' }}>
              ↺ Clear
            </button>
          )}
        </div>
      </div>

      {/* Conversation */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '20px 10%' }}>
        {messages.length === 0 && !processing ? (
          <div style={{ height: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', color: 'var(--muted)', textAlign: 'center', gap: '16px' }}>
            <div style={{ width: '70px', height: '70px', borderRadius: '50%', background: 'linear-gradient(135deg, #f0f4ff, #dce8ff)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '2rem', boxShadow: '0 4px 20px rgba(13,42,94,0.10)' }}>🎤</div>
            <div>
              <div style={{ fontSize: '1rem', fontWeight: 700, color: '#475569', marginBottom: '6px' }}>Your conversation will appear here</div>
              <div style={{ fontSize: '0.82rem' }}>Click the microphone below to start recording</div>
            </div>
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px', paddingBottom: '20px' }}>
            {messages.map(msg => (
              <div key={msg.id} className="fade-up" style={{ display: 'flex', gap: '10px', flexDirection: msg.role === 'user' ? 'row-reverse' : 'row', alignItems: 'flex-start' }}>
                <div style={{
                  width: '32px', height: '32px', borderRadius: '50%', flexShrink: 0,
                  display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.9rem',
                  background: msg.role === 'user'
                    ? 'linear-gradient(135deg, #0d2a5e, #1a4a9e)'
                    : 'linear-gradient(135deg, #f0f4ff, #e8eeff)',
                  color: msg.role === 'user' ? '#fff' : '#1a4a9e',
                  boxShadow: '0 1px 6px rgba(13,42,94,0.15)',
                }}>
                  {msg.role === 'user' ? '🎙' : 'F'}
                </div>
                <div style={{ maxWidth: '80%', display: 'flex', flexDirection: 'column', gap: '8px' }}>
                  {msg.role === 'user'
                    ? <div className="bubble-user">{msg.content}</div>
                    : <div className="bubble-ai">
                        <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.content}</ReactMarkdown>
                      </div>
                  }
                  {msg.audio_b64 && (
                    <audio controls src={`data:audio/mpeg;base64,${msg.audio_b64}`} style={{ width: '100%', borderRadius: '8px', marginTop: '4px' }} />
                  )}
                  {msg.charts?.map((c, i) => {
                    const { data, layout } = JSON.parse(c)
                    return (
                      <div key={i} style={{ borderRadius: '12px', overflow: 'hidden', border: '1px solid var(--border)', marginTop: '8px' }}>
                        <Plot data={data} layout={{ ...layout, autosize: true, paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', margin: { l: 10, r: 10, t: 44, b: 10 } }} config={{ displayModeBar: false, responsive: true }} style={{ width: '100%', minHeight: '240px' }} useResizeHandler />
                      </div>
                    )
                  })}
                </div>
              </div>
            ))}
            {processing && (
              <div className="fade-in" style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
                <div style={{ width: '32px', height: '32px', borderRadius: '50%', background: 'linear-gradient(135deg, #f0f4ff, #e8eeff)', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#1a4a9e', fontSize: '0.8rem' }}>F</div>
                <div className="bubble-ai" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span className="spinner" /><span style={{ color: 'var(--muted)', fontSize: '0.84rem' }}>{status || 'Processing…'}</span>
                </div>
              </div>
            )}
            <div ref={bottomRef} />
          </div>
        )}
      </div>

      {/* Record section */}
      <div style={{ padding: '16px 24px 24px', borderTop: '1px solid var(--border)', background: 'var(--bg)', flexShrink: 0 }}>
        {/* Speed slider */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '12px', marginBottom: '18px' }}>
          <span style={{ fontSize: '0.75rem', color: 'var(--muted)', fontWeight: 600 }}>Speed:</span>
          <input type="range" min={0.75} max={1.5} step={0.25} value={speed} onChange={e => setSpeed(Number(e.target.value))} style={{ width: '120px', accentColor: 'var(--blue)' }} />
          <span style={{ fontSize: '0.75rem', color: 'var(--navy)', fontWeight: 700, minWidth: '30px' }}>{speed}×</span>
        </div>

        {/* Record button */}
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '12px' }}>
          <button
            className={`btn-record ${recording ? 'recording' : 'idle'}`}
            onClick={handleRecord}
            disabled={processing}
            title={recording ? 'Stop recording' : 'Start recording'}
          >
            {recording
              ? <svg width="22" height="22" viewBox="0 0 24 24" fill="white"><rect x="6" y="6" width="12" height="12" rx="2"/></svg>
              : <svg width="22" height="22" viewBox="0 0 24 24" fill="white"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>
            }
          </button>
          <div style={{ fontSize: '0.78rem', color: recording ? '#ef4444' : 'var(--muted)', fontWeight: recording ? 700 : 400, textAlign: 'center' }}>
            {recording ? '● Recording — click to stop' : processing ? status : 'Click to record your question'}
          </div>
          <div style={{ fontSize: '0.68rem', color: '#b0bdd0' }}>
            Whisper · FIPSAR LangGraph · gpt-4o-mini-tts
          </div>
        </div>
      </div>
    </div>
  )
}
