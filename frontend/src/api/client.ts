import axios from 'axios'

const api = axios.create({ baseURL: '/api' })

// ── Types ──────────────────────────────────────────────────────────────────

export interface KPIData {
  leads: number
  prospects: number
  invalid: number
  sent: number
  opened: number
  clicked: number
  unsubscribed: number
  conversion_rate: number
}

export interface ChartsData {
  funnel: string | null
  email: string | null
  conversion: string | null
  segments: string | null
  trend: string | null
}

export interface FilterOptions {
  channels: string[]
  journeys: string[]
}

export interface ChatResponse {
  response: string
  charts: string[]
}

export interface FRELResponse {
  response: string
  charts: string[]
  email_sent: boolean
  email_meta: { to: string; subject: string; sent_at: string; charts_attached: number } | null
}

export interface VoiceChatResponse {
  response: string
  audio_b64: string | null
  charts: string[]
}

// ── Analytics ──────────────────────────────────────────────────────────────

export const getFilters = (): Promise<FilterOptions> =>
  api.get('/analytics/filters').then(r => r.data)

export const getKPIs = (
  startDate: string,
  endDate: string,
  channel = 'All',
  journey = 'All',
): Promise<KPIData> =>
  api.get('/analytics/kpis', { params: { start_date: startDate, end_date: endDate, channel, journey } })
     .then(r => r.data)

export const getCharts = (
  startDate: string,
  endDate: string,
  channel = 'All',
  journey = 'All',
): Promise<ChartsData> =>
  api.get('/analytics/charts', { params: { start_date: startDate, end_date: endDate, channel, journey } })
     .then(r => r.data)

// ── Chat ───────────────────────────────────────────────────────────────────

export const sendChat = (sessionId: string, message: string): Promise<ChatResponse> =>
  api.post('/chat', { session_id: sessionId, message }).then(r => r.data)

export const resetChat = (sessionId: string) =>
  api.post(`/chat/reset?session_id=${sessionId}`).then(r => r.data)

// ── FREL ───────────────────────────────────────────────────────────────────

export const sendFREL = (sessionId: string, message: string): Promise<FRELResponse> =>
  api.post('/frel', { session_id: sessionId, message }).then(r => r.data)

export const resetFREL = (sessionId: string) =>
  api.post(`/frel/reset?session_id=${sessionId}`).then(r => r.data)

// ── Voice ──────────────────────────────────────────────────────────────────

export const transcribeAudio = async (audioBlob: Blob): Promise<string> => {
  const form = new FormData()
  form.append('audio', audioBlob, 'recording.webm')
  const r = await api.post('/voice/transcribe', form, {
    headers: { 'Content-Type': 'multipart/form-data' },
  })
  return r.data.transcript
}

export const voiceChat = (
  sessionId: string,
  transcript: string,
  voice = 'alloy',
  speed = 1.0,
): Promise<VoiceChatResponse> =>
  api.post('/voice/chat', { session_id: sessionId, transcript, voice, speed }).then(r => r.data)

// ── Status ─────────────────────────────────────────────────────────────────

export const checkSnowflake = () =>
  api.get('/status/snowflake').then(r => r.data)

export const checkEmail = () =>
  api.get('/status/email').then(r => r.data)

export const testSMTP = () =>
  api.post('/email/test-smtp').then(r => r.data)
