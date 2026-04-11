import { useState } from 'react'
import Analytics from './pages/Analytics'
import Chat from './pages/Chat'
import Voice from './pages/Voice'
import FREL from './pages/FREL'
import fipsarLogo from '../../FIPSAR_LOGO.png'

type Tab = 'analytics' | 'chat' | 'voice' | 'frel'

const TABS: { id: Tab; label: string; icon: string }[] = [
  { id: 'analytics', label: 'Analytics',      icon: '◈' },
  { id: 'chat',      label: 'Chat',           icon: '◎' },
  { id: 'voice',     label: 'Voice',          icon: '◉' },
  { id: 'frel',      label: 'FREL Agent',     icon: '◇' },
]

export default function App() {
  const [tab, setTab] = useState<Tab>('analytics')

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', background: 'var(--bg)' }}>
      {/* ── Top Navigation Bar ─────────────────────────────────────────── */}
      <nav style={{
        background: 'linear-gradient(135deg, #070f22 0%, #0d2a5e 60%, #1a4a9e 100%)',
        padding: '0 18px 0 16px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        height: '72px',
        flexShrink: 0,
        boxShadow: '0 2px 20px rgba(7,15,34,0.30)',
        position: 'sticky',
        top: 0,
        zIndex: 100,
      }}>
        {/* Brand */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <img
            src={fipsarLogo}
            alt="FIPSAR"
            style={{
              width: '46px', height: '46px', objectFit: 'contain',
              borderRadius: '12px', background: 'rgba(255,255,255,0.18)', padding: '5px',
              boxShadow: '0 8px 18px rgba(0,0,0,0.18)',
              flexShrink: 0,
            }}
            onError={e => ((e.target as HTMLImageElement).style.display = 'none')}
          />
          <div>
            <div style={{ fontSize: '1.12rem', fontWeight: 800, color: '#fff', letterSpacing: '0.3px', lineHeight: 1.05 }}>
              FIPSAR
            </div>
            <div style={{ fontSize: '0.62rem', color: '#7da5de', letterSpacing: '0.45px', textTransform: 'uppercase', marginTop: '2px' }}>
              Intelligence
            </div>
          </div>
        </div>

        {/* Tabs */}
        <div style={{
          display: 'flex', alignItems: 'center', gap: '4px',
          background: 'rgba(255,255,255,0.06)',
          borderRadius: '14px', padding: '6px',
          boxShadow: 'inset 0 0 0 1px rgba(255,255,255,0.04)',
        }}>
          {TABS.map(t => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              style={{
                display: 'flex', alignItems: 'center', gap: '7px',
                padding: '10px 24px',
                borderRadius: '10px',
                border: 'none',
                cursor: 'pointer',
                fontSize: '0.92rem',
                fontWeight: tab === t.id ? 700 : 500,
                fontFamily: 'inherit',
                letterSpacing: '0.1px',
                transition: 'all 0.15s ease',
                background: tab === t.id
                  ? 'rgba(255,255,255,0.14)'
                  : 'transparent',
                color: tab === t.id ? '#ffffff' : '#6a90c0',
                boxShadow: tab === t.id ? '0 1px 8px rgba(0,0,0,0.20)' : 'none',
              }}
            >
              <span style={{ fontSize: '1.02rem', opacity: tab === t.id ? 1 : 0.6 }}>{t.icon}</span>
              {t.label}
            </button>
          ))}
        </div>

        {/* Right: live indicator */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div style={{
            width: '7px', height: '7px', borderRadius: '50%', background: '#4ade80',
            boxShadow: '0 0 6px #4ade80',
            animation: 'pulse-dot 2s ease-in-out infinite',
          }} />
          <span style={{ fontSize: '0.78rem', color: '#6f93c8', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.5px' }}>
            Live
          </span>
        </div>
      </nav>

      {/* ── Page Content ───────────────────────────────────────────────── */}
      <main style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
        {tab === 'analytics' && <Analytics />}
        {tab === 'chat'      && <Chat />}
        {tab === 'voice'     && <Voice />}
        {tab === 'frel'      && <FREL />}
      </main>
    </div>
  )
}
