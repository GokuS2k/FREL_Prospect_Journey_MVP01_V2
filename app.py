"""
app.py
------
Streamlit UI for FIPSAR Prospect Journey Intelligence.

Four tabs (in order):
  📊 Analytics      — live KPI dashboard with filters (FIRST tab)
  💬 Chat           — multi-turn text chat
  🎤 Voice Assistant — recorder at bottom, conversation scrolls above
  📧 FREL Agent     — chat + email reports

Run with:
    streamlit run app.py
"""

from __future__ import annotations

import json
import re
import uuid
import logging

import streamlit as st
import streamlit.components.v1 as components

# Must be the first Streamlit command
st.set_page_config(
    page_title="FIPSAR Intelligence",
    page_icon="FIPSAR_LOGO.png",
    layout="wide",
    initial_sidebar_state="expanded",
)

from snowflake_connector import test_connection
from agent import chat, reset_session, set_persona
from frel_agent import frel_chat, reset_frel_session
import chart_store
from voice_assistant import transcribe_audio, text_to_speech
from config import email_config
from email_sender import test_email_connection, send_email
from analytics_dashboard import render_analytics_dashboard
from fipsar_theme import CYAN, GRADIENT, NAVY, PAGE_BG, TEXT_PRIMARY, TEXT_SECONDARY, streamlit_global_css
from semantic_model import sidebar_data_dictionary_md, PERSONAS

logging.basicConfig(level=logging.WARNING)

# ===========================================================================
# GLOBAL CSS — FIPSAR logo-aligned light theme (see fipsar_theme.py)
# ===========================================================================

st.markdown(streamlit_global_css(), unsafe_allow_html=True)


# ===========================================================================
# Shared UI helpers  (defined before tabs so every tab can call them)
# ===========================================================================

def _inject_chat_persona_selector(current_persona: str) -> None:
    """
    Inject a compact persona <select> into the left side of the Streamlit chat input bar.
    When the user changes it, the function programmatically clicks the matching option in
    the sidebar baseweb Select so Streamlit state is updated and a rerun is triggered.
    """
    _personas_js = json.dumps(PERSONAS)
    _icons_js = json.dumps({
        "General": "🌐",
        "Executive Committee": "👔",
        "Business Users": "📊",
        "Administrators Group": "🔧",
    })
    _short_js = json.dumps({
        "General": "General",
        "Executive Committee": "Executive",
        "Business Users": "Business",
        "Administrators Group": "Admins",
    })
    _current_js = json.dumps(current_persona)

    components.html(f"""
<script>
(function() {{
  var PERSONAS = {_personas_js};
  var ICONS    = {_icons_js};
  var SHORT    = {_short_js};
  var current  = {_current_js};

  function inject() {{
    try {{
      var doc = window.parent.document;
      var container = doc.querySelector('[data-testid="stChatInputContainer"]');
      if (!container) return false;

      var existing = doc.getElementById('fipsar-psel-wrap');
      if (existing) {{
        var sel = doc.getElementById('fipsar-psel');
        if (sel && sel.value !== current) sel.value = current;
        return true;
      }}

      // Push the textarea right to make room
      var ta = container.querySelector('textarea');
      if (ta) ta.style.paddingLeft = '155px';

      // Wrapper sits inside the input container on the left
      var wrap = doc.createElement('div');
      wrap.id = 'fipsar-psel-wrap';
      wrap.style.cssText = [
        'position:absolute', 'left:10px', 'top:50%',
        'transform:translateY(-50%)', 'z-index:9999'
      ].join(';');

      var sel = doc.createElement('select');
      sel.id = 'fipsar-psel';
      sel.title = 'AI Persona';
      sel.style.cssText = [
        'background:#f0f4ff',
        'border:1.5px solid #c7d2fe',
        'border-radius:8px',
        'padding:5px 8px',
        'font-size:0.71rem',
        'font-family:Inter,system-ui,sans-serif',
        'color:#0033A0',
        'font-weight:700',
        'cursor:pointer',
        'outline:none',
        'width:148px',
        'box-shadow:0 1px 4px rgba(0,51,160,0.12)',
        'appearance:auto'
      ].join(';');

      PERSONAS.forEach(function(p) {{
        var o = doc.createElement('option');
        o.value = p;
        o.text  = ICONS[p] + ' ' + SHORT[p];
        if (p === current) o.selected = true;
        sel.appendChild(o);
      }});

      sel.addEventListener('change', function(e) {{
        clickSidebarPersona(doc, e.target.value);
      }});

      wrap.appendChild(sel);
      container.style.position = 'relative';
      container.appendChild(wrap);
      return true;
    }} catch(err) {{ return false; }}
  }}

  // Click the matching option inside the sidebar persona selectbox
  function clickSidebarPersona(doc, persona) {{
    var sidebar = doc.querySelector('[data-testid="stSidebar"]');
    if (!sidebar) return;
    var boxes = sidebar.querySelectorAll('[data-testid="stSelectbox"]');
    for (var i = 0; i < boxes.length; i++) {{
      var trigger = boxes[i].querySelector('[data-baseweb="select"] > div:first-child');
      if (!trigger) continue;
      trigger.click();
      (function(box) {{
        setTimeout(function() {{
          // baseweb renders the listbox in a portal at body level
          var opts = doc.querySelectorAll('[data-baseweb="menu"] [role="option"]');
          for (var j = 0; j < opts.length; j++) {{
            if (opts[j].textContent.trim() === persona) {{ opts[j].click(); break; }}
          }}
        }}, 120);
      }})(boxes[i]);
      break;
    }}
  }}

  // Retry until the chat input renders
  var attempts = 0;
  var timer = setInterval(function() {{
    if (inject() || attempts++ > 100) clearInterval(timer);
  }}, 250);

  // Re-inject after every Streamlit re-render (chat input re-mounts)
  try {{
    var obs = new MutationObserver(function() {{
      if (!window.parent.document.getElementById('fipsar-psel-wrap')) inject();
    }});
    obs.observe(window.parent.document.body, {{childList:true, subtree:true}});
  }} catch(e) {{}}
}})();
</script>
""", height=0)


def _render_email_badge(meta: dict) -> None:
    """Render the green 'EMAIL SENT' confirmation card."""
    charts_note = (
        f" · {meta.get('charts_attached', 0)} chart(s) embedded"
        if meta.get("charts_attached", 0) > 0 else ""
    )
    st.markdown(
        f"""<div style="background:#f0fdf4;border:1px solid #86efac;border-radius:12px;padding:16px 20px;margin-top:12px;
        box-shadow:0 2px 12px rgba(5,150,105,0.12)">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
            <span style="font-size:1.1rem">✅</span>
            <span style="color:#166534;font-size:0.82rem;font-weight:700;
                         text-transform:uppercase;letter-spacing:0.8px">
                Email Sent Successfully
            </span>
        </div>
        <div style="color:#14532d;font-size:0.88rem;line-height:1.7">
            <b>To:</b> {meta.get("to", "")}<br>
            <b>Subject:</b> {meta.get("subject", "FIPSAR Report")}<br>
            <b>Sent at:</b> {meta.get("sent_at", "")}{charts_note}
        </div></div>""",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Auto-scroll helper
# ---------------------------------------------------------------------------

_SCROLL_JS = """
<script>
setTimeout(function() {
    var doc = window.parent.document;
    var main = doc.querySelector('section[data-testid="stMain"]')
             || doc.querySelector('[data-testid="stMain"]')
             || doc.querySelector('section.main')
             || doc.querySelector('.main');
    if (main) { main.scrollTop = main.scrollHeight; }
    doc.querySelectorAll('[data-testid="stVerticalBlockBorderWrapper"]')
       .forEach(function(c) { c.scrollTop = c.scrollHeight; });
    window.parent.scrollTo(0, 99999);
}, 120);
</script>
"""

def _scroll_to_bottom() -> None:
    components.html(_SCROLL_JS, height=0, scrolling=False)


def _split_followups_from_assistant(text: str) -> tuple[str, list[str]]:
    """
    Strip the ## Follow-ups section from assistant markdown for display; return follow-up
    strings as separate items for clickable buttons.
    """
    if not text or not text.strip():
        return "", []
    pat = re.compile(
        r"\n{0,2}##\s*Follow-?ups(?:\s+questions)?\s*\n(?P<block>[\s\S]*)$",
        re.IGNORECASE,
    )
    m = pat.search(text)
    if not m:
        return text, []
    main = text[: m.start()].rstrip()
    block = m.group("block") or ""
    questions: list[str] = []
    for raw in block.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith(("- ", "* ", "– ")):
            questions.append(line[2:].strip())
        elif re.match(r"^\d+\.\s+", line):
            questions.append(re.sub(r"^\d+\.\s+", "", line).strip())
    return main, [q for q in questions if q]


# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if "messages" not in st.session_state:
    st.session_state.messages = []
if "chat_followup_pending" not in st.session_state:
    st.session_state.chat_followup_pending = None
if "snowflake_ok" not in st.session_state:
    st.session_state.snowflake_ok = None
if "persona" not in st.session_state:
    st.session_state.persona = "General"

# Voice
if "voice_session_id" not in st.session_state:
    st.session_state.voice_session_id = str(uuid.uuid4())
if "voice_messages" not in st.session_state:
    st.session_state.voice_messages = []
if "last_audio_key" not in st.session_state:
    st.session_state.last_audio_key = None

# FREL Agent
if "frel_session_id" not in st.session_state:
    st.session_state.frel_session_id = str(uuid.uuid4())
if "frel_messages" not in st.session_state:
    st.session_state.frel_messages = []
if "email_test_result" not in st.session_state:
    st.session_state.email_test_result = None


# ===========================================================================
# SIDEBAR
# ===========================================================================

with st.sidebar:
    # Brand header — logo + title
    _logo_col1, _logo_col2, _logo_col3 = st.columns([0.4, 2.2, 0.4])
    with _logo_col2:
        st.image("FIPSAR_LOGO.png", use_container_width=True)
    st.markdown(f"""
    <div style="text-align:center;padding:6px 0 14px">
        <div style="font-size:1.1rem;font-weight:800;color:{TEXT_PRIMARY};letter-spacing:0.4px">
            FIPSAR Intelligence
        </div>
        <div style="height:3px;background:{GRADIENT};border-radius:2px;margin:10px auto 0;max-width:140px"></div>
        <div style="font-size:0.71rem;color:{TEXT_SECONDARY};margin-top:10px;letter-spacing:0.3px">
            SFMC Prospect Journey · LangGraph + Snowflake
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # ── Persona selector ──────────────────────────────────────────────────
    _PERSONA_META = {
        "General":               ("🌐", "Broad-purpose · everyday questions"),
        "Executive Committee":   ("👔", "Concise · outcomes · strategic view"),
        "Business Users":        ("📊", "Actionable · campaign & funnel focus"),
        "Administrators Group":  ("🔧", "Detailed diagnostics · SQL included"),
    }
    st.markdown(f"""<div style="font-size:0.72rem;font-weight:700;color:{NAVY};
        text-transform:uppercase;letter-spacing:1.2px;margin-bottom:8px">
        🎭 AI Persona</div>""", unsafe_allow_html=True)

    _sel_persona = st.selectbox(
        "Select persona",
        options=PERSONAS,
        index=PERSONAS.index(st.session_state.persona),
        label_visibility="collapsed",
        key="persona_selector",
    )
    if _sel_persona != st.session_state.persona:
        st.session_state.persona = _sel_persona

    _icon, _desc = _PERSONA_META[st.session_state.persona]
    st.markdown(
        f"""<div style="background:#f0f4ff;border:1px solid #c7d2fe;border-radius:8px;
        padding:7px 12px;font-size:0.76rem;color:{NAVY};margin-top:4px;line-height:1.6">
        {_icon}&nbsp; <b>{st.session_state.persona}</b><br>
        <span style="color:#64748b;font-size:0.71rem">{_desc}</span>
        </div>""",
        unsafe_allow_html=True,
    )

    st.divider()

    # ── Snowflake connection ──────────────────────────────────────────────
    st.markdown(f"""<div style="font-size:0.72rem;font-weight:700;color:{NAVY};
        text-transform:uppercase;letter-spacing:1.2px;margin-bottom:8px">
        🔌 Connection Status</div>""", unsafe_allow_html=True)

    if st.button("Test Snowflake Connection", use_container_width=True):
        with st.spinner("Connecting..."):
            st.session_state.snowflake_ok = test_connection()

    if st.session_state.snowflake_ok is True:
        st.markdown("""<div style="background:rgba(34,197,94,0.15);border:1px solid rgba(34,197,94,0.4);
            border-radius:8px;padding:7px 12px;font-size:0.78rem;color:#4ade80;font-weight:600;
            display:flex;align-items:center;gap:6px;margin-top:4px">
            <span>●</span> Snowflake Connected</div>""", unsafe_allow_html=True)
    elif st.session_state.snowflake_ok is False:
        st.markdown("""<div style="background:rgba(239,68,68,0.15);border:1px solid rgba(239,68,68,0.4);
            border-radius:8px;padding:7px 12px;font-size:0.78rem;color:#f87171;font-weight:600;
            display:flex;align-items:center;gap:6px;margin-top:4px">
            <span>●</span> Connection Failed</div>""", unsafe_allow_html=True)
    else:
        st.markdown("""<div style="background:#f1f5f9;border:1px solid #e2e8f0;
            border-radius:8px;padding:7px 12px;font-size:0.77rem;color:#64748b;margin-top:4px">
            Click above to test connection</div>""", unsafe_allow_html=True)

    st.divider()

    # ── Email status ──────────────────────────────────────────────────────
    st.markdown(f"""<div style="font-size:0.72rem;font-weight:700;color:{NAVY};
        text-transform:uppercase;letter-spacing:1.2px;margin-bottom:6px">
        📧 Email (FREL Agent)</div>""", unsafe_allow_html=True)

    st.markdown(
        f"""<div style="font-size:0.73rem;color:{TEXT_SECONDARY};margin-bottom:8px">
        Recipient: <span style="color:{TEXT_PRIMARY};font-family:monospace">{email_config.to_address}</span>
        </div>""",
        unsafe_allow_html=True,
    )

    col_etest, col_esend = st.columns(2)
    with col_etest:
        if st.button("Test SMTP", use_container_width=True, key="test_smtp"):
            with st.spinner("Testing..."):
                st.session_state.email_test_result = test_email_connection()
    with col_esend:
        if st.button("Send Test", use_container_width=True, key="send_test_email"):
            with st.spinner("Sending..."):
                st.session_state.email_test_result = send_email(
                    subject="FIPSAR Intelligence — Email Test",
                    report_markdown=(
                        "## Connection Test\n\nThis is a test email from FIPSAR Intelligence.\n\n"
                        "Your email configuration is working correctly.\n\n"
                        "- **SMTP Host:** " + email_config.smtp_host + "\n"
                        "- **From:** " + (email_config.from_address or email_config.smtp_user) + "\n"
                        "- **To:** " + email_config.to_address
                    ),
                )

    if st.session_state.email_test_result is not None:
        r = st.session_state.email_test_result
        if r.get("success"):
            st.markdown(
                f"""<div style="background:rgba(34,197,94,0.12);border:1px solid rgba(34,197,94,0.35);
                border-radius:8px;padding:7px 10px;font-size:0.73rem;color:#4ade80;margin-top:6px">
                ✅ {r["message"][:80]}</div>""",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"""<div style="background:rgba(239,68,68,0.12);border:1px solid rgba(239,68,68,0.35);
                border-radius:8px;padding:7px 10px;font-size:0.73rem;color:#f87171;margin-top:6px">
                ⚠️ {r["message"][:100]}</div>""",
                unsafe_allow_html=True,
            )
            with st.expander("Gmail App Password guide", expanded=False):
                st.markdown(
                    "1. myaccount.google.com → Security → 2-Step Verification\n"
                    "2. App Passwords → App: Mail · Device: Other → name `FIPSAR`\n"
                    "3. Copy 16-char code → paste as `EMAIL_SMTP_PASSWORD` in `.env`\n"
                    "4. Restart app → click Test SMTP"
                )

    st.divider()

    # ── Sessions ──────────────────────────────────────────────────────────
    st.markdown(f"""<div style="font-size:0.72rem;font-weight:700;color:{NAVY};
        text-transform:uppercase;letter-spacing:1.2px;margin-bottom:8px">
        🔄 Sessions</div>""", unsafe_allow_html=True)

    st.markdown(
        f"""<div style="font-size:0.71rem;color:{TEXT_SECONDARY};line-height:1.9;margin-bottom:8px">
        💬 Chat: <code style="color:{NAVY};background:#e0f2fe;
        padding:1px 5px;border-radius:4px">{st.session_state.session_id[:8]}…</code><br>
        🎤 Voice: <code style="color:{NAVY};background:#e0f2fe;
        padding:1px 5px;border-radius:4px">{st.session_state.voice_session_id[:8]}…</code><br>
        📧 FREL: <code style="color:{NAVY};background:#e0f2fe;
        padding:1px 5px;border-radius:4px">{st.session_state.frel_session_id[:8]}…</code>
        </div>""",
        unsafe_allow_html=True,
    )

    col_new, col_clear = st.columns(2)
    with col_new:
        if st.button("New Session", use_container_width=True):
            for key in ["session_id", "voice_session_id", "frel_session_id"]:
                st.session_state[key] = str(uuid.uuid4())
            st.session_state.messages = []
            st.session_state.chat_followup_pending = None
            st.session_state.voice_messages = []
            st.session_state.frel_messages = []
            st.session_state.last_audio_key = None
            st.rerun()
    with col_clear:
        if st.button("Clear All", use_container_width=True):
            reset_session(st.session_state.session_id)
            reset_frel_session(st.session_state.frel_session_id)
            st.session_state.messages = []
            st.session_state.chat_followup_pending = None
            st.session_state.voice_messages = []
            st.session_state.frel_messages = []
            st.session_state.last_audio_key = None
            st.rerun()

    st.divider()

    _dd = sidebar_data_dictionary_md()
    if _dd:
        with st.expander("📖 Data dictionary (key tables)", expanded=False):
            st.markdown(_dd)

    # ── Sample questions ──────────────────────────────────────────────────
    st.markdown(f"""<div style="font-size:0.72rem;font-weight:700;color:{NAVY};
        text-transform:uppercase;letter-spacing:1.2px;margin-bottom:8px">
        💡 Sample Questions</div>""", unsafe_allow_html=True)
    SAMPLE_QUESTIONS: dict[str, list[str]] = {
        "📈 Funnel & Drop Analysis": [
            "Give me a full funnel summary — leads to prospects to engagement.",
            "Show me the funnel chart for all time.",
            "Why is there a volume drop? What are the top rejection reasons?",
            "Show me the lead-to-prospect conversion rate.",
        ],
        "❌ Rejections & DQ": [
            "Who got dropped and why? List all rejection reasons.",
            "Show me a chart of rejection reasons.",
            "How many NULL_EMAIL rejections are there?",
            "What are the SUPPRESSED and FATAL_ERROR patterns?",
        ],
        "📧 SFMC Journey & Events": [
            "What are the SFMC event counts broken down by journey?",
            "Show me an SFMC engagement chart.",
            "How is the Welcome journey performing?",
            "Which journey stage has the highest bounce rate?",
        ],
        "🔍 Prospect Trace": [
            "Trace prospect with email john.doe@example.com through the pipeline.",
            "Show me the journey history for MASTER_PATIENT_ID P001.",
        ],
        "🤖 AI & Scores": [
            "What is the conversion and drop-off probability for active prospects?",
            "Show me a conversion segment chart.",
            "Which prospects are at risk of dropping off?",
        ],
        "📅 Trends": [
            "Show me the monthly intake trend for 2026.",
            "Plot weekly lead and prospect volume for January 2026.",
        ],
        "🔬 Observability": [
            "Show pipeline run health for the last 30 days.",
            "Are there any data quality issues I should know about?",
        ],
    }

    for category, questions in SAMPLE_QUESTIONS.items():
        with st.expander(category, expanded=False):
            for q in questions:
                if st.button(q, key=f"sample_{q[:30]}", use_container_width=True):
                    st.session_state.messages.append({"role": "user", "content": q})
                    with st.spinner("Thinking..."):
                        set_persona(st.session_state.persona)
                        chart_store.set_session(st.session_state.session_id)
                        response = chat(st.session_state.session_id, q)
                    pending_charts = chart_store.pop_all(st.session_state.session_id)
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": response,
                        "charts": pending_charts,
                    })
                    st.rerun()


# ===========================================================================
# Four tabs
# ===========================================================================

tab_analytics, tab_chat, tab_voice, tab_frel = st.tabs([
    "  📊   Analytics  ",
    "  💬   Chat  ",
    "  🎤   Voice Assistant  ",
    "  📧   FREL Agent  ",
])


# ===========================================================================
# TAB 1 — ANALYTICS DASHBOARD
# ===========================================================================

with tab_analytics:
    render_analytics_dashboard()


# ===========================================================================
# TAB 2 — CHAT
# ===========================================================================

with tab_chat:

    if st.session_state.chat_followup_pending:
        _fq = st.session_state.chat_followup_pending
        st.session_state.chat_followup_pending = None
        st.session_state.messages.append({"role": "user", "content": _fq})
        with st.spinner("Querying Snowflake and reasoning…"):
            set_persona(st.session_state.persona)
            chart_store.set_session(st.session_state.session_id)
            _resp = chat(st.session_state.session_id, _fq)
        _charts = chart_store.pop_all(st.session_state.session_id)
        st.session_state.messages.append({
            "role": "assistant",
            "content": _resp,
            "charts": _charts,
        })
        _scroll_to_bottom()
        st.rerun()

    st.markdown(f"""
    <div style="background:{PAGE_BG};border:1px solid #e2e8f0;border-radius:16px;padding:22px 28px;margin-bottom:18px;
         box-shadow:0 2px 16px rgba(0,0,0,0.06);display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px">
        <div style="display:flex;align-items:center;gap:16px">
            <div style="width:44px;height:44px;background:{GRADIENT};border-radius:50%;display:flex;align-items:center;justify-content:center;
                        font-size:1.25rem;flex-shrink:0;box-shadow:0 2px 10px rgba(0,51,160,0.2)">💬</div>
            <div>
                <div style="font-size:1.15rem;font-weight:800;color:{TEXT_PRIMARY};letter-spacing:0.2px;line-height:1.2">
                    Prospect Journey Intelligence
                </div>
                <div style="height:2px;background:{GRADIENT};border-radius:1px;margin-top:8px;max-width:180px"></div>
                <div style="font-size:0.78rem;color:{TEXT_SECONDARY};margin-top:8px;display:flex;gap:12px;flex-wrap:wrap">
                    <span>Lead Intake</span><span>·</span><span>SFMC Journeys</span><span>·</span><span>Engagement</span><span>·</span><span>AI Scores</span>
                </div>
            </div>
        </div>
        <div style="font-size:0.68rem;color:{TEXT_SECONDARY};font-weight:600;text-align:right;letter-spacing:0.4px;text-transform:uppercase">
            GPT-4o · LangGraph<br><span style="color:{NAVY}">Snowflake</span><br>
            <span style="background:#e0e7ff;color:{NAVY};border-radius:5px;padding:2px 8px;font-size:0.65rem;font-weight:700;letter-spacing:0.3px">
                {st.session_state.persona}
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Message history
    chat_container = st.container()
    with chat_container:
        if not st.session_state.messages:
            st.markdown(f"""
            <div style="text-align:center;padding:60px 20px 48px;color:{TEXT_SECONDARY}">
                <div style="width:64px;height:64px;margin:0 auto 16px;background:{GRADIENT};
                            border-radius:50%;display:flex;align-items:center;
                            justify-content:center;font-size:1.8rem;
                            box-shadow:0 4px 16px rgba(0,51,160,0.15)">🧠</div>
                <div style="font-size:1rem;font-weight:700;color:{TEXT_PRIMARY};margin-bottom:8px">
                    How can we help?
                </div>
                <div style="font-size:0.84rem;max-width:420px;margin:0 auto;line-height:1.7;color:{TEXT_SECONDARY}">
                    Ask about leads, SFMC journeys, engagement metrics, or data quality.<br>
                    Or use <b style="color:{NAVY}">Sample Questions</b> in the sidebar.
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            for _mi, msg in enumerate(st.session_state.messages):
                _fus: list[str] = []
                _msg_charts: list = []
                with st.chat_message(msg["role"]):
                    if msg["role"] == "assistant":
                        _body, _fus = _split_followups_from_assistant(msg["content"])
                        st.markdown(_body)
                        _msg_charts = msg.get("charts", [])
                    else:
                        st.markdown(msg["content"])
                # Charts and follow-up buttons rendered OUTSIDE the gradient bubble
                # so they are never clipped or tinted by the gradient CSS
                for _ci, fig in enumerate(_msg_charts):
                    st.plotly_chart(fig, use_container_width=True, key=f"chart_hist_{_mi}_{_ci}")
                if _fus:
                    st.caption("Next questions")
                    for _fj, _q in enumerate(_fus):
                        _lbl = _q if len(_q) <= 80 else _q[:77] + "…"
                        if st.button(
                            _lbl,
                            key=f"chat_fu_{_mi}_{_fj}",
                            help=_q,
                            use_container_width=True,
                        ):
                            st.session_state.chat_followup_pending = _q
                            st.rerun()

    # Inject persona selector into the left side of the chat input bar
    _inject_chat_persona_selector(st.session_state.persona)

    # Sticky chat input
    if user_input := st.chat_input("Ask about your prospect journey data…", key="chat_input"):
        st.session_state.messages.append({"role": "user", "content": user_input})
        with chat_container:
            with st.chat_message("user"):
                st.markdown(user_input)
            with st.chat_message("assistant"):
                with st.spinner("Querying Snowflake and reasoning…"):
                    set_persona(st.session_state.persona)
                    chart_store.set_session(st.session_state.session_id)
                    response = chat(st.session_state.session_id, user_input)
                pending_charts = chart_store.pop_all(st.session_state.session_id)
                _b_live, _fu_live = _split_followups_from_assistant(response)
                st.markdown(_b_live)
            # Charts rendered OUTSIDE the gradient bubble — avoids CSS tinting/clipping
            for _ci, fig in enumerate(pending_charts):
                st.plotly_chart(fig, use_container_width=True, key=f"chart_live_{_ci}")
            if _fu_live:
                st.caption("Next questions")
                for _fj, _q in enumerate(_fu_live):
                    _lbl = _q if len(_q) <= 80 else _q[:77] + "…"
                    if st.button(
                        _lbl,
                        key=f"chat_fu_live_{_fj}",
                        help=_q,
                        use_container_width=True,
                    ):
                        st.session_state.chat_followup_pending = _q
                        st.rerun()

        st.session_state.messages.append({
            "role": "assistant",
            "content": response,
            "charts": pending_charts,
        })
        _scroll_to_bottom()

    # Footer
    st.markdown(f"""
    <div style="text-align:center;margin-top:14px;padding-top:10px;border-top:1px solid #e2e8f0">
        <span style="font-size:0.71rem;color:{TEXT_SECONDARY};letter-spacing:0.3px">
            FIPSAR Intelligence &nbsp;·&nbsp; Snowflake &nbsp;·&nbsp; GPT-4o via LangGraph
        </span>
    </div>
    """, unsafe_allow_html=True)


# ===========================================================================
# TAB 3 — VOICE ASSISTANT
# ===========================================================================

with tab_voice:

    st.markdown(f"""
    <div style="background:{PAGE_BG};border:1px solid #e2e8f0;border-radius:16px;padding:20px 26px;margin-bottom:18px;box-shadow:0 2px 12px rgba(0,0,0,0.06)">
        <div style="display:flex;align-items:center;gap:14px">
            <div style="font-size:2rem;width:48px;height:48px;background:{GRADIENT};border-radius:12px;display:flex;align-items:center;justify-content:center">🎤</div>
            <div>
                <div style="font-size:1.1rem;font-weight:800;color:{TEXT_PRIMARY}">Voice Assistant</div>
                <div style="font-size:0.8rem;color:{TEXT_SECONDARY};margin-top:4px">
                    Whisper transcription · LangGraph · TTS playback
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    _, col_vclear = st.columns([5, 1])
    with col_vclear:
        if st.button("🗑 Clear", use_container_width=True, key="voice_clear"):
            st.session_state.voice_session_id = str(uuid.uuid4())
            st.session_state.voice_messages = []
            st.session_state.last_audio_key = None
            st.rerun()

    st.divider()

    # Conversation history
    n_msgs = len(st.session_state.voice_messages)
    box_height = min(120 + n_msgs * 80, 520)
    voice_box = st.container(height=box_height, border=False)

    with voice_box:
        if st.session_state.voice_messages:
            for msg in st.session_state.voice_messages:
                if msg["role"] == "user":
                    st.markdown(
                        f"""<div style="background:#f8fafc;border:1px solid #e2e8f0;border-left:4px solid {CYAN};
                        border-radius:12px;padding:12px 16px;margin:8px 0;box-shadow:0 1px 8px rgba(0,0,0,0.05)">
                        <div style="color:{NAVY};font-size:0.72rem;font-weight:700;
                             text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px">
                            🎙 You said
                        </div>
                        <div style="color:{TEXT_PRIMARY};font-size:0.92rem;line-height:1.5">
                            {msg["content"]}
                        </div></div>""",
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f"""<div style="background:{GRADIENT};border-radius:12px;
                        padding:10px 16px 6px;margin:6px 0 2px;box-shadow:0 2px 10px rgba(0,51,160,0.2)">
                        <div style="color:#ffffff;font-size:0.72rem;font-weight:700;
                             text-transform:uppercase;letter-spacing:0.8px">
                            🤖 FIPSAR AI
                        </div></div>""",
                        unsafe_allow_html=True,
                    )
                    with st.expander("View response", expanded=True):
                        st.markdown(msg["content"])
                    if msg.get("audio"):
                        st.audio(msg["audio"], format="audio/mp3", autoplay=False)
                    else:
                        st.warning("Audio unavailable for this response.")
                    for fig in msg.get("charts", []):
                        st.plotly_chart(fig, use_container_width=True)
        else:
            st.markdown("""
            <div style="text-align:center;padding:40px 20px;color:#64748b">
                <div style="font-size:3rem;margin-bottom:10px">🎤</div>
                <div style="font-size:0.9rem;font-weight:600;color:#475569">
                    Your conversation will appear here
                </div>
                <div style="font-size:0.8rem;margin-top:6px;color:#94a3b8">
                    Record your question using the microphone below
                </div>
            </div>
            """, unsafe_allow_html=True)

    # Recorder section
    st.divider()

    with st.expander("⚙️ Voice Settings", expanded=False):
        voice_choice = st.selectbox(
            "AI Response Voice",
            ["alloy", "echo", "fable", "onyx", "nova", "shimmer"],
            index=0,
        )
        speech_speed = st.slider("Playback Speed", 0.75, 1.5, 1.0, 0.25)

    audio_input = st.audio_input(
        "🎙 Click to start recording · click again to stop · response appears above",
        key="voice_recorder",
    )

    st.markdown("""<div style="text-align:center;margin-top:6px">
        <span style="font-size:0.71rem;color:#94a3b8">
            Whisper (transcription) · FIPSAR LangGraph Agent · gpt-4o-mini-tts
        </span></div>""", unsafe_allow_html=True)

    # Process new recording
    if audio_input is not None:
        audio_bytes = audio_input.read()
        audio_hash = hash(audio_bytes)

        if st.session_state.last_audio_key != audio_hash:
            st.session_state.last_audio_key = audio_hash

            with st.spinner("Transcribing…"):
                transcript = transcribe_audio(audio_bytes)

            if not transcript:
                st.error("Could not transcribe. Please try again.")
            else:
                st.session_state.voice_messages.append({
                    "role": "user", "content": transcript, "audio": None, "charts": [],
                })

                with st.spinner("Querying Snowflake and reasoning…"):
                    chart_store.set_session(st.session_state.voice_session_id)
                    ai_response = chat(st.session_state.voice_session_id, transcript)
                pending_charts = chart_store.pop_all(st.session_state.voice_session_id)

                with st.spinner("Generating audio…"):
                    response_audio = text_to_speech(
                        ai_response, voice=voice_choice, speed=speech_speed,
                    )

                st.session_state.voice_messages.append({
                    "role": "assistant",
                    "content": ai_response,
                    "audio": response_audio,
                    "charts": pending_charts,
                })
                st.rerun()

    _scroll_to_bottom()


# ===========================================================================
# TAB 4 — FREL AGENT
# ===========================================================================

with tab_frel:

    # Header
    st.markdown(
        f"""<div style="background:{PAGE_BG};border:1px solid #e2e8f0;border-radius:16px;padding:22px 28px;margin-bottom:18px;
        box-shadow:0 2px 14px rgba(0,0,0,0.06)">
        <div style="display:flex;align-items:center;gap:14px">
            <div style="font-size:1.8rem;width:52px;height:52px;background:{GRADIENT};border-radius:12px;display:flex;align-items:center;justify-content:center">📧</div>
            <div>
                <div style="font-size:1.15rem;font-weight:800;color:{TEXT_PRIMARY}">FREL Agent</div>
                <div style="font-size:0.8rem;color:{TEXT_SECONDARY};margin-top:4px;max-width:640px">
                    Data intelligence + email delivery · Say <b style="color:{NAVY}">"send it over email"</b>
                    → <b>{email_config.to_address}</b>
                </div>
            </div>
        </div></div>""",
        unsafe_allow_html=True,
    )

    col_info, col_fclr = st.columns([4, 1])
    with col_info:
        if email_config.is_configured:
            st.markdown(
                f"""<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;
                padding:9px 14px;font-size:0.82rem;color:#166534;display:flex;align-items:center;gap:8px">
                <span style="font-size:1rem">✅</span>
                Email ready · Reports sent to <b>{email_config.to_address}</b>
                </div>""",
                unsafe_allow_html=True,
            )
        else:
            st.markdown("""<div style="background:#fffbeb;border:1px solid #fde68a;border-radius:10px;
                padding:9px 14px;font-size:0.82rem;color:#92400e;display:flex;align-items:center;gap:8px">
                <span>⚠️</span> Email not configured — check sidebar.
                </div>""", unsafe_allow_html=True)
    with col_fclr:
        if st.button("🗑 Clear", use_container_width=True, key="frel_clear"):
            reset_frel_session(st.session_state.frel_session_id)
            st.session_state.frel_session_id = str(uuid.uuid4())
            st.session_state.frel_messages = []
            st.rerun()

    st.divider()

    with st.expander("💡 Example requests", expanded=False):
        st.markdown(
            """
| Request | What happens |
|---|---|
| `Send me the funnel report for January 2026` | Queries → emails full report |
| `Email me the rejection analysis with a chart` | Queries + chart → emails with embedded image |
| `Send the SFMC journey performance report` | Queries all journeys → emails table |
| `Email details about prospect john.doe@example.com` | Traces → emails report |
| `Give me the conversion analysis and email it` | Shows in UI + emails |
            """
        )

    # Message history
    frel_container = st.container()
    with frel_container:
        if not st.session_state.frel_messages:
            st.markdown(
                f"""<div style="text-align:center;padding:50px 20px;color:#94a3b8">
                <div style="font-size:3rem;margin-bottom:12px">📬</div>
                <div style="font-size:1rem;font-weight:600;color:#64748b;margin-bottom:6px">
                    Ready to assist
                </div>
                <div style="font-size:0.85rem;max-width:440px;margin:0 auto;line-height:1.6">
                    Ask any question about your prospect data.<br>
                    Say <b>"send it over email"</b> and the full report
                    will be delivered to <b>{email_config.to_address}</b>.
                </div></div>""",
                unsafe_allow_html=True,
            )
        else:
            for msg in st.session_state.frel_messages:
                if msg["role"] == "user":
                    with st.chat_message("user"):
                        st.markdown(msg["content"])
                else:
                    with st.chat_message("assistant"):
                        st.markdown(msg["content"])
                        if msg.get("email_sent") and msg.get("email_meta"):
                            _render_email_badge(msg["email_meta"])
                        for fig in msg.get("charts", []):
                            st.plotly_chart(fig, use_container_width=True)

    # Sticky chat input
    if frel_input := st.chat_input(
        "Ask a question or say 'send me the funnel report over email'…",
        key="frel_input",
    ):
        st.session_state.frel_messages.append({"role": "user", "content": frel_input})
        with frel_container:
            with st.chat_message("user"):
                st.markdown(frel_input)

            with st.chat_message("assistant"):
                with st.spinner("Analysing data and preparing report…"):
                    chart_store.set_session(st.session_state.frel_session_id)
                    frel_response = frel_chat(st.session_state.frel_session_id, frel_input)

                pending_charts = chart_store.pop_all(st.session_state.frel_session_id)
                st.markdown(frel_response)

                email_sent = (
                    "✅ Email sent successfully" in frel_response
                    or "email sent" in frel_response.lower()
                )
                email_meta = None
                if email_sent:
                    import re
                    from datetime import datetime as _dt
                    sm = re.search(r"Subject:\s*['\"]?([^\n'\"]+)", frel_response)
                    tm = re.search(r"To:\s*([^\s\n]+@[^\s\n]+)", frel_response)
                    cm = re.search(r"(\d+)\s+chart", frel_response)
                    email_meta = {
                        "to": tm.group(1) if tm else email_config.to_address,
                        "subject": sm.group(1).strip() if sm else "FIPSAR Report",
                        "sent_at": _dt.now().strftime("%B %d, %Y  %I:%M %p"),
                        "charts_attached": int(cm.group(1)) if cm else 0,
                    }
                    _render_email_badge(email_meta)

                for fig in pending_charts:
                    st.plotly_chart(fig, use_container_width=True)

        st.session_state.frel_messages.append({
            "role": "assistant",
            "content": frel_response,
            "charts": pending_charts,
            "email_sent": email_sent,
            "email_meta": email_meta,
        })
        _scroll_to_bottom()

    # Footer
    st.markdown(
        f"""<div style="text-align:center;margin-top:12px">
        <span style="font-size:0.73rem;color:#94a3b8">
            FREL Agent · 18 tools · Reports sent to {email_config.to_address}
        </span></div>""",
        unsafe_allow_html=True,
    )
