"""
fipsar_theme.py
---------------
Single source of truth for FIPSAR logo-aligned colors and shared Streamlit CSS.

Palette: cyan #00AEEF → navy #0033A0 gradient, black typography, white surfaces.
"""

from __future__ import annotations

# ── Brand ─────────────────────────────────────────────────────────────────
CYAN = "#00AEEF"
NAVY = "#0033A0"
BLACK = "#000000"
TEXT_PRIMARY = "#0f172a"
TEXT_SECONDARY = "#475569"
TEXT_MUTED = "#64748b"

SURFACE = "#FFFFFF"
PAGE_BG = "#FAFBFC"
SIDEBAR_BG = "#F8FAFC"
BORDER = "#E2E8F0"
BORDER_STRONG = "#CBD5E1"

SHADOW_SM = "0 1px 8px rgba(0,0,0,0.06)"
SHADOW_MD = "0 2px 16px rgba(0,0,0,0.07)"
SHADOW_LG = "0 4px 24px rgba(0,51,160,0.08)"

RADIUS_SM = "6px"
RADIUS_MD = "8px"
RADIUS_LG = "14px"

GRADIENT = f"linear-gradient(135deg, {CYAN} 0%, {NAVY} 100%)"
GRADIENT_VERTICAL = f"linear-gradient(168deg, {CYAN} 0%, #4A90D9 50%, {NAVY} 100%)"

# Semantic (charts, alerts) — readable on white
SUCCESS = "#059669"
WARNING = "#D97706"
DANGER = "#DC2626"
INFO = "#0284C7"
ACCENT_PURPLE = "#7C3AED"

# Plotly (light theme)
PLOT_PAPER = "#FFFFFF"
PLOT_BG = "#FAFBFC"
PLOT_TEXT = "#0f172a"
PLOT_GRID = "#E2E8F0"
PLOT_MUTED = "#64748b"


def streamlit_global_css() -> str:
    """Inject once via st.markdown(..., unsafe_allow_html=True)."""
    return f"""
<style>
html, body, [class*="css"] {{
    font-family: 'Inter', 'Segoe UI', system-ui, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
}}
.block-container {{
    padding-top: 0.6rem !important;
    padding-bottom: 2.5rem !important;
    max-width: 1480px;
}}

/* Main area — light */
[data-testid="stMain"]       {{ background: {PAGE_BG} !important; }}
[data-testid="stMain"] > div {{ background: {PAGE_BG} !important; }}

/* Sidebar — light */
[data-testid="stSidebar"] {{
    background: {SIDEBAR_BG} !important;
    border-right: 1px solid {BORDER} !important;
    box-shadow: 4px 0 20px rgba(0,0,0,0.04) !important;
}}
[data-testid="stSidebar"] > div {{ background: transparent !important; }}

[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] small,
[data-testid="stSidebar"] li    {{ color: {TEXT_SECONDARY} !important; }}

[data-testid="stSidebar"] h1 {{
    color: {TEXT_PRIMARY} !important;
    font-size: 1.1rem !important;
    font-weight: 800 !important;
    letter-spacing: 0.4px;
}}
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {{
    color: {NAVY} !important;
    font-size: 0.68rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 1.4px !important;
}}
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {{
    color: {TEXT_MUTED} !important;
    font-size: 0.71rem !important;
}}
[data-testid="stSidebar"] hr {{
    border-color: {BORDER} !important;
    margin: 10px 0 !important;
}}

[data-testid="stSidebar"] .stButton > button {{
    background: {SURFACE} !important;
    color: {NAVY} !important;
    border: 1px solid {BORDER} !important;
    border-radius: {RADIUS_MD} !important;
    font-size: 0.74rem !important;
    font-weight: 600 !important;
    padding: 6px 10px !important;
    transition: all 0.18s ease !important;
}}
[data-testid="stSidebar"] .stButton > button:hover {{
    background: linear-gradient(135deg, rgba(0,174,239,0.12) 0%, rgba(0,51,160,0.08) 100%) !important;
    border-color: {CYAN} !important;
    color: {TEXT_PRIMARY} !important;
    box-shadow: {SHADOW_SM} !important;
}}

[data-testid="stSidebar"] [data-testid="stExpander"] {{
    background: {SURFACE} !important;
    border: 1px solid {BORDER} !important;
    border-radius: {RADIUS_MD} !important;
}}
[data-testid="stSidebar"] [data-testid="stExpander"] summary {{
    color: {TEXT_PRIMARY} !important;
    font-size: 0.76rem !important;
}}
[data-testid="stSidebar"] [data-testid="stExpander"] .stButton > button {{
    background: transparent !important;
    font-size: 0.72rem !important;
    text-align: left !important;
    padding: 5px 8px !important;
    border-radius: {RADIUS_SM} !important;
    white-space: normal !important;
    height: auto !important;
    line-height: 1.35 !important;
}}
[data-testid="stSidebar"] [data-testid="stExpander"] .stButton > button:hover {{
    background: rgba(0,174,239,0.1) !important;
}}
[data-testid="stSidebar"] [data-testid="stAlert"] {{
    background: {SURFACE} !important;
    border-radius: {RADIUS_MD} !important;
    border-left: 3px solid {CYAN} !important;
}}
[data-testid="stSidebar"] [data-testid="stImage"] {{
    display: flex !important;
    justify-content: center !important;
    margin: 10px auto 0 !important;
}}
[data-testid="stSidebar"] [data-testid="stImage"] img {{
    border-radius: 10px !important;
    background: {SURFACE} !important;
    padding: 6px !important;
    box-shadow: {SHADOW_SM} !important;
}}
[data-testid="stSidebar"] [data-testid="stHorizontalBlock"] {{ gap: 0 !important; padding: 0 !important; }}
[data-testid="stSidebar"] .stImage > img {{ max-width: 80px !important; }}

/* Tabs */
[data-testid="stTabs"] > div:first-child {{
    position: sticky;
    top: 0;
    z-index: 999;
    background: {PAGE_BG} !important;
    padding: 10px 0 6px;
    border-bottom: 1px solid {BORDER};
}}
[data-baseweb="tab-list"] {{
    background: {SURFACE} !important;
    border-radius: {RADIUS_LG} !important;
    padding: 5px !important;
    gap: 3px !important;
    box-shadow: {SHADOW_MD}, 0 0 0 1px {BORDER} !important;
    border: none !important;
}}
[data-baseweb="tab"] {{
    background: transparent !important;
    border-radius: 10px !important;
    padding: 10px 26px !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    color: {TEXT_MUTED} !important;
    border: none !important;
    transition: all 0.15s ease !important;
}}
[data-baseweb="tab"]:hover {{
    background: rgba(0,174,239,0.08) !important;
    color: {NAVY} !important;
}}
[aria-selected="true"][data-baseweb="tab"] {{
    background: {GRADIENT} !important;
    color: #ffffff !important;
    box-shadow: 0 3px 12px rgba(0,51,160,0.22) !important;
    font-weight: 700 !important;
}}
[data-baseweb="tab-highlight"] {{ background: transparent !important; height: 0 !important; }}
[data-baseweb="tab-border"]    {{ display: none !important; }}

/* Chat — user: light surface; assistant: brand gradient + white text */
[data-testid="stChatMessage"] {{
    border-radius: 16px !important;
    margin: 4px 0 10px !important;
    padding: 2px 10px !important;
    border: 1px solid {BORDER} !important;
    background: {SURFACE} !important;
    box-shadow: {SHADOW_SM} !important;
}}

[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]),
[data-testid="stChatMessage"]:has([aria-label*="user" i]) {{
    background: #f1f5f9 !important;
    border: 1px solid {BORDER} !important;
    box-shadow: {SHADOW_SM} !important;
}}
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) p,
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) .stMarkdown p,
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) li,
[data-testid="stChatMessage"]:has([aria-label*="user" i]) p,
[data-testid="stChatMessage"]:has([aria-label*="user" i]) .stMarkdown p {{
    color: {TEXT_PRIMARY} !important;
}}

[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]),
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) {{
    background: {GRADIENT} !important;
    border: none !important;
    box-shadow: 0 2px 16px rgba(0,51,160,0.2) !important;
}}
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) p,
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) .stMarkdown p,
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) li,
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) td,
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) th,
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) p,
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) .stMarkdown p {{
    color: #ffffff !important;
}}
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) code,
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) code {{
    background: rgba(0,0,0,0.25) !important;
    color: #f8fafc !important;
}}
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) pre,
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) pre {{
    background: rgba(0,0,0,0.2) !important;
    border: 1px solid rgba(255,255,255,0.2) !important;
}}
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) table,
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) table {{
    background: rgba(255,255,255,0.12) !important;
}}
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) th,
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) td,
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) th,
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) td {{
    border-color: rgba(255,255,255,0.25) !important;
}}

/* Fix for invisible text on buttons and captions inside assistant messages */
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) .stButton > button p,
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) .stButton > button p,
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) .stButton > button,
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) .stButton > button {{
    color: {NAVY} !important;
    background: rgba(255,255,255,0.92) !important;
    border: 1px solid rgba(0,51,160,0.25) !important;
}}
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) .stButton > button:hover,
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) .stButton > button:hover {{
    background: #ffffff !important;
    border-color: {NAVY} !important;
    color: {NAVY} !important;
}}
/* "Next questions" caption label */
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) [data-testid="stCaptionContainer"],
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) [data-testid="stCaptionContainer"],
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) [data-testid="stCaptionContainer"] p,
[data-testid="stChatMessage"]:has([aria-label*="assistant" i]) [data-testid="stCaptionContainer"] p {{
    color: rgba(255,255,255,0.85) !important;
}}

[data-testid="stChatInput"] {{
    border-radius: 28px !important;
    box-shadow: {SHADOW_MD} !important;
    border: 2px solid {BORDER} !important;
    background: {SURFACE} !important;
}}
[data-testid="stChatInput"]:focus-within {{
    border-color: {CYAN} !important;
    box-shadow: 0 0 0 4px rgba(0,174,239,0.12), {SHADOW_MD} !important;
}}

.stButton > button {{
    border-radius: {RADIUS_MD} !important;
    font-weight: 600 !important;
    font-size: 0.81rem !important;
    border: 1px solid {BORDER} !important;
}}
.stButton > button[kind="primary"] {{
    background: {GRADIENT} !important;
    color: #fff !important;
    border: none !important;
}}
.stButton > button:hover {{
    transform: translateY(-1px) !important;
    box-shadow: {SHADOW_MD} !important;
}}

[data-testid="stPlotlyChart"] {{
    background: {SURFACE} !important;
    border-radius: 16px !important;
    box-shadow: {SHADOW_SM} !important;
    border: 1px solid {BORDER} !important;
    padding: 4px !important;
}}

[data-testid="stExpander"] {{
    border-radius: 12px !important;
    border: 1px solid {BORDER} !important;
    background: {SURFACE} !important;
    box-shadow: {SHADOW_SM} !important;
}}

[data-testid="stAlert"] {{ border-radius: 10px !important; }}

[data-testid="stSelectbox"] > div > div,
[data-testid="stDateInput"] > div > div {{
    border-radius: {RADIUS_MD} !important;
    border-color: {BORDER} !important;
}}

::-webkit-scrollbar {{ width: 4px; height: 4px; }}
::-webkit-scrollbar-track {{ background: #f1f5f9; border-radius: 8px; }}
::-webkit-scrollbar-thumb {{ background: #94a3b8; border-radius: 8px; }}

[data-testid="stSpinner"] > div {{ color: {NAVY} !important; }}

[data-testid="stHorizontalBlock"] hr {{
    border-color: {BORDER} !important;
    margin: 6px 0 !important;
}}
</style>
"""
