#!/usr/bin/env python3
"""
ZYN Sales Intelligence — Dashboard Interativo v5
Drill-down completo: Gestora → Fundo → Papel → Emissor → Devedor
Design minimalista com identidade visual ZYN Capital
"""
import sys
import json
from pathlib import Path
from io import BytesIO

sys.path.insert(0, str(Path(__file__).resolve().parent))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import subprocess

from config.settings import DATA_DIR, OUTPUT_DIR
from src.analyzer import build_investor_profiles, match_deal_to_investors
from src.family_offices import load_family_offices, add_investor, search_by_appetite
from src.report_generator import export_investor_profiles, export_deal_matching
from src.notion_pipeline import (
    pipeline_to_df, active_deals, deals_by_status,
    investor_frequency, match_pipeline_to_cvm, pipeline_sync_date,
)
from src.notion_gestao import (
    receitas_df, despesas_df, fluxo_df, leads_df,
    kpis_resumo, gestao_sync_date, sync_gestao, load_cache,
)
from src.us_market import (
    load_us_holdings, load_us_profiles, refresh_us_data,
    us_market_summary, match_us_investors_to_deal,
)


def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Dados") -> bytes:
    """Converte DataFrame para bytes Excel prontos para download."""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]
        for col_cells in ws.columns:
            max_len = max(len(str(c.value or "")) for c in col_cells)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 3, 50)
    return buf.getvalue()


def safe_min(s):
    vals = s.dropna()
    return vals.min() if len(vals) > 0 else "—"

def safe_max(s):
    vals = s.dropna()
    return vals.max() if len(vals) > 0 else "—"

def excel_btn(df: pd.DataFrame, filename: str, label: str = "📥 Exportar Excel", key: str = None):
    """Botão de download Excel inline."""
    st.download_button(
        label=label,
        data=to_excel_bytes(df),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=key,
    )

# === CONFIG ===
st.set_page_config(
    page_title="ZYN Sales Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# === CORES ZYN ===
NAVY = "#223040"
GRAY = "#8B9197"
GREEN = "#2E7D4F"
WHITE = "#FFFFFF"
LIGHT_BG = "#F5F6F8"
DARK_NAVY = "#1a2530"
ACCENT_GREEN = "#34905a"

# Paleta para charts
CHART_COLORS = [NAVY, GREEN, "#E6A817", "#8B5CF6", GRAY, "#D4526E", "#2196F3", "#FF6B6B"]

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;500;600;700&display=swap');

    /* === Base === */
    .stApp {{
        background-color: {LIGHT_BG};
        font-family: 'Montserrat', -apple-system, BlinkMacSystemFont, sans-serif;
    }}
    .stApp [data-testid="stAppViewContainer"] {{
        font-family: 'Montserrat', -apple-system, BlinkMacSystemFont, sans-serif;
    }}
    h1, h2, h3, h4, h5, h6,
    .stMarkdown, p, span, div, label, .stSelectbox, .stMultiSelect, .stTextInput {{
        font-family: 'Montserrat', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }}

    /* === Sidebar === */
    section[data-testid="stSidebar"] {{
        background: linear-gradient(180deg, {NAVY} 0%, {DARK_NAVY} 100%);
        border-right: none;
    }}
    section[data-testid="stSidebar"] * {{
        color: white !important;
    }}
    section[data-testid="stSidebar"] hr {{
        border-color: rgba(255,255,255,0.1);
    }}

    /* === Sidebar nav buttons === */
    section[data-testid="stSidebar"] .stButton > button {{
        background: transparent !important;
        color: rgba(255,255,255,0.7) !important;
        border: none !important;
        text-align: left !important;
        justify-content: flex-start !important;
        font-size: 0.82rem !important;
        font-weight: 400 !important;
        padding: 0.35rem 0.8rem !important;
        margin: 0 !important;
        border-radius: 5px !important;
        box-shadow: none !important;
        transition: all 0.12s ease !important;
    }}
    section[data-testid="stSidebar"] .stButton > button:hover {{
        background: rgba(255,255,255,0.06) !important;
        color: white !important;
    }}
    /* Active nav button (primary type) */
    section[data-testid="stSidebar"] .stButton > button[kind="primary"] {{
        background: rgba(46,125,79,0.15) !important;
        color: white !important;
        font-weight: 500 !important;
        border-left: 2px solid {GREEN} !important;
        border-radius: 0 5px 5px 0 !important;
    }}
    /* Remove Streamlit element gaps in sidebar */
    section[data-testid="stSidebar"] .stElementContainer {{
        margin: 0 !important;
        padding: 0 !important;
    }}
    section[data-testid="stSidebar"] .stVerticalBlock {{
        gap: 0 !important;
    }}

    /* === Page Header === */
    .main-header {{
        background: {NAVY};
        padding: 1.6rem 2rem;
        border-radius: 10px;
        margin-bottom: 1.8rem;
        color: white;
        position: relative;
        overflow: hidden;
    }}
    .main-header::before {{
        content: '';
        position: absolute;
        top: 0; right: 0;
        width: 200px; height: 100%;
        background: linear-gradient(135deg, transparent 0%, rgba(46,125,79,0.15) 100%);
    }}
    .main-header h1 {{
        color: white;
        margin: 0;
        font-size: 1.5rem;
        font-weight: 600;
        font-family: 'Montserrat', sans-serif !important;
        letter-spacing: -0.02em;
    }}
    .main-header p {{
        color: rgba(255,255,255,0.55);
        margin: 0.25rem 0 0;
        font-size: 0.82rem;
        font-weight: 400;
        letter-spacing: 0.01em;
    }}

    /* === Metric Cards === */
    .metric-card {{
        background: white;
        border-radius: 8px;
        padding: 1.1rem 1.2rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.03);
        border-left: 3px solid {GREEN};
        transition: box-shadow 0.2s ease;
    }}
    .metric-card:hover {{
        box-shadow: 0 4px 12px rgba(0,0,0,0.07);
    }}
    .metric-value {{
        font-size: 1.6rem;
        font-weight: 700;
        color: {NAVY};
        font-family: 'Montserrat', sans-serif !important;
        letter-spacing: -0.03em;
        line-height: 1.2;
    }}
    .metric-label {{
        font-size: 0.72rem;
        color: {GRAY};
        margin-top: 0.25rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}

    /* === Section Headers === */
    .stApp h2 {{
        font-family: 'Montserrat', sans-serif !important;
        color: {NAVY};
        font-weight: 600;
        font-size: 1.15rem;
        letter-spacing: -0.01em;
    }}
    .stApp h3 {{
        font-family: 'Montserrat', sans-serif !important;
        color: {NAVY};
        font-weight: 600;
        font-size: 1rem;
    }}

    /* === DataFrames === */
    .stDataFrame {{
        border-radius: 8px;
        overflow: hidden;
    }}
    [data-testid="stDataFrame"] {{
        border: 1px solid rgba(0,0,0,0.06);
        border-radius: 8px;
    }}

    /* === Buttons === */
    .stDownloadButton > button,
    .stButton > button {{
        font-family: 'Montserrat', sans-serif !important;
        font-weight: 500;
        font-size: 0.82rem;
        border-radius: 6px;
        letter-spacing: 0.01em;
    }}
    .stDownloadButton > button {{
        background: {NAVY};
        color: white;
        border: none;
    }}
    .stDownloadButton > button:hover {{
        background: {DARK_NAVY};
    }}
    button[kind="primary"] {{
        background: {GREEN} !important;
        border: none !important;
    }}
    button[kind="primary"]:hover {{
        background: {ACCENT_GREEN} !important;
    }}

    /* === Inputs === */
    .stTextInput input, .stNumberInput input {{
        font-family: 'Montserrat', sans-serif !important;
        border-radius: 6px;
        font-size: 0.85rem;
    }}
    .stSelectbox > div > div,
    .stMultiSelect > div > div {{
        border-radius: 6px;
    }}
    .stTextInput label, .stNumberInput label, .stSelectbox label, .stMultiSelect label {{
        font-size: 0.78rem;
        font-weight: 500;
        color: {NAVY};
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }}

    /* === Metrics (Streamlit native) === */
    [data-testid="stMetric"] {{
        background: white;
        padding: 0.8rem 1rem;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        border-left: 3px solid {GREEN};
        overflow: visible !important;
        min-width: 0 !important;
    }}
    [data-testid="stMetricLabel"] {{
        font-size: 0.68rem !important;
        text-transform: uppercase;
        letter-spacing: 0.03em;
        font-weight: 500;
        white-space: normal !important;
        overflow: visible !important;
        text-overflow: unset !important;
        line-height: 1.3 !important;
    }}
    [data-testid="stMetricLabel"] p {{
        white-space: normal !important;
        overflow: visible !important;
        text-overflow: unset !important;
    }}
    [data-testid="stMetricValue"] {{
        font-family: 'Montserrat', sans-serif !important;
        font-weight: 700;
        color: {NAVY};
        white-space: normal !important;
        overflow: visible !important;
        text-overflow: unset !important;
        word-break: break-word !important;
        font-size: clamp(1rem, 2vw, 1.8rem) !important;
        line-height: 1.2 !important;
    }}
    [data-testid="stMetricValue"] div {{
        white-space: normal !important;
        overflow: visible !important;
        text-overflow: unset !important;
    }}

    /* === Custom info row === */
    .info-row {{
        display: flex;
        gap: 2rem;
        flex-wrap: wrap;
        margin: 0.5rem 0 1rem;
    }}
    .info-row .info-item {{
        font-size: 0.85rem;
        color: {NAVY};
    }}
    .info-row .info-item strong {{
        font-weight: 600;
    }}

    /* === Dividers === */
    hr {{
        border: none;
        border-top: 1px solid rgba(0,0,0,0.06);
        margin: 1.5rem 0;
    }}

    /* === Expanders === */
    .streamlit-expanderHeader {{
        font-family: 'Montserrat', sans-serif !important;
        font-weight: 500;
        font-size: 0.88rem;
    }}

    /* === Sidebar brand mark === */
    .sidebar-brand {{
        padding: 0.8rem 0 0.6rem;
        text-align: left;
        padding-left: 0.5rem;
    }}
    .sidebar-brand .brand-zyn {{
        color: white !important;
        font-family: 'Montserrat', sans-serif !important;
        font-weight: 700;
        font-size: 1.75rem;
        letter-spacing: 0.06em;
        margin: 0;
        line-height: 1;
        display: inline-block;
    }}
    .sidebar-brand .brand-dot {{
        color: {GREEN};
        font-size: 1.75rem;
        font-weight: 700;
    }}
    .sidebar-brand .brand-capital {{
        color: rgba(255,255,255,0.45);
        font-family: 'Montserrat', sans-serif !important;
        font-size: 0.65rem;
        font-weight: 400;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        margin-top: 0.1rem;
    }}
    .sidebar-brand .brand-sub {{
        color: rgba(255,255,255,0.3);
        font-family: 'Montserrat', sans-serif !important;
        font-size: 0.55rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        margin-top: 0.4rem;
        padding-top: 0.4rem;
        border-top: 1px solid rgba(255,255,255,0.08);
    }}
    .sidebar-stats {{
        font-size: 0.75rem;
        color: rgba(255,255,255,0.5);
        line-height: 1.7;
    }}
    .sidebar-stats strong {{
        color: rgba(255,255,255,0.75);
    }}

    /* === Misc === */
    .stAlert {{
        border-radius: 8px;
    }}
    .stSpinner {{
        font-family: 'Montserrat', sans-serif !important;
    }}
    /* Hide Streamlit branding */
    footer {{ visibility: hidden; }}
    #MainMenu {{ visibility: hidden; }}

    /* === Sidebar — kill all Streamlit default backgrounds === */
    section[data-testid="stSidebar"] div[data-testid="stVerticalBlockBorderWrapper"],
    section[data-testid="stSidebar"] div[data-testid="stVerticalBlockBorderWrapper"] > div,
    section[data-testid="stSidebar"] div[data-testid="element-container"] {{
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
    }}

    /* === Sidebar link buttons === */
    section[data-testid="stSidebar"] .stLinkButton a {{
        font-size: 0.78rem !important;
        font-weight: 500 !important;
        color: rgba(255,255,255,0.6) !important;
        text-decoration: none !important;
        border: 1px solid rgba(255,255,255,0.12) !important;
        background: transparent !important;
        border-radius: 6px !important;
        transition: all 0.15s ease !important;
    }}
    section[data-testid="stSidebar"] .stLinkButton a:hover {{
        color: white !important;
        border-color: {GREEN} !important;
        background: rgba(46,125,79,0.12) !important;
    }}

    /* === Sidebar footer === */
    .sidebar-footer {{
        font-size: 0.62rem;
        color: rgba(255,255,255,0.25);
        letter-spacing: 0.03em;
        line-height: 1.5;
        text-align: center;
        padding-top: 0.3rem;
    }}
    .sidebar-footer a {{
        color: rgba(255,255,255,0.35) !important;
        text-decoration: none;
    }}
    .sidebar-footer a:hover {{
        color: {GREEN} !important;
    }}
</style>
""", unsafe_allow_html=True)


# === DATA ===
@st.cache_data(ttl=3600)
def load_positions():
    cache = DATA_DIR / "positions_enriched.csv"
    if cache.exists():
        return pd.read_csv(cache, low_memory=False)
    return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_profiles():
    cache = DATA_DIR / "investor_profiles.csv"
    if cache.exists():
        return pd.read_csv(cache, low_memory=False)
    positions = load_positions()
    if not positions.empty:
        return build_investor_profiles(positions)
    return pd.DataFrame()


def fmt_br(value):
    """Formato monetário brasileiro completo, sem abreviar (R$ 91.212,00)."""
    if not isinstance(value, (int, float)):
        return str(value) if value else "—"
    if pd.isna(value) or value == 0:
        return "—"
    # Formata com separador de milhar (.) e decimal (,)
    negativo = value < 0
    v = abs(value)
    if v >= 100:
        formatted = f"{v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    else:
        formatted = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"-R$ {formatted}" if negativo else f"R$ {formatted}"


def fmt(value):
    if not isinstance(value, (int, float)):
        return str(value) if value else "—"
    if pd.isna(value) or value == 0:
        return "—"
    v = abs(value)
    if v >= 1e9:
        return f"R$ {value/1e9:.2f}B"
    if v >= 1e6:
        return f"R$ {value/1e6:.1f}M"
    if v >= 1e3:
        return f"R$ {value/1e3:.0f}K"
    return f"R$ {value:,.0f}"


import urllib.parse as _urlparse


def share_buttons(title: str, body: str):
    """Render compact WhatsApp + Email share icons in top-right corner."""
    wa_text = f"*{title}*\n{body}\n\n_Enviado via ZYN Sales Intelligence_"
    wa_url = f"https://wa.me/?text={_urlparse.quote(wa_text)}"
    mail_subj = _urlparse.quote(title)
    mail_body = _urlparse.quote(f"{body}\n\nEnviado via ZYN Sales Intelligence")
    mail_url = f"mailto:?subject={mail_subj}&body={mail_body}"
    st.markdown(
        f'<div style="position:fixed;top:60px;right:18px;z-index:999;display:flex;gap:6px;">'
        f'<a href="{wa_url}" target="_blank" title="Compartilhar no WhatsApp" '
        f'style="background:#25D366;color:white;width:34px;height:34px;border-radius:50%;'
        f'display:flex;align-items:center;justify-content:center;text-decoration:none;font-size:18px;'
        f'box-shadow:0 2px 6px rgba(0,0,0,.2);">💬</a>'
        f'<a href="{mail_url}" title="Enviar por E-mail" '
        f'style="background:#223040;color:white;width:34px;height:34px;border-radius:50%;'
        f'display:flex;align-items:center;justify-content:center;text-decoration:none;font-size:18px;'
        f'box-shadow:0 2px 6px rgba(0,0,0,.2);">✉️</a>'
        f'</div>',
        unsafe_allow_html=True,
    )


# === SIDEBAR ===
with st.sidebar:
    # ── Brand ──
    st.markdown("""<div class="sidebar-brand">
        <svg viewBox="0 0 220 100" xmlns="http://www.w3.org/2000/svg" style="width:140px;height:auto;">
            <text x="110" y="52" text-anchor="middle" fill="#FFFFFF" font-family="Montserrat,Helvetica,Arial,sans-serif" font-weight="700" font-size="52" letter-spacing="4">ZYN</text>
            <text x="110" y="78" text-anchor="middle" fill="rgba(255,255,255,0.45)" font-family="Montserrat,Helvetica,Arial,sans-serif" font-weight="400" font-size="20" letter-spacing="8">CAPITAL</text>
            <line x1="40" y1="86" x2="180" y2="86" stroke="rgba(255,255,255,0.08)" stroke-width="1"/>
            <text x="110" y="97" text-anchor="middle" fill="rgba(255,255,255,0.3)" font-family="Montserrat,Helvetica,Arial,sans-serif" font-weight="400" font-size="8" letter-spacing="3">SALES INTELLIGENCE</text>
        </svg>
    </div>""", unsafe_allow_html=True)

    # ── Navigation ──
    _SECTIONS = {
        "BRASIL": ["Visão Geral", "Gestoras", "Fundos & Papéis", "Emissores",
                    "Devedores", "Fundos com Caixa", "Matching"],
        "INTERNACIONAL": ["Visão Geral US", "Fund Managers", "Holdings Brasil", "Matching US"],
        "GESTÃO": ["Pipeline", "Pipeline x Investidores", "Oportunidades", "Alertas"],
        "MERCADO": ["Cotações"],
        "CONFIG": ["Base Manual", "Atualizar"],
    }

    # Painel Executivo — standalone at top
    if st.button("Painel Executivo", key="nav_painel", use_container_width=True,
                  type="primary" if st.session_state.get("active_page") == "Painel Executivo" else "secondary"):
        st.session_state.active_page = "Painel Executivo"
        st.rerun()

    for section, items in _SECTIONS.items():
        st.markdown(f'<p style="font-size:0.6rem;font-weight:600;letter-spacing:0.12em;'
                    f'color:rgba(255,255,255,0.3);margin:1rem 0 0.3rem 0;padding:0;">'
                    f'{section}</p>', unsafe_allow_html=True)
        for item in items:
            active = st.session_state.get("active_page") == item
            if st.button(
                item, key=f"nav_{item}",
                use_container_width=True,
                type="primary" if active else "secondary",
            ):
                st.session_state.active_page = item
                st.rerun()

    page = st.session_state.get("active_page", "Painel Executivo")

    # ── Stats ──
    st.markdown("---")
    positions = load_positions()
    if not positions.empty:
        cache_path = DATA_DIR / "positions_enriched.csv"
        gestoras = positions["gestora"].nunique() if "gestora" in positions.columns else 0
        fundos = positions['cnpj_fundo'].nunique()
        atualizado = datetime.fromtimestamp(cache_path.stat().st_mtime).strftime('%d/%m/%Y') if cache_path.exists() else "—"
        st.markdown(f"""<div class="sidebar-stats">
            <strong>{len(positions):,}</strong> posições · <strong>{gestoras}</strong> gestoras · <strong>{fundos:,}</strong> fundos<br>
            Atualizado: <strong>{atualizado}</strong>
        </div>""", unsafe_allow_html=True)

    # ── External Links ──
    st.markdown("---")
    st.link_button("Análise de Crédito", "https://zyn-credit-engine.streamlit.app/", use_container_width=True)
    st.link_button("Financeiro", "https://zyn-financeiro.streamlit.app/", use_container_width=True)

    # ── Footer ──
    st.markdown("---")
    st.markdown("""<div class="sidebar-footer">
        ZYN Capital &copy; 2026<br>
        Crédito Estruturado &middot; M&A
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════
# PAINEL EXECUTIVO
# ══════════════════════════════════════════
if page == "Painel Executivo":
    st.markdown("""<div class="main-header">
        <h1>Painel Executivo</h1>
        <p>Gestão ZYN Capital — Pipeline, Receitas, Despesas, Fluxo de Caixa e Indicadores</p>
    </div>""", unsafe_allow_html=True)

    # Check cache
    _gc = load_cache()
    if not _gc:
        st.warning("Dados de gestão não carregados. Vá em **Atualizar** → Sync Painel Notion.")
        st.stop()

    _sync_dt = gestao_sync_date()
    st.caption(f"Última sincronização: {_sync_dt}")

    # Load data
    _kpis = kpis_resumo()
    _rec = receitas_df()
    _desp = despesas_df()
    _fluxo = fluxo_df()
    _leads = leads_df()
    _pipe = pipeline_to_df()
    _ativos = _pipe[_pipe["Status"] != "Declinado"] if not _pipe.empty else pd.DataFrame()

    # ── Share buttons ──
    _quentes = len(_ativos[_ativos["Status"] == "Quente"]) if not _ativos.empty and "Status" in _ativos.columns else 0
    _mornos = len(_ativos[_ativos["Status"] == "Morno"]) if not _ativos.empty and "Status" in _ativos.columns else 0
    _frios = len(_ativos[_ativos["Status"] == "Frio"]) if not _ativos.empty and "Status" in _ativos.columns else 0
    _conv = f"{_kpis['leads_convertidos']}/{_kpis['leads_total']}" if _kpis.get("leads_total") else "—"
    _hoje = datetime.now().strftime("%d/%m/%Y")
    _deal_lines = ""
    if not _ativos.empty and len(_ativos) <= 15:
        _dl = []
        for _, d in _ativos.iterrows():
            _n = str(d.get("Cliente", "")).strip()[:25]
            _s = str(d.get("Status", ""))
            _v = fmt_br(d["Valor"]) if pd.notna(d.get("Valor")) else "—"
            _e = {"Quente": "🔴", "Morno": "🟡", "Frio": "🔵"}.get(_s, "⚪")
            _dl.append(f"  {_e} {_n} — {_v}")
        _deal_lines = "\n\nDeals ativos:\n" + "\n".join(_dl)
    share_buttons(
        f"ZYN Capital — Painel Executivo ({_hoje})",
        f"Pipeline: {fmt_br(_kpis['pipe_total'])} ({_kpis['pipe_count']} deals)\n"
        f"  Quentes: {_quentes} | Mornos: {_mornos} | Frios: {_frios}\n\n"
        f"Receita Recebida: {fmt_br(_kpis['rec_recebida'])}\n"
        f"Receita Confirmada: {fmt_br(_kpis['rec_confirmada'])}\n"
        f"Receita Prevista: {fmt_br(_kpis['rec_prevista'])}\n\n"
        f"Despesas: {fmt_br(_kpis['desp_paga'])}\n"
        f"Burn Rate: {fmt_br(_kpis['burn_rate'])}/mês\n"
        f"Saldo C6: {fmt_br(_kpis['saldo_atual'])}\n"
        f"Runway: {_kpis['runway_meses']:.1f} meses\n\n"
        f"Leads: {_kpis['leads_ativos']} ativos | Conversão: {_conv}"
        f"{_deal_lines}",
    )

    # ── TABS ──
    tab_resumo, tab_pipe, tab_rec, tab_desp, tab_fluxo, tab_leads, tab_indic = st.tabs([
        "Resumo", "Pipeline", "Receitas", "Despesas", "Fluxo de Caixa", "Leads", "Indicadores",
    ])

    # ━━━━━━ TAB RESUMO ━━━━━━
    with tab_resumo:
        st.markdown("### Resumo Executivo — 2026")

        r1, r2, r3, r4 = st.columns(4)
        r1.markdown(f'<div class="metric-card"><div class="metric-value">{fmt_br(_kpis["pipe_total"])}</div><div class="metric-label">Pipeline Total ({_kpis["pipe_count"]} deals)</div></div>', unsafe_allow_html=True)
        r2.markdown(f'<div class="metric-card"><div class="metric-value">{fmt_br(_kpis["rec_recebida"])}</div><div class="metric-label">Receita Recebida</div></div>', unsafe_allow_html=True)
        r3.markdown(f'<div class="metric-card"><div class="metric-value">{fmt_br(_kpis["saldo_atual"])}</div><div class="metric-label">Saldo Banco C6</div></div>', unsafe_allow_html=True)
        r4.markdown(f'<div class="metric-card"><div class="metric-value">{fmt_br(_kpis["burn_rate"])}/mês</div><div class="metric-label">Burn Rate</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        r5, r6, r7, r8 = st.columns(4)
        r5.markdown(f'<div class="metric-card"><div class="metric-value">{fmt_br(_kpis["rec_confirmada"])}</div><div class="metric-label">Receita Confirmada</div></div>', unsafe_allow_html=True)
        r6.markdown(f'<div class="metric-card"><div class="metric-value">{fmt_br(_kpis["rec_prevista"])}</div><div class="metric-label">Receita Prevista</div></div>', unsafe_allow_html=True)
        r7.markdown(f'<div class="metric-card"><div class="metric-value">{_kpis["runway_meses"]:.1f} meses</div><div class="metric-label">Runway</div></div>', unsafe_allow_html=True)
        r8.markdown(f'<div class="metric-card"><div class="metric-value">{_kpis["leads_ativos"]}</div><div class="metric-label">Leads Ativos</div></div>', unsafe_allow_html=True)

        # ── Deal Flow Board ──
        st.markdown("---")
        st.markdown("### Deal Flow")

        # Calculate stage counts and values using Fase field
        _leads_ativos = _kpis["leads_ativos"]
        _leads_convertidos = _kpis["leads_convertidos"]

        # Fase-based counts (from Notion "Fase" multi_select)
        def _has_fase(df, fase):
            """Check if deal Fase contains the given value."""
            if df.empty or "Fase" not in df.columns:
                return df.head(0)
            return df[df["Fase"].str.contains(fase, case=False, na=False)]

        _em_analise = _has_fase(_ativos, "Em Analise")
        _ts_enviado = _has_fase(_ativos, "TS enviado ao cliente")
        _on_hold = _has_fase(_ativos, "On Hold")

        # Declinados (Leads + Pipeline)
        _leads_decl = len(_leads[_leads["status"] == "Declinado"]) if not _leads.empty and "status" in _leads.columns else 0
        _pipe_decl = _pipe[_pipe["Status"] == "Declinado"] if not _pipe.empty else pd.DataFrame()
        _n_decl = _leads_decl + len(_pipe_decl)

        _n_analise = len(_em_analise)
        _n_ts_env = len(_ts_enviado)
        _n_hold = len(_on_hold)
        _v_analise = _em_analise["Valor"].sum() if not _em_analise.empty else 0
        _v_ts_env = _ts_enviado["Valor"].sum() if not _ts_enviado.empty else 0

        # Operações (from data/operacoes.json)
        _ops_file = DATA_DIR / "operacoes.json"
        _ops = []
        if _ops_file.exists():
            with open(_ops_file, "r", encoding="utf-8") as _f:
                _ops = json.load(_f).get("operacoes", [])
        _n_ops = len(_ops)
        _v_ops = sum(o.get("valor_operacao", 0) or 0 for o in _ops)
        _fee_contratado = sum(o.get("fee_total", 0) or 0 for o in _ops)

        # GCB breakdown — Operações
        _ops_gcb = [o for o in _ops if "GCB" in (o.get("investidor", "") or "").upper()]
        _n_ops_gcb = len(_ops_gcb)
        _v_ops_gcb = sum(o.get("valor_operacao", 0) or 0 for o in _ops_gcb)
        _fee_gcb = sum(o.get("fee_total", 0) or 0 for o in _ops_gcb)

        # GCB breakdown — TS Enviado (deals com GCB em "Analisando")
        _ts_env_gcb = _ts_enviado[_ts_enviado["Analisando"].apply(
            lambda x: "GCB" in (x if isinstance(x, list) else [])
        )] if not _ts_enviado.empty and "Analisando" in _ts_enviado.columns else pd.DataFrame()
        _n_ts_gcb = len(_ts_env_gcb)
        _v_ts_gcb = _ts_env_gcb["Valor"].sum() if not _ts_env_gcb.empty else 0

        # Receita prevista = Fee Total dos TS enviados (estimativa 2% do valor)
        _fee_ts_env = _v_ts_env * 0.02 if _v_ts_env else 0
        _fee_ts_gcb = _v_ts_gcb * 0.02 if _v_ts_gcb else 0

        # Total analisado (Leads + Pipeline inteiro)
        _total_analisado = _kpis["leads_total"] + len(_pipe)

        # Funnel stages
        _stages = [
            ("Analisado", _total_analisado, f"Leads: {_kpis['leads_total']} · Pipe: {len(_pipe)}", None, "#455A64"),
            ("Leads Ativos", _leads_ativos, None, None, "#607D8B"),
            ("Em Análise", _n_analise, fmt_br(_v_analise), None, "#1E88E5"),
            ("TS Enviado", _n_ts_env, fmt_br(_v_ts_env),
             f"GCB: {_n_ts_gcb} deals · {fmt_br(_v_ts_gcb)}" if _n_ts_gcb else None, "#FB8C00"),
            ("Operações", _n_ops, fmt_br(_v_ops),
             f"GCB: {_n_ops_gcb} ops · {fmt_br(_v_ops_gcb)}" if _n_ops_gcb else None, GREEN),
            ("Rec. Prevista", None, fmt_br(_fee_ts_env),
             f"GCB: {fmt_br(_fee_ts_gcb)}" if _fee_ts_gcb else None, "#7B1FA2"),
            ("Rec. Contratada", None, fmt_br(_fee_contratado),
             f"GCB: {fmt_br(_fee_gcb)}" if _fee_gcb else None, "#00897B"),
            ("Rec. Recebida", None, fmt_br(_kpis["rec_recebida"]), None, GREEN),
            ("Declinado", _n_decl, f"Leads: {_leads_decl} · Pipe: {len(_pipe_decl)}", None, "#E53935"),
        ]

        # Render board as columns
        _board_cols = st.columns(len(_stages))
        for col, (label, count, value, gcb_detail, color) in zip(_board_cols, _stages):
            _count_html = f'<div style="font-size:1.6rem;font-weight:700;color:{color};">{count}</div>' if count is not None else ''
            _value_html = f'<div style="font-size:0.85rem;color:#223040;margin-top:0.15rem;">{value}</div>' if value else ''
            _gcb_html = (f'<div style="font-size:0.65rem;color:#D4AF37;font-weight:600;margin-top:0.3rem;'
                         f'border-top:1px solid rgba(212,175,55,0.2);padding-top:0.25rem;">{gcb_detail}</div>'
                         if gcb_detail else '')
            col.markdown(
                f'<div style="text-align:center;padding:0.8rem 0.3rem;border-top:3px solid {color};'
                f'background:#f8f9fb;border-radius:0 0 6px 6px;min-height:100px;">'
                f'<div style="font-size:0.65rem;font-weight:600;letter-spacing:0.05em;color:#8B9197;'
                f'text-transform:uppercase;margin-bottom:0.4rem;">{label}</div>'
                f'{_count_html}{_value_html}{_gcb_html}'
                f'</div>',
                unsafe_allow_html=True,
            )

        # Conversion rates
        st.markdown("<br>", unsafe_allow_html=True)
        _conv_leads = f"{_leads_convertidos}/{_kpis['leads_total']}" if _kpis.get("leads_total") else "—"
        _conv_pct = f"({_leads_convertidos*100/_kpis['leads_total']:.0f}%)" if _kpis.get("leads_total") and _kpis["leads_total"] > 0 else ""
        _pipe_to_ts = _n_ts_env + _n_ops
        _pipe_total_ativos = len(_ativos) if not _ativos.empty else 0
        _conv_pipe = f"{_pipe_to_ts}/{_pipe_total_ativos} ({_pipe_to_ts*100/max(_pipe_total_ativos,1):.0f}%)" if _pipe_total_ativos else "—"
        _conv_rec = f"{_kpis['rec_recebida']*100/max(_fee_contratado,1):.0f}%" if _fee_contratado else "—"

        cv1, cv2, cv3 = st.columns(3)
        cv1.markdown(f'<div style="text-align:center;font-size:0.75rem;color:#8B9197;">'
                     f'Lead → Pipeline: <strong style="color:#223040;">{_conv_leads} {_conv_pct}</strong></div>',
                     unsafe_allow_html=True)
        cv2.markdown(f'<div style="text-align:center;font-size:0.75rem;color:#8B9197;">'
                     f'Pipeline → TS/Op: <strong style="color:#223040;">{_conv_pipe}</strong></div>',
                     unsafe_allow_html=True)
        cv3.markdown(f'<div style="text-align:center;font-size:0.75rem;color:#8B9197;">'
                     f'Contratada → Recebida: <strong style="color:#223040;">{_conv_rec}</strong></div>',
                     unsafe_allow_html=True)

        # ── Deals by stage detail ──
        st.markdown("---")
        st.markdown("### Deals por Estágio")

        def _render_deal_cards(title, df_filtered, color):
            """Render deal cards for a stage."""
            if df_filtered.empty:
                st.caption(f"Nenhum deal em {title}")
                return
            for _, d in df_filtered.iterrows():
                _nome = str(d.get("Cliente", "")).strip()[:30]
                _val = fmt_br(d["Valor"]) if pd.notna(d.get("Valor")) and d.get("Valor") else "—"
                _tipo = str(d.get("Tipo", ""))
                _socio = str(d.get("Sócio", ""))
                _analisando = d.get("Analisando", [])
                _is_gcb = "GCB" in (_analisando if isinstance(_analisando, list) else [])
                _gcb_badge = (' <span style="background:#D4AF37;color:white;font-size:0.55rem;'
                              'padding:1px 5px;border-radius:3px;font-weight:700;vertical-align:middle;">GCB</span>'
                              if _is_gcb else '')
                _border_color = "#D4AF37" if _is_gcb else color
                _bg = "rgba(212,175,55,0.06)" if _is_gcb else "#f8f9fb"
                st.markdown(
                    f'<div style="padding:0.5rem 0.7rem;margin:0.25rem 0;border-left:3px solid {_border_color};'
                    f'background:{_bg};border-radius:0 4px 4px 0;font-size:0.8rem;">'
                    f'<strong style="color:#223040;">{_nome}</strong>{_gcb_badge}'
                    f'<span style="float:right;color:{color};font-weight:600;">{_val}</span><br>'
                    f'<span style="color:#8B9197;font-size:0.7rem;">{_tipo} · {_socio}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        if not _ativos.empty or _ops:
            dc1, dc2, dc3, dc4 = st.columns(4)
            with dc1:
                st.markdown(f'<p style="font-size:0.7rem;font-weight:600;color:{GREEN};text-transform:uppercase;'
                            f'letter-spacing:0.08em;">Operações ({_n_ops})</p>', unsafe_allow_html=True)
                for op in _ops:
                    _nome = (op.get("operacao") or op.get("cliente", ""))[:30]
                    _val = fmt_br(op["valor_operacao"]) if op.get("valor_operacao") else "—"
                    _fee = fmt_br(op["fee_total"]) if op.get("fee_total") else "—"
                    _st_op = op.get("status_operacao", "")
                    _is_gcb_op = "GCB" in (op.get("investidor", "") or "").upper()
                    _gcb_badge_op = (' <span style="background:#D4AF37;color:white;font-size:0.55rem;'
                                     'padding:1px 5px;border-radius:3px;font-weight:700;vertical-align:middle;">GCB</span>'
                                     if _is_gcb_op else '')
                    _border_op = "#D4AF37" if _is_gcb_op else GREEN
                    _bg_op = "rgba(212,175,55,0.06)" if _is_gcb_op else "#f8f9fb"
                    st.markdown(
                        f'<div style="padding:0.5rem 0.7rem;margin:0.25rem 0;border-left:3px solid {_border_op};'
                        f'background:{_bg_op};border-radius:0 4px 4px 0;font-size:0.8rem;">'
                        f'<strong style="color:#223040;">{_nome}</strong>{_gcb_badge_op}'
                        f'<span style="float:right;color:{GREEN};font-weight:600;">{_val}</span><br>'
                        f'<span style="color:#8B9197;font-size:0.7rem;">{_st_op} · Fee: {_fee}</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
            with dc2:
                st.markdown(f'<p style="font-size:0.7rem;font-weight:600;color:#FB8C00;text-transform:uppercase;'
                            f'letter-spacing:0.08em;">TS Enviado ({_n_ts_env})</p>', unsafe_allow_html=True)
                _render_deal_cards("TS Enviado", _ts_enviado, "#FB8C00")
            with dc3:
                st.markdown(f'<p style="font-size:0.7rem;font-weight:600;color:#1E88E5;text-transform:uppercase;'
                            f'letter-spacing:0.08em;">Em Análise ({_n_analise})</p>', unsafe_allow_html=True)
                _render_deal_cards("Em Análise", _em_analise, "#1E88E5")
            with dc4:
                st.markdown(f'<p style="font-size:0.7rem;font-weight:600;color:#607D8B;text-transform:uppercase;'
                            f'letter-spacing:0.08em;">On Hold ({_n_hold})</p>', unsafe_allow_html=True)
                _render_deal_cards("On Hold", _on_hold, "#607D8B")

        # ── Funil de Receita ──
        st.markdown("---")
        st.markdown("### Funil de Receita 2026")
        funil_data = pd.DataFrame([
            {"Estágio": "Recebido", "Valor": _kpis["rec_recebida"]},
            {"Estágio": "Confirmado", "Valor": _kpis["rec_confirmada"]},
            {"Estágio": "Previsto", "Valor": _kpis["rec_prevista"]},
        ])
        fig_funil = px.bar(funil_data, x="Estágio", y="Valor", color="Estágio",
                           color_discrete_map={"Recebido": GREEN, "Confirmado": "#1E88E5", "Previsto": GRAY},
                           text_auto=True)
        fig_funil.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                                height=300, showlegend=False, margin=dict(l=0,r=0,t=10,b=0))
        fig_funil.update_yaxes(tickprefix="R$ ", separatethousands=True)
        fig_funil.update_traces(texttemplate='R$ %{y:,.0f}', textposition='outside')
        st.plotly_chart(fig_funil, use_container_width=True)

    # ━━━━━━ TAB PIPELINE ━━━━━━
    with tab_pipe:
        st.markdown("### Pipeline — Deals Ativos")

        if not _ativos.empty:
            # KPIs
            pk1, pk2, pk3, pk4 = st.columns(4)
            pk1.metric("Deals Ativos", len(_ativos))
            _quentes = len(_ativos[_ativos["Status"] == "Quente"]) if "Status" in _ativos.columns else 0
            pk2.metric("Quentes", _quentes)
            pk3.metric("Pipeline Total", fmt_br(_ativos["Valor"].sum()))
            _ticket = _ativos["Valor"].dropna()
            pk4.metric("Ticket Médio", fmt_br(_ticket.mean()) if len(_ticket) > 0 else "—")

            # Deals por status
            st.markdown("---")
            col_ps, col_pt = st.columns(2)
            with col_ps:
                st.markdown("##### Por Status")
                status_ct = _ativos["Status"].value_counts().reset_index()
                status_ct.columns = ["Status", "Deals"]
                _status_colors = {"Quente": "#E53935", "Morno": "#FB8C00", "Frio": "#1E88E5",
                                  "TS Assinado - enviado Operações": GREEN}
                fig_st = px.pie(status_ct, values="Deals", names="Status", hole=0.45,
                                color="Status", color_discrete_map=_status_colors)
                fig_st.update_layout(height=280, margin=dict(l=0,r=0,t=10,b=0),
                                     plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_st, use_container_width=True)

            with col_pt:
                st.markdown("##### Por Tipo de Operação")
                if "Tipo" in _ativos.columns:
                    tipo_ct = _ativos.groupby("Tipo")["Valor"].agg(["sum", "count"]).reset_index()
                    tipo_ct.columns = ["Tipo", "Volume", "Deals"]
                    tipo_ct = tipo_ct.sort_values("Volume", ascending=True)
                    fig_tipo = px.bar(tipo_ct, y="Tipo", x="Volume", orientation="h",
                                      text="Deals", color_discrete_sequence=[GREEN])
                    fig_tipo.update_layout(height=280, margin=dict(l=0,r=20,t=10,b=0),
                                           plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    fig_tipo.update_xaxes(tickprefix="R$ ", separatethousands=True)
                    fig_tipo.update_traces(texttemplate='%{text} deals', textposition='auto')
                    st.plotly_chart(fig_tipo, use_container_width=True)

            # Deals por sócio
            st.markdown("---")
            st.markdown("##### Deals por Sócio")
            if "Sócio" in _ativos.columns:
                socio_ct = _ativos.groupby("Sócio").agg(
                    deals=("Cliente", "count"),
                    volume=("Valor", "sum"),
                ).reset_index().sort_values("volume", ascending=False)
                for _, sr in socio_ct.iterrows():
                    st.markdown(f"""
                    <div style="background:white;border-radius:6px;padding:0.7rem 1rem;margin-bottom:0.4rem;
                                border-left:4px solid {GREEN};display:flex;justify-content:space-between;align-items:center;">
                        <div><strong>{sr['Sócio'] or '—'}</strong></div>
                        <div style="text-align:right;color:{NAVY};font-weight:600;">
                            {sr['deals']} deals &nbsp;·&nbsp; {fmt_br(sr['volume'])}
                        </div>
                    </div>""", unsafe_allow_html=True)

            # Tabela completa
            st.markdown("---")
            st.markdown("##### Todos os Deals")
            tbl_p = _ativos[["Cliente", "Status", "Tipo", "Valor", "Sócio", "Instrumento"]].copy()
            tbl_p["Valor"] = tbl_p["Valor"].apply(fmt)
            st.dataframe(tbl_p, use_container_width=True, height=400)
        else:
            st.info("Nenhum deal ativo no pipeline.")

    # ━━━━━━ TAB RECEITAS ━━━━━━
    with tab_rec:
        st.markdown("### Receitas — 2026")

        if not _rec.empty:
            rec_2026 = _rec[_rec["ano"] == "2026"].copy() if "ano" in _rec.columns else _rec.copy()

            rk1, rk2, rk3, rk4 = st.columns(4)
            _r_bruto = rec_2026["valor_bruto"].sum() if "valor_bruto" in rec_2026.columns else 0
            _r_liq = rec_2026["valor_liq"].sum() if "valor_liq" in rec_2026.columns else 0
            _r_recebido = rec_2026[rec_2026["status"] == "Recebido"]["valor_liq"].sum() if "status" in rec_2026.columns else 0
            _r_finder = rec_2026["fee_finder_valor"].sum() if "fee_finder_valor" in rec_2026.columns and rec_2026["fee_finder_valor"].notna().any() else 0
            rk1.metric("Receita Bruta", fmt_br(_r_bruto))
            rk2.metric("Líquido ZYN", fmt_br(_r_liq))
            rk3.metric("Recebido", fmt_br(_r_recebido))
            rk4.metric("Fee Finders", fmt_br(_r_finder))

            # Por mês
            st.markdown("---")
            col_rm, col_rs = st.columns(2)
            with col_rm:
                st.markdown("##### Receita por Mês")
                if "mes" in rec_2026.columns:
                    from src.notion_gestao import MESES_ORDER
                    rec_mes = rec_2026.groupby("mes")["valor_liq"].sum().reset_index()
                    rec_mes.columns = ["Mês", "Líquido"]
                    rec_mes["_idx"] = rec_mes["Mês"].apply(lambda m: MESES_ORDER.index(m) if m in MESES_ORDER else 99)
                    rec_mes = rec_mes.sort_values("_idx").drop(columns=["_idx"])
                    fig_rm = px.bar(rec_mes, x="Mês", y="Líquido", color_discrete_sequence=[GREEN])
                    fig_rm.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0),
                                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    fig_rm.update_yaxes(tickprefix="R$ ", separatethousands=True)
                    st.plotly_chart(fig_rm, use_container_width=True)

            with col_rs:
                st.markdown("##### Por Status")
                if "status" in rec_2026.columns:
                    rec_st = rec_2026.groupby("status")["valor_liq"].sum().reset_index()
                    rec_st.columns = ["Status", "Valor"]
                    _rec_colors = {"Recebido": GREEN, "Confirmado": "#1E88E5", "Previsto": "#FB8C00", "Atrasado": "#E53935", "Cancelado": GRAY}
                    fig_rs = px.pie(rec_st, values="Valor", names="Status", hole=0.45,
                                    color="Status", color_discrete_map=_rec_colors)
                    fig_rs.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0),
                                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(fig_rs, use_container_width=True)

            # Por produto
            st.markdown("---")
            col_rp, col_rsoc = st.columns(2)
            with col_rp:
                st.markdown("##### Por Produto")
                if "produto" in rec_2026.columns:
                    rec_prod = rec_2026.groupby("produto")["valor_liq"].sum().reset_index()
                    rec_prod.columns = ["Produto", "Líquido"]
                    rec_prod = rec_prod.sort_values("Líquido", ascending=True)
                    fig_rp = px.bar(rec_prod, y="Produto", x="Líquido", orientation="h",
                                     color_discrete_sequence=[GREEN])
                    fig_rp.update_layout(height=300, margin=dict(l=0,r=20,t=10,b=0),
                                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    fig_rp.update_xaxes(tickprefix="R$ ", separatethousands=True)
                    st.plotly_chart(fig_rp, use_container_width=True)

            with col_rsoc:
                st.markdown("##### Por Sócio")
                if "socio" in rec_2026.columns:
                    rec_soc = rec_2026.groupby("socio")["valor_liq"].sum().reset_index()
                    rec_soc.columns = ["Sócio", "Líquido"]
                    rec_soc = rec_soc.sort_values("Líquido", ascending=False)
                    fig_rsoc = px.pie(rec_soc, values="Líquido", names="Sócio", hole=0.45,
                                      color_discrete_sequence=CHART_COLORS)
                    fig_rsoc.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0),
                                           plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(fig_rsoc, use_container_width=True)

            # Tabela detalhada
            st.markdown("---")
            st.markdown("##### Detalhe das Receitas")
            tbl_r = rec_2026[["cliente", "tipo_receita", "produto", "valor_bruto", "valor_liq",
                              "status", "socio", "mes", "originador"]].copy()
            tbl_r["valor_bruto"] = tbl_r["valor_bruto"].apply(fmt)
            tbl_r["valor_liq"] = tbl_r["valor_liq"].apply(fmt)
            tbl_r = tbl_r.rename(columns={
                "cliente": "Operação", "tipo_receita": "Tipo", "produto": "Produto",
                "valor_bruto": "Bruto", "valor_liq": "Líq. ZYN", "status": "Status",
                "socio": "Sócio", "mes": "Mês", "originador": "Finder",
            })
            st.dataframe(tbl_r, use_container_width=True, height=400)
        else:
            st.info("Nenhuma receita carregada.")

    # ━━━━━━ TAB DESPESAS ━━━━━━
    with tab_desp:
        st.markdown("### Despesas — 2026")

        if not _desp.empty:
            desp_2026 = _desp[_desp["ano"] == "2026"].copy() if "ano" in _desp.columns else _desp.copy()

            dk1, dk2, dk3, dk4 = st.columns(4)
            _d_pago = desp_2026[desp_2026["status"] == "Pago"]["valor"].sum() if "status" in desp_2026.columns else 0
            _d_pendente = desp_2026[desp_2026["status"] == "Pendente"]["valor"].sum() if "status" in desp_2026.columns else 0
            _d_total = desp_2026["valor"].sum() if "valor" in desp_2026.columns else 0
            _meses_d = desp_2026[desp_2026["status"] == "Pago"]["mes"].nunique() if "mes" in desp_2026.columns else 1
            dk1.metric("Total Despesas", fmt_br(_d_total))
            dk2.metric("Pago", fmt_br(_d_pago))
            dk3.metric("Pendente", fmt_br(_d_pendente))
            dk4.metric("Burn Rate /mês", fmt_br(_d_pago / max(_meses_d, 1)))

            # Por categoria
            st.markdown("---")
            col_dc, col_dm = st.columns(2)
            with col_dc:
                st.markdown("##### Por Categoria")
                if "categoria" in desp_2026.columns:
                    desp_cat = desp_2026.groupby("categoria")["valor"].sum().reset_index()
                    desp_cat.columns = ["Categoria", "Valor"]
                    desp_cat = desp_cat.sort_values("Valor", ascending=True)
                    fig_dc = px.bar(desp_cat, y="Categoria", x="Valor", orientation="h",
                                     color_discrete_sequence=["#E53935"])
                    fig_dc.update_layout(height=350, margin=dict(l=0,r=20,t=10,b=0),
                                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    fig_dc.update_xaxes(tickprefix="R$ ", separatethousands=True)
                    st.plotly_chart(fig_dc, use_container_width=True)

            with col_dm:
                st.markdown("##### Por Mês")
                if "mes" in desp_2026.columns:
                    from src.notion_gestao import MESES_ORDER
                    desp_mes = desp_2026.groupby("mes")["valor"].sum().reset_index()
                    desp_mes.columns = ["Mês", "Valor"]
                    desp_mes["_idx"] = desp_mes["Mês"].apply(lambda m: MESES_ORDER.index(m) if m in MESES_ORDER else 99)
                    desp_mes = desp_mes.sort_values("_idx").drop(columns=["_idx"])
                    fig_dm = px.bar(desp_mes, x="Mês", y="Valor", color_discrete_sequence=["#E53935"])
                    fig_dm.update_layout(height=350, margin=dict(l=0,r=0,t=10,b=0),
                                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    fig_dm.update_yaxes(tickprefix="R$ ", separatethousands=True)
                    st.plotly_chart(fig_dm, use_container_width=True)

            # Top despesas
            st.markdown("---")
            st.markdown("##### Maiores Despesas")
            tbl_d = desp_2026[["descricao", "categoria", "valor", "status", "fornecedor", "mes"]].copy()
            tbl_d = tbl_d.sort_values("valor", ascending=False)
            tbl_d["valor"] = tbl_d["valor"].apply(fmt)
            tbl_d = tbl_d.rename(columns={
                "descricao": "Descrição", "categoria": "Categoria", "valor": "Valor",
                "status": "Status", "fornecedor": "Fornecedor", "mes": "Mês",
            })
            st.dataframe(tbl_d, use_container_width=True, height=400)
        else:
            st.info("Nenhuma despesa carregada.")

    # ━━━━━━ TAB FLUXO DE CAIXA ━━━━━━
    with tab_fluxo:
        st.markdown("### Fluxo de Caixa — 2026")

        if not _fluxo.empty:
            fk1, fk2, fk3, fk4 = st.columns(4)
            _saldo_banco = _fluxo[_fluxo["saldo_banco"].notna()]["saldo_banco"].iloc[-1] if _fluxo["saldo_banco"].notna().any() else 0
            _rec_prev_total = _fluxo["receita_prevista"].sum() if "receita_prevista" in _fluxo.columns else 0
            _desp_prev_total = _fluxo["despesa_prevista"].sum() if "despesa_prevista" in _fluxo.columns else 0
            _rec_real_total = _fluxo["receita_realizada"].sum() if "receita_realizada" in _fluxo.columns else 0
            fk1.metric("Saldo C6 Atual", fmt_br(_saldo_banco))
            fk2.metric("Receita Prev. Anual", fmt_br(_rec_prev_total))
            fk3.metric("Receita Real. YTD", fmt_br(_rec_real_total))
            fk4.metric("Despesa Prev. Anual", fmt_br(_desp_prev_total))

            # Gráfico receita vs despesa por mês
            st.markdown("---")
            st.markdown("##### Receita vs Despesa Mensal")
            fluxo_chart = _fluxo[["mes_ano", "receita_prevista", "receita_realizada",
                                   "despesa_prevista", "despesa_realizada"]].copy()
            fluxo_chart = fluxo_chart.fillna(0)

            fig_fluxo = go.Figure()
            fig_fluxo.add_trace(go.Bar(x=fluxo_chart["mes_ano"], y=fluxo_chart["receita_realizada"],
                                        name="Receita Realizada", marker_color=GREEN))
            fig_fluxo.add_trace(go.Bar(x=fluxo_chart["mes_ano"], y=fluxo_chart["receita_prevista"],
                                        name="Receita Prevista", marker_color="rgba(46,125,79,0.3)"))
            fig_fluxo.add_trace(go.Bar(x=fluxo_chart["mes_ano"], y=[-v for v in fluxo_chart["despesa_realizada"]],
                                        name="Despesa Realizada", marker_color="#E53935"))
            fig_fluxo.add_trace(go.Bar(x=fluxo_chart["mes_ano"], y=[-v for v in fluxo_chart["despesa_prevista"]],
                                        name="Despesa Prevista", marker_color="rgba(229,57,53,0.3)"))
            fig_fluxo.update_layout(
                barmode="overlay", height=400,
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=0,r=20,t=10,b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            fig_fluxo.update_yaxes(tickprefix="R$ ", separatethousands=True)
            st.plotly_chart(fig_fluxo, use_container_width=True)

            # Saldo acumulado
            if _fluxo["saldo_acumulado"].notna().any():
                st.markdown("##### Saldo Acumulado")
                fig_saldo = go.Figure()
                fig_saldo.add_trace(go.Scatter(
                    x=_fluxo["mes_ano"], y=_fluxo["saldo_acumulado"],
                    mode="lines+markers", name="Saldo Acumulado",
                    line=dict(color=NAVY, width=3), marker=dict(size=8),
                ))
                if _fluxo["saldo_banco"].notna().any():
                    fig_saldo.add_trace(go.Scatter(
                        x=_fluxo[_fluxo["saldo_banco"].notna()]["mes_ano"],
                        y=_fluxo[_fluxo["saldo_banco"].notna()]["saldo_banco"],
                        mode="markers", name="Saldo Banco C6",
                        marker=dict(color=GREEN, size=12, symbol="diamond"),
                    ))
                fig_saldo.update_layout(height=300, margin=dict(l=0,r=20,t=10,b=0),
                                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                fig_saldo.update_yaxes(tickprefix="R$ ", separatethousands=True)
                st.plotly_chart(fig_saldo, use_container_width=True)

            # Tabela
            st.markdown("---")
            st.markdown("##### Detalhe Mensal")
            tbl_f = _fluxo[["mes_ano", "receita_prevista", "receita_realizada",
                             "despesa_prevista", "despesa_realizada", "saldo_mes",
                             "saldo_acumulado", "saldo_banco", "status"]].copy()
            for c in ["receita_prevista", "receita_realizada", "despesa_prevista",
                       "despesa_realizada", "saldo_mes", "saldo_acumulado", "saldo_banco"]:
                if c in tbl_f.columns:
                    tbl_f[c] = tbl_f[c].apply(fmt)
            tbl_f = tbl_f.rename(columns={
                "mes_ano": "Mês", "receita_prevista": "Rec. Prev.", "receita_realizada": "Rec. Real.",
                "despesa_prevista": "Desp. Prev.", "despesa_realizada": "Desp. Real.",
                "saldo_mes": "Saldo Mês", "saldo_acumulado": "Saldo Acum.",
                "saldo_banco": "Saldo C6", "status": "Status",
            })
            st.dataframe(tbl_f, use_container_width=True)
        else:
            st.info("Nenhum dado de fluxo de caixa.")

    # ━━━━━━ TAB LEADS ━━━━━━
    with tab_leads:
        st.markdown("### Leads & Prospecção")

        if not _leads.empty:
            lk1, lk2, lk3, lk4 = st.columns(4)
            lk1.metric("Total Leads", len(_leads))
            _l_andamento = len(_leads[_leads["status"].isin(["Em andamento", "Nao iniciada", "Não iniciada"])]) if "status" in _leads.columns else 0
            _l_convertido = len(_leads[_leads["status"] == "Enviado para Pipeline"]) if "status" in _leads.columns else 0
            _l_declinado = len(_leads[_leads["status"] == "Declinado"]) if "status" in _leads.columns else 0
            lk2.metric("Em Andamento", _l_andamento)
            lk3.metric("Convertidos", _l_convertido)
            lk4.metric("Declinados", _l_declinado)

            # Funnel
            st.markdown("---")
            col_lf, col_ls = st.columns(2)
            with col_lf:
                st.markdown("##### Funil de Leads")
                if "status" in _leads.columns:
                    lead_st = _leads["status"].value_counts().reset_index()
                    lead_st.columns = ["Status", "Leads"]
                    fig_lf = px.bar(lead_st, x="Status", y="Leads", color="Status",
                                     color_discrete_sequence=CHART_COLORS)
                    fig_lf.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0),
                                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                                         showlegend=False)
                    st.plotly_chart(fig_lf, use_container_width=True)

            with col_ls:
                st.markdown("##### Por Setor")
                if "setor" in _leads.columns and _leads["setor"].notna().any():
                    lead_setor = _leads[_leads["setor"] != ""]["setor"].value_counts().reset_index()
                    lead_setor.columns = ["Setor", "Leads"]
                    fig_ls = px.pie(lead_setor, values="Leads", names="Setor", hole=0.45,
                                    color_discrete_sequence=CHART_COLORS)
                    fig_ls.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0),
                                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(fig_ls, use_container_width=True)

            # Por origem e sócio
            st.markdown("---")
            col_lo, col_lsoc = st.columns(2)
            with col_lo:
                st.markdown("##### Por Origem")
                if "origem" in _leads.columns and _leads["origem"].notna().any():
                    lead_orig = _leads[_leads["origem"] != ""]["origem"].value_counts().reset_index()
                    lead_orig.columns = ["Origem", "Leads"]
                    fig_lo = px.bar(lead_orig, x="Origem", y="Leads", color_discrete_sequence=[GREEN])
                    fig_lo.update_layout(height=280, margin=dict(l=0,r=0,t=10,b=0),
                                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(fig_lo, use_container_width=True)

            with col_lsoc:
                st.markdown("##### Por Sócio")
                if "socio" in _leads.columns and _leads["socio"].notna().any():
                    lead_soc = _leads[_leads["socio"] != ""]["socio"].value_counts().reset_index()
                    lead_soc.columns = ["Sócio", "Leads"]
                    fig_lsoc = px.pie(lead_soc, values="Leads", names="Sócio", hole=0.45,
                                      color_discrete_sequence=CHART_COLORS)
                    fig_lsoc.update_layout(height=280, margin=dict(l=0,r=0,t=10,b=0),
                                           plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(fig_lsoc, use_container_width=True)

            # Tabela
            st.markdown("---")
            st.markdown("##### Todos os Leads")
            lead_cols = ["cliente", "status", "setor", "socio", "ticket", "volume",
                         "probabilidade", "urgencia", "origem", "originacao"]
            avail_lc = [c for c in lead_cols if c in _leads.columns]
            tbl_l = _leads[avail_lc].copy()
            if "ticket" in tbl_l.columns:
                tbl_l["ticket"] = tbl_l["ticket"].apply(fmt)
            if "volume" in tbl_l.columns:
                tbl_l["volume"] = tbl_l["volume"].apply(fmt)
            tbl_l = tbl_l.rename(columns={
                "cliente": "Cliente", "status": "Status", "setor": "Setor",
                "socio": "Sócio", "ticket": "Ticket Est.", "volume": "Volume Op.",
                "probabilidade": "Prob.", "urgencia": "Urgência", "origem": "Origem",
                "originacao": "Originação",
            })
            st.dataframe(tbl_l, use_container_width=True, height=400)
        else:
            st.info("Nenhum lead carregado.")

    # ━━━━━━ TAB INDICADORES ━━━━━━
    with tab_indic:
        st.markdown("### Indicadores de Performance")

        ik1, ik2, ik3, ik4 = st.columns(4)
        ik1.markdown(f'<div class="metric-card"><div class="metric-value">{fmt_br(_kpis["fee_medio"])}</div><div class="metric-label">Fee Médio Líq./Deal</div></div>', unsafe_allow_html=True)

        # Taxa de conversão leads -> pipeline
        _conv_rate = (_kpis["leads_convertidos"] / max(_kpis["leads_total"], 1) * 100)
        ik2.markdown(f'<div class="metric-card"><div class="metric-value">{_conv_rate:.0f}%</div><div class="metric-label">Conversão Lead → Pipe</div></div>', unsafe_allow_html=True)

        # Receita por sócio
        _rec_por_socio = _kpis["rec_recebida"] / 3 if _kpis["rec_recebida"] > 0 else 0
        ik3.markdown(f'<div class="metric-card"><div class="metric-value">{fmt_br(_rec_por_socio)}</div><div class="metric-label">Receita/Sócio (média)</div></div>', unsafe_allow_html=True)

        # ROI: receita / despesa
        _roi = _kpis["rec_recebida"] / max(_kpis["desp_paga"], 1)
        ik4.markdown(f'<div class="metric-card"><div class="metric-value">{_roi:.1f}x</div><div class="metric-label">ROI (Receita/Despesa)</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Gráfico: Receita vs Despesa acumulada
        st.markdown("---")
        st.markdown("##### Receita vs Despesa — Acumulado Mensal")
        if not _rec.empty and not _desp.empty:
            from src.notion_gestao import MESES_ORDER
            rec_2026 = _rec[_rec["ano"] == "2026"].copy() if "ano" in _rec.columns else _rec.copy()
            desp_2026 = _desp[_desp["ano"] == "2026"].copy() if "ano" in _desp.columns else _desp.copy()

            rec_m = rec_2026.groupby("mes")["valor_liq"].sum().reset_index()
            rec_m.columns = ["Mês", "Receita"]
            desp_m = desp_2026.groupby("mes")["valor"].sum().reset_index()
            desp_m.columns = ["Mês", "Despesa"]

            merged = pd.merge(rec_m, desp_m, on="Mês", how="outer").fillna(0)
            merged["_idx"] = merged["Mês"].apply(lambda m: MESES_ORDER.index(m) if m in MESES_ORDER else 99)
            merged = merged.sort_values("_idx").drop(columns=["_idx"])
            merged["Rec. Acum."] = merged["Receita"].cumsum()
            merged["Desp. Acum."] = merged["Despesa"].cumsum()

            fig_acum = go.Figure()
            fig_acum.add_trace(go.Scatter(x=merged["Mês"], y=merged["Rec. Acum."],
                                           mode="lines+markers", name="Receita Acum.",
                                           line=dict(color=GREEN, width=3)))
            fig_acum.add_trace(go.Scatter(x=merged["Mês"], y=merged["Desp. Acum."],
                                           mode="lines+markers", name="Despesa Acum.",
                                           line=dict(color="#E53935", width=3)))
            fig_acum.update_layout(height=350, margin=dict(l=0,r=20,t=10,b=0),
                                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            fig_acum.update_yaxes(tickprefix="R$ ", separatethousands=True)
            st.plotly_chart(fig_acum, use_container_width=True)

        # Quadro resumo
        st.markdown("---")
        st.markdown("##### Quadro Resumo")
        resumo_data = [
            {"Indicador": "Pipeline Total", "Valor": fmt_br(_kpis["pipe_total"]), "Obs": f"{_kpis['pipe_count']} deals ativos"},
            {"Indicador": "Receita Bruta 2026", "Valor": fmt_br(_kpis["rec_total_bruto"]), "Obs": "Todas as receitas"},
            {"Indicador": "Receita Líquida Recebida", "Valor": fmt_br(_kpis["rec_recebida"]), "Obs": "Efetivamente recebido"},
            {"Indicador": "Receita Confirmada", "Valor": fmt_br(_kpis["rec_confirmada"]), "Obs": "Aguardando recebimento"},
            {"Indicador": "Receita Prevista", "Valor": fmt_br(_kpis["rec_prevista"]), "Obs": "Projeção de closings"},
            {"Indicador": "Despesas Pagas", "Valor": fmt_br(_kpis["desp_paga"]), "Obs": "YTD efetivo"},
            {"Indicador": "Burn Rate Mensal", "Valor": fmt_br(_kpis["burn_rate"]), "Obs": "Média mensal paga"},
            {"Indicador": "Saldo Banco C6", "Valor": fmt_br(_kpis["saldo_atual"]), "Obs": "Último saldo confirmado"},
            {"Indicador": "Runway", "Valor": f"{_kpis['runway_meses']:.1f} meses", "Obs": "Saldo / Burn Rate"},
            {"Indicador": "Leads Ativos", "Valor": str(_kpis["leads_ativos"]), "Obs": f"de {_kpis['leads_total']} total"},
            {"Indicador": "Conversão Lead→Pipe", "Valor": f"{_conv_rate:.0f}%", "Obs": f"{_kpis['leads_convertidos']} convertidos"},
        ]
        st.dataframe(pd.DataFrame(resumo_data), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════
# VISÃO GERAL
# ══════════════════════════════════════════
elif page == "Visão Geral":
    st.markdown("""<div class="main-header">
        <h1>Visão Geral do Mercado</h1>
        <p>Renda Fixa Estruturada — Dados CVM (últimos meses disponíveis)</p>
    </div>""", unsafe_allow_html=True)

    positions = load_positions()
    if positions.empty:
        st.error("Nenhum dado. Vá em Atualizar.")
        st.stop()

    share_buttons("ZYN — Visão Geral do Mercado", f"Volume Total: {fmt(positions['vl_posicao'].sum())}\nFundos: {positions['cnpj_fundo'].nunique()}\nGestoras: {positions['gestora'].nunique()}\nPosições: {len(positions):,}")

    # --- Busca Universal ---
    busca_global = st.text_input(
        "Buscar por nome, CPF, CNPJ, fundo, gestora, devedor, emissor, ticker...",
        "", key="vg_busca", placeholder="Ex: MRV, 60.701.190, Raízen, FIDC, CDI...",
    )

    if busca_global:
        q = busca_global.strip()
        # Busca em todas as colunas de texto relevantes
        search_cols = ["gestora", "nome_fundo", "devedor", "emissor", "cnpj_fundo",
                       "cnpj_emissor", "cnpj_gestora", "ticker_devedor", "tipo_ativo",
                       "classe_anbima", "administrador", "indexador", "descricao_ativo"]
        mask = pd.Series(False, index=positions.index)
        for col in search_cols:
            if col in positions.columns:
                mask = mask | positions[col].astype(str).str.contains(q, case=False, na=False)
        hits = positions[mask]

        st.success(f"**{len(hits):,}** posições encontradas para \"{q}\" — {hits['cnpj_fundo'].nunique()} fundos, {hits['gestora'].nunique() if 'gestora' in hits.columns else 0} gestoras")

        if not hits.empty:
            # KPIs do resultado
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Volume", fmt(hits["vl_posicao"].sum()))
            k2.metric("Posições", f"{len(hits):,}")
            k3.metric("Fundos", hits["cnpj_fundo"].nunique())
            k4.metric("Gestoras", hits["gestora"].nunique() if "gestora" in hits.columns else 0)
            k5.metric("Devedores", hits["devedor"].nunique() if "devedor" in hits.columns else 0)

            # Tabs com resultados
            tab_pos, tab_gest, tab_dev, tab_fundos = st.tabs(["Posições", "Gestoras", "Devedores", "Fundos"])

            with tab_pos:
                show_cols = ["gestora", "nome_fundo", "tipo_ativo", "devedor", "emissor",
                             "vl_posicao", "indexador", "spread", "dt_vencimento"]
                avail = [c for c in show_cols if c in hits.columns]
                tbl = hits[avail].head(500).copy()
                if "vl_posicao" in tbl.columns:
                    tbl["vl_posicao"] = tbl["vl_posicao"].apply(fmt)
                if "spread" in tbl.columns:
                    tbl["spread"] = tbl["spread"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
                tbl = tbl.rename(columns={
                    "gestora": "Gestora", "nome_fundo": "Fundo", "tipo_ativo": "Tipo",
                    "devedor": "Devedor", "emissor": "Emissor", "vl_posicao": "Valor",
                    "indexador": "Indexador", "spread": "Spread", "dt_vencimento": "Vencimento",
                })
                st.dataframe(tbl, use_container_width=True, height=400)
                excel_btn(tbl, f"zyn_busca_{q[:20]}.xlsx", key="exp_busca")

            with tab_gest:
                if "gestora" in hits.columns:
                    g_agg = hits.groupby("gestora").agg(
                        volume=("vl_posicao", "sum"), n_fundos=("cnpj_fundo", "nunique"),
                        n_posicoes=("vl_posicao", "count"),
                        tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
                    ).reset_index().sort_values("volume", ascending=False)
                    g_agg["Vol."] = g_agg["volume"].apply(fmt)
                    st.dataframe(
                        g_agg.rename(columns={"gestora": "Gestora", "n_fundos": "Fundos", "n_posicoes": "Posições", "tipos": "Tipos"})[["Gestora", "Vol.", "Fundos", "Posições", "Tipos"]],
                        use_container_width=True, height=350,
                    )

            with tab_dev:
                if "devedor" in hits.columns:
                    d_agg = hits[hits["devedor"].notna() & ~hits["devedor"].str.contains("Cedente", na=False)].groupby("devedor").agg(
                        volume=("vl_posicao", "sum"), n_gestoras=("gestora", "nunique"),
                        n_fundos=("cnpj_fundo", "nunique"),
                        tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
                    ).reset_index().sort_values("volume", ascending=False)
                    d_agg["Vol."] = d_agg["volume"].apply(fmt)
                    st.dataframe(
                        d_agg.rename(columns={"devedor": "Devedor", "n_gestoras": "Gestoras", "n_fundos": "Fundos", "tipos": "Tipos"})[["Devedor", "Vol.", "Gestoras", "Fundos", "Tipos"]],
                        use_container_width=True, height=350,
                    )

            with tab_fundos:
                f_agg = hits.groupby(["cnpj_fundo", "nome_fundo"]).agg(
                    gestora=("gestora", "first"),
                    volume=("vl_posicao", "sum"), n_posicoes=("vl_posicao", "count"),
                    tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
                ).reset_index().sort_values("volume", ascending=False)
                f_agg["Vol."] = f_agg["volume"].apply(fmt)
                st.dataframe(
                    f_agg.rename(columns={"cnpj_fundo": "CNPJ", "nome_fundo": "Fundo", "gestora": "Gestora", "n_posicoes": "Posições", "tipos": "Tipos"})[["CNPJ", "Fundo", "Gestora", "Vol.", "Posições", "Tipos"]],
                    use_container_width=True, height=350,
                )

        st.markdown("---")

    # --- Visão Geral (KPIs + Charts) ---
    c1, c2, c3, c4 = st.columns(4)
    for col, val, label in [
        (c1, fmt(positions["vl_posicao"].sum()), "Volume Total"),
        (c2, f"{positions['cnpj_fundo'].nunique():,}", "Fundos Únicos"),
        (c3, f"{positions['gestora'].nunique() if 'gestora' in positions.columns else 0}", "Gestoras"),
        (c4, f"{len(positions):,}", "Posições"),
    ]:
        col.markdown(f'<div class="metric-card"><div class="metric-value">{val}</div><div class="metric-label">{label}</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Volume por Tipo de Ativo")
        vt = positions.groupby("tipo_ativo")["vl_posicao"].sum().reset_index()
        vt.columns = ["Tipo", "Volume"]
        fig = px.bar(vt.sort_values("Volume"), x="Volume", y="Tipo", orientation="h",
                     color_discrete_sequence=[GREEN])
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=300,
                          margin=dict(l=0, r=20, t=10, b=0))
        fig.update_xaxes(tickformat=",.0s", tickprefix="R$ ")
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Fundos por Tipo")
        ft = positions.groupby("tipo_ativo")["cnpj_fundo"].nunique().reset_index()
        ft.columns = ["Tipo", "Fundos"]
        fig2 = px.pie(ft, values="Fundos", names="Tipo", hole=0.4,
                      color_discrete_sequence=CHART_COLORS)
        fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=300,
                           margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig2, use_container_width=True)

    # Top gestoras
    st.subheader("Top 20 Gestoras por Volume em RF Estruturada")
    if "gestora" in positions.columns:
        tg = positions.groupby("gestora")["vl_posicao"].sum().reset_index().sort_values("vl_posicao", ascending=False).head(20)
        tg.columns = ["Gestora", "Volume"]
        fig3 = px.bar(tg, x="Gestora", y="Volume", color_discrete_sequence=[NAVY])
        fig3.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=400,
                           xaxis_tickangle=-45, margin=dict(l=0, r=20, t=10, b=120))
        fig3.update_yaxes(tickformat=",.0s", tickprefix="R$ ")
        st.plotly_chart(fig3, use_container_width=True)

    # Top emissores
    st.subheader("Top 20 Emissores por Volume Captado")
    if "emissor" in positions.columns:
        te = positions[positions["emissor"].notna()].groupby("emissor").agg(
            volume=("vl_posicao", "sum"),
            n_fundos=("cnpj_fundo", "nunique"),
            n_gestoras=("gestora", "nunique"),
            tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
        ).reset_index().sort_values("volume", ascending=False).head(20)
        te_display = te.copy()
        te_display["Vol."] = te_display["volume"].apply(fmt)
        te_display = te_display.rename(columns={"emissor": "Emissor", "n_fundos": "Fundos", "n_gestoras": "Gestoras", "tipos": "Tipos"})
        fig4 = px.bar(te, x="emissor", y="volume", color_discrete_sequence=[GREEN])
        fig4.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=400,
                           xaxis_tickangle=-45, margin=dict(l=0, r=20, t=10, b=120),
                           xaxis_title="Emissor", yaxis_title="Volume")
        fig4.update_yaxes(tickformat=",.0s", tickprefix="R$ ")
        st.plotly_chart(fig4, use_container_width=True)
        st.dataframe(te_display[["Emissor", "Vol.", "Fundos", "Gestoras", "Tipos"]], use_container_width=True)

    # Export visão geral
    st.markdown("---")
    overview_data = positions.groupby("tipo_ativo").agg(
        volume=("vl_posicao", "sum"), fundos=("cnpj_fundo", "nunique"),
        gestoras=("gestora", "nunique"), posicoes=("vl_posicao", "count"),
    ).reset_index()
    overview_data.columns = ["Tipo", "Volume", "Fundos", "Gestoras", "Posições"]
    excel_btn(overview_data, "zyn_visao_geral.xlsx", key="exp_visao")


# ══════════════════════════════════════════
# GESTORAS (com drill-down para fundos)
# ══════════════════════════════════════════
elif page == "Gestoras":
    st.markdown("""<div class="main-header">
        <h1>Gestoras — Drill-down</h1>
        <p>Selecione uma gestora para ver seus fundos e posições detalhadas</p>
    </div>""", unsafe_allow_html=True)

    positions = load_positions()
    profiles = load_profiles()
    if positions.empty:
        st.error("Nenhum dado.")
        st.stop()

    share_buttons("ZYN — Gestoras", f"{len(profiles)} gestoras mapeadas\nVolume total RF: {fmt(profiles['vol_total'].sum()) if not profiles.empty else '—'}")

    # Filtros
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        tipo_filter = st.multiselect("Tipo de Ativo", ["NC", "CRI", "CRA", "CPR-F", "DEBENTURE"])
    with col_f2:
        vol_min = st.number_input("Volume mín. RF (R$ M)", value=0, step=10)
    with col_f3:
        search = st.text_input("Buscar gestora", "")

    # Filtra profiles
    filtered = profiles.copy() if not profiles.empty else pd.DataFrame()
    if not filtered.empty:
        if tipo_filter:
            for t in tipo_filter:
                c = f"vol_{t}"
                if c in filtered.columns:
                    filtered = filtered[filtered[c] > 0]
        if vol_min > 0:
            filtered = filtered[filtered["vol_total"] >= vol_min * 1e6]
        if search:
            filtered = filtered[filtered["gestora"].str.contains(search, case=False, na=False)]

    st.markdown(f"**{len(filtered)}** gestoras")

    # Tabela resumo
    if not filtered.empty:
        display_cols = ["gestora", "n_fundos", "vol_total", "vol_NC", "vol_CRI", "vol_CRA", "vol_CPR-F", "vol_DEBENTURE", "tipo_preferido", "ticket_medio", "indexador_principal"]
        avail = [c for c in display_cols if c in filtered.columns]
        tbl = filtered[avail].copy()
        rename = {"gestora": "Gestora", "n_fundos": "Fundos", "vol_total": "Vol. Total", "vol_NC": "NC", "vol_CRI": "CRI", "vol_CRA": "CRA", "vol_CPR-F": "CPR-F", "vol_DEBENTURE": "Debênture", "tipo_preferido": "Pref.", "ticket_medio": "Ticket Médio", "indexador_principal": "Indexador"}
        tbl = tbl.rename(columns={k: v for k, v in rename.items() if k in tbl.columns})
        # Keep raw for export
        tbl_export = tbl.copy()
        for c in ["Vol. Total", "NC", "CRI", "CRA", "CPR-F", "Debênture", "Ticket Médio"]:
            if c in tbl.columns:
                tbl[c] = tbl[c].apply(fmt)
        st.dataframe(tbl.head(100), use_container_width=True, height=350)
        excel_btn(tbl_export, "zyn_gestoras.xlsx", key="exp_gestoras")

    # === DRILL-DOWN: Selecionar gestora ===
    st.markdown("---")
    st.subheader("🔍 Drill-down — Fundos da Gestora")

    gestora_list = filtered["gestora"].dropna().tolist() if not filtered.empty else positions["gestora"].dropna().unique().tolist()
    selected_gestora = st.selectbox("Selecione a gestora", [""] + sorted(gestora_list))

    if selected_gestora:
        g_positions = positions[positions["gestora"] == selected_gestora]

        # KPIs da gestora
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Fundos", g_positions["cnpj_fundo"].nunique())
        c2.metric("Volume Total RF", fmt(g_positions["vl_posicao"].sum()))
        c3.metric("Posições", len(g_positions))
        c4.metric("Ticket Médio", fmt(g_positions["vl_posicao"].mean()))

        # Lista de fundos
        st.markdown("#### Fundos desta gestora")
        fundos = (
            g_positions.groupby(["cnpj_fundo", "nome_fundo"])
            .agg(
                volume=("vl_posicao", "sum"),
                n_posicoes=("vl_posicao", "count"),
                tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
                pl=("pl_fundo", "first"),
                classe=("classe_anbima", "first"),
            )
            .reset_index()
            .sort_values("volume", ascending=False)
        )

        fundos_display = fundos.copy()
        fundos_display["Volume RF"] = fundos_display["volume"].apply(fmt)
        fundos_display["PL"] = fundos_display["pl"].apply(lambda x: fmt(x) if pd.notna(x) else "—")
        fundos_display = fundos_display.rename(columns={
            "cnpj_fundo": "CNPJ", "nome_fundo": "Fundo", "n_posicoes": "Posições",
            "tipos": "Tipos Comprados", "classe": "Classe ANBIMA",
        })
        fundos_show = fundos_display[["CNPJ", "Fundo", "Volume RF", "Posições", "Tipos Comprados", "PL", "Classe ANBIMA"]]
        st.dataframe(fundos_show, use_container_width=True, height=300)
        excel_btn(fundos_show, f"zyn_fundos_{selected_gestora[:30]}.xlsx", key="exp_fundos_gestora")

        # === DRILL-DOWN: Selecionar fundo ===
        st.markdown("---")
        st.subheader("📄 Papéis comprados pelo fundo")

        fundo_options = fundos["nome_fundo"].tolist()
        selected_fundo = st.selectbox("Selecione o fundo", [""] + fundo_options)

        if selected_fundo:
            f_positions = g_positions[g_positions["nome_fundo"] == selected_fundo].copy()

            # KPIs do fundo
            cnpj_f = f_positions["cnpj_fundo"].iloc[0]
            pl_val = f_positions["pl_fundo"].iloc[0] if "pl_fundo" in f_positions.columns and pd.notna(f_positions["pl_fundo"].iloc[0]) else None
            classe_val = f_positions["classe_anbima"].iloc[0] if "classe_anbima" in f_positions.columns and pd.notna(f_positions["classe_anbima"].iloc[0]) else "—"
            admin_val = f_positions["administrador"].iloc[0] if "administrador" in f_positions.columns and pd.notna(f_positions["administrador"].iloc[0]) else "—"
            pub_val = f_positions["publico_alvo"].iloc[0] if "publico_alvo" in f_positions.columns and pd.notna(f_positions["publico_alvo"].iloc[0]) else "—"
            tipos_fundo = ", ".join(sorted(f_positions["tipo_ativo"].unique()))

            c1, c2, c3 = st.columns(3)
            c1.metric("Volume RF", fmt(f_positions["vl_posicao"].sum()))
            c2.metric("N. Papéis", len(f_positions))
            c3.metric("PL Fundo", fmt(pl_val) if pl_val else "—")

            st.markdown(f"""<div class="info-row">
                <div class="info-item"><strong>CNPJ</strong>: {cnpj_f}</div>
                <div class="info-item"><strong>Administrador</strong>: {admin_val}</div>
                <div class="info-item"><strong>Público Alvo</strong>: {pub_val}</div>
                <div class="info-item"><strong>Tipos</strong>: {tipos_fundo}</div>
                <div class="info-item"><strong>Classe</strong>: {classe_val}</div>
            </div>""", unsafe_allow_html=True)

            # Charts lado a lado: por tipo e por indexador
            if len(f_positions["tipo_ativo"].unique()) > 1 or ("indexador" in f_positions.columns and f_positions["indexador"].notna().any()):
                ch1, ch2 = st.columns(2)
                with ch1:
                    by_type = f_positions.groupby("tipo_ativo")["vl_posicao"].sum().reset_index()
                    by_type.columns = ["Tipo", "Volume"]
                    fig = px.pie(by_type, values="Volume", names="Tipo", hole=0.4,
                                 color_discrete_sequence=CHART_COLORS)
                    fig.update_layout(height=220, margin=dict(l=0, r=0, t=20, b=0), title_text="Volume por Tipo")
                    st.plotly_chart(fig, use_container_width=True)
                with ch2:
                    if "indexador" in f_positions.columns and f_positions["indexador"].notna().any():
                        by_idx = f_positions[f_positions["indexador"].notna()].groupby("indexador")["vl_posicao"].sum().reset_index()
                        by_idx.columns = ["Indexador", "Volume"]
                        fig2 = px.pie(by_idx, values="Volume", names="Indexador", hole=0.4,
                                      color_discrete_sequence=CHART_COLORS)
                        fig2.update_layout(height=220, margin=dict(l=0, r=0, t=20, b=0), title_text="Volume por Indexador")
                        st.plotly_chart(fig2, use_container_width=True)

            # Tabela completa de papéis com TODAS as informações
            st.markdown("#### Todos os papéis neste fundo")
            paper_cols = {
                "tipo_ativo": "Tipo",
                "tp_ativo": "Subtipo",
                "tp_aplicacao": "Aplicação",
                "devedor": "Devedor",
                "ticker_devedor": "Ticker",
                "emissor": "Emissor/Securitizadora",
                "cnpj_emissor": "CNPJ Emissor",
                "cd_ativo": "Código B3",
                "descricao_ativo": "Descrição",
                "isin": "ISIN",
                "vl_posicao": "Valor Posição",
                "vl_custo": "Valor Custo",
                "qt_posicao": "Quantidade",
                "dt_vencimento": "Vencimento",
                "indexador": "Indexador",
                "pct_indexador": "% Indexador",
                "spread": "Spread",
                "taxa_pre": "Taxa Pré",
                "bloco": "Bloco CDA",
                "dt_competencia": "Competência",
            }
            avail_cols = {k: v for k, v in paper_cols.items() if k in f_positions.columns}
            papers = f_positions[list(avail_cols.keys())].rename(columns=avail_cols).copy()

            # Formata
            if "Valor Posição" in papers.columns:
                papers["Valor Posição"] = papers["Valor Posição"].apply(fmt)
            if "Valor Custo" in papers.columns:
                papers["Valor Custo"] = papers["Valor Custo"].apply(lambda x: fmt(x) if pd.notna(x) else "—")
            if "Spread" in papers.columns:
                papers["Spread"] = papers["Spread"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
            if "Taxa Pré" in papers.columns:
                papers["Taxa Pré"] = papers["Taxa Pré"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
            if "% Indexador" in papers.columns:
                papers["% Indexador"] = papers["% Indexador"].apply(lambda x: f"{x:.0f}%" if pd.notna(x) and x != 0 else "—")

            st.dataframe(papers, use_container_width=True, height=450)
            excel_btn(papers, f"zyn_papeis_{selected_fundo[:30]}.xlsx", key="exp_papeis_gestora")

            # Emissores deste fundo
            if "Emissor" in papers.columns:
                st.markdown("#### 🏭 Emissores neste fundo")
                em_fundo = f_positions[f_positions["emissor"].notna()].groupby(["emissor", "cnpj_emissor"]).agg(
                    volume=("vl_posicao", "sum"), papeis=("vl_posicao", "count"),
                    tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
                    vencimento_max=("dt_vencimento", safe_max),
                ).reset_index().sort_values("volume", ascending=False)
                em_fundo_disp = em_fundo.copy()
                em_fundo_disp["Vol."] = em_fundo_disp["volume"].apply(fmt)
                em_fundo_disp = em_fundo_disp.rename(columns={
                    "emissor": "Emissor", "cnpj_emissor": "CNPJ Emissor",
                    "papeis": "Papéis", "tipos": "Tipos", "vencimento_max": "Venc. Máx",
                })
                st.dataframe(em_fundo_disp[["Emissor", "CNPJ Emissor", "Vol.", "Papéis", "Tipos", "Venc. Máx"]], use_container_width=True)



# ══════════════════════════════════════════
# FUNDOS & PAPÉIS (busca direta)
# ══════════════════════════════════════════
elif page == "Fundos & Papéis":
    st.markdown("""<div class="main-header">
        <h1>Fundos & Papéis</h1>
        <p>Busque qualquer fundo ou emissor — veja cada papel comprado</p>
    </div>""", unsafe_allow_html=True)

    positions = load_positions()
    if positions.empty:
        st.error("Nenhum dado.")
        st.stop()

    share_buttons("ZYN — Fundos & Papéis", f"{positions['cnpj_fundo'].nunique()} fundos\nVolume: {fmt(positions['vl_posicao'].sum())}")

    # Filtros
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        tipo_f = st.multiselect("Tipo de Ativo", ["NC", "CRI", "CRA", "CPR-F", "DEBENTURE"], key="fp_tipo")
    with col2:
        busca_fundo = st.text_input("Buscar fundo (nome)", "", key="fp_fundo")
    with col3:
        busca_emissor = st.text_input("Buscar emissor", "", key="fp_emissor")
    with col4:
        busca_isin = st.text_input("Buscar ISIN", "", key="fp_isin")

    filtered = positions.copy()
    if tipo_f:
        filtered = filtered[filtered["tipo_ativo"].isin(tipo_f)]
    if busca_fundo:
        filtered = filtered[filtered["nome_fundo"].str.contains(busca_fundo, case=False, na=False)]
    if busca_emissor and "emissor" in filtered.columns:
        filtered = filtered[filtered["emissor"].str.contains(busca_emissor, case=False, na=False)]
    if busca_isin and "isin" in filtered.columns:
        filtered = filtered[filtered["isin"].str.contains(busca_isin, case=False, na=False)]

    st.markdown(f"**{len(filtered):,}** posições encontradas | **{filtered['cnpj_fundo'].nunique()}** fundos")

    # Agregado por fundo
    st.subheader("Fundos")
    fundos_agg = (
        filtered.groupby(["cnpj_fundo", "nome_fundo", "gestora"])
        .agg(
            volume=("vl_posicao", "sum"),
            n_papeis=("vl_posicao", "count"),
            tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
            classe=("classe_anbima", "first"),
            pl=("pl_fundo", "first"),
        )
        .reset_index()
        .sort_values("volume", ascending=False)
    )

    fundos_tbl = fundos_agg.head(200).copy()
    fundos_tbl["Vol. RF"] = fundos_tbl["volume"].apply(fmt)
    fundos_tbl["PL"] = fundos_tbl["pl"].apply(lambda x: fmt(x) if pd.notna(x) else "—")
    fundos_tbl = fundos_tbl.rename(columns={
        "cnpj_fundo": "CNPJ", "nome_fundo": "Fundo", "gestora": "Gestora",
        "n_papeis": "Papéis", "tipos": "Tipos", "classe": "Classe",
    })
    fundos_show = fundos_tbl[["CNPJ", "Fundo", "Gestora", "Vol. RF", "Papéis", "Tipos", "PL", "Classe"]]
    st.dataframe(fundos_show, use_container_width=True, height=350)
    excel_btn(fundos_show, "zyn_fundos_busca.xlsx", key="exp_fundos_busca")

    # Drill-down nos papéis
    st.markdown("---")
    st.subheader("📄 Detalhes dos papéis")

    fundo_sel = st.selectbox("Selecione o fundo para ver papéis", [""] + fundos_agg["nome_fundo"].head(200).tolist(), key="fp_sel")

    if fundo_sel:
        f_pos = filtered[filtered["nome_fundo"] == fundo_sel].copy()

        _vol = fmt(f_pos["vl_posicao"].sum())
        _papeis = len(f_pos)
        _cnpj = f_pos["cnpj_fundo"].iloc[0] if "cnpj_fundo" in f_pos.columns else "—"
        _pl = fmt(f_pos["pl_fundo"].iloc[0]) if "pl_fundo" in f_pos.columns and pd.notna(f_pos["pl_fundo"].iloc[0]) else "—"
        _classe = f_pos["classe_anbima"].iloc[0] if "classe_anbima" in f_pos.columns and pd.notna(f_pos["classe_anbima"].iloc[0]) else "—"
        _gestora = f_pos["gestora"].iloc[0] if "gestora" in f_pos.columns else "—"
        _admin = f_pos["administrador"].iloc[0] if "administrador" in f_pos.columns and pd.notna(f_pos["administrador"].iloc[0]) else "—"
        _publico = f_pos["publico_alvo"].iloc[0] if "publico_alvo" in f_pos.columns and pd.notna(f_pos["publico_alvo"].iloc[0]) else "—"
        _tipos = ", ".join(sorted(f_pos["tipo_ativo"].unique())) if "tipo_ativo" in f_pos.columns else "—"

        c1, c2, c3 = st.columns(3)
        c1.metric("Volume RF", _vol)
        c2.metric("N. Papéis", _papeis)
        c3.metric("PL Fundo", _pl)

        st.markdown(f"""<div class="info-row">
            <div class="info-item"><strong>Gestora</strong>: {_gestora}</div>
            <div class="info-item"><strong>CNPJ</strong>: {_cnpj}</div>
            <div class="info-item"><strong>Administrador</strong>: {_admin}</div>
            <div class="info-item"><strong>Público Alvo</strong>: {_publico}</div>
            <div class="info-item"><strong>Tipos</strong>: {_tipos}</div>
            <div class="info-item"><strong>Classe</strong>: {_classe}</div>
        </div>""", unsafe_allow_html=True)

        # Tabela completa de papéis
        display_order = ["tipo_ativo", "devedor", "ticker_devedor", "emissor", "cd_ativo", "descricao_ativo", "isin", "vl_posicao", "qt_posicao",
                         "dt_vencimento", "indexador", "pct_indexador", "spread", "taxa_pre", "cnpj_emissor", "dt_competencia"]
        avail = [c for c in display_order if c in f_pos.columns]
        papers = f_pos[avail].copy()
        col_names = {
            "tipo_ativo": "Tipo", "devedor": "Devedor", "ticker_devedor": "Ticker",
            "emissor": "Emissor", "cd_ativo": "Código B3",
            "descricao_ativo": "Descrição",
            "isin": "ISIN", "vl_posicao": "Valor", "qt_posicao": "Qtd",
            "dt_vencimento": "Vencimento", "indexador": "Indexador",
            "pct_indexador": "% Idx", "spread": "Spread", "taxa_pre": "Taxa Pré",
            "cnpj_emissor": "CNPJ Emissor", "dt_competencia": "Competência",
        }
        papers = papers.rename(columns={k: v for k, v in col_names.items() if k in papers.columns})

        if "Valor" in papers.columns:
            papers["Valor"] = papers["Valor"].apply(fmt)
        if "Spread" in papers.columns:
            papers["Spread"] = papers["Spread"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
        if "Taxa Pré" in papers.columns:
            papers["Taxa Pré"] = papers["Taxa Pré"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
        if "% Idx" in papers.columns:
            papers["% Idx"] = papers["% Idx"].apply(lambda x: f"{x:.0f}%" if pd.notna(x) and x != 0 else "—")

        st.dataframe(papers, use_container_width=True, height=400)
        excel_btn(papers, f"zyn_papeis_{fundo_sel[:30]}.xlsx", key="exp_papeis_busca")

        # Emissores deste fundo
        if "emissor" in f_pos.columns:
            st.markdown("#### 🏭 Emissores neste fundo")
            em_f = f_pos[f_pos["emissor"].notna()].groupby(["emissor", "cnpj_emissor"]).agg(
                volume=("vl_posicao", "sum"), papeis=("vl_posicao", "count"),
                tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
                vencimento_max=("dt_vencimento", safe_max),
                indexadores=("indexador", lambda x: ", ".join(sorted(x.dropna().unique())) if x.notna().any() else "—"),
            ).reset_index().sort_values("volume", ascending=False)
            em_f_disp = em_f.copy()
            em_f_disp["Vol."] = em_f_disp["volume"].apply(fmt)
            em_f_disp = em_f_disp.rename(columns={
                "emissor": "Emissor", "cnpj_emissor": "CNPJ Emissor",
                "papeis": "Papéis", "tipos": "Tipos", "vencimento_max": "Venc. Máx",
                "indexadores": "Indexadores",
            })
            st.dataframe(em_f_disp[["Emissor", "CNPJ Emissor", "Vol.", "Papéis", "Tipos", "Indexadores", "Venc. Máx"]], use_container_width=True)

    # Mapa de emissores — completo
    st.markdown("---")
    st.subheader("🏭 Análise de Emissores")

    if "emissor" in filtered.columns:
        busca_em = st.text_input("Buscar emissor", "", key="fp_em_busca")

        em_base = filtered[filtered["emissor"].notna()]
        if busca_em:
            em_base = em_base[em_base["emissor"].str.contains(busca_em, case=False, na=False)]

        emissores = (
            em_base
            .groupby(["emissor", "cnpj_emissor"])
            .agg(
                volume=("vl_posicao", "sum"),
                n_fundos=("cnpj_fundo", "nunique"),
                n_gestoras=("gestora", "nunique"),
                n_posicoes=("vl_posicao", "count"),
                ticket_medio=("vl_posicao", "mean"),
                tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
                indexadores=("indexador", lambda x: ", ".join(sorted(x.dropna().unique())) if x.notna().any() else "—"),
                vencimento_min=("dt_vencimento", safe_min),
                vencimento_max=("dt_vencimento", safe_max),
            )
            .reset_index()
            .sort_values("volume", ascending=False)
        )

        st.markdown(f"**{len(emissores)}** emissores encontrados")

        em_disp = emissores.head(100).copy()
        em_export = em_disp.copy()
        em_disp["Volume"] = em_disp["volume"].apply(fmt)
        em_disp["Ticket Médio"] = em_disp["ticket_medio"].apply(fmt)
        em_disp = em_disp.rename(columns={
            "emissor": "Emissor", "cnpj_emissor": "CNPJ", "n_fundos": "Fundos",
            "n_gestoras": "Gestoras", "n_posicoes": "Posições", "tipos": "Tipos",
            "indexadores": "Indexadores", "vencimento_min": "Venc. Mín", "vencimento_max": "Venc. Máx",
        })
        st.dataframe(em_disp[["Emissor", "CNPJ", "Volume", "Ticket Médio", "Fundos", "Gestoras", "Posições", "Tipos", "Indexadores", "Venc. Mín", "Venc. Máx"]], use_container_width=True, height=400)
        em_export_renamed = em_export.rename(columns={
            "emissor": "Emissor", "cnpj_emissor": "CNPJ", "volume": "Volume",
            "ticket_medio": "Ticket Médio", "n_fundos": "Fundos", "n_gestoras": "Gestoras",
            "n_posicoes": "Posições", "tipos": "Tipos", "indexadores": "Indexadores",
            "vencimento_min": "Venc. Mín", "vencimento_max": "Venc. Máx",
        })
        excel_btn(em_export_renamed, "zyn_emissores.xlsx", key="exp_emissores")

        # Chart top emissores
        top_em = emissores.head(15)
        fig_em = px.bar(top_em, x="emissor", y="volume", color="n_gestoras",
                        color_continuous_scale=["#E0E0E0", GREEN],
                        labels={"emissor": "Emissor", "volume": "Volume", "n_gestoras": "Nº Gestoras"})
        fig_em.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=400,
                             xaxis_tickangle=-45, margin=dict(l=0, r=20, t=10, b=120))
        fig_em.update_yaxes(tickformat=",.0s", tickprefix="R$ ")
        st.plotly_chart(fig_em, use_container_width=True)

        # Drill-down: selecionar emissor
        st.markdown("---")
        st.subheader("🔍 Drill-down — Quem comprou este emissor?")
        em_list = emissores["emissor"].head(200).tolist()
        sel_emissor = st.selectbox("Selecione o emissor", [""] + em_list, key="fp_em_sel")

        if sel_emissor:
            em_pos = em_base[em_base["emissor"] == sel_emissor]
            cnpj_em = em_pos["cnpj_emissor"].iloc[0] if "cnpj_emissor" in em_pos.columns else "—"

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Volume Total", fmt(em_pos["vl_posicao"].sum()))
            c2.metric("Fundos", em_pos["cnpj_fundo"].nunique())
            c3.metric("Gestoras", em_pos["gestora"].nunique())
            c4.metric("Papéis", len(em_pos))
            st.markdown(f'<div class="info-row"><div class="info-item"><strong>CNPJ Emissor</strong>: {cnpj_em}</div></div>', unsafe_allow_html=True)

            # Gestoras que compram este emissor
            st.markdown("##### Gestoras compradoras")
            gest_em = em_pos.groupby("gestora").agg(
                volume=("vl_posicao", "sum"), fundos=("cnpj_fundo", "nunique"),
                papeis=("vl_posicao", "count"),
                tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
            ).reset_index().sort_values("volume", ascending=False)
            gest_em_disp = gest_em.copy()
            gest_em_disp["Vol."] = gest_em_disp["volume"].apply(fmt)
            gest_em_disp = gest_em_disp.rename(columns={
                "gestora": "Gestora", "fundos": "Fundos", "papeis": "Papéis", "tipos": "Tipos",
            })
            st.dataframe(gest_em_disp[["Gestora", "Vol.", "Fundos", "Papéis", "Tipos"]], use_container_width=True)
            excel_btn(gest_em_disp[["Gestora", "Vol.", "Fundos", "Papéis", "Tipos"]], f"zyn_compradores_{sel_emissor[:30]}.xlsx", key="exp_compradores_em")

            # Papéis individuais deste emissor
            st.markdown("##### Todos os papéis deste emissor")
            em_papers_cols = ["gestora", "nome_fundo", "tipo_ativo", "devedor", "ticker_devedor", "cd_ativo", "descricao_ativo", "isin",
                              "vl_posicao", "dt_vencimento", "indexador", "pct_indexador", "spread", "taxa_pre", "dt_competencia"]
            avail_em = [c for c in em_papers_cols if c in em_pos.columns]
            em_papers = em_pos[avail_em].copy().sort_values("vl_posicao", ascending=False)
            em_papers_export = em_papers.copy()
            if "vl_posicao" in em_papers.columns:
                em_papers["vl_posicao"] = em_papers["vl_posicao"].apply(fmt)
            if "spread" in em_papers.columns:
                em_papers["spread"] = em_papers["spread"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
            if "taxa_pre" in em_papers.columns:
                em_papers["taxa_pre"] = em_papers["taxa_pre"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
            if "pct_indexador" in em_papers.columns:
                em_papers["pct_indexador"] = em_papers["pct_indexador"].apply(lambda x: f"{x:.0f}%" if pd.notna(x) and x != 0 else "—")
            em_papers = em_papers.rename(columns={
                "gestora": "Gestora", "nome_fundo": "Fundo", "tipo_ativo": "Tipo",
                "devedor": "Devedor", "ticker_devedor": "Ticker", "cd_ativo": "Código B3",
                "descricao_ativo": "Descrição", "isin": "ISIN", "vl_posicao": "Valor",
                "dt_vencimento": "Vencimento", "indexador": "Indexador",
                "pct_indexador": "% Idx", "spread": "Spread", "taxa_pre": "Taxa Pré",
                "dt_competencia": "Competência",
            })
            st.dataframe(em_papers, use_container_width=True, height=400)
            em_papers_export = em_papers_export.rename(columns={
                "gestora": "Gestora", "nome_fundo": "Fundo", "tipo_ativo": "Tipo",
                "devedor": "Devedor", "ticker_devedor": "Ticker", "cd_ativo": "Código B3",
                "descricao_ativo": "Descrição", "isin": "ISIN", "vl_posicao": "Valor",
                "dt_vencimento": "Vencimento", "indexador": "Indexador",
                "pct_indexador": "% Idx", "spread": "Spread", "taxa_pre": "Taxa Pré",
                "dt_competencia": "Competência",
            })
            excel_btn(em_papers_export, f"zyn_papeis_emissor_{sel_emissor[:30]}.xlsx", key="exp_papeis_emissor")


# ══════════════════════════════════════════
# EMISSORES (página dedicada)
# ══════════════════════════════════════════
elif page == "Emissores":
    st.markdown("""<div class="main-header">
        <h1>Análise de Emissores</h1>
        <p>Mapeamento completo: quem emitiu, quem comprou, volumes, prazos, indexadores</p>
    </div>""", unsafe_allow_html=True)

    positions = load_positions()
    if positions.empty or "emissor" not in positions.columns:
        st.error("Nenhum dado.")
        st.stop()

    share_buttons("ZYN — Emissores", f"{positions['emissor'].nunique() if 'emissor' in positions.columns else 0} emissores\nVolume: {fmt(positions['vl_posicao'].sum())}")

    em_base = positions[positions["emissor"].notna()]

    # Filtros
    col1, col2, col3 = st.columns(3)
    with col1:
        tipo_em = st.multiselect("Tipo de Ativo", ["NC", "CRI", "CRA", "CPR-F", "DEBENTURE"], key="em_tipo")
    with col2:
        busca_em = st.text_input("Buscar emissor (nome/CNPJ)", "", key="em_busca")
    with col3:
        idx_em = st.multiselect("Indexador", sorted(em_base["indexador"].dropna().unique().tolist()), key="em_idx")

    if tipo_em:
        em_base = em_base[em_base["tipo_ativo"].isin(tipo_em)]
    if busca_em:
        em_base = em_base[
            em_base["emissor"].str.contains(busca_em, case=False, na=False)
            | em_base["cnpj_emissor"].str.contains(busca_em, case=False, na=False)
        ]
    if idx_em:
        em_base = em_base[em_base["indexador"].isin(idx_em)]

    # KPIs
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.markdown(f'<div class="metric-card"><div class="metric-value">{em_base["emissor"].nunique()}</div><div class="metric-label">Emissores</div></div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="metric-card"><div class="metric-value">{fmt(em_base["vl_posicao"].sum())}</div><div class="metric-label">Volume Total</div></div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="metric-card"><div class="metric-value">{em_base["cnpj_fundo"].nunique()}</div><div class="metric-label">Fundos Compradores</div></div>', unsafe_allow_html=True)
    c4.markdown(f'<div class="metric-card"><div class="metric-value">{em_base["gestora"].nunique()}</div><div class="metric-label">Gestoras</div></div>', unsafe_allow_html=True)
    c5.markdown(f'<div class="metric-card"><div class="metric-value">{len(em_base):,}</div><div class="metric-label">Posições</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Ranking de emissores
    emissores_full = em_base.groupby(["emissor", "cnpj_emissor"]).agg(
        volume=("vl_posicao", "sum"),
        n_fundos=("cnpj_fundo", "nunique"),
        n_gestoras=("gestora", "nunique"),
        n_posicoes=("vl_posicao", "count"),
        ticket_medio=("vl_posicao", "mean"),
        tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
        indexadores=("indexador", lambda x: ", ".join(sorted(x.dropna().unique())) if x.notna().any() else "—"),
        vencimento_min=("dt_vencimento", safe_min),
        vencimento_max=("dt_vencimento", safe_max),
    ).reset_index().sort_values("volume", ascending=False)

    st.subheader(f"Ranking — {len(emissores_full)} emissores")

    # Chart top 20
    top20 = emissores_full.head(20)
    fig = px.bar(top20, x="emissor", y="volume", color="n_gestoras",
                 color_continuous_scale=["#E0E0E0", GREEN],
                 labels={"emissor": "Emissor", "volume": "Volume", "n_gestoras": "Gestoras"})
    fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=400,
                      xaxis_tickangle=-45, margin=dict(l=0, r=20, t=10, b=120))
    fig.update_yaxes(tickformat=",.0s", tickprefix="R$ ")
    st.plotly_chart(fig, use_container_width=True)

    # Tabela completa
    em_tbl = emissores_full.head(200).copy()
    em_tbl_export = em_tbl.copy()
    em_tbl["Vol."] = em_tbl["volume"].apply(fmt)
    em_tbl["Ticket"] = em_tbl["ticket_medio"].apply(fmt)
    em_tbl = em_tbl.rename(columns={
        "emissor": "Emissor", "cnpj_emissor": "CNPJ", "n_fundos": "Fundos",
        "n_gestoras": "Gestoras", "n_posicoes": "Posições", "tipos": "Tipos",
        "indexadores": "Indexadores", "vencimento_min": "Venc. Mín", "vencimento_max": "Venc. Máx",
    })
    st.dataframe(em_tbl[["Emissor", "CNPJ", "Vol.", "Ticket", "Fundos", "Gestoras", "Posições", "Tipos", "Indexadores", "Venc. Mín", "Venc. Máx"]],
                 use_container_width=True, height=400)
    em_tbl_export = em_tbl_export.rename(columns={
        "emissor": "Emissor", "cnpj_emissor": "CNPJ", "volume": "Volume",
        "ticket_medio": "Ticket Médio", "n_fundos": "Fundos", "n_gestoras": "Gestoras",
        "n_posicoes": "Posições", "tipos": "Tipos", "indexadores": "Indexadores",
        "vencimento_min": "Venc. Mín", "vencimento_max": "Venc. Máx",
    })
    excel_btn(em_tbl_export, "zyn_emissores_ranking.xlsx", key="exp_em_ranking")

    # Drill-down
    st.markdown("---")
    st.subheader("🔍 Drill-down — Detalhes do Emissor")
    sel_em = st.selectbox("Selecione o emissor", [""] + emissores_full["emissor"].head(300).tolist(), key="em_drill")

    if sel_em:
        em_pos = em_base[em_base["emissor"] == sel_em]
        cnpj_em = em_pos["cnpj_emissor"].iloc[0] if "cnpj_emissor" in em_pos.columns else "—"

        st.markdown(f"**{sel_em}** — CNPJ: `{cnpj_em}`")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Volume Captado", fmt(em_pos["vl_posicao"].sum()))
        c2.metric("Fundos Compradores", em_pos["cnpj_fundo"].nunique())
        c3.metric("Gestoras", em_pos["gestora"].nunique())
        c4.metric("Nº Emissões", len(em_pos))

        # Volume por tipo de ativo
        col_l, col_r = st.columns(2)
        with col_l:
            st.markdown("##### Volume por Tipo")
            vol_tipo = em_pos.groupby("tipo_ativo")["vl_posicao"].sum().reset_index()
            vol_tipo.columns = ["Tipo", "Volume"]
            fig_t = px.pie(vol_tipo, values="Volume", names="Tipo", hole=0.4,
                           color_discrete_sequence=CHART_COLORS)
            fig_t.update_layout(height=250, margin=dict(l=0, r=0, t=20, b=0))
            st.plotly_chart(fig_t, use_container_width=True)

        with col_r:
            st.markdown("##### Volume por Indexador")
            vol_idx = em_pos[em_pos["indexador"].notna()].groupby("indexador")["vl_posicao"].sum().reset_index()
            vol_idx.columns = ["Indexador", "Volume"]
            if not vol_idx.empty:
                fig_i = px.pie(vol_idx, values="Volume", names="Indexador", hole=0.4,
                               color_discrete_sequence=CHART_COLORS)
                fig_i.update_layout(height=250, margin=dict(l=0, r=0, t=20, b=0))
                st.plotly_chart(fig_i, use_container_width=True)

        # Gestoras compradoras
        st.markdown("##### Gestoras que compraram")
        gest_comp = em_pos.groupby("gestora").agg(
            volume=("vl_posicao", "sum"),
            fundos=("cnpj_fundo", "nunique"),
            papeis=("vl_posicao", "count"),
            tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
        ).reset_index().sort_values("volume", ascending=False)
        gest_disp = gest_comp.copy()
        gest_disp["Vol."] = gest_disp["volume"].apply(fmt)
        gest_disp = gest_disp.rename(columns={
            "gestora": "Gestora", "fundos": "Fundos", "papeis": "Papéis", "tipos": "Tipos",
        })
        st.dataframe(gest_disp[["Gestora", "Vol.", "Fundos", "Papéis", "Tipos"]], use_container_width=True)
        excel_btn(gest_disp[["Gestora", "Vol.", "Fundos", "Papéis", "Tipos"]],
                  f"zyn_compradores_{sel_em[:30]}.xlsx", key="exp_gest_emissor")

        # Todos os papéis
        st.markdown("##### Todos os papéis emitidos")
        paper_cols = ["gestora", "nome_fundo", "tipo_ativo", "devedor", "ticker_devedor", "cd_ativo", "descricao_ativo", "isin",
                      "vl_posicao", "vl_custo", "qt_posicao", "dt_vencimento",
                      "indexador", "pct_indexador", "spread", "taxa_pre", "dt_competencia"]
        avail = [c for c in paper_cols if c in em_pos.columns]
        em_papers = em_pos[avail].copy().sort_values("vl_posicao", ascending=False)
        em_papers_export = em_papers.copy()
        if "vl_posicao" in em_papers.columns:
            em_papers["vl_posicao"] = em_papers["vl_posicao"].apply(fmt)
        if "vl_custo" in em_papers.columns:
            em_papers["vl_custo"] = em_papers["vl_custo"].apply(lambda x: fmt(x) if pd.notna(x) else "—")
        if "spread" in em_papers.columns:
            em_papers["spread"] = em_papers["spread"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
        if "taxa_pre" in em_papers.columns:
            em_papers["taxa_pre"] = em_papers["taxa_pre"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
        if "pct_indexador" in em_papers.columns:
            em_papers["pct_indexador"] = em_papers["pct_indexador"].apply(lambda x: f"{x:.0f}%" if pd.notna(x) and x != 0 else "—")
        col_rename = {
            "gestora": "Gestora", "nome_fundo": "Fundo", "tipo_ativo": "Tipo",
            "devedor": "Devedor", "ticker_devedor": "Ticker", "cd_ativo": "Código B3",
            "descricao_ativo": "Descrição", "isin": "ISIN", "vl_posicao": "Valor",
            "vl_custo": "Custo", "qt_posicao": "Qtd", "dt_vencimento": "Vencimento",
            "indexador": "Indexador", "pct_indexador": "% Idx", "spread": "Spread",
            "taxa_pre": "Taxa Pré", "dt_competencia": "Competência",
        }
        em_papers = em_papers.rename(columns={k: v for k, v in col_rename.items() if k in em_papers.columns})
        st.dataframe(em_papers, use_container_width=True, height=400)
        em_papers_export = em_papers_export.rename(columns={k: v for k, v in col_rename.items() if k in em_papers_export.columns})
        excel_btn(em_papers_export, f"zyn_papeis_{sel_em[:30]}.xlsx", key="exp_papeis_em")


# ══════════════════════════════════════════
# DEVEDORES / CEDENTES / BENEFICIÁRIOS
# ══════════════════════════════════════════
elif page == "Devedores":
    st.markdown("""<div class="main-header">
        <h1>Devedores / Cedentes / Beneficiários</h1>
        <p>Mapeamento completo: Instrumento → Devedor → Taxa → Vencimento → Investidor</p>
    </div>""", unsafe_allow_html=True)

    positions = load_positions()
    if positions.empty:
        st.error("Nenhum dado. Vá em Atualizar.")
        st.stop()

    # Base: posições com devedor identificado
    dev_base = positions.copy()
    has_devedor = dev_base["devedor"].notna() if "devedor" in dev_base.columns else pd.Series(False, index=dev_base.index)

    share_buttons("ZYN — Devedores/Cedentes", f"{dev_base['devedor'].nunique() if 'devedor' in dev_base.columns else 0} devedores\nVolume: {fmt(dev_base['vl_posicao'].sum())}")

    # --- Filtros ---
    st.markdown("### Filtros")
    fc1, fc2, fc3, fc4 = st.columns(4)
    with fc1:
        dev_tipo_f = st.multiselect("Instrumento", ["NC", "CRI", "CRA", "CPR-F", "DEBENTURE"], key="dev_tipo")
    with fc2:
        dev_busca = st.text_input("Buscar devedor (nome/CNPJ)", "", key="dev_busca")
    with fc3:
        dev_idx_options = sorted(dev_base["indexador"].dropna().unique().tolist()) if "indexador" in dev_base.columns else []
        dev_idx_f = st.multiselect("Indexador", dev_idx_options, key="dev_idx")
    with fc4:
        dev_gestora_busca = st.text_input("Buscar gestora compradora", "", key="dev_gestora")

    if dev_tipo_f:
        dev_base = dev_base[dev_base["tipo_ativo"].isin(dev_tipo_f)]
    if dev_busca and "devedor" in dev_base.columns:
        dev_base = dev_base[
            dev_base["devedor"].str.contains(dev_busca, case=False, na=False)
            | (dev_base["cnpj_emissor"].str.contains(dev_busca, case=False, na=False) if "cnpj_emissor" in dev_base.columns else False)
        ]
    if dev_idx_f and "indexador" in dev_base.columns:
        dev_base = dev_base[dev_base["indexador"].isin(dev_idx_f)]
    if dev_gestora_busca and "gestora" in dev_base.columns:
        dev_base = dev_base[dev_base["gestora"].str.contains(dev_gestora_busca, case=False, na=False)]

    # --- KPIs ---
    st.markdown("<br>", unsafe_allow_html=True)
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    n_devedores = dev_base["devedor"].nunique() if "devedor" in dev_base.columns else 0
    k1.markdown(f'<div class="metric-card"><div class="metric-value">{n_devedores}</div><div class="metric-label">Devedores</div></div>', unsafe_allow_html=True)
    k2.markdown(f'<div class="metric-card"><div class="metric-value">{fmt(dev_base["vl_posicao"].sum())}</div><div class="metric-label">Volume Total</div></div>', unsafe_allow_html=True)
    k3.markdown(f'<div class="metric-card"><div class="metric-value">{len(dev_base):,}</div><div class="metric-label">Posições</div></div>', unsafe_allow_html=True)
    n_tipos = dev_base["tipo_ativo"].nunique()
    k4.markdown(f'<div class="metric-card"><div class="metric-value">{n_tipos}</div><div class="metric-label">Instrumentos</div></div>', unsafe_allow_html=True)
    k5.markdown(f'<div class="metric-card"><div class="metric-value">{dev_base["cnpj_fundo"].nunique()}</div><div class="metric-label">Fundos Compradores</div></div>', unsafe_allow_html=True)
    k6.markdown(f'<div class="metric-card"><div class="metric-value">{dev_base["gestora"].nunique() if "gestora" in dev_base.columns else 0}</div><div class="metric-label">Gestoras</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Ranking de devedores ---
    if "devedor" in dev_base.columns:
        dev_with_name = dev_base[
            dev_base["devedor"].notna()
            & ~dev_base["devedor"].str.contains("Cedente via|Cedente não", na=False, regex=True)
        ].copy()

        devedores_agg = dev_with_name.groupby("devedor").agg(
            volume=("vl_posicao", "sum"),
            n_posicoes=("vl_posicao", "count"),
            n_fundos=("cnpj_fundo", "nunique"),
            n_gestoras=("gestora", "nunique"),
            ticket_medio=("vl_posicao", "mean"),
            instrumentos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
            indexadores=("indexador", lambda x: ", ".join(sorted(x.dropna().unique())) if x.notna().any() else "—"),
            vencimento_min=("dt_vencimento", safe_min),
            vencimento_max=("dt_vencimento", safe_max),
            emissor=("emissor", lambda x: x.dropna().iloc[0] if x.notna().any() else "—"),
        ).reset_index().sort_values("volume", ascending=False)

        # Top 20 chart
        st.subheader(f"Ranking — {len(devedores_agg)} devedores")
        top_dev = devedores_agg.head(20)
        fig_dev = px.bar(
            top_dev, x="devedor", y="volume", color="n_gestoras",
            color_continuous_scale=["#E0E0E0", GREEN],
            labels={"devedor": "Devedor", "volume": "Volume", "n_gestoras": "Gestoras"},
        )
        fig_dev.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=420,
            xaxis_tickangle=-45, margin=dict(l=0, r=20, t=10, b=140),
        )
        fig_dev.update_yaxes(tickformat=",.0s", tickprefix="R$ ")
        st.plotly_chart(fig_dev, use_container_width=True)

        # Tabela completa
        dev_tbl = devedores_agg.head(500).copy()
        dev_tbl_export = dev_tbl.copy()
        dev_tbl["Vol."] = dev_tbl["volume"].apply(fmt)
        dev_tbl["Ticket"] = dev_tbl["ticket_medio"].apply(fmt)
        dev_tbl = dev_tbl.rename(columns={
            "devedor": "Devedor", "emissor": "Emissor/Securitizadora",
            "n_posicoes": "Posições", "n_fundos": "Fundos", "n_gestoras": "Gestoras",
            "instrumentos": "Instrumentos", "indexadores": "Indexadores",
            "vencimento_min": "Venc. Mín", "vencimento_max": "Venc. Máx",
        })
        st.dataframe(
            dev_tbl[["Devedor", "Emissor/Securitizadora", "Vol.", "Ticket", "Instrumentos",
                      "Indexadores", "Fundos", "Gestoras", "Posições", "Venc. Mín", "Venc. Máx"]],
            use_container_width=True, height=450,
        )
        dev_tbl_export = dev_tbl_export.rename(columns={
            "devedor": "Devedor", "emissor": "Emissor/Securitizadora", "volume": "Volume",
            "ticket_medio": "Ticket Médio", "n_posicoes": "Posições", "n_fundos": "Fundos",
            "n_gestoras": "Gestoras", "instrumentos": "Instrumentos", "indexadores": "Indexadores",
            "vencimento_min": "Venc. Mín", "vencimento_max": "Venc. Máx",
        })
        excel_btn(dev_tbl_export, "zyn_devedores_ranking.xlsx", key="exp_dev_ranking")

        # --- DRILL-DOWN: Selecionar devedor ---
        st.markdown("---")
        st.subheader("🔍 Drill-down — Detalhes do Devedor")
        dev_list = devedores_agg["devedor"].head(500).tolist()
        sel_dev = st.selectbox("Selecione o devedor", [""] + dev_list, key="dev_drill")

        if sel_dev:
            dev_pos = dev_with_name[dev_with_name["devedor"] == sel_dev]

            st.markdown(f"### {sel_dev}")
            d1, d2, d3, d4 = st.columns(4)
            d1.metric("Volume Total", fmt(dev_pos["vl_posicao"].sum()))
            d2.metric("Fundos", dev_pos["cnpj_fundo"].nunique())
            d3.metric("Gestoras", dev_pos["gestora"].nunique() if "gestora" in dev_pos.columns else 0)
            d4.metric("Posições", len(dev_pos))
            tipos_dev = ", ".join(sorted(dev_pos["tipo_ativo"].unique()))
            st.markdown(f'<div class="info-row"><div class="info-item"><strong>Instrumentos</strong>: {tipos_dev}</div></div>', unsafe_allow_html=True)

            # Charts: volume por instrumento e por indexador
            chart_l, chart_r = st.columns(2)
            with chart_l:
                st.markdown("##### Volume por Instrumento")
                vol_instr = dev_pos.groupby("tipo_ativo")["vl_posicao"].sum().reset_index()
                vol_instr.columns = ["Instrumento", "Volume"]
                fig_vi = px.pie(vol_instr, values="Volume", names="Instrumento", hole=0.4,
                                color_discrete_sequence=CHART_COLORS)
                fig_vi.update_layout(height=260, margin=dict(l=0, r=0, t=20, b=0))
                st.plotly_chart(fig_vi, use_container_width=True)

            with chart_r:
                st.markdown("##### Volume por Indexador")
                if "indexador" in dev_pos.columns and dev_pos["indexador"].notna().any():
                    vol_ix = dev_pos[dev_pos["indexador"].notna()].groupby("indexador")["vl_posicao"].sum().reset_index()
                    vol_ix.columns = ["Indexador", "Volume"]
                    fig_ix = px.pie(vol_ix, values="Volume", names="Indexador", hole=0.4,
                                    color_discrete_sequence=CHART_COLORS)
                    fig_ix.update_layout(height=260, margin=dict(l=0, r=0, t=20, b=0))
                    st.plotly_chart(fig_ix, use_container_width=True)
                else:
                    st.info("Sem dados de indexador.")

            # Tabela: quem comprou este devedor
            st.markdown("##### Gestoras / Fundos que compraram este devedor")
            compradores = dev_pos.groupby(["gestora", "nome_fundo", "cnpj_fundo"]).agg(
                volume=("vl_posicao", "sum"),
                n_papeis=("vl_posicao", "count"),
                tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
                indexadores=("indexador", lambda x: ", ".join(sorted(x.dropna().unique())) if x.notna().any() else "—"),
                vencimento_max=("dt_vencimento", safe_max),
            ).reset_index().sort_values("volume", ascending=False)
            comp_disp = compradores.copy()
            comp_disp["Vol."] = comp_disp["volume"].apply(fmt)
            comp_disp = comp_disp.rename(columns={
                "gestora": "Gestora", "nome_fundo": "Fundo", "cnpj_fundo": "CNPJ Fundo",
                "n_papeis": "Papéis", "tipos": "Instrumentos", "indexadores": "Indexadores",
                "vencimento_max": "Venc. Máx",
            })
            st.dataframe(
                comp_disp[["Gestora", "Fundo", "CNPJ Fundo", "Vol.", "Papéis", "Instrumentos", "Indexadores", "Venc. Máx"]],
                use_container_width=True, height=350,
            )
            excel_btn(comp_disp, f"zyn_compradores_devedor_{sel_dev[:30]}.xlsx", key="exp_comp_dev")

            # Tabela: todos os papéis deste devedor
            st.markdown("##### Todos os papéis — Instrumento / Taxa / Vencimento / Investidor")
            paper_cols_dev = [
                "tipo_ativo", "emissor", "cnpj_emissor", "descricao_ativo", "isin", "cd_ativo",
                "vl_posicao", "dt_vencimento", "indexador", "pct_indexador", "spread", "taxa_pre",
                "gestora", "nome_fundo", "cnpj_fundo", "dt_competencia",
            ]
            avail_dev = [c for c in paper_cols_dev if c in dev_pos.columns]
            dev_papers = dev_pos[avail_dev].copy().sort_values("vl_posicao", ascending=False)
            dev_papers_export = dev_papers.copy()

            # Formatação
            if "vl_posicao" in dev_papers.columns:
                dev_papers["vl_posicao"] = dev_papers["vl_posicao"].apply(fmt)
            if "spread" in dev_papers.columns:
                dev_papers["spread"] = dev_papers["spread"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
            if "taxa_pre" in dev_papers.columns:
                dev_papers["taxa_pre"] = dev_papers["taxa_pre"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
            if "pct_indexador" in dev_papers.columns:
                dev_papers["pct_indexador"] = dev_papers["pct_indexador"].apply(lambda x: f"{x:.0f}%" if pd.notna(x) and x != 0 else "—")

            col_rename_dev = {
                "tipo_ativo": "Instrumento", "emissor": "Emissor/Securitizadora",
                "cnpj_emissor": "CNPJ Emissor", "descricao_ativo": "Descrição",
                "isin": "ISIN", "cd_ativo": "Código B3", "vl_posicao": "Valor",
                "dt_vencimento": "Vencimento", "indexador": "Indexador",
                "pct_indexador": "% Idx", "spread": "Spread", "taxa_pre": "Taxa Pré",
                "gestora": "Gestora", "nome_fundo": "Fundo", "cnpj_fundo": "CNPJ Fundo",
                "dt_competencia": "Competência",
            }
            dev_papers = dev_papers.rename(columns={k: v for k, v in col_rename_dev.items() if k in dev_papers.columns})
            st.dataframe(dev_papers, use_container_width=True, height=450)
            dev_papers_export = dev_papers_export.rename(columns={k: v for k, v in col_rename_dev.items() if k in dev_papers_export.columns})
            excel_btn(dev_papers_export, f"zyn_papeis_devedor_{sel_dev[:30]}.xlsx", key="exp_papeis_dev")

            # Timeline de vencimentos
            if "dt_vencimento" in dev_pos.columns and dev_pos["dt_vencimento"].notna().any():
                st.markdown("##### Timeline de Vencimentos")
                timeline = dev_pos[dev_pos["dt_vencimento"].notna()].copy()
                timeline["dt_vencimento"] = pd.to_datetime(timeline["dt_vencimento"], errors="coerce")
                timeline = timeline[timeline["dt_vencimento"].notna()]
                if not timeline.empty:
                    tl_agg = timeline.groupby([pd.Grouper(key="dt_vencimento", freq="Q"), "tipo_ativo"])["vl_posicao"].sum().reset_index()
                    tl_agg.columns = ["Trimestre", "Instrumento", "Volume"]
                    fig_tl = px.bar(tl_agg, x="Trimestre", y="Volume", color="Instrumento",
                                    color_discrete_sequence=CHART_COLORS)
                    fig_tl.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=300,
                                         margin=dict(l=0, r=20, t=10, b=0))
                    fig_tl.update_yaxes(tickformat=",.0s", tickprefix="R$ ")
                    st.plotly_chart(fig_tl, use_container_width=True)

    else:
        st.info("Coluna 'devedor' não encontrada. Execute a atualização para enriquecer os dados.")


# ══════════════════════════════════════════
# FUNDOS COM CAIXA
# ══════════════════════════════════════════
elif page == "Fundos com Caixa":
    st.markdown("""<div class="main-header">
        <h1>Fundos — Capacidade Estimada</h1>
        <p>Capacidade adicional estimada de alocação em crédito privado (target = % atual + 10pp, max 80%)</p>
    </div>""", unsafe_allow_html=True)

    positions = load_positions()
    if positions.empty:
        st.error("Nenhum dado.")
        st.stop()

    share_buttons("ZYN — Fundos com Caixa", f"{positions['cnpj_fundo'].nunique()} fundos\nVolume RF: {fmt(positions['vl_posicao'].sum())}")

    # Build fund-level aggregation
    fundos_raw = positions.groupby(["cnpj_fundo", "nome_fundo", "gestora"]).agg(
        vol_rf=("vl_posicao", "sum"),
        pl=("pl_fundo", "first"),
        n_papeis=("vl_posicao", "count"),
        tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))),
        classe=("classe_anbima", "first"),
        publico=("publico_alvo", "first"),
        administrador=("administrador", "first"),
        n_devedores=("devedor", "nunique"),
    ).reset_index()

    fundos_raw = fundos_raw[fundos_raw["pl"].notna() & (fundos_raw["pl"] > 0)].copy()
    fundos_raw["pct_rf"] = (fundos_raw["vol_rf"] / fundos_raw["pl"] * 100).round(1)

    # Capacidade Estimada: assume fundo pode alocar +10pp além do atual, cap 80%
    _pct_dec = fundos_raw["pct_rf"] / 100
    _target = (_pct_dec + 0.10).clip(upper=0.80)
    fundos_raw["caixa"] = (fundos_raw["pl"] * _target - fundos_raw["vol_rf"]).clip(lower=0)

    # --- Filtros ---
    st.markdown("### Filtros")
    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        caixa_tipo_f = st.multiselect("Tipo de Ativo que compra", ["NC", "CRI", "CRA", "CPR-F", "DEBENTURE"], key="cx_tipo")
    with fc2:
        caixa_min = st.number_input("Capac. mínima (R$ M)", value=10, step=10, key="cx_min")
    with fc3:
        min_pct = st.number_input("% mín. alocado RF", value=5.0, step=1.0, key="cx_pct_min")
    with fc4:
        caixa_busca = st.text_input("Buscar fundo/gestora", "", key="cx_busca")
    with fc5:
        classe_options = sorted(fundos_raw["classe"].dropna().unique().tolist())
        caixa_classe = st.multiselect("Classe ANBIMA", classe_options, key="cx_classe")

    fundos_f = fundos_raw.copy()
    # Filtro mínimo % alocação em RF (remove fundos DI, equity etc.)
    fundos_f = fundos_f[fundos_f["pct_rf"] >= min_pct]
    if caixa_tipo_f:
        for t in caixa_tipo_f:
            fundos_f = fundos_f[fundos_f["tipos"].str.contains(t, na=False)]
    if caixa_min > 0:
        fundos_f = fundos_f[fundos_f["caixa"] >= caixa_min * 1e6]
    if caixa_busca:
        fundos_f = fundos_f[
            fundos_f["nome_fundo"].str.contains(caixa_busca, case=False, na=False)
            | fundos_f["gestora"].str.contains(caixa_busca, case=False, na=False)
        ]
    if caixa_classe:
        fundos_f = fundos_f[fundos_f["classe"].isin(caixa_classe)]

    fundos_f = fundos_f.sort_values("caixa", ascending=False)

    # --- KPIs ---
    st.markdown("<br>", unsafe_allow_html=True)
    k1, k2, k3, k4 = st.columns(4)
    k1.markdown(f'<div class="metric-card"><div class="metric-value">{len(fundos_f)}</div><div class="metric-label">Fundos</div></div>', unsafe_allow_html=True)
    k2.markdown(f'<div class="metric-card"><div class="metric-value">{fmt(fundos_f["caixa"].sum())}</div><div class="metric-label">Capacidade Estimada Total</div></div>', unsafe_allow_html=True)
    k3.markdown(f'<div class="metric-card"><div class="metric-value">{fmt(fundos_f["pl"].sum())}</div><div class="metric-label">PL Total</div></div>', unsafe_allow_html=True)
    k4.markdown(f'<div class="metric-card"><div class="metric-value">{fundos_f["gestora"].nunique()}</div><div class="metric-label">Gestoras</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Top 20 chart ---
    st.subheader("Top 20 Fundos com Maior Capacidade Estimada")
    st.caption("Capacidade = PL × (% atual + 10pp, max 80%) − vol. atual em RF. Filtro padrão exclui fundos com < 5% em crédito privado.")
    top20_cx = fundos_f.head(20)
    fig_cx = px.bar(
        top20_cx, x="nome_fundo", y="caixa",
        color="pct_rf", color_continuous_scale=["#E0E0E0", GREEN],
        labels={"nome_fundo": "Fundo", "caixa": "Capacidade Est.", "pct_rf": "% RF"},
        hover_data=["gestora", "pl", "vol_rf"],
    )
    fig_cx.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=420,
        xaxis_tickangle=-45, margin=dict(l=0, r=20, t=10, b=180),
    )
    fig_cx.update_yaxes(tickformat=",.0s", tickprefix="R$ ")
    st.plotly_chart(fig_cx, use_container_width=True)

    # --- Tabela ---
    st.subheader(f"Tabela — {len(fundos_f)} fundos")
    tbl_cx = fundos_f.head(500).copy()
    tbl_cx_exp = tbl_cx.copy()
    tbl_cx["Capac. Est."] = tbl_cx["caixa"].apply(fmt)
    tbl_cx["PL"] = tbl_cx["pl"].apply(fmt)
    tbl_cx["Vol. RF"] = tbl_cx["vol_rf"].apply(fmt)
    tbl_cx["% RF"] = tbl_cx["pct_rf"].apply(lambda x: f"{x:.1f}%")
    tbl_cx = tbl_cx.rename(columns={
        "cnpj_fundo": "CNPJ", "nome_fundo": "Fundo", "gestora": "Gestora",
        "n_papeis": "Papéis", "tipos": "Tipos que Compra", "classe": "Classe ANBIMA",
        "publico": "Público Alvo", "administrador": "Administrador",
        "n_devedores": "Devedores",
    })
    st.dataframe(
        tbl_cx[["CNPJ", "Fundo", "Gestora", "Capac. Est.", "PL", "Vol. RF", "% RF",
                "Tipos que Compra", "Papéis", "Devedores", "Classe ANBIMA", "Público Alvo"]],
        use_container_width=True, height=450,
    )
    tbl_cx_exp = tbl_cx_exp.rename(columns={
        "cnpj_fundo": "CNPJ", "nome_fundo": "Fundo", "gestora": "Gestora",
        "vol_rf": "Volume RF", "pl": "PL", "caixa": "Capacidade Estimada",
        "pct_rf": "% Alocado RF", "n_papeis": "Papéis", "tipos": "Tipos que Compra",
        "classe": "Classe ANBIMA", "publico": "Público Alvo", "administrador": "Administrador",
        "n_devedores": "Devedores",
    })
    excel_btn(tbl_cx_exp, "zyn_fundos_caixa.xlsx", key="exp_fundos_caixa")

    # --- Drill-down ---
    st.markdown("---")
    st.subheader("🔍 Drill-down — Carteira do Fundo")
    fundo_sel_cx = st.selectbox("Selecione o fundo", [""] + fundos_f["nome_fundo"].head(300).tolist(), key="cx_drill")

    if fundo_sel_cx:
        f_pos = positions[positions["nome_fundo"] == fundo_sel_cx].copy()

        pl_f = f_pos["pl_fundo"].iloc[0] if "pl_fundo" in f_pos.columns and pd.notna(f_pos["pl_fundo"].iloc[0]) else 0
        vol_f = f_pos["vl_posicao"].sum()
        pct_f = vol_f / pl_f if pl_f > 0 else 0
        target_f = min(pct_f + 0.10, 0.80)
        caixa_f = max(pl_f * target_f - vol_f, 0) if pl_f > 0 else 0

        dx1, dx2, dx3, dx4 = st.columns(4)
        dx1.metric("PL", fmt(pl_f))
        dx2.metric("Volume RF", fmt(vol_f))
        dx3.metric("Capac. Est.", fmt(caixa_f))
        dx4.metric("% Alocado", f"{pct_f*100:.1f}%" if pl_f > 0 else "—")

        g_name = f_pos["gestora"].iloc[0] if "gestora" in f_pos.columns else "—"
        admin = f_pos["administrador"].iloc[0] if "administrador" in f_pos.columns and pd.notna(f_pos["administrador"].iloc[0]) else "—"
        publico = f_pos["publico_alvo"].iloc[0] if "publico_alvo" in f_pos.columns and pd.notna(f_pos["publico_alvo"].iloc[0]) else "—"
        classe = f_pos["classe_anbima"].iloc[0] if "classe_anbima" in f_pos.columns and pd.notna(f_pos["classe_anbima"].iloc[0]) else "—"
        _cnpj_cx = f_pos["cnpj_fundo"].iloc[0] if "cnpj_fundo" in f_pos.columns else "—"
        _tipos_cx = ", ".join(sorted(f_pos["tipo_ativo"].unique())) if "tipo_ativo" in f_pos.columns else "—"

        st.markdown(f"""<div class="info-row">
            <div class="info-item"><strong>Gestora</strong>: {g_name}</div>
            <div class="info-item"><strong>CNPJ</strong>: {_cnpj_cx}</div>
            <div class="info-item"><strong>Administrador</strong>: {admin}</div>
            <div class="info-item"><strong>Público Alvo</strong>: {publico}</div>
            <div class="info-item"><strong>Tipos</strong>: {_tipos_cx}</div>
            <div class="info-item"><strong>Classe</strong>: {classe}</div>
            <div class="info-item"><strong>Papéis</strong>: {len(f_pos)}</div>
        </div>""", unsafe_allow_html=True)

        # Charts
        ch1_cx, ch2_cx = st.columns(2)
        with ch1_cx:
            by_tipo = f_pos.groupby("tipo_ativo")["vl_posicao"].sum().reset_index()
            by_tipo.columns = ["Tipo", "Volume"]
            fig_t = px.pie(by_tipo, values="Volume", names="Tipo", hole=0.4,
                           color_discrete_sequence=CHART_COLORS)
            fig_t.update_layout(height=250, margin=dict(l=0, r=0, t=20, b=0), title_text="Composição por Tipo")
            st.plotly_chart(fig_t, use_container_width=True)
        with ch2_cx:
            if "devedor" in f_pos.columns and f_pos["devedor"].notna().any():
                top_dev = f_pos.groupby("devedor")["vl_posicao"].sum().nlargest(8).reset_index()
                top_dev.columns = ["Devedor", "Volume"]
                fig_d = px.bar(top_dev, x="Devedor", y="Volume", color_discrete_sequence=[GREEN])
                fig_d.update_layout(height=250, margin=dict(l=0, r=20, t=20, b=80), title_text="Top Devedores",
                                    xaxis_tickangle=-30)
                fig_d.update_yaxes(tickformat=",.0s", tickprefix="R$ ")
                st.plotly_chart(fig_d, use_container_width=True)

        # Todos os papéis
        st.markdown("##### Todos os papéis do fundo")
        paper_cols_cx = [
            "tipo_ativo", "devedor", "ticker_devedor", "emissor", "cnpj_emissor",
            "cd_ativo", "descricao_ativo", "isin", "vl_posicao", "vl_custo",
            "qt_posicao", "dt_vencimento", "indexador", "pct_indexador", "spread",
            "taxa_pre", "bloco", "dt_competencia",
        ]
        avail_cx = [c for c in paper_cols_cx if c in f_pos.columns]
        papers_cx = f_pos[avail_cx].copy().sort_values("vl_posicao", ascending=False)
        papers_cx_exp = papers_cx.copy()
        if "vl_posicao" in papers_cx.columns:
            papers_cx["vl_posicao"] = papers_cx["vl_posicao"].apply(fmt)
        if "vl_custo" in papers_cx.columns:
            papers_cx["vl_custo"] = papers_cx["vl_custo"].apply(lambda x: fmt(x) if pd.notna(x) else "—")
        if "spread" in papers_cx.columns:
            papers_cx["spread"] = papers_cx["spread"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
        if "taxa_pre" in papers_cx.columns:
            papers_cx["taxa_pre"] = papers_cx["taxa_pre"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
        if "pct_indexador" in papers_cx.columns:
            papers_cx["pct_indexador"] = papers_cx["pct_indexador"].apply(lambda x: f"{x:.0f}%" if pd.notna(x) and x != 0 else "—")
        col_rename_cx = {
            "tipo_ativo": "Tipo", "devedor": "Devedor", "ticker_devedor": "Ticker",
            "emissor": "Emissor", "cnpj_emissor": "CNPJ Emissor",
            "cd_ativo": "Código B3", "descricao_ativo": "Descrição",
            "isin": "ISIN", "vl_posicao": "Valor", "vl_custo": "Custo",
            "qt_posicao": "Qtd", "dt_vencimento": "Vencimento",
            "indexador": "Indexador", "pct_indexador": "% Idx", "spread": "Spread",
            "taxa_pre": "Taxa Pré", "bloco": "Bloco", "dt_competencia": "Competência",
        }
        papers_cx = papers_cx.rename(columns={k: v for k, v in col_rename_cx.items() if k in papers_cx.columns})
        st.dataframe(papers_cx, use_container_width=True, height=450)
        papers_cx_exp = papers_cx_exp.rename(columns={k: v for k, v in col_rename_cx.items() if k in papers_cx_exp.columns})
        excel_btn(papers_cx_exp, f"zyn_carteira_{fundo_sel_cx[:30]}.xlsx", key="exp_carteira_cx")

    # --- Agrupamento por gestora ---
    st.markdown("---")
    st.subheader("Caixa Agregado por Gestora")
    gest_caixa = fundos_f.groupby("gestora").agg(
        caixa_total=("caixa", "sum"),
        pl_total=("pl", "sum"),
        vol_rf_total=("vol_rf", "sum"),
        n_fundos=("cnpj_fundo", "nunique"),
        tipos=("tipos", lambda x: ", ".join(sorted(set(", ".join(x).split(", "))))),
    ).reset_index().sort_values("caixa_total", ascending=False)
    gest_caixa["pct_rf"] = (gest_caixa["vol_rf_total"] / gest_caixa["pl_total"] * 100).round(1)
    gest_cx_disp = gest_caixa.head(100).copy()
    gest_cx_exp = gest_cx_disp.copy()
    gest_cx_disp["Caixa"] = gest_cx_disp["caixa_total"].apply(fmt)
    gest_cx_disp["PL"] = gest_cx_disp["pl_total"].apply(fmt)
    gest_cx_disp["Vol. RF"] = gest_cx_disp["vol_rf_total"].apply(fmt)
    gest_cx_disp["% RF"] = gest_cx_disp["pct_rf"].apply(lambda x: f"{x:.1f}%")
    gest_cx_disp = gest_cx_disp.rename(columns={
        "gestora": "Gestora", "n_fundos": "Fundos", "tipos": "Tipos que Compra",
    })
    st.dataframe(
        gest_cx_disp[["Gestora", "Caixa", "PL", "Vol. RF", "% RF", "Fundos", "Tipos que Compra"]],
        use_container_width=True, height=350,
    )
    gest_cx_exp = gest_cx_exp.rename(columns={
        "gestora": "Gestora", "caixa_total": "Caixa", "pl_total": "PL",
        "vol_rf_total": "Volume RF", "pct_rf": "% Alocado RF",
        "n_fundos": "Fundos", "tipos": "Tipos que Compra",
    })
    excel_btn(gest_cx_exp, "zyn_gestoras_caixa.xlsx", key="exp_gest_caixa")


# ══════════════════════════════════════════
# MATCHING
# ══════════════════════════════════════════
elif page == "Matching":
    st.markdown("""<div class="main-header">
        <h1>Matching por Operação</h1>
        <p>Encontre os melhores investidores para cada deal — com drill-down até o fundo</p>
    </div>""", unsafe_allow_html=True)

    positions = load_positions()
    profiles = load_profiles()
    if positions.empty or profiles.empty:
        st.error("Nenhum dado.")
        st.stop()

    share_buttons("ZYN — Matching por Operação", f"{len(profiles)} perfis de investidores disponíveis")

    st.subheader("Parâmetros da Operação")
    c1, c2, c3, c4 = st.columns(4)
    deal_tipo = c1.selectbox("Tipo", ["CRA", "CRI", "NC", "CPR-F", "DEBENTURE"])
    deal_vol = c2.number_input("Volume (R$ M)", value=50, step=10, min_value=1)
    deal_prazo = c3.number_input("Prazo (anos)", value=3.0, step=0.5, min_value=0.5)
    deal_idx = c4.selectbox("Indexador", ["CDI", "IPCA", "PRE", "IGP-M"])
    deal_nome = st.text_input("Nome (opcional)", f"Operação {deal_tipo}")

    run = st.button("🎯 Buscar Investidores", type="primary")

    if run:
        deal = {"nome": deal_nome, "tipo": deal_tipo, "volume": deal_vol * 1e6,
                "prazo_anos": deal_prazo, "indexador": deal_idx}

        with st.spinner("Calculando..."):
            matching = match_deal_to_investors(deal, profiles, top_n=50, min_score=0.2)

        if matching.empty:
            st.warning("Nenhum investidor encontrado.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Investidores", len(matching))
            c2.metric("Score máximo", f"{matching['score_total'].max():.0%}")
            c3.metric("Score ≥ 70%", len(matching[matching["score_total"] >= 0.7]))

            # Gráfico
            top30 = matching.head(30)
            colors = [GREEN if s >= 0.7 else "#E6A817" if s >= 0.5 else GRAY for s in top30["score_total"]]
            fig = go.Figure(go.Bar(
                y=top30["gestora"].str[:40], x=top30["score_total"], orientation="h",
                marker_color=colors, text=[f"{s:.0%}" for s in top30["score_total"]], textposition="outside",
            ))
            fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                              height=max(400, len(top30) * 28), yaxis=dict(autorange="reversed"),
                              xaxis=dict(tickformat=".0%", range=[0, 1.1]),
                              margin=dict(l=280, r=50, t=10, b=20))
            st.plotly_chart(fig, use_container_width=True)

            # Tabela
            display = matching.copy()
            for c in ["pl_total", "vol_total_rf", "ticket_medio"]:
                if c in display.columns:
                    display[c] = display[c].apply(fmt)
            for c in ["score_total", "score_tipo", "score_volume", "score_prazo", "score_indexador", "score_historico"]:
                if c in display.columns:
                    display[c] = display[c].apply(lambda x: f"{x:.0%}")
            display = display.rename(columns={
                "gestora": "Gestora", "score_total": "Score", "n_fundos": "Fundos",
                "pl_total": "PL Total", "vol_total_rf": "Vol. RF", "ticket_medio": "Ticket",
                "tipo_preferido": "Pref.", "indexador_principal": "Indexador",
                "score_tipo": "S.Tipo", "score_volume": "S.Vol", "score_prazo": "S.Prazo",
                "score_indexador": "S.Idx", "score_historico": "S.Hist",
            })
            st.dataframe(display, use_container_width=True, height=400)
            excel_btn(display, f"zyn_matching_{deal_tipo}.xlsx", key="exp_matching")

            # === DRILL-DOWN: ver fundos da gestora selecionada ===
            st.markdown("---")
            st.subheader(f"🔍 Fundos que compraram {deal_tipo} — Drill-down")

            gestora_match_list = matching["gestora"].tolist()
            sel_gestora = st.selectbox("Selecione gestora do ranking", [""] + gestora_match_list, key="match_drill")

            if sel_gestora:
                g_pos = positions[(positions["gestora"] == sel_gestora) & (positions["tipo_ativo"] == deal_tipo)]
                if g_pos.empty:
                    g_pos = positions[positions["gestora"] == sel_gestora]

                fundos_g = (
                    g_pos.groupby(["cnpj_fundo", "nome_fundo"])
                    .agg(volume=("vl_posicao", "sum"), n_papeis=("vl_posicao", "count"),
                         tipos=("tipo_ativo", lambda x: ", ".join(sorted(x.unique()))))
                    .reset_index().sort_values("volume", ascending=False)
                )
                fundos_g["Vol."] = fundos_g["volume"].apply(fmt)
                st.dataframe(
                    fundos_g.rename(columns={"cnpj_fundo": "CNPJ", "nome_fundo": "Fundo", "n_papeis": "Papéis", "tipos": "Tipos"})[["CNPJ", "Fundo", "Vol.", "Papéis", "Tipos"]],
                    use_container_width=True,
                )

                # Papéis individuais
                sel_fundo = st.selectbox("Ver papéis do fundo", [""] + fundos_g["nome_fundo"].tolist(), key="match_fundo")
                if sel_fundo:
                    f_pos = g_pos[g_pos["nome_fundo"] == sel_fundo]
                    cols_show = ["tipo_ativo", "devedor", "ticker_devedor", "emissor", "cd_ativo", "descricao_ativo", "isin", "vl_posicao",
                                 "dt_vencimento", "indexador", "spread", "taxa_pre"]
                    avail = [c for c in cols_show if c in f_pos.columns]
                    papers = f_pos[avail].copy()
                    if "vl_posicao" in papers.columns:
                        papers["vl_posicao"] = papers["vl_posicao"].apply(fmt)
                    if "spread" in papers.columns:
                        papers["spread"] = papers["spread"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0 else "—")
                    papers = papers.rename(columns={
                        "tipo_ativo": "Tipo", "devedor": "Devedor", "ticker_devedor": "Ticker",
                        "emissor": "Emissor", "cd_ativo": "Código B3",
                        "descricao_ativo": "Descrição",
                        "isin": "ISIN", "vl_posicao": "Valor", "dt_vencimento": "Vencimento",
                        "indexador": "Indexador", "spread": "Spread", "taxa_pre": "Taxa Pré",
                    })
                    st.dataframe(papers, use_container_width=True, height=300)
                    excel_btn(papers, f"zyn_papeis_match_{sel_fundo[:30]}.xlsx", key="exp_papeis_match")

            # FOs manuais
            fo = search_by_appetite(deal_tipo)
            if fo:
                st.markdown("---")
                st.subheader(f"🏦 Base Manual — Apetite para {deal_tipo}")
                fo_cols = ["nome", "tipo", "ticket_min", "ticket_max", "indexador_pref", "notas"]
                fo_df = pd.DataFrame(fo)
                fo_avail = [c for c in fo_cols if c in fo_df.columns]
                fo_df = fo_df[fo_avail]
                fo_rename = {"nome": "Nome", "tipo": "Tipo", "ticket_min": "Ticket Mín", "ticket_max": "Ticket Máx", "indexador_pref": "Indexador", "notas": "Notas"}
                fo_df = fo_df.rename(columns={k: v for k, v in fo_rename.items() if k in fo_df.columns})
                if "Ticket Mín" in fo_df.columns:
                    fo_df["Ticket Mín"] = fo_df["Ticket Mín"].apply(fmt)
                if "Ticket Máx" in fo_df.columns:
                    fo_df["Ticket Máx"] = fo_df["Ticket Máx"].apply(fmt)
                st.dataframe(fo_df, use_container_width=True)

            if st.button("📥 Exportar Excel Completo"):
                export_deal_matching(deal, matching)
                st.success(f"Exportado em {OUTPUT_DIR}/")



# ══════════════════════════════════════════
# BASE MANUAL
# ══════════════════════════════════════════
elif page == "Base Manual":
    st.markdown("""<div class="main-header">
        <h1>Base Manual de Investidores</h1>
        <p>Family Offices, Tesourarias, Seguradoras e Previdência</p>
    </div>""", unsafe_allow_html=True)

    fo_base = load_family_offices()
    if fo_base:
        tipos = sorted(set(inv.get("tipo", "") for inv in fo_base))
        tipo_f = st.multiselect("Filtrar por tipo", tipos, default=tipos)
        filtered = [inv for inv in fo_base if inv.get("tipo", "") in tipo_f]
        if filtered:
            df = pd.DataFrame(filtered)
            cols = ["nome", "tipo", "apetite", "ticket_min", "ticket_max", "indexador_pref", "contato_nome", "contato_email", "notas"]
            avail = [c for c in cols if c in df.columns]
            display = df[avail].copy()
            display.columns = [c.replace("_", " ").title() for c in avail]
            if "Apetite" in display.columns:
                display["Apetite"] = display["Apetite"].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
            if "Ticket Min" in display.columns:
                display["Ticket Min"] = display["Ticket Min"].apply(fmt)
            if "Ticket Max" in display.columns:
                display["Ticket Max"] = display["Ticket Max"].apply(fmt)
            st.dataframe(display, use_container_width=True, height=400)
            excel_btn(display, "zyn_base_manual.xlsx", key="exp_base_manual")
        st.markdown(f"**Total**: {len(filtered)}")

    st.markdown("---")
    st.subheader("Adicionar Investidor")
    with st.form("add"):
        c1, c2 = st.columns(2)
        nome = c1.text_input("Nome")
        tipo = c1.selectbox("Tipo", ["Family Office", "Tesouraria Banco", "Seguradora", "Previdência", "Outro"])
        apetite = c2.multiselect("Apetite", ["NC", "CRI", "CRA", "CPR-F", "DEBENTURE"])
        indexador = c2.selectbox("Indexador", ["", "CDI", "IPCA", "PRE"])
        contato = c1.text_input("Contato")
        email = c2.text_input("E-mail")
        tc1, tc2 = st.columns(2)
        t_min = tc1.number_input("Ticket mín (R$ M)", value=0)
        t_max = tc2.number_input("Ticket máx (R$ M)", value=0)
        notas = st.text_area("Notas")
        if st.form_submit_button("Adicionar", type="primary") and nome:
            add_investor({"nome": nome, "tipo": tipo, "apetite": apetite, "indexador_pref": indexador,
                          "contato_nome": contato, "contato_email": email,
                          "ticket_min": t_min * 1e6, "ticket_max": t_max * 1e6,
                          "notas": notas, "origem": "manual", "ativo": True})
            st.success(f"✅ {nome} adicionado!")
            st.rerun()


# ══════════════════════════════════════════
# ATUALIZAR
# ══════════════════════════════════════════
elif page == "Atualizar":
    st.markdown("""<div class="main-header">
        <h1>Atualizar Dados CVM</h1>
        <p>Baixar dados atualizados, enriquecer cedentes/devedores e recalcular perfis</p>
    </div>""", unsafe_allow_html=True)

    cache = DATA_DIR / "positions_enriched.csv"
    if cache.exists():
        mod = datetime.fromtimestamp(cache.stat().st_mtime)
        age = (datetime.now() - mod).days
        st.info(f"Última atualização: **{mod.strftime('%d/%m/%Y %H:%M')}** ({age} dias)")
        if age >= 30:
            st.warning("⚠️ Dados com mais de 30 dias. Recomendamos atualizar.")
    else:
        st.warning("Nenhum dado carregado.")

    # Atualização automática
    st.markdown("---")
    st.subheader("Atualização Automática")
    st.markdown("""
    A base é atualizada automaticamente **no dia 1 e 15 de cada mês** às 9h via cron job.

    **Pipeline completo:**
    1. Baixa CDA (posições de fundos) — dados.cvm.gov.br
    2. Baixa cadastro de fundos (gestora, admin, PL)
    3. Identifica devedores — NC/Debênture: emissor direto; BLC_4: ticker B3
    4. Enriquece CRI/CRA com cedentes/devedores — informes mensais CVM (SECURIT)
    5. Calcula perfis de investidores e scores de matching
    """)

    # Atualização manual
    st.markdown("---")
    st.subheader("Atualização Manual")
    months = st.slider("Meses de dados", 1, 6, 3)
    export = st.checkbox("Exportar para Notion", True)

    if st.button("🔄 Atualizar Agora", type="primary"):
        cmd = [sys.executable, "main.py", "--months", str(months)]
        if export:
            cmd.append("--export-notion")
        with st.spinner("Baixando dados CVM e enriquecendo cedentes/devedores..."):
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    cwd=str(Path(__file__).resolve().parent), timeout=900)
        if result.returncode == 0:
            st.success("✅ Atualizado com sucesso!")
            st.cache_data.clear()
            with st.expander("Log completo"):
                st.code(result.stdout)
        else:
            st.error("Erro na atualização:")
            st.code(result.stderr or result.stdout)

    # Status dos dados
    st.markdown("---")
    st.subheader("Status dos Dados")
    data_files = {
        "positions_enriched.csv": "Posições enriquecidas",
        "investor_profiles.csv": "Perfis de investidores",
        "devedor_mapping.csv": "Mapeamento cedentes/devedores CRI/CRA",
        "cvm_cedentes_devedores.csv": "Base cedentes CVM (raw)",
        "cvm_classes_cri_cra.csv": "Classes CRI/CRA CVM",
        "cvm_gerais_cri_cra.csv": "Dados gerais CRI/CRA CVM",
    }
    for fname, desc in data_files.items():
        fpath = DATA_DIR / fname
        if fpath.exists():
            fmod = datetime.fromtimestamp(fpath.stat().st_mtime).strftime('%d/%m/%Y %H:%M')
            fsize = fpath.stat().st_size
            size_str = f"{fsize/1e6:.1f} MB" if fsize > 1e6 else f"{fsize/1e3:.0f} KB"
            st.markdown(f"- **{desc}**: {fmod} ({size_str})")
        else:
            st.markdown(f"- **{desc}**: *não encontrado*")

    # === SYNC NOTION (TUDO) ===
    st.markdown("---")
    st.subheader("Sincronizar Notion (Tudo)")

    _sc1, _sc2 = st.columns(2)
    with _sc1:
        st.info(f"Pipeline: **{pipeline_sync_date()}**")
    with _sc2:
        st.info(f"Painel Executivo: **{gestao_sync_date()}**")

    st.markdown("""
    Sincroniza **tudo de uma vez**: Pipeline (deals) + Painel Executivo (Receitas, Despesas,
    Fluxo de Caixa, Leads, Extrato C6). Atualização automática toda **segunda-feira às 9:30**.
    """)

    if st.button("🔄 Sincronizar Tudo (Notion)", type="primary", key="btn_sync_all"):
        col_log1, col_log2 = st.columns(2)

        # Sync Pipeline
        with col_log1:
            with st.spinner("Pipeline..."):
                try:
                    sync_result = subprocess.run(
                        [sys.executable, "sync_notion_auto.py"],
                        capture_output=True, text=True,
                        cwd=str(Path(__file__).resolve().parent),
                        timeout=300,
                    )
                    if sync_result.returncode == 0:
                        st.success("Pipeline sincronizado!")
                        with st.expander("Log Pipeline"):
                            st.code(sync_result.stdout)
                    else:
                        st.error("Erro Pipeline:")
                        st.code(sync_result.stderr or sync_result.stdout)
                except Exception as e:
                    st.error(f"Pipeline: {e}")

        # Sync Gestão
        with col_log2:
            with st.spinner("Painel Executivo..."):
                try:
                    sync_gestao()
                    st.success("Painel Executivo sincronizado!")
                except Exception as e:
                    st.error(f"Painel: {e}")

        st.cache_data.clear()
        st.balloons()

    st.markdown(f"""
    <div style="font-size:0.8rem;color:{GRAY};margin-top:0.5rem;">
        <strong>Pipeline:</strong> Notion Pipeline DB → pipeline.json (47 deals)<br>
        <strong>Painel:</strong> 6 DBs Notion → gestao_cache.json (Receitas, Despesas, Fluxo, Leads, Extrato C6)<br>
        <strong>Automático:</strong> Segunda-feira 9:30 via Claude Code
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════
# PIPELINE (Notion)
# ══════════════════════════════════════════
elif page == "Pipeline":
    st.markdown("""<div class="main-header">
        <h1>Pipeline ZYN Capital</h1>
        <p>Operações ativas do Notion — sync semanal automático</p>
    </div>""", unsafe_allow_html=True)

    pipe_df = pipeline_to_df()
    if pipe_df.empty:
        st.warning("Nenhum dado de Pipeline. Execute o sync via /sales ou atualize manualmente.")
        st.stop()

    sync_dt = pipeline_sync_date()
    active = pipe_df[pipe_df["Status"] != "Declinado"]
    declinados = pipe_df[pipe_df["Status"] == "Declinado"]

    share_buttons("ZYN — Pipeline", f"{len(active)} deals ativos\nVolume: {fmt_br(active['Valor'].sum()) if not active.empty else '—'}")

    # KPIs
    vol_ativo = active["Valor"].dropna().sum()
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Deals Ativos", len(active))
    k2.metric("Volume Ativo", fmt(vol_ativo))
    k3.metric("Declinados", len(declinados))
    k4.metric("Sync", sync_dt)

    # --- Deals Ativos ---
    st.markdown("---")
    st.subheader("Operações Ativas")

    # Filters
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        pipe_status_f = st.multiselect("Status", sorted(active["Status"].dropna().unique().tolist()), key="pipe_st")
    with fc2:
        pipe_socio_f = st.multiselect("Sócio", sorted(active["Sócio"].dropna().unique().tolist()), key="pipe_soc")
    with fc3:
        pipe_tipo_f = st.multiselect("Tipo Operação", sorted(active["Tipo"].dropna().unique().tolist()), key="pipe_tipo")

    disp = active.copy()
    if pipe_status_f:
        disp = disp[disp["Status"].isin(pipe_status_f)]
    if pipe_socio_f:
        disp = disp[disp["Sócio"].isin(pipe_socio_f)]
    if pipe_tipo_f:
        disp = disp[disp["Tipo"].isin(pipe_tipo_f)]

    # Table with clickable Notion links
    for _, row in disp.iterrows():
        status_color = {"Quente": "#E53935", "Morno": "#FB8C00", "Frio": "#1E88E5", "TS Assinado - enviado Operações": GREEN}.get(row["Status"], GRAY)
        val_str = fmt(row["Valor"]) if pd.notna(row["Valor"]) else "—"
        analisando_str = ", ".join(row["Analisando"]) if isinstance(row["Analisando"], list) and row["Analisando"] else "—"
        notion_url = row.get("Notion URL", "")

        st.markdown(f"""
        <div style="background:white;border-radius:8px;padding:1rem 1.2rem;margin-bottom:0.6rem;
                    border-left:4px solid {status_color};box-shadow:0 1px 3px rgba(0,0,0,0.04);
                    display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:0.5rem;">
            <div style="flex:1;min-width:200px;">
                <div style="font-weight:600;font-size:1rem;color:{NAVY};">{row['Cliente']}</div>
                <div style="font-size:0.78rem;color:{GRAY};margin-top:0.2rem;">
                    {row['Tipo'] or '—'} · {row['Instrumento'] if pd.notna(row.get('Instrumento')) and row['Instrumento'] else '—'} · {row['Sócio'] if pd.notna(row.get('Sócio')) and row['Sócio'] else '—'} · {row['Originador'] if pd.notna(row.get('Originador')) and row['Originador'] else '—'}
                </div>
            </div>
            <div style="text-align:right;min-width:120px;">
                <div style="font-weight:700;font-size:1.1rem;color:{NAVY};">{val_str}</div>
                <span style="display:inline-block;background:{status_color};color:white;padding:0.15rem 0.5rem;
                       border-radius:4px;font-size:0.7rem;font-weight:500;margin-top:0.2rem;">{row['Status']}</span>
            </div>
            <div style="width:100%;font-size:0.75rem;color:{GRAY};margin-top:0.3rem;">
                <strong>Analisando:</strong> {analisando_str}
                {"&nbsp;&nbsp;|&nbsp;&nbsp;<a href='" + notion_url + "' target='_blank' style='color:" + GREEN + ";text-decoration:none;font-weight:500;'>Abrir no Notion ↗</a>" if notion_url else ""}
            </div>
        </div>""", unsafe_allow_html=True)

    # Charts
    st.markdown("---")
    ch1, ch2 = st.columns(2)
    with ch1:
        st.subheader("Volume por Tipo")
        vol_tipo = active[active["Valor"].notna()].groupby("Tipo")["Valor"].sum().reset_index()
        if not vol_tipo.empty:
            fig_vt = px.pie(vol_tipo, values="Valor", names="Tipo", hole=0.4, color_discrete_sequence=CHART_COLORS)
            fig_vt.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=300, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig_vt, use_container_width=True)

    with ch2:
        st.subheader("Deals por Sócio")
        by_socio = active.groupby("Sócio").agg(deals=("Cliente", "count"), volume=("Valor", "sum")).reset_index()
        if not by_socio.empty:
            fig_soc = px.bar(by_socio, x="Sócio", y="deals", color_discrete_sequence=[NAVY],
                             hover_data=["volume"])
            fig_soc.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=300, margin=dict(l=0,r=20,t=10,b=0))
            st.plotly_chart(fig_soc, use_container_width=True)

    # Investidores mais acionados
    st.markdown("---")
    st.subheader("Investidores Mais Acionados")
    inv_freq = investor_frequency(active)
    if not inv_freq.empty:
        fig_inv = px.bar(inv_freq.head(15), x="Investidor", y="Deals", color_discrete_sequence=[GREEN])
        fig_inv.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=350,
                              xaxis_tickangle=-45, margin=dict(l=0,r=20,t=10,b=100))
        st.plotly_chart(fig_inv, use_container_width=True)
        st.dataframe(inv_freq, use_container_width=True, height=300)

    excel_btn(disp.drop(columns=["Analisando", "Notion URL"], errors="ignore"), "zyn_pipeline.xlsx", key="exp_pipeline")


# ══════════════════════════════════════════
# PIPELINE x INVESTIDORES (Matching CVM + Score de Aderência)
# ══════════════════════════════════════════
elif page == "Pipeline x Investidores":
    st.markdown("""<div class="main-header">
        <h1>Pipeline x Investidores CVM</h1>
        <p>Score de Aderência: ranking de gestoras por compatibilidade com cada deal (volume, ticket, diversificação)</p>
    </div>""", unsafe_allow_html=True)

    pipe_df = active_deals()
    positions = load_positions()

    if pipe_df.empty:
        st.warning("Nenhum deal ativo no Pipeline.")
        st.stop()
    if positions.empty:
        st.warning("Nenhum dado CVM. Atualize primeiro.")
        st.stop()

    matching = match_pipeline_to_cvm(pipe_df, positions)

    if matching.empty:
        st.info("Nenhum matching encontrado.")
        st.stop()

    share_buttons("ZYN — Pipeline x Investidores", "Matching automático Pipeline vs CVM")

    # Filters
    f1, f2, f3 = st.columns(3)
    deal_list = sorted(matching["Deal"].unique().tolist())
    selected_deal = f1.selectbox("Deal", ["Todos"] + deal_list, key="pxi_deal")
    min_score = f2.slider("Score mínimo", 0, 100, 0, key="pxi_score")
    show_only_new = f3.checkbox("Apenas novos (não analisando)", key="pxi_new")

    if selected_deal != "Todos":
        matching = matching[matching["Deal"] == selected_deal]
    if min_score > 0:
        matching = matching[matching["Score"] >= min_score]
    if show_only_new:
        matching = matching[~matching["Já Analisando"]]

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Deals", matching["Deal"].nunique())
    k2.metric("Gestoras Matching", matching["Gestora CVM"].nunique())
    ja_count = matching[matching["Já Analisando"]]["Gestora CVM"].nunique()
    k3.metric("Já Analisando", ja_count)
    novos = matching[~matching["Já Analisando"]]["Gestora CVM"].nunique()
    k4.metric("Novos Targets", novos)

    # Score legend
    st.markdown(f"""
    <div style="display:flex;gap:1.5rem;margin:0.5rem 0 1rem;font-size:0.75rem;color:{GRAY};">
        <span><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#2E7D4F;margin-right:4px;"></span> Score 70+ (Alta aderência)</span>
        <span><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#E6A817;margin-right:4px;"></span> Score 40-69 (Média)</span>
        <span><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#D4526E;margin-right:4px;"></span> Score &lt;40 (Baixa)</span>
        <span>&nbsp;&nbsp;|&nbsp;&nbsp;Volume = posição histórica no tipo · Ticket = média por operação · Fundos = nº de veículos ativos</span>
    </div>""", unsafe_allow_html=True)

    # Results per deal
    for deal_name in matching["Deal"].unique():
        deal_data = matching[matching["Deal"] == deal_name].copy()
        deal_val = deal_data["Valor Deal"].iloc[0]
        deal_tipo = deal_data["Tipo"].iloc[0]
        notion_url = deal_data["Notion URL"].iloc[0] if "Notion URL" in deal_data.columns else ""
        val_str = fmt(deal_val) if pd.notna(deal_val) else "—"
        n_ja = deal_data["Já Analisando"].sum()
        n_novos = len(deal_data) - n_ja
        link_html = f"&nbsp;&nbsp;<a href='{notion_url}' target='_blank' style='color:{GREEN};text-decoration:none;font-weight:500;font-size:0.8rem;'>Notion ↗</a>" if notion_url else ""

        st.markdown(f"""
        <div style="background:white;border-radius:8px;padding:1rem 1.2rem;margin:0.8rem 0 0.3rem;
                    border-left:4px solid {GREEN};box-shadow:0 1px 3px rgba(0,0,0,0.04);">
            <span style="font-weight:700;font-size:1rem;color:{NAVY};">{deal_name}</span>
            <span style="color:{GRAY};font-size:0.85rem;"> — {deal_tipo} — {val_str}</span>
            <span style="color:{GRAY};font-size:0.75rem;margin-left:1rem;">{n_ja} analisando · {n_novos} novos targets</span>
            {link_html}
        </div>""", unsafe_allow_html=True)

        tbl = deal_data[["Gestora CVM", "Score", "Volume Histórico", "Ticket Médio", "Fundos Ativos", "Já Analisando"]].copy()
        tbl["Volume Histórico"] = tbl["Volume Histórico"].apply(fmt)
        tbl["Ticket Médio"] = tbl["Ticket Médio"].apply(fmt)
        tbl["Já Analisando"] = tbl["Já Analisando"].apply(lambda x: "✔ Sim" if x else "—")
        tbl = tbl.sort_values("Score", ascending=False).reset_index(drop=True)
        tbl.index = tbl.index + 1
        tbl.index.name = "#"
        st.dataframe(tbl, use_container_width=True, height=min(len(tbl) * 40 + 50, 500))

    # Export
    st.markdown("---")
    export_match = matching.copy()
    export_match["Valor Deal"] = export_match["Valor Deal"].apply(lambda x: x if pd.notna(x) else 0)
    export_match["Volume Histórico"] = export_match["Volume Histórico"].apply(lambda x: x if pd.notna(x) else 0)
    excel_btn(export_match.drop(columns=["Notion URL"], errors="ignore"), "zyn_pipeline_matching.xlsx", key="exp_pxi")


# ══════════════════════════════════════════
# OPORTUNIDADES (Reverse Origination)
# ══════════════════════════════════════════
elif page == "Oportunidades":
    st.markdown("""<div class="main-header">
        <h1>Oportunidades de Mercado</h1>
        <p>Capacidade estimada x Tipos de ativo — onde há demanda para originação</p>
    </div>""", unsafe_allow_html=True)

    positions = load_positions()
    if positions.empty:
        st.error("Nenhum dado CVM.")
        st.stop()

    share_buttons("ZYN — Oportunidades", "Deals com retorno pendente e alertas")

    # Build fund-level with cash
    fundos_opp = positions.groupby(["cnpj_fundo", "nome_fundo", "gestora"]).agg(
        vol_rf=("vl_posicao", "sum"),
        pl=("pl_fundo", "first"),
        tipos=("tipo_ativo", lambda x: list(x.unique())),
    ).reset_index()
    fundos_opp = fundos_opp[fundos_opp["pl"].notna() & (fundos_opp["pl"] > 0)].copy()
    fundos_opp["pct_rf"] = (fundos_opp["vol_rf"] / fundos_opp["pl"] * 100).round(1)
    # Filtrar fundos com < 5% em RF (não são fundos de crédito)
    fundos_opp = fundos_opp[fundos_opp["pct_rf"] >= 5.0].copy()
    # Capacidade Estimada: target = atual + 10pp, max 80%
    _pct_dec_opp = fundos_opp["pct_rf"] / 100
    _target_opp = (_pct_dec_opp + 0.10).clip(upper=0.80)
    fundos_opp["caixa"] = (fundos_opp["pl"] * _target_opp - fundos_opp["vol_rf"]).clip(lower=0)

    # Aggregate available cash by asset type
    tipo_demand = {}
    for _, row in fundos_opp.iterrows():
        if row["caixa"] > 0:
            for t in row["tipos"]:
                tipo_demand[t] = tipo_demand.get(t, 0) + row["caixa"]

    demand_df = pd.DataFrame([
        {"Tipo Ativo": k, "Capacidade Estimada": v}
        for k, v in sorted(tipo_demand.items(), key=lambda x: -x[1])
    ])

    if not demand_df.empty:
        # KPIs
        total_cash = fundos_opp[fundos_opp["caixa"] > 0]["caixa"].sum()
        k1, k2, k3 = st.columns(3)
        k1.metric("Capacidade Total", fmt(total_cash))
        k2.metric("Fundos com Capacidade", len(fundos_opp[fundos_opp["caixa"] > 0]))
        k3.metric("Gestoras", fundos_opp[fundos_opp["caixa"] > 0]["gestora"].nunique())

        st.markdown("---")
        st.subheader("Demanda por Tipo de Ativo")
        st.markdown("*Capacidade estimada em fundos que já compram cada tipo (fundos com ≥5% alocado em RF)*")

        fig_opp = px.bar(demand_df, x="Tipo Ativo", y="Capacidade Estimada", color_discrete_sequence=[GREEN])
        fig_opp.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", height=400,
                              margin=dict(l=0,r=20,t=10,b=0))
        fig_opp.update_yaxes(tickformat=",.0s", tickprefix="R$ ")
        st.plotly_chart(fig_opp, use_container_width=True)

        demand_disp = demand_df.copy()
        demand_disp["Capacidade Estimada"] = demand_disp["Capacidade Estimada"].apply(fmt)
        st.dataframe(demand_disp, use_container_width=True)

    # Top gestoras com mais capacidade
    st.markdown("---")
    st.subheader("Top Gestoras com Capacidade para Alocação")

    gest_cash = fundos_opp[fundos_opp["caixa"] > 0].groupby("gestora").agg(
        caixa=("caixa", "sum"),
        pl=("pl", "sum"),
        fundos=("cnpj_fundo", "nunique"),
    ).reset_index().sort_values("caixa", ascending=False).head(30)

    if not gest_cash.empty:
        gest_cash["pct"] = (gest_cash["caixa"] / gest_cash["pl"] * 100).round(1)
        gest_disp = gest_cash.copy()
        gest_exp = gest_cash.copy()
        gest_disp["Capac."] = gest_disp["caixa"].apply(fmt)
        gest_disp["PL"] = gest_disp["pl"].apply(fmt)
        gest_disp["% Capac."] = gest_disp["pct"].apply(lambda x: f"{x:.1f}%")
        gest_disp = gest_disp.rename(columns={"gestora": "Gestora", "fundos": "Fundos"})
        st.dataframe(gest_disp[["Gestora", "Capac.", "PL", "% Capac.", "Fundos"]], use_container_width=True, height=400)
        excel_btn(gest_exp.rename(columns={"gestora": "Gestora", "caixa": "Capacidade Est.", "pl": "PL", "pct": "% Capac.", "fundos": "Fundos"}),
                  "zyn_oportunidades.xlsx", key="exp_opp")

    # Pipeline overlay
    pipe_df = active_deals()
    if not pipe_df.empty:
        st.markdown("---")
        st.subheader("Pipeline vs. Caixa de Mercado")
        st.markdown("*Seus deals ativos vs. capacidade estimada no mercado para o mesmo tipo*")

        tipo_map_rev = {"CRI": "CRI", "CRA": "CRA", "Agro": "CPR-F", "DCM": "NC", "CCB": "NC",
                        "Crédito Bancário": "NC", "FIDC": "DEBENTURE", "Equity": "NC", "Cota FIDC": "DEBENTURE"}

        overlay_rows = []
        for _, deal in pipe_df.iterrows():
            tipo_cvm = tipo_map_rev.get(deal["Tipo"], "NC")
            caixa_mercado = tipo_demand.get(tipo_cvm, 0)
            overlay_rows.append({
                "Deal": deal["Cliente"],
                "Tipo": deal["Tipo"],
                "Valor": deal["Valor"],
                "Tipo CVM": tipo_cvm,
                "Caixa Mercado": caixa_mercado,
                "Cobertura": f"{caixa_mercado / deal['Valor']:.0f}x" if pd.notna(deal["Valor"]) and deal["Valor"] > 0 else "—",
            })
        overlay = pd.DataFrame(overlay_rows)
        overlay_disp = overlay.copy()
        overlay_disp["Valor"] = overlay_disp["Valor"].apply(lambda x: fmt(x) if pd.notna(x) else "—")
        overlay_disp["Caixa Mercado"] = overlay_disp["Caixa Mercado"].apply(fmt)
        st.dataframe(overlay_disp[["Deal", "Tipo", "Valor", "Tipo CVM", "Caixa Mercado", "Cobertura"]], use_container_width=True)


# ══════════════════════════════════════════
# ALERTAS
# ══════════════════════════════════════════
elif page == "Alertas":
    st.markdown("""<div class="main-header">
        <h1>Alertas & Follow-ups</h1>
        <p>Retornos pendentes, deals sem ação e movimentações relevantes</p>
    </div>""", unsafe_allow_html=True)

    pipe_df = pipeline_to_df()
    if pipe_df.empty:
        st.warning("Nenhum dado de Pipeline.")
        st.stop()

    share_buttons("ZYN — Alertas", "Alertas de pipeline e retornos vencidos")

    active = pipe_df[pipe_df["Status"] != "Declinado"].copy()
    today = datetime.now().strftime("%Y-%m-%d")

    # --- Retornos vencidos ---
    st.subheader("Retornos Vencidos")
    retornos = active[active["Cobrar Retorno"].notna()].copy()
    retornos["vencido"] = retornos["Cobrar Retorno"] < today
    vencidos = retornos[retornos["vencido"]]

    if not vencidos.empty:
        for _, row in vencidos.iterrows():
            notion_url = row.get("Notion URL", "")
            link = f"<a href='{notion_url}' target='_blank' style='color:{GREEN};text-decoration:none;'>Abrir ↗</a>" if notion_url else ""
            st.markdown(f"""
            <div style="background:#FFF3E0;border-radius:6px;padding:0.8rem 1rem;margin-bottom:0.4rem;
                        border-left:3px solid #E65100;font-size:0.88rem;">
                <strong>{row['Cliente']}</strong> — Retorno era {row['Cobrar Retorno']}
                &nbsp;·&nbsp; {row['Sócio']} &nbsp;·&nbsp; {row['Tipo']}
                &nbsp;&nbsp;{link}
            </div>""", unsafe_allow_html=True)
    else:
        st.success("Nenhum retorno vencido.")

    # --- Retornos próximos (7 dias) ---
    st.markdown("---")
    st.subheader("Retornos nos Próximos 7 Dias")
    prox_7 = datetime.now() + timedelta(days=7)
    prox_7_str = prox_7.strftime("%Y-%m-%d")
    proximos = retornos[(retornos["Cobrar Retorno"] >= today) & (retornos["Cobrar Retorno"] <= prox_7_str)]

    if not proximos.empty:
        for _, row in proximos.iterrows():
            notion_url = row.get("Notion URL", "")
            link = f"<a href='{notion_url}' target='_blank' style='color:{GREEN};text-decoration:none;'>Abrir ↗</a>" if notion_url else ""
            st.markdown(f"""
            <div style="background:#E3F2FD;border-radius:6px;padding:0.8rem 1rem;margin-bottom:0.4rem;
                        border-left:3px solid #1565C0;font-size:0.88rem;">
                <strong>{row['Cliente']}</strong> — Retorno: {row['Cobrar Retorno']}
                &nbsp;·&nbsp; {row['Sócio']} &nbsp;·&nbsp; {row['Tipo']}
                &nbsp;&nbsp;{link}
            </div>""", unsafe_allow_html=True)
    else:
        st.info("Nenhum retorno agendado para os próximos 7 dias.")

    # --- Deals sem investidor analisando ---
    st.markdown("---")
    st.subheader("Deals sem Investidor Definido")
    sem_inv = active[active["Analisando"].apply(lambda x: not x if isinstance(x, list) else True)]
    if not sem_inv.empty:
        for _, row in sem_inv.iterrows():
            notion_url = row.get("Notion URL", "")
            link = f"<a href='{notion_url}' target='_blank' style='color:{GREEN};text-decoration:none;'>Abrir ↗</a>" if notion_url else ""
            val_str = fmt(row["Valor"]) if pd.notna(row["Valor"]) else "—"
            st.markdown(f"""
            <div style="background:#FCE4EC;border-radius:6px;padding:0.8rem 1rem;margin-bottom:0.4rem;
                        border-left:3px solid #C62828;font-size:0.88rem;">
                <strong>{row['Cliente']}</strong> — {row['Tipo']} — {val_str}
                &nbsp;·&nbsp; {row['Sócio']}
                &nbsp;&nbsp;{link}
            </div>""", unsafe_allow_html=True)
    else:
        st.success("Todos os deals ativos têm investidores analisando.")

    # --- Resumo Pipeline ---
    st.markdown("---")
    st.subheader("Resumo do Pipeline")
    status_counts = deals_by_status(pipe_df)
    if status_counts:
        res_data = pd.DataFrame([{"Status": k, "Deals": v} for k, v in status_counts.items()])
        fig_res = px.pie(res_data, values="Deals", names="Status", hole=0.4,
                         color_discrete_sequence=CHART_COLORS)
        fig_res.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                              height=300, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig_res, use_container_width=True)


# ══════════════════════════════════════════
# MERCADO US — Visão Geral
# ══════════════════════════════════════════
elif page == "Visão Geral US":
    st.markdown("""<div class="main-header">
        <h1>🇺🇸 Mercado US — Visão Geral</h1>
        <p>Investidores americanos com exposição ao Brasil — Fonte: SEC EDGAR / N-PORT</p>
    </div>""", unsafe_allow_html=True)

    us_holdings = load_us_holdings(DATA_DIR)
    us_profiles = load_us_profiles(DATA_DIR)

    if us_holdings.empty:
        st.warning("Base US ainda não carregada. Clique abaixo para baixar dados do SEC EDGAR.")
        if st.button("🔄 Baixar dados SEC EDGAR", type="primary", key="us_download"):
            progress = st.progress(0, text="Iniciando download SEC EDGAR...")
            def update_progress(pct, msg):
                progress.progress(pct, text=msg)
            us_holdings, us_profiles = refresh_us_data(
                DATA_DIR, max_managers=50, progress_callback=update_progress
            )
            if not us_holdings.empty:
                st.success(f"✅ {len(us_profiles)} gestoras US com exposição Brasil identificadas!")
                st.rerun()
            else:
                st.error("Nenhum dado encontrado. Verifique conexão com SEC EDGAR.")
    else:
        summary = us_market_summary(us_holdings, us_profiles)

        # KPI cards
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Gestoras US", f"{summary['total_managers']}")
        with c2:
            st.metric("Posições Brasil", f"{summary['total_positions']:,}")
        with c3:
            vol = summary['total_volume_usd']
            if vol >= 1e9:
                st.metric("Volume Total", f"US$ {vol/1e9:.1f}B")
            else:
                st.metric("Volume Total", f"US$ {vol/1e6:.0f}M")
        with c4:
            corp_pct = summary['vol_corporate'] / max(summary['total_volume_usd'], 1) * 100
            st.metric("% Corporativo", f"{corp_pct:.0f}%")

        st.markdown("---")

        # Two columns: top managers + top issuers
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top Gestoras US — Exposição Brasil")
            if us_profiles is not None and not us_profiles.empty:
                top10 = us_profiles.nlargest(10, "Vol. Brasil (USD)").copy()
                top10["Vol. Brasil (USD)"] = top10["Vol. Brasil (USD)"].apply(
                    lambda x: f"US$ {x/1e6:.0f}M" if x < 1e9 else f"US$ {x/1e9:.1f}B"
                )
                st.dataframe(
                    top10[["Manager", "Vol. Brasil (USD)", "% Corporativo", "Nº Posições BR"]],
                    use_container_width=True, hide_index=True,
                )

        with col2:
            st.subheader("Top Emissores Brasileiros (por volume US)")
            if summary["top_issuers"]:
                issuers_df = pd.DataFrame(
                    [{"Emissor": k[:50], "Volume USD": v} for k, v in summary["top_issuers"].items()]
                )
                fig = px.bar(
                    issuers_df, x="Volume USD", y="Emissor", orientation="h",
                    color_discrete_sequence=["#2E7D4F"],
                )
                fig.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    height=400, margin=dict(l=0, r=0, t=10, b=0),
                    yaxis=dict(autorange="reversed"),
                    xaxis_title="Volume (USD)",
                )
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Sovereign vs Corporate breakdown
        st.subheader("Soberano vs Corporativo")
        sov_corp = pd.DataFrame([
            {"Tipo": "Soberano", "Volume": summary["vol_sovereign"]},
            {"Tipo": "Corporativo", "Volume": summary["vol_corporate"]},
        ])
        fig_pie = px.pie(
            sov_corp, values="Volume", names="Tipo",
            color_discrete_sequence=["#223040", "#2E7D4F"],
            hole=0.4,
        )
        fig_pie.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig_pie, use_container_width=True)

        # Refresh button
        st.markdown("---")
        col_r1, col_r2 = st.columns([3, 1])
        with col_r2:
            if st.button("🔄 Atualizar SEC", key="us_refresh"):
                progress = st.progress(0, text="Atualizando...")
                def update_progress(pct, msg):
                    progress.progress(pct, text=msg)
                us_holdings, us_profiles = refresh_us_data(
                    DATA_DIR, max_managers=50, progress_callback=update_progress
                )
                st.success("Dados atualizados!")
                st.rerun()


# ══════════════════════════════════════════
# MERCADO US — Fund Managers
# ══════════════════════════════════════════
elif page == "Fund Managers":
    st.markdown("""<div class="main-header">
        <h1>🏦 Fund Managers</h1>
        <p>Gestoras americanas com posições em ativos brasileiros</p>
    </div>""", unsafe_allow_html=True)

    us_profiles = load_us_profiles(DATA_DIR)

    if us_profiles.empty:
        st.info("Base US não carregada. Vá em **Visão Geral US** para baixar dados.")
    else:
        # Search
        search = st.text_input("🔍 Buscar gestora", "", key="us_mgr_search")
        if search:
            us_profiles = us_profiles[
                us_profiles["Manager"].str.contains(search, case=False, na=False)
            ]

        st.caption(f"{len(us_profiles)} gestoras encontradas")

        # Format for display
        display_df = us_profiles.copy()
        display_df["Vol. Brasil"] = display_df["Vol. Brasil (USD)"].apply(
            lambda x: f"US$ {x/1e6:.0f}M" if x < 1e9 else f"US$ {x/1e9:.1f}B"
        )
        display_df["Vol. Corporativo"] = display_df["Vol. Corporativo (USD)"].apply(
            lambda x: f"US$ {x/1e6:.0f}M" if x < 1e9 else f"US$ {x/1e9:.1f}B"
        )

        st.dataframe(
            display_df[["Manager", "Vol. Brasil", "Vol. Corporativo",
                        "% Corporativo", "Nº Fundos", "Nº Posições BR",
                        "Prazo Médio (anos)", "Top Emissores BR", "Filing Date"]],
            use_container_width=True, hide_index=True, height=600,
        )

        # Export
        excel_btn(us_profiles, "zyn_us_fund_managers.xlsx", key="us_mgr_export")

        # Detail view
        st.markdown("---")
        st.subheader("Detalhe por Gestora")
        selected = st.selectbox(
            "Selecione uma gestora", us_profiles["Manager"].tolist(), key="us_mgr_detail"
        )
        if selected:
            mgr = us_profiles[us_profiles["Manager"] == selected].iloc[0]
            c1, c2, c3 = st.columns(3)
            with c1:
                vol = mgr["Vol. Brasil (USD)"]
                st.metric("Volume Brasil", f"US$ {vol/1e6:.0f}M" if vol < 1e9 else f"US$ {vol/1e9:.1f}B")
            with c2:
                st.metric("% Corporativo", f"{mgr['% Corporativo']}%")
            with c3:
                st.metric("Posições", f"{mgr['Nº Posições BR']}")

            # Holdings detail
            us_holdings = load_us_holdings(DATA_DIR)
            if not us_holdings.empty:
                mgr_holdings = us_holdings[us_holdings["manager"] == selected].copy()
                if not mgr_holdings.empty:
                    mgr_holdings["Volume"] = mgr_holdings["val_usd"].apply(
                        lambda x: f"US$ {x/1e6:.1f}M" if abs(x) >= 1e6 else f"US$ {x/1e3:.0f}K"
                    )
                    st.dataframe(
                        mgr_holdings[["name", "title", "Volume", "asset_cat",
                                      "isin", "maturity", "currency"]].rename(columns={
                            "name": "Emissor", "title": "Título", "asset_cat": "Tipo",
                            "isin": "ISIN", "maturity": "Vencimento", "currency": "Moeda",
                        }),
                        use_container_width=True, hide_index=True,
                    )


# ══════════════════════════════════════════
# MERCADO US — Holdings Brasil
# ══════════════════════════════════════════
elif page == "Holdings Brasil":
    st.markdown("""<div class="main-header">
        <h1>📊 Holdings Brasil</h1>
        <p>Todas as posições em ativos brasileiros detidas por fundos americanos</p>
    </div>""", unsafe_allow_html=True)

    us_holdings = load_us_holdings(DATA_DIR)

    if us_holdings.empty:
        st.info("Base US não carregada. Vá em **Visão Geral US** para baixar dados.")
    else:
        # Filters
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            managers = ["Todos"] + sorted(us_holdings["manager"].unique().tolist())
            sel_mgr = st.selectbox("Gestora", managers, key="us_hold_mgr")
        with col_f2:
            asset_cats = ["Todos"] + sorted(us_holdings["asset_cat"].dropna().unique().tolist())
            sel_cat = st.selectbox("Tipo de Ativo", asset_cats, key="us_hold_cat")
        with col_f3:
            min_val = st.number_input("Volume mínimo (USD M)", value=0.0, step=1.0, key="us_hold_min")

        filtered = us_holdings.copy()
        if sel_mgr != "Todos":
            filtered = filtered[filtered["manager"] == sel_mgr]
        if sel_cat != "Todos":
            filtered = filtered[filtered["asset_cat"] == sel_cat]
        if min_val > 0:
            filtered = filtered[filtered["val_usd"] >= min_val * 1e6]

        st.caption(f"{len(filtered):,} posições | Volume: US$ {filtered['val_usd'].sum()/1e6:,.0f}M")

        # Display
        display = filtered.copy()
        display["Volume USD"] = display["val_usd"].apply(
            lambda x: f"US$ {x/1e6:.1f}M" if abs(x) >= 1e6 else f"US$ {x/1e3:.0f}K"
        )
        display["% Fundo"] = display["pct_val"].apply(lambda x: f"{x:.2f}%")

        st.dataframe(
            display[["manager", "fund_name", "name", "title", "Volume USD",
                     "% Fundo", "asset_cat", "isin", "cusip", "maturity",
                     "currency"]].rename(columns={
                "manager": "Gestora", "fund_name": "Fundo", "name": "Emissor",
                "title": "Título", "asset_cat": "Tipo", "isin": "ISIN",
                "cusip": "CUSIP", "maturity": "Vencimento", "currency": "Moeda",
            }),
            use_container_width=True, hide_index=True, height=600,
        )

        # Top emitters chart
        st.markdown("---")
        st.subheader("Volume por Emissor")
        top_emit = (
            filtered.groupby("name")["val_usd"]
            .sum()
            .sort_values(ascending=False)
            .head(20)
            .reset_index()
        )
        top_emit.columns = ["Emissor", "Volume"]
        top_emit["Emissor"] = top_emit["Emissor"].str[:40]

        fig = px.bar(
            top_emit, x="Volume", y="Emissor", orientation="h",
            color_discrete_sequence=["#223040"],
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            height=500, margin=dict(l=0, r=0, t=10, b=0),
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig, use_container_width=True)

        excel_btn(filtered, "zyn_us_holdings_brasil.xlsx", key="us_hold_export")


# ══════════════════════════════════════════
# MERCADO US — Matching US
# ══════════════════════════════════════════
elif page == "Matching US":
    st.markdown("""<div class="main-header">
        <h1>🎯 Matching US</h1>
        <p>Cruze operações do Pipeline ZYN com investidores americanos</p>
    </div>""", unsafe_allow_html=True)

    us_profiles = load_us_profiles(DATA_DIR)
    us_holdings = load_us_holdings(DATA_DIR)

    if us_profiles.empty:
        st.info("Base US não carregada. Vá em **Visão Geral US** para baixar dados.")
    else:
        # Deal input
        st.subheader("Parâmetros da Operação")
        col1, col2, col3 = st.columns(3)
        with col1:
            deal_issuer = st.text_input("Emissor / Devedor", "", key="us_match_issuer")
        with col2:
            deal_type = st.selectbox("Tipo", ["CRI", "CRA", "Debênture", "NC", "CPR-F"], key="us_match_type")
        with col3:
            deal_amount = st.number_input("Volume (US$ M)", value=10.0, step=1.0, key="us_match_amount")

        # Pipeline integration
        st.markdown("---")
        st.subheader("Ou selecione do Pipeline")
        try:
            pipeline = pipeline_to_df()
            if not pipeline.empty and "Nome" in pipeline.columns:
                deal_options = ["(Manual)"] + pipeline["Nome"].dropna().tolist()
                sel_deal = st.selectbox("Operação do Pipeline", deal_options, key="us_match_pipeline")
                if sel_deal != "(Manual)":
                    deal_row = pipeline[pipeline["Nome"] == sel_deal].iloc[0]
                    deal_issuer = str(deal_row.get("Nome", ""))
                    deal_type = str(deal_row.get("Produto", "Debênture"))
                    vol = deal_row.get("Volume", 0)
                    try:
                        deal_amount = float(vol) / 5.0 / 1e6  # BRL to USD rough
                    except (ValueError, TypeError):
                        pass
        except Exception:
            pass

        if st.button("🎯 Calcular Matching", type="primary", key="us_match_go"):
            results = match_us_investors_to_deal(
                us_profiles, us_holdings,
                deal_type=deal_type,
                deal_amount_usd=deal_amount * 1e6,
                deal_issuer=deal_issuer,
            )

            if results.empty:
                st.warning("Nenhum match encontrado.")
            else:
                st.success(f"✅ {len(results)} gestoras US ranqueadas")

                # Format
                display = results.copy()
                display["Vol. Brasil"] = display["Vol. Brasil (USD)"].apply(
                    lambda x: f"US$ {x/1e6:.0f}M" if x < 1e9 else f"US$ {x/1e9:.1f}B"
                )
                display["Score"] = display["Match Score"].apply(lambda x: f"{x:.0f}")

                st.dataframe(
                    display[["Manager", "Vol. Brasil", "% Corporativo",
                             "Nº Posições BR", "Score", "Top Emissores BR"]],
                    use_container_width=True, hide_index=True,
                )

                excel_btn(results, "zyn_matching_us.xlsx", key="us_match_export")


# ══════════════════════════════════════════
# COTAÇÕES — Minimalista & Dinâmico
# ══════════════════════════════════════════
elif page == "Cotações":
    from pages.cotacoes import fetch_all_data, fmt, delta_color
    import pandas as pd

    D = fetch_all_data()
    R = D['rates']
    C = D['cambio']
    idx = D['indices']
    COM = D['commodities']
    T = D['treasuries']

    # Helper: só mostra delta se != 0
    def _delta(pct):
        if pct is None or pct == 0:
            return None
        return f"{pct:+.2f}%"

    def _delta_pp(v):
        if v is None or v == 0:
            return None
        return f"{v:+.2f} pp"

    # Unidades curtas para commodities
    _UNIT_SHORT = {
        'USD cents/bushel': '¢/bu', 'USD cents/lb': '¢/lb',
        'USD/bbl': '$/bbl', 'USD/MMBtu': '$/MMBtu',
        'USD/oz troy': '$/oz', 'USD (proxy Vale)': '$',
    }

    # ─── Header compacto ───
    hdr1, hdr2 = st.columns([7, 3])
    with hdr1:
        st.markdown(f"""<div style="display:flex;align-items:baseline;gap:12px;">
            <span style="font-size:22px;font-weight:700;letter-spacing:3px;color:{NAVY};">COTAÇÕES</span>
            <span style="font-size:10px;color:{GRAY};letter-spacing:1px;">{D['timestamp']} · {D['ok']} APIs · {', '.join(D['sources'])}</span>
        </div>""", unsafe_allow_html=True)
    with hdr2:
        if st.button("Atualizar", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    tab_c1, tab_c2, tab_c3, tab_c4 = st.tabs([
        "VISÃO GERAL", "RENDA FIXA", "COMMODITIES", "SPREADS & REF."
    ])

    # ═══ TAB 1 — Visão Geral ═══
    with tab_c1:
        # Taxas — sem unidade no valor para não truncar
        st.caption("TAXAS BÁSICAS · BCB")
        cols = st.columns(6)
        for i, (label, key) in enumerate([
            ('Selic', 'selic'), ('CDI', 'cdi'), ('IPCA 12m', 'ipca_12m'),
            ('IGP-M 12m', 'igpm_12m'), ('TR', 'tr'), ('Poupança', 'poupanca'),
        ]):
            with cols[i]:
                if key in R:
                    v = R[key]['valor']
                    prev = R[key].get('prev')
                    d = round(v - prev, 4) if prev else None
                    st.metric(label, f"{fmt(v)}%", _delta_pp(d),
                              delta_color=delta_color(d) if d else "off")

        # Câmbio — fallback: se AwesomeAPI falhou, usa Yahoo para USD e EUR
        st.caption("CÂMBIO · AwesomeAPI + BCB")
        cols = st.columns(5)
        cambio_display = [
            ('Dólar', 'USDBRL', 'USDBRL=X', 4),
            ('Euro', 'EURBRL', 'EURBRL=X', 4),
            ('Libra', 'GBPBRL', 'GBPBRL=X', 4),
            ('PTAX', None, None, 4),
            ('Bitcoin', 'BTCUSD', 'BTC-USD', 0),
        ]
        for i, (label, awesome_key, yahoo_key, dec) in enumerate(cambio_display):
            with cols[i]:
                if awesome_key and awesome_key in C:
                    bid = C[awesome_key]['bid']
                    pct = C[awesome_key]['pct']
                    prefix = '$ ' if awesome_key == 'BTCUSD' else 'R$ '
                    st.metric(label, f"{prefix}{fmt(bid, dec)}", _delta(pct),
                              delta_color=delta_color(pct) if pct else "off")
                elif label == 'PTAX' and 'ptax' in R:
                    st.metric(label, f"R$ {fmt(R['ptax']['valor'], 4)}")
                    st.caption(f"Ref.: {R['ptax']['data']}")
                elif yahoo_key and yahoo_key.replace('=X', '').lower() in idx:
                    # Yahoo fallback
                    d = idx[yahoo_key.replace('=X', '').lower()]
                    st.metric(label, f"R$ {fmt(d['price'], dec)}", _delta(d['pct']),
                              delta_color=delta_color(d['pct']) if d['pct'] else "off")
                else:
                    st.metric(label, "—")

        # Índices
        st.caption("ÍNDICES · Yahoo Finance + BCB")
        cols = st.columns(5)
        for i, (label, key, dec) in enumerate([
            ('Ibovespa', 'ibovespa', 0), ('S&P 500', 'sp500', 0),
            ('IFIX', 'ifix', 0), ('DXY', 'dxy', 2), ('IMA-B', 'imab', 0),
        ]):
            with cols[i]:
                if key == 'imab' and key in R:
                    v = R[key]['valor']
                    prev = R[key].get('prev')
                    pct = round(((v / prev) - 1) * 100, 2) if prev else 0
                    st.metric(label, f"{fmt(v, dec)}", _delta(pct),
                              delta_color=delta_color(pct) if pct else "off")
                elif key in idx:
                    d = idx[key]
                    st.metric(label, f"{fmt(d['price'], dec)}", _delta(d['pct']),
                              delta_color=delta_color(d['pct']) if d['pct'] else "off")
                else:
                    st.metric(label, "—")

    # ═══ TAB 2 — Renda Fixa ═══
    with tab_c2:
        st.caption("TESOURO DIRETO · Referência")
        tesouro_rows = []
        for b in D['tesouro']:
            tc = f"Selic + {fmt(b['taxa_compra'], 4)}%" if b['tipo'] == 'Selic' else f"{fmt(b['taxa_compra'])}%"
            tv = f"Selic + {fmt(b['taxa_venda'], 4)}%" if b['tipo'] == 'Selic' else f"{fmt(b['taxa_venda'])}%"
            tesouro_rows.append({'Título': b['nome'], 'Venc.': b['vencimento'],
                                 'Compra': tc, 'Venda': tv,
                                 'PU Compra': f"R$ {fmt(b['pu_compra'])}", 'PU Venda': f"R$ {fmt(b['pu_venda'])}"})
        st.dataframe(pd.DataFrame(tesouro_rows), use_container_width=True, hide_index=True)

        focus = D['focus']
        if any(bool(v) for v in focus.values()):
            st.caption("EXPECTATIVAS FOCUS · BCB")
            focus_rows = []
            for ind, anos in focus.items():
                row = {'Indicador': ind}
                for yr in ['2025', '2026', '2027', '2028']:
                    v = anos.get(yr)
                    row[yr] = f"{fmt(v)}" if v is not None else "—"
                focus_rows.append(row)
            st.dataframe(pd.DataFrame(focus_rows), use_container_width=True, hide_index=True)

    # ═══ TAB 3 — Commodities ═══
    with tab_c3:
        def _show_com(label, key, cols_ref, col_idx):
            with cols_ref[col_idx]:
                if key in COM:
                    d = COM[key]
                    u = _UNIT_SHORT.get(d['unit'], d['unit'])
                    st.metric(label, f"{fmt(d['valor'])} {u}", _delta(d['pct']),
                              delta_color=delta_color(d['pct']) if d['pct'] else "off")
                else:
                    st.metric(label, "—")

        st.caption("AGRO · Yahoo Finance")
        cols = st.columns(4)
        for i, (lb, k) in enumerate([('Soja', 'soja'), ('Milho', 'milho'), ('Café', 'cafe'), ('Açúcar', 'acucar')]):
            _show_com(lb, k, cols, i)
        cols = st.columns(4)
        for i, (lb, k) in enumerate([('Algodão', 'algodao'), ('Trigo', 'trigo'), ('Boi Gordo', 'boi_gordo')]):
            _show_com(lb, k, cols, i)

        st.caption("ENERGIA")
        cols = st.columns(3)
        for i, (lb, k) in enumerate([('Brent', 'petroleo_brent'), ('WTI', 'petroleo_wti'), ('Gás Natural', 'gas_natural')]):
            _show_com(lb, k, cols, i)

        st.caption("METAIS")
        cols = st.columns(3)
        for i, (lb, k) in enumerate([('Ouro', 'ouro'), ('Prata', 'prata'), ('Ferro', 'ferro')]):
            _show_com(lb, k, cols, i)

    # ═══ TAB 4 — Spreads & Ref. Internacional ═══
    with tab_c4:
        selic = R.get('selic', {}).get('valor', 14.25)
        st.caption(f"SPREADS CRÉDITO ESTRUTURADO · Selic {fmt(selic)}%")
        spreads_data = [
            ['CRA Senior', 'AAA', '3-5a', 'CDI+1,5~2,5%', f'{fmt(selic+1.5)}~{fmt(selic+2.5)}%'],
            ['CRA Mezanino', 'AA/A', '3-5a', 'CDI+3,0~5,0%', f'{fmt(selic+3)}~{fmt(selic+5)}%'],
            ['CRI Senior', 'AAA', '5-8a', 'IPCA+7,0~8,5%', 'IPCA+7,0~8,5%'],
            ['Deb. Infra', 'AAA', '7-10a', 'IPCA+6,5~8,0%', 'IPCA+6,5~8,0%'],
            ['FIDC Senior', 'AAA', '2-3a', 'CDI+1,8~3,0%', f'{fmt(selic+1.8)}~{fmt(selic+3)}%'],
            ['FIDC Mezanino', 'A/BBB', '2-3a', 'CDI+4,0~7,0%', f'{fmt(selic+4)}~{fmt(selic+7)}%'],
            ['FIDC Sub', 'NR', '2-3a', 'CDI+8,0~15,0%', f'{fmt(selic+8)}~{fmt(selic+15)}%'],
            ['S&LB Rural', 'N/A', '3-7a', 'CDI+3,0~5,5%', f'{fmt(selic+3)}~{fmt(selic+5.5)}%'],
            ['NCom/CCB', 'Varia', '1-3a', 'CDI+2,0~6,0%', f'{fmt(selic+2)}~{fmt(selic+6)}%'],
        ]
        st.dataframe(pd.DataFrame(spreads_data, columns=['Instrumento', 'Rating', 'Prazo', 'Spread', 'All-In']),
                      use_container_width=True, hide_index=True)

        st.caption("REFERÊNCIA INTERNACIONAL · Yahoo Finance + BCB")
        cols = st.columns(5)
        for i, (label, key) in enumerate([
            ('UST 2Y', 'ust_2y'), ('UST 5Y', 'ust_5y'), ('UST 10Y', 'ust_10y'),
            ('UST 30Y', 'ust_30y'), ('CDS BR', 'cds_br'),
        ]):
            with cols[i]:
                if key in T:
                    d = T[key]
                    val = fmt(d['rate'], 0) if key == 'cds_br' else f"{fmt(d['rate'], 3)}%"
                    lbl_unit = ' bps' if key == 'cds_br' else ''
                    pct = d.get('pct', 0)
                    st.metric(label, f"{val}{lbl_unit}", _delta(pct) if d.get('source') == 'live' else None,
                              delta_color=delta_color(pct) if pct else "off")
