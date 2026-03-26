#!/usr/bin/env python3
"""
ZYN Capital — Painel de Cotações (Streamlit)
Dados ao vivo: BCB, AwesomeAPI, Yahoo Finance, BCB Focus
"""
import streamlit as st
import requests
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ─── Page config ───
st.set_page_config(
    page_title="ZYN — Cotações",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── ZYN CSS ───
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;500;600;700&display=swap');
html, body, [class*="st-"] { font-family: 'Montserrat', sans-serif; }
.block-container { padding-top: 1rem; max-width: 1400px; }
header[data-testid="stHeader"] { background: #223040; }
.stTabs [data-baseweb="tab-list"] { gap: 8px; }
.stTabs [data-baseweb="tab"] {
    font-family: 'Montserrat', sans-serif;
    font-size: 11px; font-weight: 600;
    letter-spacing: 1px; text-transform: uppercase;
    color: #8B9197; padding: 8px 16px;
}
.stTabs [aria-selected="true"] { color: #2E7D4F !important; border-bottom-color: #2E7D4F !important; }
div[data-testid="stMetricValue"] { font-size: 1.5rem; font-weight: 700; }
div[data-testid="stMetricDelta"] > div { font-size: 0.75rem; }
.zyn-header {
    background: #223040; color: white; padding: 12px 24px;
    display: flex; align-items: center; justify-content: space-between;
    border-radius: 8px; margin-bottom: 16px;
}
.zyn-header .logo { font-size: 20px; font-weight: 700; letter-spacing: 3px; }
.zyn-header .sub { font-size: 11px; color: #8B9197; letter-spacing: 1px; margin-left: 16px; }
.zyn-header .ts { font-size: 10px; color: #8B9197; text-align: right; }
.zyn-header .ts b { color: #38a863; }
.section-label {
    font-size: 10px; font-weight: 600; color: #8B9197;
    text-transform: uppercase; letter-spacing: 2px;
    margin: 16px 0 8px 0; padding-left: 2px;
}
.src-tag { font-weight: 400; font-size: 9px; color: #b0b5ba; letter-spacing: 0; margin-left: 6px; }
</style>
""", unsafe_allow_html=True)

# ─── Session state for auto-refresh ───
if 'last_fetch' not in st.session_state:
    st.session_state.last_fetch = 0
    st.session_state.data = None
if 'auto_tab' not in st.session_state:
    st.session_state.auto_tab = 0

# ─── API Functions ───
SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept': 'application/json',
})

def fetch(url, timeout=12):
    try:
        r = SESSION.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return None

def bcb_serie(serie, n=2):
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados/ultimos/{n}?formato=json"
    return fetch(url)

def bcb_focus(indicator, year):
    import urllib.parse
    ind_enc = urllib.parse.quote(indicator)
    url = (
        f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
        f"ExpectativasMercadoAnuais?$top=1"
        f"&$filter=Indicador%20eq%20'{ind_enc}'"
        f"%20and%20DataReferencia%20eq%20'{year}'"
        f"&$orderby=Data%20desc&$format=json"
    )
    data = fetch(url)
    if data and data.get('value') and len(data['value']) > 0:
        return data['value'][0].get('Mediana')
    return None

def yahoo_quote(ticker, timeout=12):
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=2d'
    data = fetch(url, timeout)
    if data and 'chart' in data:
        try:
            meta = data['chart']['result'][0]['meta']
            price = meta['regularMarketPrice']
            prev = meta.get('previousClose', price)
            pct = ((price / prev) - 1) * 100 if prev else 0
            ts = meta.get('regularMarketTime', 0)
            dt_str = datetime.fromtimestamp(ts).strftime('%d/%m/%Y %H:%M') if ts else ''
            return {'price': round(price, 4), 'prev': round(prev, 4), 'pct': round(pct, 2), 'date': dt_str}
        except:
            pass
    return None


@st.cache_data(ttl=90)
def fetch_all_data():
    """Fetch all market data in parallel."""
    result = {
        'timestamp': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        'rates': {}, 'cambio': {}, 'indices': {}, 'commodities': {},
        'focus': {}, 'treasuries': {}, 'tesouro': [],
        'ok': 0, 'err': 0, 'sources': set(),
    }
    lock = threading.Lock()

    def inc_ok(src='BCB'):
        with lock:
            result['ok'] += 1
            result['sources'].add(src)

    def inc_err():
        with lock:
            result['err'] += 1

    # ─── BCB Rates ───
    def task_bcb(name, serie):
        try:
            d = bcb_serie(serie)
            if d:
                last = d[-1]
                prev = d[-2] if len(d) > 1 else None
                with lock:
                    result['rates'][name] = {
                        'valor': float(last['valor']),
                        'data': last['data'],
                        'prev': float(prev['valor']) if prev else None,
                    }
                inc_ok()
            else:
                inc_err()
        except:
            inc_err()

    # ─── IGP-M 12m ───
    def task_igpm():
        try:
            d = bcb_serie(189, 13)
            if d and len(d) >= 12:
                acc = 1
                for x in d[-12:]:
                    acc *= (1 + float(x['valor']) / 100)
                with lock:
                    result['rates']['igpm_12m'] = {
                        'valor': round((acc - 1) * 100, 4),
                        'data': d[-1]['data'],
                        'ultimo_mes': float(d[-1]['valor']),
                    }
                inc_ok()
        except:
            inc_err()

    # ─── Cambio ───
    def task_cambio():
        try:
            data = fetch("https://economia.awesomeapi.com.br/json/last/USD-BRL,EUR-BRL,BTC-USD,GBP-BRL")
            if data:
                with lock:
                    for key, val in data.items():
                        result['cambio'][key] = {
                            'bid': float(val['bid']),
                            'ask': float(val['ask']),
                            'high': float(val['high']),
                            'low': float(val['low']),
                            'pct': float(val['pctChange']),
                            'name': val.get('name', ''),
                        }
                inc_ok('AwesomeAPI')
        except:
            inc_err()

    # ─── Focus ───
    def task_focus(ind, yr):
        try:
            val = bcb_focus(ind, yr)
            if val is not None:
                with lock:
                    if ind not in result['focus']:
                        result['focus'][ind] = {}
                    result['focus'][ind][yr] = val
                return True
        except:
            pass
        return False

    # ─── Yahoo commodity ───
    def task_yahoo_commodity(name, ticker, unit, dec=2):
        try:
            d = yahoo_quote(ticker)
            if d:
                with lock:
                    result['commodities'][name] = {
                        'valor': round(d['price'], dec),
                        'prev': round(d['prev'], dec),
                        'pct': d['pct'],
                        'unit': unit,
                        'date': d['date'],
                    }
                inc_ok('Yahoo Finance')
        except:
            inc_err()

    # ─── Yahoo index ───
    def task_yahoo_index(name, ticker):
        try:
            d = yahoo_quote(ticker)
            if d:
                with lock:
                    result['indices'][name] = {
                        'price': round(d['price'], 2),
                        'pct': d['pct'],
                        'prev': round(d['prev'], 2),
                    }
                inc_ok('Yahoo Finance')
        except:
            inc_err()

    # ─── Yahoo treasury ───
    def task_yahoo_treasury(name, ticker):
        try:
            d = yahoo_quote(ticker)
            if d:
                with lock:
                    result['treasuries'][name] = {
                        'rate': round(d['price'], 3),
                        'pct': d['pct'],
                        'source': 'live',
                    }
                inc_ok('Yahoo Finance')
        except:
            inc_err()

    # ─── CDS ───
    def task_cds():
        try:
            d = bcb_serie(25407)
            if d:
                with lock:
                    result['treasuries']['cds_br'] = {
                        'rate': float(d[-1]['valor']),
                        'data': d[-1]['data'],
                        'prev': float(d[-2]['valor']) if len(d) > 1 else None,
                        'source': 'live',
                    }
                inc_ok()
        except:
            inc_err()

    # ═══ Run all in parallel ═══
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = []

        # BCB rates
        for name, serie in {'selic': 432, 'cdi': 4389, 'ipca_12m': 13522,
                            'tr': 226, 'poupanca': 195, 'ptax': 1, 'imab': 12466}.items():
            futures.append(pool.submit(task_bcb, name, serie))

        futures.append(pool.submit(task_igpm))
        futures.append(pool.submit(task_cambio))

        # Focus
        for ind in ['IPCA', 'IGP-M', 'Selic', 'Câmbio', 'PIB Total']:
            result['focus'][ind] = {}
            for yr in ['2025', '2026', '2027', '2028']:
                futures.append(pool.submit(task_focus, ind, yr))

        # Commodities
        yahoo_commodities = {
            'soja': ('ZS=F', 'USD cents/bushel', 2),
            'milho': ('ZC=F', 'USD cents/bushel', 2),
            'cafe': ('KC=F', 'USD cents/lb', 2),
            'acucar': ('SB=F', 'USD cents/lb', 2),
            'algodao': ('CT=F', 'USD cents/lb', 2),
            'trigo': ('ZW=F', 'USD cents/bushel', 2),
            'boi_gordo': ('LE=F', 'USD cents/lb', 2),
            'petroleo_brent': ('BZ=F', 'USD/bbl', 2),
            'petroleo_wti': ('CL=F', 'USD/bbl', 2),
            'gas_natural': ('NG=F', 'USD/MMBtu', 3),
            'ouro': ('GC=F', 'USD/oz troy', 0),
            'prata': ('SI=F', 'USD/oz troy', 2),
            'ferro': ('VALE', 'USD (proxy Vale)', 2),
        }
        for name, (ticker, unit, dec) in yahoo_commodities.items():
            futures.append(pool.submit(task_yahoo_commodity, name, ticker, unit, dec))

        # Indices
        for name, ticker in {'ibovespa': '^BVSP', 'sp500': '^GSPC', 'ifix': 'IFIX11.SA', 'dxy': 'DX-Y.NYB'}.items():
            futures.append(pool.submit(task_yahoo_index, name, ticker))

        # US Treasuries
        for name, ticker in {'ust_2y': '^IRX', 'ust_5y': '^FVX', 'ust_10y': '^TNX', 'ust_30y': '^TYX'}.items():
            futures.append(pool.submit(task_yahoo_treasury, name, ticker))

        futures.append(pool.submit(task_cds))

        for f in as_completed(futures):
            pass

    # Focus source
    if any(bool(v) for v in result['focus'].values()):
        result['sources'].add('BCB Focus')

    # Tesouro Direto reference data
    result['tesouro'] = [
        {'nome': 'Tesouro Selic 2027', 'vencimento': '2027-03-01', 'taxa_compra': 0.0764, 'taxa_venda': 0.1264, 'pu_compra': 14556.38, 'pu_venda': 14512.15, 'tipo': 'Selic'},
        {'nome': 'Tesouro Selic 2029', 'vencimento': '2029-03-01', 'taxa_compra': 0.1410, 'taxa_venda': 0.1910, 'pu_compra': 14398.82, 'pu_venda': 14342.05, 'tipo': 'Selic'},
        {'nome': 'Tesouro IPCA+ 2029', 'vencimento': '2029-05-15', 'taxa_compra': 7.42, 'taxa_venda': 7.62, 'pu_compra': 3198.45, 'pu_venda': 3172.31, 'tipo': 'IPCA+'},
        {'nome': 'Tesouro IPCA+ 2032', 'vencimento': '2032-08-15', 'taxa_compra': 7.05, 'taxa_venda': 7.25, 'pu_compra': 4312.88, 'pu_venda': 4276.15, 'tipo': 'IPCA+'},
        {'nome': 'Tesouro IPCA+ 2035', 'vencimento': '2035-05-15', 'taxa_compra': 6.92, 'taxa_venda': 7.12, 'pu_compra': 2245.67, 'pu_venda': 2218.34, 'tipo': 'IPCA+'},
        {'nome': 'Tesouro IPCA+ 2040', 'vencimento': '2040-08-15', 'taxa_compra': 6.78, 'taxa_venda': 6.98, 'pu_compra': 1578.23, 'pu_venda': 1552.40, 'tipo': 'IPCA+'},
        {'nome': 'Tesouro IPCA+ 2045', 'vencimento': '2045-05-15', 'taxa_compra': 6.68, 'taxa_venda': 6.88, 'pu_compra': 1298.12, 'pu_venda': 1275.40, 'tipo': 'IPCA+'},
        {'nome': 'Tesouro IPCA+ c/ Juros 2032', 'vencimento': '2032-08-15', 'taxa_compra': 7.05, 'taxa_venda': 7.25, 'pu_compra': 4312.88, 'pu_venda': 4276.15, 'tipo': 'IPCA+ Juros'},
        {'nome': 'Tesouro IPCA+ c/ Juros 2040', 'vencimento': '2040-08-15', 'taxa_compra': 6.78, 'taxa_venda': 6.98, 'pu_compra': 4125.67, 'pu_venda': 4089.23, 'tipo': 'IPCA+ Juros'},
        {'nome': 'Tesouro IPCA+ c/ Juros 2055', 'vencimento': '2055-05-15', 'taxa_compra': 6.58, 'taxa_venda': 6.78, 'pu_compra': 3898.75, 'pu_venda': 3862.41, 'tipo': 'IPCA+ Juros'},
        {'nome': 'Tesouro Prefixado 2028', 'vencimento': '2028-01-01', 'taxa_compra': 14.20, 'taxa_venda': 14.45, 'pu_compra': 742.85, 'pu_venda': 738.12, 'tipo': 'Pre'},
        {'nome': 'Tesouro Prefixado 2031', 'vencimento': '2031-01-01', 'taxa_compra': 13.65, 'taxa_venda': 13.90, 'pu_compra': 502.18, 'pu_venda': 497.45, 'tipo': 'Pre'},
        {'nome': 'Tesouro Prefixado c/ Juros 2035', 'vencimento': '2035-01-01', 'taxa_compra': 13.20, 'taxa_venda': 13.45, 'pu_compra': 812.47, 'pu_venda': 806.23, 'tipo': 'Pre Juros'},
    ]

    # Fallbacks
    for k, v in {'ust_2y': 3.95, 'ust_5y': 4.02, 'ust_10y': 4.25, 'ust_30y': 4.68}.items():
        if k not in result['treasuries']:
            result['treasuries'][k] = {'rate': v, 'source': 'ref', 'pct': 0}

    result['sources'] = list(result['sources'])
    return result


def fmt(v, d=2):
    return f"{v:,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def delta_color(v):
    return "normal" if v >= 0 else "inverse"


# ─── FETCH DATA ───
D = fetch_all_data()

# ─── Header ───
st.markdown(f"""
<div class="zyn-header">
    <div style="display:flex;align-items:center;">
        <span class="logo">ZYN</span>
        <span class="sub">PAINEL DE COTAÇÕES</span>
    </div>
    <div class="ts">
        <b>● AO VIVO</b><br>
        {D['timestamp']} — {D['ok']} APIs OK | {', '.join(D['sources'])}
    </div>
</div>
""", unsafe_allow_html=True)

# ─── Navigation ───
col_nav1, col_nav2 = st.columns([8, 2])
with col_nav2:
    if st.button("🔄 Atualizar", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ─── Tabs ───
tab1, tab2, tab3, tab4 = st.tabs([
    "VISÃO GERAL", "RENDA FIXA & CURVAS", "COMMODITIES", "SPREADS & REF. INT."
])

# ═══════════════════════════════════════════
# TAB 1 — Visão Geral
# ═══════════════════════════════════════════
with tab1:
    R = D['rates']
    C = D['cambio']
    idx = D['indices']

    # Taxas Básicas
    st.markdown('<div class="section-label">Taxas Básicas <span class="src-tag">Banco Central do Brasil — ao vivo</span></div>', unsafe_allow_html=True)
    cols = st.columns(6)
    rate_items = [
        ('Selic Meta', 'selic', '% a.a.'),
        ('CDI Anualizado', 'cdi', '% a.a.'),
        ('IPCA 12m', 'ipca_12m', '%'),
        ('IGP-M 12m', 'igpm_12m', '%'),
        ('TR Mensal', 'tr', '%'),
        ('Poupança', 'poupanca', '% a.a.'),
    ]
    for i, (label, key, unit) in enumerate(rate_items):
        with cols[i]:
            if key in R:
                v = R[key]['valor']
                prev = R[key].get('prev')
                delta = round(v - prev, 4) if prev else None
                delta_str = f"{delta:+.2f} pp" if delta else None
                st.metric(label, f"{fmt(v)} {unit}", delta_str, delta_color=delta_color(delta) if delta else "off")
                st.caption(f"Ref.: {R[key]['data']}")
            else:
                st.metric(label, "--", None)

    # Câmbio
    st.markdown('<div class="section-label">Câmbio <span class="src-tag">AwesomeAPI — tempo real</span></div>', unsafe_allow_html=True)
    cols = st.columns(5)
    cambio_items = [
        ('Dólar', 'USDBRL'),
        ('Euro', 'EURBRL'),
        ('Libra', 'GBPBRL'),
        ('PTAX', None),
        ('Bitcoin', 'BTCUSD'),
    ]
    for i, (label, key) in enumerate(cambio_items):
        with cols[i]:
            if key and key in C:
                bid = C[key]['bid']
                pct = C[key]['pct']
                prefix = 'R$ ' if 'BRL' in key else '$ '
                dec = 0 if key == 'BTCUSD' else 4
                st.metric(label, f"{prefix}{fmt(bid, dec)}", f"{pct:+.2f}%", delta_color=delta_color(pct))
                st.caption(f"Máx: {fmt(C[key]['high'], dec)} | Mín: {fmt(C[key]['low'], dec)}")
            elif label == 'PTAX' and 'ptax' in R:
                st.metric(label, f"R$ {fmt(R['ptax']['valor'], 4)}", None)
                st.caption(f"Ref.: {R['ptax']['data']}")
            else:
                st.metric(label, "--", None)

    # Índices
    st.markdown('<div class="section-label">Índices <span class="src-tag">Yahoo Finance + BCB — tempo real</span></div>', unsafe_allow_html=True)
    cols = st.columns(5)
    idx_items = [
        ('Ibovespa', 'ibovespa', 0),
        ('S&P 500', 'sp500', 0),
        ('IFIX', 'ifix', 0),
        ('DXY', 'dxy', 2),
        ('IMA-B', 'imab', 0),
    ]
    for i, (label, key, dec) in enumerate(idx_items):
        with cols[i]:
            if key == 'imab' and key in R:
                v = R[key]['valor']
                prev = R[key].get('prev')
                pct_v = round(((v / prev) - 1) * 100, 2) if prev else 0
                st.metric(label, f"{fmt(v, dec)} pts", f"{pct_v:+.2f}%", delta_color=delta_color(pct_v))
            elif key in idx:
                d = idx[key]
                st.metric(label, f"{fmt(d['price'], dec)} pts", f"{d['pct']:+.2f}%", delta_color=delta_color(d['pct']))
            else:
                st.metric(label, "--", None)


# ═══════════════════════════════════════════
# TAB 2 — Renda Fixa & Curvas
# ═══════════════════════════════════════════
with tab2:
    import pandas as pd

    # Tesouro Direto
    st.markdown('<div class="section-label">Tesouro Direto <span class="src-tag">Dados de referência</span></div>', unsafe_allow_html=True)
    tesouro_data = []
    for b in D['tesouro']:
        tc = f"Selic + {fmt(b['taxa_compra'], 4)}%" if b['tipo'] == 'Selic' else f"{fmt(b['taxa_compra'])}%"
        tv = f"Selic + {fmt(b['taxa_venda'], 4)}%" if b['tipo'] == 'Selic' else f"{fmt(b['taxa_venda'])}%"
        tesouro_data.append({
            'Título': b['nome'],
            'Vencimento': b['vencimento'],
            'Taxa Compra': tc,
            'Taxa Venda': tv,
            'PU Compra': f"R$ {fmt(b['pu_compra'])}",
            'PU Venda': f"R$ {fmt(b['pu_venda'])}",
        })
    st.dataframe(pd.DataFrame(tesouro_data), use_container_width=True, hide_index=True)

    # Curvas (Plotly)
    import plotly.graph_objects as go

    col1, col2 = st.columns(2)

    # Curva IPCA+
    ntnb = [b for b in D['tesouro'] if 'IPCA+' in b['nome'] and 'Juros' not in b['nome']]
    ntnb.sort(key=lambda x: x['vencimento'])
    if ntnb:
        with col1:
            st.markdown('<div class="section-label">Curva Real — IPCA+ (NTN-B)</div>', unsafe_allow_html=True)
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=[b['vencimento'][:4] for b in ntnb],
                y=[b['taxa_compra'] for b in ntnb],
                mode='lines+markers+text',
                text=[f"{b['taxa_compra']:.2f}%" for b in ntnb],
                textposition='top center',
                line=dict(color='#2E7D4F', width=3),
                marker=dict(size=8, color='#2E7D4F'),
                fill='tozeroy',
                fillcolor='rgba(46,125,79,0.08)',
            ))
            fig.update_layout(
                height=300, margin=dict(l=40, r=20, t=20, b=40),
                yaxis_title='% a.a.', xaxis_title='Vencimento',
                plot_bgcolor='white', paper_bgcolor='white',
                font=dict(family='Montserrat'),
            )
            fig.update_yaxes(gridcolor='#e8ebee')
            st.plotly_chart(fig, use_container_width=True)

    # Curva Prefixados
    pre = [b for b in D['tesouro'] if 'Prefixado' in b['nome'] and 'Juros' not in b['nome']]
    pre.sort(key=lambda x: x['vencimento'])
    if pre:
        with col2:
            st.markdown('<div class="section-label">Curva Pré — Prefixados</div>', unsafe_allow_html=True)
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=[b['vencimento'][:4] for b in pre],
                y=[b['taxa_compra'] for b in pre],
                mode='lines+markers+text',
                text=[f"{b['taxa_compra']:.2f}%" for b in pre],
                textposition='top center',
                line=dict(color='#223040', width=3),
                marker=dict(size=8, color='#223040'),
                fill='tozeroy',
                fillcolor='rgba(34,48,64,0.08)',
            ))
            fig.update_layout(
                height=300, margin=dict(l=40, r=20, t=20, b=40),
                yaxis_title='% a.a.', xaxis_title='Vencimento',
                plot_bgcolor='white', paper_bgcolor='white',
                font=dict(family='Montserrat'),
            )
            fig.update_yaxes(gridcolor='#e8ebee')
            st.plotly_chart(fig, use_container_width=True)

    # Focus
    focus = D['focus']
    st.markdown('<div class="section-label">Expectativas Focus <span class="src-tag">BCB / Relatório Focus — ao vivo</span></div>', unsafe_allow_html=True)
    focus_data = []
    for ind, anos in focus.items():
        row = {'Indicador': ind}
        for yr in ['2025', '2026', '2027', '2028']:
            v = anos.get(yr)
            row[yr] = f"{fmt(v)}" if v is not None else "--"
        focus_data.append(row)
    if focus_data:
        st.dataframe(pd.DataFrame(focus_data), use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════
# TAB 3 — Commodities
# ═══════════════════════════════════════════
with tab3:
    COM = D['commodities']

    def show_commodity(label, key, cols_ref, idx):
        with cols_ref[idx]:
            if key in COM:
                d = COM[key]
                st.metric(label, f"{fmt(d['valor'])} {d['unit']}", f"{d['pct']:+.2f}%", delta_color=delta_color(d['pct']))
                if d.get('date'):
                    st.caption(f"Ref.: {d['date']}")
            else:
                st.metric(label, "--", None)

    # Agro
    st.markdown('<div class="section-label">Agro & Pecuária <span class="src-tag">Yahoo Finance — tempo real</span></div>', unsafe_allow_html=True)
    cols = st.columns(4)
    agro = [('Soja (CBOT)', 'soja'), ('Milho (CBOT)', 'milho'), ('Café Arábica (ICE)', 'cafe'), ('Açúcar (ICE)', 'acucar')]
    for i, (lb, k) in enumerate(agro):
        show_commodity(lb, k, cols, i)

    cols = st.columns(4)
    agro2 = [('Algodão (ICE)', 'algodao'), ('Trigo (CBOT)', 'trigo'), ('Live Cattle (CME)', 'boi_gordo')]
    for i, (lb, k) in enumerate(agro2):
        show_commodity(lb, k, cols, i)

    # Energia
    st.markdown('<div class="section-label">Energia <span class="src-tag">Yahoo Finance — tempo real</span></div>', unsafe_allow_html=True)
    cols = st.columns(3)
    energia = [('Petróleo Brent', 'petroleo_brent'), ('Petróleo WTI', 'petroleo_wti'), ('Gás Natural', 'gas_natural')]
    for i, (lb, k) in enumerate(energia):
        show_commodity(lb, k, cols, i)

    # Metais
    st.markdown('<div class="section-label">Metais <span class="src-tag">Yahoo Finance — tempo real</span></div>', unsafe_allow_html=True)
    cols = st.columns(3)
    metais = [('Ouro (COMEX)', 'ouro'), ('Prata (COMEX)', 'prata'), ('Ferro (proxy Vale)', 'ferro')]
    for i, (lb, k) in enumerate(metais):
        show_commodity(lb, k, cols, i)


# ═══════════════════════════════════════════
# TAB 4 — Spreads & Referência Internacional
# ═══════════════════════════════════════════
with tab4:
    import pandas as pd

    selic = R.get('selic', {}).get('valor', 14.25)

    st.markdown(f'<div class="section-label">Spreads — Crédito Estruturado <span class="src-tag">Calculado sobre Selic {fmt(selic)}%</span></div>', unsafe_allow_html=True)

    spreads = [
        ['CRA Senior (Agro)', 'AAA', '3-5a', 'CDI + 1,50% a 2,50%', f'{fmt(selic+1.5)}% a {fmt(selic+2.5)}%'],
        ['CRA Mezanino', 'AA/A', '3-5a', 'CDI + 3,00% a 5,00%', f'{fmt(selic+3)}% a {fmt(selic+5)}%'],
        ['CRI Senior (Imob)', 'AAA', '5-8a', 'IPCA + 7,00% a 8,50%', 'IPCA + 7,00% a 8,50%'],
        ['Debênture Infra', 'AAA', '7-10a', 'IPCA + 6,50% a 8,00%', 'IPCA + 6,50% a 8,00%'],
        ['FIDC Senior', 'AAA', '2-3a', 'CDI + 1,80% a 3,00%', f'{fmt(selic+1.8)}% a {fmt(selic+3)}%'],
        ['FIDC Mezanino', 'A/BBB', '2-3a', 'CDI + 4,00% a 7,00%', f'{fmt(selic+4)}% a {fmt(selic+7)}%'],
        ['FIDC Subordinada', 'NR', '2-3a', 'CDI + 8,00% a 15,00%', f'{fmt(selic+8)}% a {fmt(selic+15)}%'],
        ['Sale & Leaseback Rural', 'N/A', '3-7a', 'CDI + 3,00% a 5,50%', f'{fmt(selic+3)}% a {fmt(selic+5.5)}%'],
        ['CCB / Nota Comercial', 'Varia', '1-3a', 'CDI + 2,00% a 6,00%', f'{fmt(selic+2)}% a {fmt(selic+6)}%'],
        ['Debênture Corporate', 'AA/A', '3-5a', 'CDI + 1,50% a 3,50%', f'{fmt(selic+1.5)}% a {fmt(selic+3.5)}%'],
    ]
    df_spreads = pd.DataFrame(spreads, columns=['Instrumento', 'Rating', 'Prazo', 'Spread', 'All-In'])
    st.dataframe(df_spreads, use_container_width=True, hide_index=True)

    # Referência Internacional
    T = D['treasuries']
    st.markdown('<div class="section-label">Referência Internacional <span class="src-tag">Yahoo Finance + BCB — tempo real</span></div>', unsafe_allow_html=True)
    cols = st.columns(5)
    treasury_items = [
        ('US Treasury 2Y', 'ust_2y'),
        ('US Treasury 5Y', 'ust_5y'),
        ('US Treasury 10Y', 'ust_10y'),
        ('US Treasury 30Y', 'ust_30y'),
        ('CDS Brasil 5Y', 'cds_br'),
    ]
    for i, (label, key) in enumerate(treasury_items):
        with cols[i]:
            if key in T:
                d = T[key]
                unit = 'bps' if key == 'cds_br' else '% a.a.'
                val = fmt(d['rate'], 0) if key == 'cds_br' else fmt(d['rate'], 3)
                pct = d.get('pct', 0)
                src = d.get('source', 'ref')
                st.metric(label, f"{val} {unit}", f"{pct:+.2f}%" if src == 'live' else "REF.", delta_color=delta_color(pct) if src == 'live' else "off")
            else:
                st.metric(label, "--", None)


# ─── Footer ───
st.markdown("---")
col_f1, col_f2 = st.columns([6, 4])
with col_f1:
    st.caption("ZYN Capital Assessoria Financeira — Dados ao vivo via BCB, AwesomeAPI, Yahoo Finance, BCB Focus")
with col_f2:
    st.caption(f"🔗 [Sales Intelligence](https://zyn-sales-intelligence.streamlit.app/) | Atualizado: {D['timestamp']}")


# ─── Auto-refresh via meta tag (every 2 min) ───
st.markdown(
    '<meta http-equiv="refresh" content="120">',
    unsafe_allow_html=True,
)
