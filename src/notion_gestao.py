#!/usr/bin/env python3
"""
ZYN Sales Intelligence — Módulo de Gestão (Notion API)
Busca dados de Receitas, Despesas, Fluxo de Caixa, Leads e Extrato Bancário
para alimentar o Painel Executivo no dashboard Streamlit.
"""
import json
import os
import sys
from pathlib import Path
from datetime import datetime

import requests
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CACHE_FILE = DATA_DIR / "gestao_cache.json"

# IDs corretos dos DBs Notion (verificados Mar/2026)
RECEITAS_DB = "a4600183-5383-416f-88fa-c82f9b5db178"
DESPESAS_DB = "f0447e5d-d0e4-4b62-aeb6-484d7e35138c"
FLUXO_DB = "ae51c677-6292-45d3-9d7b-2a8187825e0a"
LEADS_DB = "2f68e4de-57c8-81e8-9606-e573d49a2b14"
EXTRATO_DB = "af187e25-0858-4daf-8fb6-ac5cd2b37880"
ORIGINADORES_DB = "3178e4de-57c8-8064-a09c-eec441ea7360"
PIPELINE_DB = "2f68e4de-57c8-81f3-bcaa-cc73f28fd5d5"

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

MESES_ORDER = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
]


def _get_token() -> str:
    token = os.environ.get("NOTION_TOKEN", "")
    if token:
        return token
    try:
        import streamlit as st
        token = st.secrets.get("NOTION_TOKEN", "")
        if token:
            return token
    except Exception:
        pass
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("NOTION_TOKEN="):
                return line.split("=", 1)[1].strip().strip("\"'")
    return ""


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _query_all(token: str, db_id: str, filter_body: dict | None = None) -> list[dict]:
    """Busca todas as páginas de um DB Notion."""
    hdrs = _headers(token)
    results = []
    has_more = True
    cursor = None
    while has_more:
        body: dict = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        if filter_body:
            body["filter"] = filter_body
        resp = requests.post(
            f"{NOTION_API}/databases/{db_id}/query",
            headers=hdrs, json=body, timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        cursor = data.get("next_cursor")
    return results


# ── Property extractors ──────────────────────────────────────

def _title(prop) -> str:
    if not isinstance(prop, dict):
        return ""
    items = prop.get("title", [])
    return items[0].get("plain_text", "") if items else ""


def _rich_text(prop) -> str:
    if not isinstance(prop, dict):
        return ""
    items = prop.get("rich_text", [])
    return items[0].get("plain_text", "") if items else ""


def _select(prop) -> str:
    if not isinstance(prop, dict):
        return ""
    sel = prop.get("select")
    return sel.get("name", "") if sel and isinstance(sel, dict) else ""


def _multi_select(prop) -> list:
    if not isinstance(prop, dict):
        return []
    ms = prop.get("multi_select", [])
    return [i.get("name", "") for i in ms if isinstance(i, dict)]


def _number(prop):
    if not isinstance(prop, dict):
        return None
    return prop.get("number")


def _date(prop) -> str | None:
    if not isinstance(prop, dict):
        return None
    dt = prop.get("date")
    return dt.get("start") if dt and isinstance(dt, dict) else None


def _checkbox(prop) -> bool:
    if not isinstance(prop, dict):
        return False
    return prop.get("checkbox", False)


def _formula_number(prop):
    if not isinstance(prop, dict):
        return None
    f = prop.get("formula")
    if f and isinstance(f, dict):
        return f.get("number")
    return None


def _formula_string(prop):
    if not isinstance(prop, dict):
        return None
    f = prop.get("formula")
    if f and isinstance(f, dict):
        return f.get("string")
    return None


def _email(prop) -> str:
    if not isinstance(prop, dict):
        return ""
    return prop.get("email") or ""


def _phone(prop) -> str:
    if not isinstance(prop, dict):
        return ""
    return prop.get("phone_number") or ""


# ── Parsers por DB ───────────────────────────────────────────

def _parse_receita(page: dict) -> dict | None:
    p = page.get("properties", {})
    cliente = _title(p.get("Operação Cliente") or p.get("Operacao Cliente") or p.get("Name", {}))
    if not cliente:
        return None
    return {
        "cliente": cliente.strip(),
        "tipo_receita": _select(p.get("Tipo de Receita") or p.get("Tipo Receita Temp", {})),
        "produto": _select(p.get("Produto", {})),
        "valor_bruto": _number(p.get("Valor Bruto", {})),
        "valor_liquido": _number(p.get("Valor Liquido Zyn") or p.get("Valor Líquido Zyn", {})),
        "liquido_formula": _formula_number(p.get("Liquido Zyn R$") or p.get("Líquido Zyn R$", {})),
        "fee_finder_pct": _number(p.get("Perc Fee Finder", {})),
        "fee_finder_valor": _formula_number(p.get("Fee Finder R$", {})),
        "status": _select(p.get("Status", {})),
        "socio": _select(p.get("Socio Responsavel") or p.get("Sócio Responsável", {})),
        "mes": _select(p.get("Mes Competencia") or p.get("Mês Competência", {})),
        "ano": _select(p.get("Ano", {})),
        "data_prevista": _date(p.get("Data Prevista", {})),
        "data_realizada": _date(p.get("Data Realizada", {})),
        "originador": _rich_text(p.get("Originador Finder", {})),
        "nf_emitida": _checkbox(p.get("Nota Fiscal Emitida", {})),
    }


def _parse_despesa(page: dict) -> dict | None:
    p = page.get("properties", {})
    desc = _title(p.get("Descrição") or p.get("Descricao") or p.get("Name", {}))
    if not desc:
        return None
    return {
        "descricao": desc.strip(),
        "categoria": _select(p.get("Categoria", {})),
        "valor": _number(p.get("Valor", {})),
        "status": _select(p.get("Status", {})),
        "recorrencia": _select(p.get("Recorrencia") or p.get("Recorrência", {})),
        "forma_pagamento": _select(p.get("Forma Pagamento", {})),
        "fornecedor": _rich_text(p.get("Fornecedor", {})),
        "mes": _select(p.get("Mes Competencia") or p.get("Mês Competência", {})),
        "ano": _select(p.get("Ano", {})),
        "data_vencimento": _date(p.get("Data Vencimento", {})),
        "data_pagamento": _date(p.get("Data Pagamento", {})),
    }


def _parse_fluxo(page: dict) -> dict | None:
    p = page.get("properties", {})
    mes_ano = _title(p.get("Mes Ano") or p.get("Mês Ano") or p.get("Name", {}))
    if not mes_ano:
        return None
    return {
        "mes_ano": mes_ano.strip(),
        "mes": _select(p.get("Mes") or p.get("Mês", {})),
        "receita_prevista": _number(p.get("Receita Prevista", {})),
        "receita_realizada": _number(p.get("Receita Realizada", {})),
        "despesa_prevista": _number(p.get("Despesa Prevista", {})),
        "despesa_realizada": _number(p.get("Despesa Realizada", {})),
        "saldo_mes": _formula_number(p.get("Saldo do Mes") or p.get("Saldo do Mês", {})),
        "saldo_acumulado": _number(p.get("Saldo Acumulado", {})),
        "saldo_banco": _number(p.get("Saldo Banco C6", {})),
        "diferenca_banco": _formula_number(p.get("Diferenca vs Banco") or p.get("Diferença vs Banco", {})),
        "status": _select(p.get("Status", {})),
        "obs": _rich_text(p.get("Observacoes") or p.get("Observações", {})),
    }


def _parse_lead(page: dict) -> dict | None:
    p = page.get("properties", {})
    cliente = _title(p.get("Cliente") or p.get("Name", {}))
    if not cliente:
        return None
    return {
        "cliente": cliente.strip(),
        "status": _select(p.get("Status", {})),
        "status_rel": _select(p.get("Status do Relacionamento", {})),
        "setor": _select(p.get("Setor", {})),
        "segmento": _multi_select(p.get("Segmento", {})),
        "socio": _select(p.get("Socio Responsavel") or p.get("Sócio Responsável", {})),
        "ticket": _number(p.get("Ticket Estimado", {})),
        "volume": _number(p.get("Volume Operacao") or p.get("Volume Operação", {})),
        "probabilidade": _select(p.get("Probabilidade", {})),
        "urgencia": _select(p.get("Urgencia") or p.get("Urgência", {})),
        "rating": _select(p.get("Rating Preliminar", {})),
        "origem": _select(p.get("Origem Lead", {})),
        "kit_banco": _select(p.get("Kit Banco", {})),
        "data_retorno": _date(p.get("Data Retorno", {})),
        "originacao": _rich_text(p.get("Originacao") or p.get("Originação", {})),
    }


def _parse_extrato(page: dict) -> dict | None:
    p = page.get("properties", {})
    nome = _title(p.get("Nome") or p.get("Name", {}))
    return {
        "nome": nome.strip() if nome else "",
        "data": _date(p.get("Data", {})),
        "tipo": _select(p.get("Tipo", {})),
        "historico": _select(p.get("Historico") or p.get("Histórico", {})),
        "valor": _number(p.get("Valor", {})),
        "saldo": _number(p.get("Saldo", {})),
        "status_conciliacao": _select(p.get("Status Conciliacao") or p.get("Status Conciliação", {})),
        "mes_ref": _select(p.get("Mes Referencia") or p.get("Mês Referência", {})),
    }


# ── Sync principal ───────────────────────────────────────────

def sync_gestao(token: str | None = None) -> dict:
    """Sincroniza todos os DBs de gestão e salva cache local."""
    if not token:
        token = _get_token()
    if not token:
        raise ValueError("NOTION_TOKEN não encontrado")

    print("🔄 Sincronizando dados de gestão do Notion...")

    # Receitas
    print("  📊 Receitas...")
    receitas_raw = _query_all(token, RECEITAS_DB)
    receitas = [r for r in (map(_parse_receita, receitas_raw)) if r]
    print(f"     {len(receitas)} receitas")

    # Despesas
    print("  💸 Despesas...")
    despesas_raw = _query_all(token, DESPESAS_DB)
    despesas = [d for d in (map(_parse_despesa, despesas_raw)) if d]
    print(f"     {len(despesas)} despesas")

    # Fluxo de Caixa
    print("  💰 Fluxo de Caixa...")
    fluxo_raw = _query_all(token, FLUXO_DB)
    fluxo = [f for f in (map(_parse_fluxo, fluxo_raw)) if f]
    print(f"     {len(fluxo)} meses")

    # Leads
    print("  🎯 Leads...")
    leads_raw = _query_all(token, LEADS_DB)
    leads = [l for l in (map(_parse_lead, leads_raw)) if l]
    print(f"     {len(leads)} leads")

    # Extrato
    print("  🏦 Extrato Bancário...")
    extrato_raw = _query_all(token, EXTRATO_DB)
    extrato = [e for e in (map(_parse_extrato, extrato_raw)) if e]
    print(f"     {len(extrato)} lançamentos")

    cache = {
        "sync_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "receitas": receitas,
        "despesas": despesas,
        "fluxo": fluxo,
        "leads": leads,
        "extrato": extrato,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    print(f"✅ Cache salvo: {CACHE_FILE}")
    return cache


def load_cache() -> dict:
    """Carrega cache local. Retorna dict vazio se não existir."""
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def gestao_sync_date() -> str:
    """Retorna data do último sync."""
    cache = load_cache()
    return cache.get("sync_date", "—")


# ── DataFrames ───────────────────────────────────────────────

def receitas_df() -> pd.DataFrame:
    cache = load_cache()
    data = cache.get("receitas", [])
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    # Valor líquido: pegar do campo manual ou da fórmula
    df["valor_liq"] = df["valor_liquido"].fillna(df["liquido_formula"])
    return df


def despesas_df() -> pd.DataFrame:
    cache = load_cache()
    data = cache.get("despesas", [])
    return pd.DataFrame(data) if data else pd.DataFrame()


def fluxo_df() -> pd.DataFrame:
    cache = load_cache()
    data = cache.get("fluxo", [])
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    # Ordenar por mês
    df["_mes_idx"] = df["mes"].apply(lambda m: MESES_ORDER.index(m) if m in MESES_ORDER else 99)
    df = df.sort_values("_mes_idx").drop(columns=["_mes_idx"])
    return df


def leads_df() -> pd.DataFrame:
    cache = load_cache()
    data = cache.get("leads", [])
    return pd.DataFrame(data) if data else pd.DataFrame()


def extrato_df() -> pd.DataFrame:
    cache = load_cache()
    data = cache.get("extrato", [])
    return pd.DataFrame(data) if data else pd.DataFrame()


# ── KPIs calculados ──────────────────────────────────────────

def kpis_resumo(ano: str = "2026") -> dict:
    """Calcula KPIs principais para o Painel Executivo."""
    rec = receitas_df()
    desp = despesas_df()
    fluxo = fluxo_df()
    leads = leads_df()

    # Filtrar por ano
    rec_ano = rec[rec["ano"] == ano] if not rec.empty and "ano" in rec.columns else pd.DataFrame()
    desp_ano = desp[desp["ano"] == ano] if not desp.empty and "ano" in desp.columns else pd.DataFrame()

    # Pipeline (do pipeline.json existente)
    try:
        from src.notion_pipeline import pipeline_to_df, active_deals
    except ImportError:
        from notion_pipeline import pipeline_to_df, active_deals
    pipe = pipeline_to_df()
    ativos = pipe[pipe["Status"] != "Declinado"] if not pipe.empty else pd.DataFrame()

    # Receita
    rec_recebida = rec_ano[rec_ano["status"] == "Recebido"]["valor_liq"].sum() if not rec_ano.empty and "valor_liq" in rec_ano.columns else 0
    rec_confirmada = rec_ano[rec_ano["status"] == "Confirmado"]["valor_liq"].sum() if not rec_ano.empty and "valor_liq" in rec_ano.columns else 0
    rec_prevista = rec_ano[rec_ano["status"] == "Previsto"]["valor_liq"].sum() if not rec_ano.empty and "valor_liq" in rec_ano.columns else 0
    rec_total_bruto = rec_ano["valor_bruto"].sum() if not rec_ano.empty and "valor_bruto" in rec_ano.columns else 0

    # Despesa
    desp_paga = desp_ano[desp_ano["status"] == "Pago"]["valor"].sum() if not desp_ano.empty else 0
    desp_pendente = desp_ano[desp_ano["status"] == "Pendente"]["valor"].sum() if not desp_ano.empty else 0

    # Burn rate (média mensal de despesas pagas)
    meses_desp = desp_ano[desp_ano["status"] == "Pago"]["mes"].nunique() if not desp_ano.empty else 1
    burn_rate = desp_paga / max(meses_desp, 1)

    # Saldo atual (último fluxo com saldo_banco)
    saldo_atual = 0
    if not fluxo.empty and "saldo_banco" in fluxo.columns:
        saldos = fluxo[fluxo["saldo_banco"].notna()]
        if not saldos.empty:
            saldo_atual = saldos.iloc[-1]["saldo_banco"]

    # Runway
    runway_meses = saldo_atual / burn_rate if burn_rate > 0 else 99

    # Pipeline
    pipe_total = ativos["Valor"].sum() if not ativos.empty and "Valor" in ativos.columns else 0
    pipe_count = len(ativos)

    # Fee médio
    fee_medio = rec_ano["valor_liq"].mean() if not rec_ano.empty and "valor_liq" in rec_ano.columns and rec_ano["valor_liq"].notna().any() else 0

    # Leads
    leads_ativos = len(leads[leads["status"].isin(["Em andamento", "Nao iniciada", "Não iniciada"])]) if not leads.empty else 0
    leads_convertidos = len(leads[leads["status"] == "Enviado para Pipeline"]) if not leads.empty else 0
    leads_total = len(leads)

    return {
        "ano": ano,
        "pipe_total": pipe_total,
        "pipe_count": pipe_count,
        "rec_recebida": rec_recebida,
        "rec_confirmada": rec_confirmada,
        "rec_prevista": rec_prevista,
        "rec_total_bruto": rec_total_bruto,
        "desp_paga": desp_paga,
        "desp_pendente": desp_pendente,
        "burn_rate": burn_rate,
        "saldo_atual": saldo_atual,
        "runway_meses": runway_meses,
        "fee_medio": fee_medio,
        "leads_ativos": leads_ativos,
        "leads_convertidos": leads_convertidos,
        "leads_total": leads_total,
    }


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    cache = sync_gestao()
    kpis = kpis_resumo()
    print("\n📋 KPIs 2026:")
    for k, v in kpis.items():
        if isinstance(v, float):
            print(f"  {k}: R$ {v:,.0f}" if v > 100 else f"  {k}: {v:.1f}")
        else:
            print(f"  {k}: {v}")
