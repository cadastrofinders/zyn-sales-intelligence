"""
ZYN Sales Intelligence — Módulo Pipeline Notion
Carrega dados do Pipeline do Notion (cache local JSON).
Sync semanal via Claude ou manual.
"""
import json
from pathlib import Path
from datetime import datetime, date
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
PIPELINE_FILE = DATA_DIR / "pipeline.json"

# Mapeamento de nomes curtos (Notion) → nomes CVM (parcial, para fuzzy match)
INVESTOR_ALIASES = {
    "BS2": ["BS2"],
    "BTG": ["BTG PACTUAL"],
    "Bside": ["BSIDE"],
    "Chimera": ["CHIMERA"],
    "EXT Capital": ["EXT CAPITAL"],
    "Exa": ["EXA CAPITAL"],
    "Fegik": ["FEGIK"],
    "Fibra": ["FIBRA EXPERTS"],
    "Fibra Asset": ["FIBRA EXPERTS"],
    "GCB": ["GCB CAPITAL"],
    "Galapagos": ["GALAPAGOS"],
    "Inco": ["INCO INVESTIMENTOS"],
    "Jive": ["JIVE"],
    "Kinea": ["KINEA"],
    "Kijani": ["KIJANI"],
    "Luso": ["LUSO BRASILEIRO"],
    "Pine": ["PINE"],
    "Vinci": ["VINCI"],
    "exes": ["EXES"],
}


def load_pipeline() -> list[dict]:
    """Carrega deals do cache JSON."""
    if not PIPELINE_FILE.exists():
        return []
    with open(PIPELINE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("deals", [])


def pipeline_sync_date() -> str:
    """Retorna data do último sync."""
    if not PIPELINE_FILE.exists():
        return "—"
    with open(PIPELINE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("sync_date", "—")


def pipeline_to_df() -> pd.DataFrame:
    """Converte pipeline para DataFrame."""
    deals = load_pipeline()
    if not deals:
        return pd.DataFrame()

    rows = []
    for d in deals:
        rows.append({
            "Cliente": d.get("cliente", ""),
            "Status": d.get("status", ""),
            "Fase": ", ".join(d.get("fase", [])),
            "Tipo": d.get("tipo_operacao", ""),
            "Instrumento": d.get("instrumento", ""),
            "Valor": d.get("valor"),
            "Sócio": d.get("socio", ""),
            "Originador": d.get("originador", ""),
            "Analisando": d.get("analisando", []),
            "Exclusividade": "Sim" if "Sim" in d.get("exclusividade", []) else "Não",
            "Envio Investidores": d.get("envio_investidores"),
            "Cobrar Retorno": d.get("cobrar_retorno"),
            "Notion URL": d.get("notion_url", ""),
        })
    return pd.DataFrame(rows)


def active_deals() -> pd.DataFrame:
    """Retorna apenas deals ativos (não declinados)."""
    df = pipeline_to_df()
    if df.empty:
        return df
    return df[df["Status"] != "Declinado"].copy()


def deals_by_status(df: pd.DataFrame = None) -> dict:
    """Agrupa deals por status."""
    if df is None:
        df = pipeline_to_df()
    if df.empty:
        return {}
    return df.groupby("Status").size().to_dict()


def investor_frequency(df: pd.DataFrame = None) -> pd.DataFrame:
    """Conta frequência de cada investidor nos deals ativos."""
    if df is None:
        df = active_deals()
    if df.empty:
        return pd.DataFrame()

    inv_count = {}
    inv_deals = {}
    for _, row in df.iterrows():
        for inv in row.get("Analisando", []):
            inv_count[inv] = inv_count.get(inv, 0) + 1
            if inv not in inv_deals:
                inv_deals[inv] = []
            inv_deals[inv].append(row["Cliente"])

    rows = [
        {"Investidor": k, "Deals": v, "Operações": ", ".join(inv_deals[k])}
        for k, v in sorted(inv_count.items(), key=lambda x: -x[1])
    ]
    return pd.DataFrame(rows)


def _fuzzy_match_investor(gestora_cvm: str, analisando_list: list) -> bool:
    """Verifica se uma gestora CVM corresponde a algum investidor da lista do Notion."""
    if not isinstance(gestora_cvm, str):
        return False
    gestora_upper = gestora_cvm.upper()
    for inv in analisando_list:
        inv_str = str(inv).strip()
        # Check aliases first
        aliases = INVESTOR_ALIASES.get(inv_str, [inv_str.upper()])
        for alias in aliases:
            if alias.upper() in gestora_upper:
                return True
    return False


def _score_aderencia(gestora_stats: dict, deal: pd.Series, all_gestora_stats: pd.DataFrame) -> float:
    """
    Calcula Score de Aderência (0-100) de uma gestora para um deal.
    Pondera: volume no tipo (40%), diversificação de fundos (20%),
    ticket médio compatível (25%), recência (15%).
    """
    score = 0.0
    volume = gestora_stats.get("volume", 0)
    n_fundos = gestora_stats.get("n_fundos", 0)
    ticket_medio = gestora_stats.get("ticket_medio", 0)
    deal_valor = deal.get("Valor", 0) or 0

    # 1. Volume no tipo (40pts) — normalizado pelo max do grupo
    max_vol = all_gestora_stats["volume"].max() if not all_gestora_stats.empty else 1
    if max_vol > 0:
        score += 40 * min(volume / max_vol, 1.0)

    # 2. Diversificação — mais fundos = mais capacidade (20pts)
    max_fundos = all_gestora_stats["n_fundos"].max() if not all_gestora_stats.empty else 1
    if max_fundos > 0:
        score += 20 * min(n_fundos / max_fundos, 1.0)

    # 3. Ticket médio compatível com deal (25pts)
    if deal_valor > 0 and ticket_medio > 0:
        ratio = min(ticket_medio, deal_valor) / max(ticket_medio, deal_valor)
        score += 25 * ratio

    # 4. Número de operações diferentes (proxy de recência) (15pts)
    n_ops = gestora_stats.get("n_operacoes", 0)
    max_ops = all_gestora_stats["n_operacoes"].max() if "n_operacoes" in all_gestora_stats.columns and not all_gestora_stats.empty else 1
    if max_ops > 0:
        score += 15 * min(n_ops / max_ops, 1.0)

    return round(score, 1)


def match_pipeline_to_cvm(pipeline_df: pd.DataFrame, positions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cruza deals do pipeline com dados CVM.
    Para cada deal ativo, encontra gestoras/fundos que investem no tipo correspondente.
    Inclui Score de Aderência e fuzzy matching para "Já Analisando".
    """
    if pipeline_df.empty or positions_df.empty:
        return pd.DataFrame()

    tipo_map = {
        "CRI": ["CRI"],
        "CRA": ["CRA"],
        "Agro": ["CPR-F", "CRA"],
        "DCM": ["NC", "DEBENTURE"],
        "CCB": ["NC"],
        "Crédito Bancário": ["NC", "DEBENTURE"],
        "FIDC": ["DEBENTURE", "FIDC"],
        "Cota FIDC": ["DEBENTURE", "FIDC"],
        "Equity": ["NC"],
        "Compra Estoque": ["CRI"],
        "SLB": ["CRI", "CRA"],
    }

    results = []
    for _, deal in pipeline_df.iterrows():
        tipos_cvm = tipo_map.get(deal["Tipo"], ["NC"])
        analisando = deal.get("Analisando", [])
        if not isinstance(analisando, list):
            analisando = []

        # Filtra posições dos tipos correspondentes
        mask = positions_df["tipo_ativo"].isin(tipos_cvm)
        relevant = positions_df[mask]

        if relevant.empty:
            continue

        # Agrupa por gestora com mais métricas
        top = relevant.groupby("gestora").agg(
            volume=("vl_posicao", "sum"),
            n_fundos=("cnpj_fundo", "nunique"),
            ticket_medio=("vl_posicao", "mean"),
            n_operacoes=("vl_posicao", "count"),
        ).reset_index().sort_values("volume", ascending=False).head(20)

        for _, g in top.iterrows():
            ja_analisando = _fuzzy_match_investor(g["gestora"], analisando)
            score = _score_aderencia(g.to_dict(), deal, top)

            results.append({
                "Deal": deal["Cliente"],
                "Valor Deal": deal["Valor"],
                "Tipo": deal["Tipo"],
                "Instrumento": deal["Instrumento"],
                "Gestora CVM": g["gestora"],
                "Volume Histórico": g["volume"],
                "Fundos Ativos": g["n_fundos"],
                "Ticket Médio": g["ticket_medio"],
                "Score": score,
                "Já Analisando": ja_analisando,
                "Notion URL": deal.get("Notion URL", ""),
            })

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(["Deal", "Score"], ascending=[True, False])
    return df


def deals_pendentes_retorno() -> pd.DataFrame:
    """Deals com cobrar_retorno vencido ou próximo (7 dias)."""
    df = active_deals()
    if df.empty:
        return df
    today = date.today()
    rows = []
    for _, row in df.iterrows():
        cr = row.get("Cobrar Retorno")
        if not cr or pd.isna(cr):
            continue
        try:
            dt = datetime.strptime(str(cr), "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue
        dias = (dt - today).days
        if dias <= 7:
            rows.append({
                "Cliente": row["Cliente"],
                "Cobrar Retorno": cr,
                "Dias": dias,
                "Status": "Vencido" if dias < 0 else ("Hoje" if dias == 0 else f"Em {dias}d"),
                "Sócio": row["Sócio"],
                "Analisando": ", ".join(row.get("Analisando", [])),
            })
    return pd.DataFrame(rows)
