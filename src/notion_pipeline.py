"""
ZYN Sales Intelligence — Módulo Pipeline Notion
Carrega dados do Pipeline do Notion (cache local JSON).
Sync semanal via Claude ou manual.
"""
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
PIPELINE_FILE = DATA_DIR / "pipeline.json"


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


def match_pipeline_to_cvm(pipeline_df: pd.DataFrame, positions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cruza deals do pipeline com dados CVM.
    Para cada deal ativo, encontra gestoras/fundos que investem no tipo correspondente.
    """
    if pipeline_df.empty or positions_df.empty:
        return pd.DataFrame()

    tipo_map = {
        "CRI": "CRI",
        "CRA": "CRA",
        "Agro": "CPR-F",
        "DCM": "NC",
        "CCB": "NC",
        "Crédito Bancário": "NC",
        "FIDC": "DEBENTURE",
        "Cota FIDC": "DEBENTURE",
        "Equity": "NC",
        "Compra Estoque": "CRI",
    }

    results = []
    for _, deal in pipeline_df.iterrows():
        tipo_cvm = tipo_map.get(deal["Tipo"], "NC")

        # Filtra posições do tipo correspondente
        mask = positions_df["tipo_ativo"] == tipo_cvm
        relevant = positions_df[mask]

        if relevant.empty:
            continue

        # Top gestoras por volume nesse tipo
        top = relevant.groupby("gestora").agg(
            volume=("vl_posicao", "sum"),
            n_fundos=("cnpj_fundo", "nunique"),
        ).reset_index().sort_values("volume", ascending=False).head(15)

        for _, g in top.iterrows():
            results.append({
                "Deal": deal["Cliente"],
                "Valor Deal": deal["Valor"],
                "Tipo": deal["Tipo"],
                "Instrumento": deal["Instrumento"],
                "Gestora CVM": g["gestora"],
                "Volume Histórico": g["volume"],
                "Fundos Ativos": g["n_fundos"],
                "Já Analisando": g["gestora"] in [str(x) for x in deal.get("Analisando", [])],
                "Notion URL": deal.get("Notion URL", ""),
            })

    return pd.DataFrame(results)
