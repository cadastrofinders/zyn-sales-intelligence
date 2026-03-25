"""
ZYN Sales Intelligence — Sincronização com Notion
Envia perfis de investidores para a base Investidores no Notion.
Usa formato de instrução para o Claude/MCP executar.
"""
import pandas as pd
from datetime import datetime


# ID da base Investidores criada no Notion
INVESTIDORES_DB_ID = "3f08c222cee3407ea4ad768c073462e6"
INVESTIDORES_DATASOURCE = "collection://e2e648db-b627-4b73-86d4-e7ce94398f8f"


def profile_to_notion_row(profile: pd.Series) -> dict:
    """Converte um perfil de investidor para formato de página Notion."""
    today = datetime.now().strftime("%Y-%m-%d")

    # Mapear indexador para opções válidas
    idx = str(profile.get("indexador_principal", "")).upper()
    idx_map = {"DI": "DI", "CDI": "CDI", "IPCA": "IPCA", "PRE": "PRE", "IGP-M": "IGP-M"}
    indexador = None
    for k, v in idx_map.items():
        if k in idx:
            indexador = v
            break

    # Mapear tipo preferido
    tipo_pref_map = {"NC": "NC", "CRI": "CRI", "CRA": "CRA", "CPR-F": "CPR-F", "DEBENTURE": "Debênture"}
    tipo_pref = tipo_pref_map.get(profile.get("tipo_preferido", ""))

    row = {
        "Gestora": str(profile.get("gestora", "")),
        "CNPJ Gestora": str(profile.get("cnpj_gestora", "")),
        "Tipo": "Asset",
        "Nº Fundos": int(profile.get("n_fundos", 0)),
        "PL Total": float(profile.get("pl_total", 0)),
        "Vol. RF Estruturada": float(profile.get("vol_total", 0)),
        "Vol. NC": float(profile.get("vol_NC", 0)),
        "Vol. CRI": float(profile.get("vol_CRI", 0)),
        "Vol. CRA": float(profile.get("vol_CRA", 0)),
        "Vol. CPR-F": float(profile.get("vol_CPR-F", 0)),
        "Vol. Debênture": float(profile.get("vol_DEBENTURE", 0)),
        "Ticket Médio": float(profile.get("ticket_medio", 0)),
        "Classe Predominante": str(profile.get("classe_predominante", "")),
        "Fonte": "CVM/CDA",
        "date:Última Atualização:start": today,
    }

    if tipo_pref:
        row["Tipo Preferido"] = tipo_pref
    if indexador:
        row["Indexador Principal"] = indexador

    prazo = profile.get("prazo_medio_anos")
    if prazo and not pd.isna(prazo):
        row["Prazo Médio (anos)"] = round(float(prazo), 1)

    return row


def generate_notion_insert_instructions(profiles: pd.DataFrame, top_n: int = 100) -> list[dict]:
    """
    Gera lista de rows para inserção no Notion.
    O skill /sales usará esses dados para chamar notion-create-pages.
    """
    top = profiles.head(top_n)
    rows = []
    for _, profile in top.iterrows():
        row = profile_to_notion_row(profile)
        # Remove valores NaN/None
        row = {k: v for k, v in row.items() if v is not None and str(v) != "nan" and v != ""}
        rows.append(row)
    return rows


def generate_matching_notes(profiles: pd.DataFrame, deals: list[dict]) -> pd.DataFrame:
    """
    Para cada perfil, gera a coluna 'Deal Match' com as operações do pipeline
    que têm maior aderência.
    """
    from src.analyzer import score_match

    if profiles.empty or not deals:
        return profiles

    profiles = profiles.copy()
    deal_matches = []

    for _, profile in profiles.iterrows():
        matches = []
        for deal in deals:
            score = score_match(deal, profile)
            if score["score_total"] >= 0.5:
                nome = deal.get("nome", "N/A")
                matches.append(f"{nome} ({score['score_total']:.0%})")
        deal_matches.append("; ".join(matches[:3]) if matches else "")

    profiles["deal_match"] = deal_matches
    return profiles
