"""
ZYN Sales Intelligence — Motor de Análise
Agrega posições por gestora/fundo, gera rankings e perfis de investidor.
"""
import pandas as pd
import numpy as np
from config.settings import FUND_CATEGORIES


def build_investor_profiles(positions: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega posições para construir perfil de cada gestora.
    Retorna um DataFrame com uma linha por gestora, incluindo:
    - Volume total alocado por tipo de ativo
    - Número de fundos
    - PL total sob gestão
    - Tipos de papel preferidos
    - Faixa de volume médio por operação
    - Perfil de prazo (quando disponível)
    """
    if positions.empty:
        return pd.DataFrame()

    # Garante coluna gestora
    if "gestora" not in positions.columns:
        positions["gestora"] = positions.get("nome_fundo", "Desconhecido")

    # === Perfil por Gestora ===
    gestora_groups = positions.groupby("gestora", dropna=False)

    profiles = []
    for gestora, group in gestora_groups:
        if pd.isna(gestora) or str(gestora).strip() == "":
            continue

        profile = {"gestora": gestora}

        # CNPJ da gestora
        profile["cnpj_gestora"] = group["cnpj_gestora"].mode().iloc[0] if "cnpj_gestora" in group.columns and not group["cnpj_gestora"].isna().all() else ""

        # Número de fundos distintos
        profile["n_fundos"] = group["cnpj_fundo"].nunique()
        profile["fundos"] = "; ".join(group["nome_fundo"].dropna().unique()[:5])

        # PL total (soma dos PLs únicos por fundo)
        if "pl_fundo" in group.columns:
            pl_por_fundo = group.drop_duplicates("cnpj_fundo")["pl_fundo"]
            profile["pl_total"] = pd.to_numeric(pl_por_fundo, errors="coerce").sum()
        else:
            profile["pl_total"] = 0

        # Volume por tipo de ativo
        vol_by_type = group.groupby("tipo_ativo")["vl_posicao"].sum()
        profile["vol_total"] = group["vl_posicao"].sum()

        for asset_type in ["NC", "CRI", "CRA", "CPR-F", "DEBENTURE"]:
            profile[f"vol_{asset_type}"] = vol_by_type.get(asset_type, 0)
            profile[f"n_ops_{asset_type}"] = len(group[group["tipo_ativo"] == asset_type])

        # Tipo preferido (maior volume)
        if not vol_by_type.empty:
            profile["tipo_preferido"] = vol_by_type.idxmax()
            profile["concentracao_tipo_pref"] = vol_by_type.max() / vol_by_type.sum() if vol_by_type.sum() > 0 else 0
        else:
            profile["tipo_preferido"] = ""
            profile["concentracao_tipo_pref"] = 0

        # Volume médio por posição
        profile["ticket_medio"] = group["vl_posicao"].mean()
        profile["ticket_mediano"] = group["vl_posicao"].median()
        profile["ticket_max"] = group["vl_posicao"].max()

        # Perfil de prazo (quando disponível)
        if "dt_vencimento" in group.columns:
            venc = pd.to_datetime(group["dt_vencimento"], errors="coerce")
            valid_venc = venc.dropna()
            if not valid_venc.empty:
                today = pd.Timestamp.now()
                prazos_anos = ((valid_venc - today).dt.days / 365.25).clip(lower=0)
                profile["prazo_medio_anos"] = prazos_anos.mean()
                profile["prazo_max_anos"] = prazos_anos.max()
                profile["prazo_min_anos"] = prazos_anos[prazos_anos > 0].min() if (prazos_anos > 0).any() else 0
            else:
                profile["prazo_medio_anos"] = None
                profile["prazo_max_anos"] = None
                profile["prazo_min_anos"] = None
        else:
            profile["prazo_medio_anos"] = None
            profile["prazo_max_anos"] = None
            profile["prazo_min_anos"] = None

        # Indexador predominante
        if "indexador" in group.columns:
            idx_counts = group["indexador"].dropna().value_counts()
            if not idx_counts.empty:
                profile["indexador_principal"] = idx_counts.index[0]
            else:
                profile["indexador_principal"] = ""
        else:
            profile["indexador_principal"] = ""

        # Spread médio (quando disponível)
        if "spread" in group.columns:
            spreads = pd.to_numeric(group["spread"], errors="coerce").dropna()
            profile["spread_medio"] = spreads.mean() if not spreads.empty else None
        else:
            profile["spread_medio"] = None

        # Classe do fundo predominante
        if "classe_anbima" in group.columns:
            cls = group["classe_anbima"].dropna().value_counts()
            profile["classe_predominante"] = cls.index[0] if not cls.empty else ""
        else:
            profile["classe_predominante"] = ""

        # Público alvo
        if "publico_alvo" in group.columns:
            pa = group["publico_alvo"].dropna().value_counts()
            profile["publico_alvo"] = pa.index[0] if not pa.empty else ""
        else:
            profile["publico_alvo"] = ""

        profiles.append(profile)

    df_profiles = pd.DataFrame(profiles)

    if df_profiles.empty:
        return df_profiles

    # Ordena por volume total
    df_profiles = df_profiles.sort_values("vol_total", ascending=False).reset_index(drop=True)

    # Score de diversificação
    type_cols = [c for c in df_profiles.columns if c.startswith("vol_") and c != "vol_total"]
    type_matrix = df_profiles[type_cols].values
    type_matrix_norm = type_matrix / (type_matrix.sum(axis=1, keepdims=True) + 1e-10)
    df_profiles["diversificacao"] = 1 - (type_matrix_norm ** 2).sum(axis=1)

    return df_profiles


def score_match(deal: dict, profile: pd.Series) -> dict:
    """
    Calcula score de aderência entre uma operação do Pipeline e um perfil de investidor.

    deal deve conter:
      - tipo: NC, CRI, CRA, CPR-F, DEBENTURE
      - volume: volume da operação (R$)
      - prazo_anos: prazo em anos (opcional)
      - indexador: CDI, IPCA, etc. (opcional)
      - taxa/spread: spread sobre indexador (opcional)

    Retorna dict com score total e breakdown.
    """
    scores = {}

    # 1. Match de tipo de ativo (peso 40%)
    tipo = deal.get("tipo", "").upper()
    vol_col = f"vol_{tipo}"
    n_ops_col = f"n_ops_{tipo}"

    if vol_col in profile.index and profile[vol_col] > 0:
        scores["tipo"] = 1.0
    elif profile.get("tipo_preferido", "") == tipo:
        scores["tipo"] = 0.8
    else:
        # Verifica se já comprou qualquer renda fixa estruturada
        scores["tipo"] = 0.2 if profile.get("vol_total", 0) > 0 else 0.0

    # 2. Match de volume/ticket (peso 25%)
    deal_vol = deal.get("volume", 0)
    ticket_med = profile.get("ticket_medio", 0) or 0
    ticket_max = profile.get("ticket_max", 0) or 0

    if deal_vol > 0 and ticket_med > 0:
        ratio = deal_vol / ticket_med
        if 0.3 <= ratio <= 3.0:
            scores["volume"] = 1.0
        elif 0.1 <= ratio <= 5.0:
            scores["volume"] = 0.6
        else:
            scores["volume"] = 0.2
    else:
        scores["volume"] = 0.5  # sem dados suficientes

    # 3. Match de prazo (peso 15%)
    deal_prazo = deal.get("prazo_anos")
    inv_prazo = profile.get("prazo_medio_anos")

    if deal_prazo and inv_prazo and not pd.isna(inv_prazo):
        diff = abs(deal_prazo - inv_prazo)
        if diff <= 1:
            scores["prazo"] = 1.0
        elif diff <= 3:
            scores["prazo"] = 0.6
        else:
            scores["prazo"] = 0.2
    else:
        scores["prazo"] = 0.5

    # 4. Match de indexador (peso 10%)
    deal_idx = deal.get("indexador", "").upper()
    inv_idx = str(profile.get("indexador_principal", "")).upper()

    if deal_idx and inv_idx:
        if deal_idx in inv_idx or inv_idx in deal_idx:
            scores["indexador"] = 1.0
        else:
            scores["indexador"] = 0.3
    else:
        scores["indexador"] = 0.5

    # 5. Histórico de atividade no tipo (peso 10%)
    n_ops = profile.get(n_ops_col, 0) if n_ops_col in profile.index else 0
    if n_ops >= 10:
        scores["historico"] = 1.0
    elif n_ops >= 5:
        scores["historico"] = 0.8
    elif n_ops >= 1:
        scores["historico"] = 0.5
    else:
        scores["historico"] = 0.1

    # Score ponderado
    weights = {"tipo": 0.40, "volume": 0.25, "prazo": 0.15, "indexador": 0.10, "historico": 0.10}
    total = sum(scores[k] * weights[k] for k in weights)

    return {
        "score_total": round(total, 3),
        "score_tipo": scores["tipo"],
        "score_volume": scores["volume"],
        "score_prazo": scores["prazo"],
        "score_indexador": scores["indexador"],
        "score_historico": scores["historico"],
    }


def match_deal_to_investors(
    deal: dict,
    profiles: pd.DataFrame,
    top_n: int = 30,
    min_score: float = 0.3,
) -> pd.DataFrame:
    """
    Dado um deal do Pipeline, retorna ranking dos melhores investidores.
    """
    if profiles.empty:
        return pd.DataFrame()

    results = []
    for _, profile in profiles.iterrows():
        scores = score_match(deal, profile)
        if scores["score_total"] >= min_score:
            row = {
                "gestora": profile["gestora"],
                "cnpj_gestora": profile.get("cnpj_gestora", ""),
                "n_fundos": profile.get("n_fundos", 0),
                "pl_total": profile.get("pl_total", 0),
                "vol_total_rf": profile.get("vol_total", 0),
                f"vol_{deal.get('tipo', '').upper()}": profile.get(f"vol_{deal.get('tipo', '').upper()}", 0),
                "ticket_medio": profile.get("ticket_medio", 0),
                "tipo_preferido": profile.get("tipo_preferido", ""),
                "indexador_principal": profile.get("indexador_principal", ""),
                "classe_predominante": profile.get("classe_predominante", ""),
                **scores,
            }
            results.append(row)

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values("score_total", ascending=False).head(top_n).reset_index(drop=True)
    df.index += 1
    df.index.name = "rank"

    return df


def generate_market_overview(positions: pd.DataFrame) -> dict:
    """Gera visão geral do mercado de renda fixa estruturada."""
    if positions.empty:
        return {}

    overview = {
        "total_posicoes": len(positions),
        "volume_total": positions["vl_posicao"].sum(),
        "fundos_unicos": positions["cnpj_fundo"].nunique(),
        "gestoras_unicas": positions["gestora"].nunique() if "gestora" in positions.columns else 0,
    }

    # Por tipo de ativo
    by_type = positions.groupby("tipo_ativo").agg(
        volume=("vl_posicao", "sum"),
        n_posicoes=("vl_posicao", "count"),
        n_fundos=("cnpj_fundo", "nunique"),
        ticket_medio=("vl_posicao", "mean"),
    ).sort_values("volume", ascending=False)
    overview["por_tipo"] = by_type

    # Top gestoras por volume
    if "gestora" in positions.columns:
        top_gestoras = positions.groupby("gestora").agg(
            volume=("vl_posicao", "sum"),
            n_posicoes=("vl_posicao", "count"),
            n_fundos=("cnpj_fundo", "nunique"),
        ).sort_values("volume", ascending=False).head(20)
        overview["top_gestoras"] = top_gestoras

    # Por indexador (quando disponível)
    if "indexador" in positions.columns:
        by_idx = positions.groupby("indexador").agg(
            volume=("vl_posicao", "sum"),
            n_posicoes=("vl_posicao", "count"),
        ).sort_values("volume", ascending=False).head(10)
        overview["por_indexador"] = by_idx

    return overview
