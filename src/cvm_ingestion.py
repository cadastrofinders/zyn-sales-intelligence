"""
ZYN Sales Intelligence — Ingestão de dados CVM
Baixa e processa dados de Composição de Carteira (CDA) e Cadastro de Fundos (CAD).
"""
import io
import zipfile
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

from config.settings import (
    ASSET_TYPES,
    CAD_URL,
    CDA_URL,
    CVM_BASE_URL,
    DATA_DIR,
    get_target_months,
)


def download_file(url: str, desc: str = "") -> bytes | None:
    """Baixa arquivo da CVM com progress bar."""
    try:
        resp = requests.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        data = io.BytesIO()
        with tqdm(total=total, unit="B", unit_scale=True, desc=desc) as pbar:
            for chunk in resp.iter_content(chunk_size=8192):
                data.write(chunk)
                pbar.update(len(chunk))
        return data.getvalue()
    except requests.RequestException as e:
        print(f"  ⚠ Erro ao baixar {url}: {e}")
        return None


def download_cda_month(month: str) -> dict[str, pd.DataFrame]:
    """Baixa ZIP do CDA de um mês e retorna DataFrames por bloco."""
    url = f"{CDA_URL}/cda_fi_{month}.zip"
    print(f"\n📥 Baixando CDA {month}...")
    content = download_file(url, desc=f"CDA {month}")
    if content is None:
        return {}

    frames = {}
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        for name in zf.namelist():
            if not name.endswith(".csv"):
                continue
            # Identificar o bloco (BLC_4, BLC_6, BLC_8, PL, etc.)
            for block in ["BLC_4", "BLC_6", "BLC_8", "PL"]:
                if block in name:
                    print(f"  📄 Processando {name}...")
                    try:
                        df = pd.read_csv(
                            io.BytesIO(zf.read(name)),
                            sep=";",
                            encoding="latin-1",
                            low_memory=False,
                        )
                        frames[block] = pd.concat(
                            [frames.get(block, pd.DataFrame()), df],
                            ignore_index=True,
                        )
                    except Exception as e:
                        print(f"  ⚠ Erro ao processar {name}: {e}")
    return frames


def download_cadastro() -> pd.DataFrame:
    """Baixa cadastro atual de fundos (Resolução CVM 175 — fundo + classe)."""
    print("\n📥 Baixando cadastro de fundos (RCVM 175)...")
    cache_fundo = DATA_DIR / "registro_fundo.csv"
    cache_classe = DATA_DIR / "registro_classe.csv"

    # Usa cache se < 7 dias
    import time
    if cache_fundo.exists() and cache_classe.exists():
        age_days = (time.time() - cache_fundo.stat().st_mtime) / 86400
        if age_days < 7:
            print("  ✓ Usando cache do cadastro (< 7 dias)")
            df_fundo = pd.read_csv(cache_fundo, sep=";", encoding="latin-1", low_memory=False)
            df_classe = pd.read_csv(cache_classe, sep=";", encoding="latin-1", low_memory=False)
            return _merge_fundo_classe(df_fundo, df_classe)

    url = f"{CVM_BASE_URL}/CAD/DADOS/registro_fundo_classe.zip"
    content = download_file(url, desc="Cadastro RCVM175")

    if content is None:
        # Fallback ao cache
        if cache_fundo.exists() and cache_classe.exists():
            df_fundo = pd.read_csv(cache_fundo, sep=";", encoding="latin-1", low_memory=False)
            df_classe = pd.read_csv(cache_classe, sep=";", encoding="latin-1", low_memory=False)
            return _merge_fundo_classe(df_fundo, df_classe)
        return pd.DataFrame()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df_fundo = pd.DataFrame()
    df_classe = pd.DataFrame()

    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        for name in zf.namelist():
            if "registro_fundo" in name and "classe" not in name and "subclasse" not in name and name.endswith(".csv"):
                print(f"  📄 {name}")
                df_fundo = pd.read_csv(io.BytesIO(zf.read(name)), sep=";", encoding="latin-1", low_memory=False)
                (DATA_DIR / "registro_fundo.csv").write_bytes(zf.read(name))
            elif "registro_classe" in name and "subclasse" not in name and name.endswith(".csv"):
                print(f"  📄 {name}")
                raw = zf.read(name)
                df_classe = pd.read_csv(io.BytesIO(raw), sep=";", encoding="latin-1", low_memory=False)
                (DATA_DIR / "registro_classe.csv").write_bytes(raw)

    print(f"  ✓ Fundos: {len(df_fundo)} | Classes: {len(df_classe)}")
    return _merge_fundo_classe(df_fundo, df_classe)


def _merge_fundo_classe(df_fundo: pd.DataFrame, df_classe: pd.DataFrame) -> pd.DataFrame:
    """Merge registro_fundo + registro_classe para ter Gestor por CNPJ_Classe."""
    if df_fundo.empty or df_classe.empty:
        return pd.DataFrame()

    # Seleciona colunas do fundo (gestor, admin)
    fundo_cols = ["ID_Registro_Fundo", "Gestor", "CPF_CNPJ_Gestor", "Administrador",
                  "CNPJ_Administrador", "Situacao"]
    fundo_cols = [c for c in fundo_cols if c in df_fundo.columns]
    df_f = df_fundo[fundo_cols].copy()

    # Filtra fundos ativos
    if "Situacao" in df_f.columns:
        df_f = df_f[df_f["Situacao"].str.contains("Funcionamento", case=False, na=False)]

    # Seleciona colunas da classe
    classe_cols = ["ID_Registro_Fundo", "CNPJ_Classe", "Denominacao_Social",
                   "Tipo_Classe", "Classificacao", "Classificacao_Anbima",
                   "Patrimonio_Liquido", "Publico_Alvo", "Condominio"]
    classe_cols = [c for c in classe_cols if c in df_classe.columns]
    df_c = df_classe[classe_cols].copy()

    # Merge: classe → fundo (via ID_Registro_Fundo)
    merged = df_c.merge(df_f, on="ID_Registro_Fundo", how="left")

    # Rename para padrão interno
    rename_map = {
        "CNPJ_Classe": "CNPJ_FUNDO",
        "Denominacao_Social": "DENOM_SOCIAL",
        "Gestor": "GESTOR",
        "CPF_CNPJ_Gestor": "CPF_CNPJ_GESTOR",
        "Administrador": "ADMIN",
        "CNPJ_Administrador": "CNPJ_ADMIN",
        "Patrimonio_Liquido": "VL_PATRIM_LIQ",
        "Classificacao_Anbima": "CLASSE_ANBIMA",
        "Classificacao": "CLASSE",
        "Publico_Alvo": "PUBLICO_ALVO",
        "Situacao": "SIT",
    }
    rename_map = {k: v for k, v in rename_map.items() if k in merged.columns}
    merged = merged.rename(columns=rename_map)

    return merged


def classify_asset(row: pd.Series, block: str) -> str | None:
    """Classifica um ativo em NC, CRI, CRA, CPR-F ou DEBENTURE."""
    tp_ativo = str(row.get("TP_ATIVO", "")).lower()
    tp_aplic = str(row.get("TP_APLIC", "")).lower()
    ds_ativo = str(row.get("DS_ATIVO", "")).upper()

    for asset_code, config in ASSET_TYPES.items():
        if block not in config["blocks"]:
            continue
        # Checa TP_ATIVO
        for pattern in config["tp_ativo_contains"]:
            if pattern.lower() in tp_ativo or pattern.lower() in tp_aplic:
                return asset_code
        # Fallback: checa DS_ATIVO
        for pattern in config["ds_ativo_contains"]:
            if pattern in ds_ativo:
                return asset_code
    return None


def extract_fixed_income_positions(
    cda_frames: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Extrai posições em renda fixa estruturada dos blocos CDA."""
    all_positions = []

    for block, df in cda_frames.items():
        if block not in ("BLC_4", "BLC_6", "BLC_8"):
            continue
        if df.empty:
            continue

        df = df.copy()
        df["_block"] = block
        df["_asset_type"] = df.apply(lambda r: classify_asset(r, block), axis=1)

        # Filtra apenas os tipos que nos interessam
        filtered = df[df["_asset_type"].notna()].copy()
        if filtered.empty:
            continue

        # Normaliza colunas
        cols = {
            "CNPJ_FUNDO_CLASSE": "cnpj_fundo",
            "DENOM_SOCIAL": "nome_fundo",
            "DT_COMPTC": "dt_competencia",
            "TP_APLIC": "tp_aplicacao",
            "TP_ATIVO": "tp_ativo",
            "VL_MERC_POS_FINAL": "vl_posicao",
            "VL_CUSTO_POS_FINAL": "vl_custo",
            "QT_POS_FINAL": "qt_posicao",
            "_asset_type": "tipo_ativo",
            "_block": "bloco",
        }

        # Colunas extras do BLC_6
        if block == "BLC_6":
            cols.update({
                "CPF_CNPJ_EMISSOR": "cnpj_emissor",
                "EMISSOR": "emissor",
                "DT_VENC": "dt_vencimento",
                "DS_INDEXADOR_POSFX": "indexador",
                "PR_INDEXADOR_POSFX": "pct_indexador",
                "PR_CUPOM_POSFX": "spread",
                "PR_TAXA_PREFX": "taxa_pre",
            })

        # Colunas extras do BLC_8
        if block == "BLC_8":
            cols.update({
                "DS_ATIVO": "descricao_ativo",
                "CPF_CNPJ_EMISSOR": "cnpj_emissor",
                "EMISSOR": "emissor",
            })

        # Colunas extras do BLC_4
        if block == "BLC_4":
            cols.update({
                "DS_ATIVO": "descricao_ativo",
                "CD_ISIN": "isin",
                "CD_ATIVO": "cd_ativo",
            })

        available_cols = {k: v for k, v in cols.items() if k in filtered.columns}
        renamed = filtered[list(available_cols.keys())].rename(columns=available_cols)
        all_positions.append(renamed)

    if not all_positions:
        return pd.DataFrame()

    result = pd.concat(all_positions, ignore_index=True)

    # Converte valores numéricos
    for col in ["vl_posicao", "vl_custo", "qt_posicao", "pct_indexador", "spread", "taxa_pre"]:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")

    return result


def _normalize_cnpj(s: str) -> str:
    """Remove pontuação do CNPJ: 00.346.750/0001-10 → 00346750000110"""
    return "".join(c for c in str(s) if c.isdigit()).zfill(14)


def enrich_with_cadastro(positions: pd.DataFrame, cadastro: pd.DataFrame) -> pd.DataFrame:
    """Enriquece posições com dados do cadastro RCVM175 (gestor, admin, classe, PL)."""
    if positions.empty or cadastro.empty:
        return positions

    # Normaliza CNPJ para merge (remove formatação)
    positions["_cnpj_norm"] = positions["cnpj_fundo"].apply(_normalize_cnpj)
    cadastro["_cnpj_norm"] = cadastro["CNPJ_FUNDO"].apply(_normalize_cnpj)

    # Seleciona colunas relevantes do cadastro
    cad_cols = [
        "CNPJ_FUNDO", "CLASSE", "CLASSE_ANBIMA", "GESTOR", "CPF_CNPJ_GESTOR",
        "ADMIN", "CNPJ_ADMIN", "VL_PATRIM_LIQ", "PUBLICO_ALVO",
    ]
    cad_available = [c for c in cad_cols if c in cadastro.columns]
    cad_slim = cadastro[cad_available].copy()

    # Remove duplicatas (pega o mais recente por CNPJ normalizado)
    cad_slim["_cnpj_norm"] = cadastro["_cnpj_norm"]
    cad_slim = cad_slim.drop_duplicates(subset=["_cnpj_norm"], keep="last")

    # Rename para merge
    cad_rename = {
        "CNPJ_FUNDO": "_cnpj_fundo_orig",
        "CLASSE": "classe_fundo",
        "CLASSE_ANBIMA": "classe_anbima",
        "GESTOR": "gestora",
        "CPF_CNPJ_GESTOR": "cnpj_gestora",
        "ADMIN": "administrador",
        "CNPJ_ADMIN": "cnpj_admin",
        "VL_PATRIM_LIQ": "pl_fundo",
        "PUBLICO_ALVO": "publico_alvo",
    }
    cad_rename = {k: v for k, v in cad_rename.items() if k in cad_slim.columns}
    cad_slim = cad_slim.rename(columns=cad_rename)

    enriched = positions.merge(cad_slim, on="_cnpj_norm", how="left")
    enriched = enriched.drop(columns=["_cnpj_norm", "_cnpj_fundo_orig"], errors="ignore")

    # Log de cobertura
    if "gestora" in enriched.columns:
        coverage = enriched["gestora"].notna().sum() / len(enriched) * 100
        print(f"  ✓ Cobertura de gestora: {coverage:.1f}% ({enriched['gestora'].nunique()} gestoras únicas)")

    return enriched


def _build_ticker_map(cda_frames: dict[str, pd.DataFrame]) -> dict[str, str]:
    """Constrói mapeamento ticker_base → nome da empresa usando dados BLC_4 (ações)."""
    ticker_map = {}
    blc4 = cda_frames.get("BLC_4", pd.DataFrame())
    if blc4.empty or "CD_ATIVO" not in blc4.columns:
        return ticker_map

    # Ações têm DS_ATIVO com nome da empresa (ex: "ITAUUNIBANCO ON  EJ  N1")
    acoes = blc4[blc4["TP_APLIC"] == "Ações"].copy()
    if not acoes.empty:
        acoes["_ticker_base"] = acoes["CD_ATIVO"].str.extract(r"^([A-Z]{3,6})\d")
        for _, row in acoes[["_ticker_base", "DS_ATIVO"]].dropna().drop_duplicates("_ticker_base").iterrows():
            name = str(row["DS_ATIVO"]).strip()
            # Limpa sufixos de classe de ação (ON, PN, UNT, etc.)
            for suffix in [" ON ", " PN ", " UNT ", "  ON", "  PN", " EJ ", " ED ", "  N1", "  N2", "  NM", "  MB"]:
                idx = name.find(suffix)
                if idx > 0:
                    name = name[:idx].strip()
                    break
            ticker_map[row["_ticker_base"]] = name

    return ticker_map


def enrich_devedor(positions: pd.DataFrame, cda_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Enriquece posições BLC_4 com dados do devedor (emissor real da debênture).

    Para BLC_4: extrai ticker de CD_ATIVO e mapeia para nome do devedor.
    Para NC/Debênture em BLC_8: emissor já É o devedor.
    Para CRA/CRI em BLC_6: emissor é a securitizadora (devedor não disponível na CDA).
    """
    if positions.empty:
        return positions

    positions = positions.copy()

    # Inicializa colunas
    if "devedor" not in positions.columns:
        positions["devedor"] = None
    if "ticker_devedor" not in positions.columns:
        positions["ticker_devedor"] = None

    # Para BLC_6/BLC_8 com emissor: diferencia securitizadora vs devedor
    if "emissor" not in positions.columns:
        positions["emissor"] = None

    # NC e Debênture: emissor = devedor direto
    mask_devedor_direto = (
        positions["emissor"].notna()
        & positions["tipo_ativo"].isin(["NC", "DEBENTURE"])
        & (positions["bloco"].isin(["BLC_8", "BLC_6"]))
    )
    positions.loc[mask_devedor_direto, "devedor"] = positions.loc[mask_devedor_direto, "emissor"]

    # CRA/CRI: emissor é securitizadora, devedor é cedente (não disponível)
    mask_securitizadora = (
        positions["emissor"].notna()
        & positions["tipo_ativo"].isin(["CRA", "CRI", "CPR-F"])
    )
    positions.loc[mask_securitizadora, "devedor"] = "Cedente não identificado (via " + positions.loc[mask_securitizadora, "emissor"].astype(str) + ")"

    # Para BLC_4: usar CD_ATIVO para extrair devedor
    if "cd_ativo" in positions.columns:
        mask_blc4 = positions["bloco"] == "BLC_4"
        blc4_pos = positions.loc[mask_blc4]

        if not blc4_pos.empty:
            # Extrai ticker base (ex: CSAN33 → CSAN)
            positions.loc[mask_blc4, "ticker_devedor"] = (
                blc4_pos["cd_ativo"].str.extract(r"^([A-Z]{3,6})\d", expand=False)
            )

            # Tenta mapear ticker → nome
            ticker_map = _build_ticker_map(cda_frames)
            if ticker_map:
                positions.loc[mask_blc4, "devedor"] = (
                    positions.loc[mask_blc4, "ticker_devedor"].map(ticker_map)
                )
                mapped = positions.loc[mask_blc4, "devedor"].notna().sum()
                total_blc4 = mask_blc4.sum()
                print(f"  ✓ Devedor BLC_4: {mapped}/{total_blc4} mapeados ({mapped/total_blc4*100:.1f}%)")

            # Fallback: usa ticker como nome se não mapeado
            no_name = mask_blc4 & positions["devedor"].isna() & positions["ticker_devedor"].notna()
            positions.loc[no_name, "devedor"] = positions.loc[no_name, "ticker_devedor"]

    devedor_count = positions["devedor"].notna().sum()
    print(f"  ✓ Devedor total: {devedor_count}/{len(positions)} ({devedor_count/len(positions)*100:.1f}%)")

    return positions


def run_ingestion(n_months: int = 3) -> pd.DataFrame:
    """Pipeline completo de ingestão."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Baixa cadastro
    cadastro = download_cadastro()
    print(f"  ✓ Cadastro: {len(cadastro)} fundos")

    # 2. Baixa CDA dos últimos meses
    months = get_target_months(n_months)
    print(f"\n📅 Meses alvo: {', '.join(months)}")

    all_frames = {}
    for month in months:
        frames = download_cda_month(month)
        for block, df in frames.items():
            all_frames[block] = pd.concat(
                [all_frames.get(block, pd.DataFrame()), df],
                ignore_index=True,
            )

    if not all_frames:
        print("⚠ Nenhum dado CDA encontrado.")
        return pd.DataFrame()

    # 3. Extrai posições de renda fixa
    print("\n🔍 Extraindo posições em renda fixa estruturada...")
    positions = extract_fixed_income_positions(all_frames)
    print(f"  ✓ {len(positions)} posições encontradas")

    if positions.empty:
        return positions

    # 4. Enriquece com cadastro
    print("\n🏢 Enriquecendo com dados de gestoras...")
    enriched = enrich_with_cadastro(positions, cadastro)

    # 5. Enriquece com dados do devedor
    print("\n🏭 Identificando devedores...")
    enriched = enrich_devedor(enriched, all_frames)

    # 6. Salva cache
    cache_path = DATA_DIR / "positions_enriched.csv"
    enriched.to_csv(cache_path, index=False)
    print(f"\n💾 Cache salvo: {cache_path}")

    return enriched


if __name__ == "__main__":
    df = run_ingestion()
    if not df.empty:
        print(f"\n✅ Total: {len(df)} posições")
        print(f"   Tipos: {df['tipo_ativo'].value_counts().to_dict()}")
        print(f"   Gestoras únicas: {df['gestora'].nunique() if 'gestora' in df.columns else 'N/A'}")
