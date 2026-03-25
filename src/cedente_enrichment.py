"""
ZYN Sales Intelligence — Enriquecimento de Cedentes/Devedores
Baixa informes mensais de CRI/CRA da CVM para identificar cedentes e devedores.
"""
import io
import zipfile
from pathlib import Path

import pandas as pd
import requests

from config.settings import DATA_DIR

CVM_SECURIT_BASE = "https://dados.cvm.gov.br/dados/SECURIT/DOC"
CVM_CIA_ABERTA_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"


def _normalize_cnpj(s) -> str:
    return "".join(c for c in str(s) if c.isdigit()).zfill(14)


def download_inf_mensal(years: list[str] = None) -> dict[str, pd.DataFrame]:
    """Baixa informes mensais de CRI e CRA (cedente/devedor, classe, geral)."""
    if years is None:
        from datetime import datetime
        y = datetime.now().year
        years = [str(y - 1), str(y)]

    all_cedentes = []
    all_classes = []
    all_gerais = []

    for tipo in ["CRI", "CRA"]:
        for year in years:
            url = f"{CVM_SECURIT_BASE}/INF_MENSAL_{tipo}/DADOS/inf_mensal_{tipo.lower()}_{year}.zip"
            print(f"  📥 {tipo} {year}...", end=" ")
            try:
                resp = requests.get(url, timeout=120)
                if resp.status_code != 200:
                    print(f"⚠ {resp.status_code}")
                    continue
            except Exception as e:
                print(f"⚠ {e}")
                continue

            zf = zipfile.ZipFile(io.BytesIO(resp.content))

            ced_file = f"inf_mensal_{tipo.lower()}_cedente_devedor_{year}.csv"
            if ced_file in zf.namelist():
                with zf.open(ced_file) as f:
                    ced = pd.read_csv(f, sep=";", encoding="latin1", low_memory=False)
                ced["_tipo_certificado"] = tipo
                all_cedentes.append(ced)

            cls_file = f"inf_mensal_{tipo.lower()}_classe_{year}.csv"
            if cls_file in zf.namelist():
                with zf.open(cls_file) as f:
                    cls = pd.read_csv(f, sep=";", encoding="latin1", low_memory=False)
                cls["_tipo_certificado"] = tipo
                all_classes.append(cls)

            gen_file = f"inf_mensal_{tipo.lower()}_geral_{year}.csv"
            if gen_file in zf.namelist():
                with zf.open(gen_file) as f:
                    gen = pd.read_csv(f, sep=";", encoding="latin1", low_memory=False)
                gen["_tipo_certificado"] = tipo
                all_gerais.append(gen)

            print(f"✓")

    return {
        "cedentes": pd.concat(all_cedentes, ignore_index=True) if all_cedentes else pd.DataFrame(),
        "classes": pd.concat(all_classes, ignore_index=True) if all_classes else pd.DataFrame(),
        "gerais": pd.concat(all_gerais, ignore_index=True) if all_gerais else pd.DataFrame(),
    }


def build_devedor_mapping(inf_mensal: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Constrói mapeamento: Codigo_Certificado → devedor CNPJ + nome.

    Retorna DataFrame com:
      - Codigo_Identificacao_Certificado
      - CNPJ_Emissora (securitizadora)
      - devedor_cnpj
      - devedor_nome (se disponível)
      - cedente_cnpj
      - tipo_certificado (CRI/CRA)
    """
    cedentes_df = inf_mensal.get("cedentes", pd.DataFrame())
    gerais_df = inf_mensal.get("gerais", pd.DataFrame())

    if cedentes_df.empty:
        return pd.DataFrame()

    # Separa devedores e cedentes
    devedores = cedentes_df[cedentes_df["Tipo"] == "Devedor"].copy()
    cedentes = cedentes_df[cedentes_df["Tipo"] == "Cedente"].copy()

    # Pega o devedor principal (maior %) por certificado
    devedores = (
        devedores.sort_values("Percentual", ascending=False)
        .drop_duplicates(["Codigo_Identificacao_Certificado"], keep="first")
    )
    devedores = devedores.rename(columns={"CNPJ": "devedor_cnpj"})
    devedores["devedor_cnpj_norm"] = devedores["devedor_cnpj"].apply(
        lambda x: _normalize_cnpj(x) if pd.notna(x) else None
    )

    # Pega o cedente principal
    cedentes = (
        cedentes.sort_values("Percentual", ascending=False)
        .drop_duplicates(["Codigo_Identificacao_Certificado"], keep="first")
    )
    cedentes = cedentes.rename(columns={"CNPJ": "cedente_cnpj"})

    # Merge devedor + cedente
    mapping = devedores[
        ["CNPJ_Emissora", "Codigo_Identificacao_Certificado", "devedor_cnpj",
         "devedor_cnpj_norm", "_tipo_certificado"]
    ].merge(
        cedentes[["Codigo_Identificacao_Certificado", "cedente_cnpj"]],
        on="Codigo_Identificacao_Certificado",
        how="left",
    )

    # Tenta resolver nomes via base de companhias abertas
    print("  📥 Baixando cadastro de companhias abertas...")
    try:
        resp = requests.get(CVM_CIA_ABERTA_URL, timeout=30)
        cias = pd.read_csv(
            io.StringIO(resp.content.decode("latin1")),
            sep=";", low_memory=False,
        )
        cias["_cnpj_norm"] = cias["CNPJ_CIA"].apply(_normalize_cnpj)
        cia_names = cias[["_cnpj_norm", "DENOM_SOCIAL"]].drop_duplicates("_cnpj_norm")

        mapping = mapping.merge(
            cia_names, left_on="devedor_cnpj_norm", right_on="_cnpj_norm", how="left"
        )
        mapping = mapping.rename(columns={"DENOM_SOCIAL": "devedor_nome"})
        mapping = mapping.drop(columns=["_cnpj_norm"], errors="ignore")

        resolved = mapping["devedor_nome"].notna().sum()
        print(f"  ✓ Nomes resolvidos: {resolved}/{len(mapping)}")
    except Exception as e:
        print(f"  ⚠ Erro ao baixar companhias: {e}")
        mapping["devedor_nome"] = None

    # Para certificados com devedor CNPJ zero, marca como pulverizado
    zero_mask = mapping["devedor_cnpj_norm"] == "00000000000000"
    mapping.loc[zero_mask, "devedor_nome"] = "Carteira Pulverizada"

    # Adiciona nome da securitizadora do geral
    if not gerais_df.empty:
        sec_names = gerais_df[["CNPJ_Emissora", "Companhia_Emissora"]].drop_duplicates("CNPJ_Emissora")
        mapping = mapping.merge(sec_names, on="CNPJ_Emissora", how="left")

    return mapping


def enrich_positions_with_cedentes(
    positions: pd.DataFrame, devedor_map: pd.DataFrame
) -> pd.DataFrame:
    """Enriquece posições de CRI/CRA com dados de cedente/devedor.

    Matching via CNPJ_Emissora (securitizadora) + padrões no descricao_ativo/ISIN.
    """
    if positions.empty or devedor_map.empty:
        return positions

    positions = positions.copy()

    # Normaliza CNPJ da securitizadora nas posições
    if "cnpj_emissor" in positions.columns:
        positions["_sec_cnpj_norm"] = positions["cnpj_emissor"].apply(
            lambda x: _normalize_cnpj(x) if pd.notna(x) else None
        )

    # Normaliza CNPJ da securitizadora no mapping
    devedor_map["_sec_cnpj_norm"] = devedor_map["CNPJ_Emissora"].apply(
        lambda x: _normalize_cnpj(x) if pd.notna(x) else None
    )

    # Estratégia 1: Match por Codigo_Identificacao_Certificado
    # Alguns BLC_8 descricao_ativo contêm o código ISIN/CETIP
    # Ex: "CRI / ISIN: BRRBRACRIUX0 / 15/10/2030 / 02773542000122"
    import re
    isin_pattern = re.compile(r"(BR[A-Z0-9]{10})")

    mask_cri_cra = positions["tipo_ativo"].isin(["CRI", "CRA", "CPR-F"])
    updated = 0

    # Build lookup dict from devedor_map
    cert_to_devedor = {}
    for _, row in devedor_map.iterrows():
        cert = row["Codigo_Identificacao_Certificado"]
        nome = row.get("devedor_nome")
        cnpj = row.get("devedor_cnpj")
        if pd.notna(nome) and nome != "Carteira Pulverizada":
            cert_to_devedor[cert] = nome
        elif pd.notna(cnpj) and str(cnpj) not in ("0", "0.0", "nan"):
            cert_to_devedor[cert] = f"CNPJ {_normalize_cnpj(cnpj)}"

    # Try matching descriptions
    for idx in positions[mask_cri_cra].index:
        desc = str(positions.at[idx, "descricao_ativo"]) if pd.notna(positions.at[idx, "descricao_ativo"]) else ""
        isin_col = str(positions.at[idx, "isin"]) if "isin" in positions.columns and pd.notna(positions.at[idx, "isin"]) else ""

        # Extract potential certificate codes from description
        matches = isin_pattern.findall(desc + " " + isin_col)
        for m in matches:
            if m in cert_to_devedor:
                positions.at[idx, "devedor"] = cert_to_devedor[m]
                updated += 1
                break

    # Estratégia 2: Para posições sem match, agrupa por securitizadora
    # e mostra o devedor mais frequente daquela securitizadora
    still_missing = mask_cri_cra & (
        positions["devedor"].isna()
        | positions["devedor"].str.startswith("Cedente via", na=False)
    )

    if still_missing.any() and "_sec_cnpj_norm" in positions.columns:
        # Build securitizadora -> most common devedor mapping
        sec_to_devedor = {}
        for sec_cnpj, group in devedor_map.groupby("_sec_cnpj_norm"):
            named = group[group["devedor_nome"].notna() & (group["devedor_nome"] != "Carteira Pulverizada")]
            if not named.empty:
                # Count occurrences of each devedor
                top = named["devedor_nome"].value_counts().head(3)
                if len(top) == 1:
                    sec_to_devedor[sec_cnpj] = top.index[0]
                else:
                    sec_to_devedor[sec_cnpj] = " | ".join(f"{n} ({c}x)" for n, c in top.items())

        for idx in positions[still_missing].index:
            sec = positions.at[idx, "_sec_cnpj_norm"]
            if sec and sec in sec_to_devedor:
                positions.at[idx, "devedor"] = sec_to_devedor[sec]
                updated += 1

    positions = positions.drop(columns=["_sec_cnpj_norm"], errors="ignore")
    print(f"  ✓ Posições CRI/CRA enriquecidas: {updated}")
    return positions


def run_cedente_enrichment(positions: pd.DataFrame) -> pd.DataFrame:
    """Pipeline completo de enriquecimento de cedentes/devedores."""
    print("\n🏭 Enriquecimento de Cedentes/Devedores CRI/CRA")

    # 1. Baixa informes mensais
    print("  Baixando informes mensais CVM...")
    inf_mensal = download_inf_mensal()

    if inf_mensal["cedentes"].empty:
        print("  ⚠ Sem dados de cedentes/devedores")
        return positions

    print(f"  ✓ Cedentes: {len(inf_mensal['cedentes'])} registros")
    print(f"  ✓ Classes: {len(inf_mensal['classes'])} séries")
    print(f"  ✓ Gerais: {len(inf_mensal['gerais'])} emissões")

    # 2. Constrói mapping
    print("  Construindo mapeamento devedor...")
    devedor_map = build_devedor_mapping(inf_mensal)

    if devedor_map.empty:
        return positions

    # 3. Salva mapping
    devedor_map.to_csv(DATA_DIR / "devedor_mapping.csv", index=False)
    print(f"  💾 Mapping salvo: {len(devedor_map)} certificados")

    # 4. Enriquece posições
    print("  Enriquecendo posições...")
    enriched = enrich_positions_with_cedentes(positions, devedor_map)

    return enriched
