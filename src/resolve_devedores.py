#!/usr/bin/env python3
"""
Resolve CNPJ → razão social para devedores/cedentes de CRI/CRA.
Usa dados CVM (cvm_cedentes_devedores.csv) + BrasilAPI para nomes.
Gera: data/devedor_mapping.json  (CNPJ → nome)
Atualiza: data/positions_enriched.csv (coluna devedor com nome real)
"""
import json
import ssl
import time
import urllib.request
import urllib.error
import pandas as pd
from pathlib import Path

# SSL context for BrasilAPI
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
MAPPING_FILE = DATA_DIR / "devedor_mapping.json"
CED_FILE = DATA_DIR / "cvm_cedentes_devedores.csv"
POS_FILE = DATA_DIR / "positions_enriched.csv"
GERAIS_FILE = DATA_DIR / "cvm_gerais_cri_cra.csv"
CLASSES_FILE = DATA_DIR / "cvm_classes_cri_cra.csv"


def _norm_cnpj(s) -> str:
    if pd.isna(s):
        return ""
    return str(s).replace(".", "").replace("/", "").replace("-", "").strip().zfill(14)


def _fmt_cnpj(s: str) -> str:
    """00000000000100 -> 00.000.000/0001-00"""
    s = s.zfill(14)
    return f"{s[:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:14]}"


def load_mapping() -> dict:
    if MAPPING_FILE.exists():
        with open(MAPPING_FILE) as f:
            return json.load(f)
    return {}


def save_mapping(mapping: dict):
    with open(MAPPING_FILE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def resolve_cnpj_brasilapi(cnpj: str) -> str | None:
    """Consulta BrasilAPI para obter razão social."""
    clean = _norm_cnpj(cnpj)
    url = f"https://brasilapi.com.br/api/cnpj/v1/{clean}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10, context=_SSL_CTX) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("razao_social", "")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        if e.code == 429:
            time.sleep(2)
            return resolve_cnpj_brasilapi(cnpj)
        return None
    except Exception:
        return None


def get_all_devedor_cnpjs() -> list[str]:
    """Retorna lista de CNPJs únicos de devedores+cedentes do CVM."""
    ced = pd.read_csv(CED_FILE, low_memory=False)

    all_cnpjs = set()
    for cnpj in ced["CNPJ"].dropna():
        c = _norm_cnpj(cnpj)
        if c and c != "00000000000000" and len(c) == 14:
            all_cnpjs.add(c)

    return sorted(all_cnpjs)


def resolve_all(batch_size: int = 50, delay: float = 0.3):
    """Resolve todos os CNPJs pendentes via BrasilAPI."""
    mapping = load_mapping()
    all_cnpjs = get_all_devedor_cnpjs()

    pending = [c for c in all_cnpjs if c not in mapping]
    print(f"CNPJs: {len(all_cnpjs)} total, {len(mapping)} já resolvidos, {len(pending)} pendentes")

    if not pending:
        print("Nenhum CNPJ pendente.")
        return mapping

    resolved = 0
    failed = 0
    for i, cnpj in enumerate(pending):
        name = resolve_cnpj_brasilapi(cnpj)
        if name:
            mapping[cnpj] = name
            resolved += 1
        else:
            mapping[cnpj] = ""  # Mark as tried
            failed += 1

        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(pending)} — {resolved} ok, {failed} falhas")
            save_mapping(mapping)

        time.sleep(delay)

    save_mapping(mapping)
    print(f"Resolução completa: {resolved} ok, {failed} falhas")
    return mapping


def build_certificate_devedor_map() -> pd.DataFrame:
    """Cria mapa: (CNPJ_Emissora, Codigo_Certificado) → lista de devedores com nome."""
    ced = pd.read_csv(CED_FILE, low_memory=False)
    mapping = load_mapping()

    devedores = ced[ced["Tipo"] == "Devedor"].copy()
    devedores["cnpj_clean"] = devedores["CNPJ"].apply(_norm_cnpj)
    devedores["emissora_clean"] = devedores["CNPJ_Emissora"].apply(_norm_cnpj)

    # Resolve names
    devedores["nome_devedor"] = devedores["cnpj_clean"].map(mapping).fillna("")

    # Keep latest reference date per certificate+devedor
    devedores = (
        devedores.sort_values("Data_Referencia", ascending=False)
        .drop_duplicates(["emissora_clean", "Codigo_Identificacao_Certificado", "cnpj_clean"], keep="first")
    )

    return devedores


def _smart_clean_doc(s) -> tuple[str, str]:
    """Limpa CNPJ/CPF e classifica como 'cnpj', 'cpf' ou 'invalid'."""
    if pd.isna(s):
        return "", "invalid"
    s = str(s).strip()
    if s.endswith(".0"):
        s = s[:-2]
    s = s.replace(".", "").replace("/", "").replace("-", "").replace(" ", "")
    if not s or s == "0" or not s.isnumeric():
        return "", "invalid"
    if len(s) <= 11:
        return s.zfill(11), "cpf"
    if len(s) <= 14:
        return s.zfill(14), "cnpj"
    return "", "invalid"


def enrich_positions_with_devedores():
    """Enriquece positions_enriched.csv com nome real do devedor de CRI/CRA."""
    pos = pd.read_csv(POS_FILE, low_memory=False)
    classes = pd.read_csv(CLASSES_FILE, low_memory=False)
    ced = pd.read_csv(CED_FILE, low_memory=False)
    mapping = load_mapping()

    cri_cra_mask = pos["tipo_ativo"].isin(["CRI", "CRA", "CPR-F"])
    print(f"CRI/CRA/CPR-F posições: {cri_cra_mask.sum()}")

    # --- Step 1: Build certificate → devedor map ---
    # Clean CVM cedentes/devedores
    devedores = ced[ced["Tipo"] == "Devedor"].copy()
    devedores["emissora_clean"] = devedores["CNPJ_Emissora"].apply(_norm_cnpj)
    devedores[["doc_clean", "doc_type"]] = pd.DataFrame(
        devedores["CNPJ"].apply(_smart_clean_doc).tolist(), index=devedores.index
    )

    # Resolve names: CNPJ via mapping, CPF as "Pessoa Física (CPF ***)"
    def resolve_name(row):
        if row["doc_type"] == "cnpj":
            return mapping.get(row["doc_clean"], "")
        elif row["doc_type"] == "cpf":
            cpf = row["doc_clean"]
            return f"Pessoa Física (CPF {cpf[:3]}.***.**{cpf[9:]}-{cpf[9:]})"
        return ""

    devedores["nome_devedor"] = devedores.apply(resolve_name, axis=1)

    # Dedup: latest per certificate+devedor
    devedores = (
        devedores.sort_values("Data_Referencia", ascending=False)
        .drop_duplicates(["emissora_clean", "Codigo_Identificacao_Certificado", "doc_clean"], keep="first")
    )

    # Aggregate per certificate: list of devedor names
    dev_agg = (
        devedores.groupby(["emissora_clean", "Codigo_Identificacao_Certificado"])
        .agg(
            devedores_nome=("nome_devedor", lambda x: " | ".join(sorted(set(n for n in x if n)))),
            n_devedores=("doc_clean", "nunique"),
        )
        .reset_index()
    )
    print(f"Certificados com devedor: {len(dev_agg)}")

    # --- Step 2: Match positions → certificates via (cnpj_emissor, dt_vencimento) ---
    classes["emissora_clean"] = classes["CNPJ_Emissora"].apply(_norm_cnpj)
    pos["emissora_clean"] = pos["cnpj_emissor"].apply(_norm_cnpj)

    # Join positions to certificates
    cert_lookup = classes[["emissora_clean", "Codigo_Identificacao_Certificado", "Data_Vencimento"]].drop_duplicates()
    pos_idx = pos[cri_cra_mask].index

    merged = pos.loc[pos_idx].merge(
        cert_lookup,
        left_on=["emissora_clean", "dt_vencimento"],
        right_on=["emissora_clean", "Data_Vencimento"],
        how="left",
    )

    # Join certificate → devedores
    merged = merged.merge(
        dev_agg[["emissora_clean", "Codigo_Identificacao_Certificado", "devedores_nome", "n_devedores"]],
        on=["emissora_clean", "Codigo_Identificacao_Certificado"],
        how="left",
    )

    # Dedup: keep row with most devedor info per original position
    merged["_dev_len"] = merged["devedores_nome"].fillna("").str.len()
    merged = merged.sort_values("_dev_len", ascending=False)

    # Map original index → best devedor name
    seen = set()
    devedor_map = {}
    for _, row in merged.iterrows():
        orig_cols = (row.get("cnpj_fundo", ""), row.get("cnpj_emissor", ""),
                     row.get("dt_vencimento", ""), row.get("vl_posicao", 0))
        if orig_cols in seen:
            continue
        seen.add(orig_cols)
        nome = row.get("devedores_nome", "")
        if pd.notna(nome) and nome:
            devedor_map[orig_cols] = nome

    # --- Step 3: Update positions ---
    updated = 0
    for idx in pos_idx:
        row = pos.loc[idx]
        key = (row.get("cnpj_fundo", ""), row.get("cnpj_emissor", ""),
               row.get("dt_vencimento", ""), row.get("vl_posicao", 0))
        if key in devedor_map:
            pos.at[idx, "devedor"] = devedor_map[key]
            updated += 1

    # Fallback: positions still without devedor
    still_missing = pos.loc[pos_idx][pos.loc[pos_idx, "devedor"].isna() | (pos.loc[pos_idx, "devedor"] == "")].index
    for idx in still_missing:
        emissor = pos.at[idx, "emissor"]
        if pd.notna(emissor):
            pos.at[idx, "devedor"] = f"Cedente via {emissor}"

    print(f"Posições atualizadas com devedor real: {updated}/{len(pos_idx)}")
    print(f"Fallback (cedente via securitizadora): {len(still_missing)}")

    # Save
    pos = pos.drop(columns=["emissora_clean"], errors="ignore")
    pos.to_csv(POS_FILE, index=False)
    print(f"Salvo: {POS_FILE}")

    return pos


if __name__ == "__main__":
    import sys

    if "--resolve" in sys.argv:
        resolve_all()
    elif "--enrich" in sys.argv:
        enrich_positions_with_devedores()
    elif "--status" in sys.argv:
        mapping = load_mapping()
        all_cnpjs = get_all_devedor_cnpjs()
        resolved = sum(1 for c in all_cnpjs if mapping.get(c))
        print(f"CNPJs: {len(all_cnpjs)} | Resolvidos: {resolved} | Pendentes: {len(all_cnpjs) - len(mapping)}")
    else:
        print("Uso:")
        print("  python3 resolve_devedores.py --resolve   # Resolve CNPJs via BrasilAPI")
        print("  python3 resolve_devedores.py --enrich    # Enriquece positions com devedores")
        print("  python3 resolve_devedores.py --status    # Status da resolução")
