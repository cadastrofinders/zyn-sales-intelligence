#!/usr/bin/env python3
"""
Resolve beneficiários finais (devedores reais) de CRI/CRA/CPR-F.

Pipeline de 10 passes para máxima cobertura:
  1. Certificate exact match via (emissora_cnpj, vencimento) → cedentes CVM
  2. ISIN extraído de descricao_ativo → certificado → cedentes ou Nome_Emissao
  3. CETIP ticker (CRI:XXXX:DDMMYY) → certificado → Nome_Emissao
  4. Datas extraídas de descricao (DDMMYY, DD/MM/YYYY) → fuzzy match classes → Nome_Emissao
  5. Company name extraído de descricao (ex: "CRI Saint Francis", "4559/CRI GAFISA")
  6. Nome_Emissao fuzzy date ±30d (usando dt_vencimento do position, se existir)
  7. Emissora aggregate (top 3 devedores por securitizadora)
  8. Entidade direta (cnpj_emissor que não é securitizadora)
  9. B3 Ticker no descricao_ativo
 10. Fallback: "Cedente via {securitizadora}"

Gera: data/devedor_mapping.json  (CNPJ → razão social)
Atualiza: data/positions_enriched.csv (coluna devedor)
"""
import json
import re
import ssl
import time
import urllib.request
import urllib.error
from datetime import timedelta

import pandas as pd
from pathlib import Path

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
MAPPING_FILE = DATA_DIR / "devedor_mapping.json"
CED_FILE = DATA_DIR / "cvm_cedentes_devedores.csv"
POS_FILE = DATA_DIR / "positions_enriched.csv"
GERAIS_FILE = DATA_DIR / "cvm_gerais_cri_cra.csv"
CLASSES_FILE = DATA_DIR / "cvm_classes_cri_cra.csv"
TICKER_FILE = DATA_DIR / "ticker_to_name.json"

_ISIN_PAT = re.compile(r"(BR[A-Z0-9]{10})")
_ISIN_DESC_PAT = re.compile(r"ISIN:\s*(BR[A-Z0-9]{8,12})")
_CETIP_PAT = re.compile(r"CRI:([A-Z]{3,8}):(\d{6})")
_DATE_DMY = re.compile(r"(\d{2}/\d{2}/\d{4})")
_DATE_6D = re.compile(r"(\d{6})")
_NAME_IN_DESC = re.compile(
    r"(?:CRI|CRA)\s+(?:/\s*)?([A-Z][A-Za-zÀ-ÿ\s&]+?)(?:\s*[-/]|\s+\d|\s*$)"
)
_NAME_PREFIXED = re.compile(
    r"\d+/(?:CRI|CRA)\s+([A-Za-zÀ-ÿ\s&]+?)(?:\s*[-/]|\s*\d)"
)

# Known securitizadora names to SKIP in name extraction
_SECURITIZADORA_NAMES = {
    "BRAZIL", "BRAZILIAN", "VIRGO", "VIRGOSEC", "CANAL", "POLO", "BSCS",
    "CASEC", "RIZA", "CIBRASEC", "BARIGUI", "GAIA", "ISEC", "BSI",
    "OPEA", "TRUE", "BARI", "CANALSEC", "ISECSEC", "PLSC", "IMWL",
    "CERT", "RECEB", "IMOB", "ISEN",
}


def _norm_cnpj(s) -> str:
    if pd.isna(s):
        return ""
    return str(s).replace(".", "").replace("/", "").replace("-", "").replace(" ", "").strip().zfill(14)


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


def _parse_6digit_date(s: str) -> pd.Timestamp | None:
    """Parse DDMMYY date string to Timestamp."""
    try:
        d, m, y = int(s[:2]), int(s[2:4]), int(s[4:6])
        if y < 50:
            y += 2000
        else:
            y += 1900
        if 1 <= m <= 12 and 1 <= d <= 31:
            return pd.Timestamp(f"{y}-{m:02d}-{d:02d}")
    except (ValueError, OverflowError):
        pass
    return None


def _extract_dates_from_desc(desc: str) -> list[pd.Timestamp]:
    """Extract all dates from description (DD/MM/YYYY and DDMMYY formats)."""
    dates = []
    # DD/MM/YYYY
    for m in _DATE_DMY.finditer(desc):
        try:
            dates.append(pd.to_datetime(m.group(1), dayfirst=True))
        except (ValueError, OverflowError):
            pass
    # 6-digit DDMMYY (between slashes or end)
    # Find sequences of exactly 6 digits surrounded by / or boundaries
    for m in re.finditer(r"(?<=[/\s])(\d{6})(?=[/\s]|$)", desc):
        dt = _parse_6digit_date(m.group(1))
        if dt:
            dates.append(dt)
    return dates


def _extract_name_from_desc(desc: str) -> str | None:
    """Extract potential company name from description."""
    # Pattern: "4559/CRI Saint Francis - 17G..."
    m = _NAME_PREFIXED.search(desc)
    if m:
        name = m.group(1).strip()
        if len(name) > 2 and name.split()[0].upper() not in _SECURITIZADORA_NAMES:
            return name

    # Pattern: "CRI GAFISA", "CRA RAIZEN"
    m = _NAME_IN_DESC.search(desc)
    if m:
        name = m.group(1).strip()
        if len(name) > 2 and name.split()[0].upper() not in _SECURITIZADORA_NAMES:
            return name

    return None


def load_mapping() -> dict:
    if MAPPING_FILE.exists():
        with open(MAPPING_FILE) as f:
            return json.load(f)
    return {}


def save_mapping(mapping: dict):
    with open(MAPPING_FILE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def resolve_cnpj_brasilapi(cnpj: str) -> str | None:
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
    ced = pd.read_csv(CED_FILE, low_memory=False)
    all_cnpjs = set()
    for cnpj in ced["CNPJ"].dropna():
        c, t = _smart_clean_doc(cnpj)
        if t == "cnpj" and c and c != "00000000000000":
            all_cnpjs.add(c)
    return sorted(all_cnpjs)


def resolve_all(delay: float = 0.3):
    """Resolve CNPJs pendentes via BrasilAPI."""
    mapping = load_mapping()
    all_cnpjs = get_all_devedor_cnpjs()
    pending = [c for c in all_cnpjs if c not in mapping]
    print(f"CNPJs: {len(all_cnpjs)} total, {len(mapping)} resolvidos, {len(pending)} pendentes")

    if not pending:
        print("Nenhum pendente.")
        return mapping

    resolved = failed = 0
    for i, cnpj in enumerate(pending):
        name = resolve_cnpj_brasilapi(cnpj)
        if name:
            mapping[cnpj] = name
            resolved += 1
        else:
            mapping[cnpj] = ""
            failed += 1
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(pending)} — {resolved} ok, {failed} falhas")
            save_mapping(mapping)
        time.sleep(delay)

    save_mapping(mapping)
    print(f"Resolução: {resolved} ok, {failed} falhas")
    return mapping


def enrich_positions_with_devedores():
    """Pipeline completo de enriquecimento: 10 passes para máxima cobertura."""
    pos = pd.read_csv(POS_FILE, low_memory=False)
    classes = pd.read_csv(CLASSES_FILE, low_memory=False)
    ced = pd.read_csv(CED_FILE, low_memory=False)
    gerais = pd.read_csv(GERAIS_FILE, low_memory=False) if GERAIS_FILE.exists() else pd.DataFrame()
    mapping = load_mapping()
    ticker_names = json.load(open(TICKER_FILE)) if TICKER_FILE.exists() else {}

    cri_mask = pos["tipo_ativo"].isin(["CRI", "CRA", "CPR-F"])
    print(f"CRI/CRA/CPR-F: {cri_mask.sum()} posições")

    # --- Normalize CNPJ columns (match by CNPJ, not by name) ---
    classes["emi_cnpj"] = classes["CNPJ_Emissora"].apply(_norm_cnpj)
    ced["emi_cnpj"] = ced["CNPJ_Emissora"].apply(_norm_cnpj)
    if not gerais.empty:
        gerais["emi_cnpj"] = gerais["CNPJ_Emissora"].apply(_norm_cnpj)
    pos["emi_cnpj"] = pos["cnpj_emissor"].apply(_norm_cnpj)
    all_emissora_cnpjs = set(classes["emi_cnpj"].unique()) | set(ced["emi_cnpj"].unique())

    # ========================================================
    # BUILD REFERENCE STRUCTURES
    # ========================================================

    # A) Certificate → devedor name (via CVM cedentes, CNPJ → BrasilAPI)
    devedores = ced[ced["Tipo"] == "Devedor"].copy()
    devedores[["doc_clean", "doc_type"]] = pd.DataFrame(
        devedores["CNPJ"].apply(_smart_clean_doc).tolist(), index=devedores.index
    )
    devedores["nome"] = devedores.apply(
        lambda r: mapping.get(r["doc_clean"], "") if r["doc_type"] == "cnpj"
        else ("Pessoa Física" if r["doc_type"] == "cpf" else ""),
        axis=1,
    )
    # Per (emissora_cnpj, certificate) → best devedor name
    cert_devedor = (
        devedores[devedores["nome"] != ""]
        .sort_values("Data_Referencia", ascending=False)
        .drop_duplicates(["emi_cnpj", "Codigo_Identificacao_Certificado"])
    )
    cert_devedor_dict = {
        (r["emi_cnpj"], r["Codigo_Identificacao_Certificado"]): r["nome"]
        for _, r in cert_devedor.iterrows()
    }
    # Also: certificate_only → devedor (ignore emissora, for ISIN matching)
    cert_only_devedor = {}
    for (emi, cert), nome in cert_devedor_dict.items():
        if cert not in cert_only_devedor:
            cert_only_devedor[cert] = nome

    # B) Certificate → Nome_Emissao (from gerais)
    cert_nome_emissao = {}
    if not gerais.empty and "Nome_Emissao" in gerais.columns:
        for _, r in gerais.iterrows():
            nome = r.get("Nome_Emissao", "")
            if pd.notna(nome) and nome and nome != "-":
                cert_nome_emissao[r["Codigo_Identificacao_Certificado"]] = str(nome).strip()

    # C) Resolve cert → best name (prefer cedentes, fallback to Nome_Emissao)
    def _cert_name(cert: str, emi_cnpj: str = "") -> str:
        if emi_cnpj:
            name = cert_devedor_dict.get((emi_cnpj, cert), "")
            if name:
                return name
        name = cert_only_devedor.get(cert, "")
        if name:
            return name
        return cert_nome_emissao.get(cert, "")

    # D) ISIN/CETIP → certificate
    isin_to_cert = {}
    cetip_to_cert = {}
    for _, r in classes.iterrows():
        cert = r["Codigo_Identificacao_Certificado"]
        isin = r.get("Codigo_ISIN")
        cetip = r.get("Codigo_CETIP")
        if pd.notna(isin) and str(isin).strip():
            isin_to_cert[str(isin).strip()] = cert
        if pd.notna(cetip) and str(cetip).strip():
            cetip_to_cert[str(cetip).strip()] = cert

    # E) (emissora_cnpj, vencimento_str) → certificate
    venc_to_cert = {}
    for _, r in classes.iterrows():
        key = (r["emi_cnpj"], str(r["Data_Vencimento"]))
        venc_to_cert[key] = r["Codigo_Identificacao_Certificado"]

    # F) Emissora CNPJ → list of (venc_date, cert) for fuzzy matching
    class_dated = classes[classes["Data_Vencimento"].notna()].copy()
    class_dated["venc_date"] = pd.to_datetime(class_dated["Data_Vencimento"], errors="coerce")
    emissora_cert_dates = {}
    for emi, group in class_dated.dropna(subset=["venc_date"]).groupby("emi_cnpj"):
        entries = []
        for _, r in group.iterrows():
            cert = r["Codigo_Identificacao_Certificado"]
            entries.append((r["venc_date"], cert))
        if entries:
            emissora_cert_dates[emi] = sorted(entries, key=lambda x: x[0])

    # G) CETIP ticker prefix → list of (cert, name) for partial matching
    cetip_prefix_map = {}  # "IMWL" → [(cert, name), ...]
    for cetip_code, cert in cetip_to_cert.items():
        # Extract prefix (letters only)
        prefix = re.match(r"^[A-Z]+", cetip_code.upper())
        if prefix:
            p = prefix.group()
            name = _cert_name(cert)
            if name:
                if p not in cetip_prefix_map:
                    cetip_prefix_map[p] = []
                cetip_prefix_map[p].append((cert, name))

    # H) Emissora aggregate: top 3 devedores by volume
    emissora_devs = {}
    for emi, group in devedores[devedores["nome"] != ""].groupby("emi_cnpj"):
        top = group["nome"].value_counts().head(3)
        emissora_devs[emi] = " | ".join(top.index) if len(top) > 0 else ""

    # ========================================================
    # RESOLUTION PASSES
    # ========================================================
    pos_idx = pos[cri_mask].index
    stats = {}

    def _is_gap(idx):
        d = pos.at[idx, "devedor"]
        if pd.isna(d) or d == "":
            return True
        return "Cedente via" in str(d) or "Cedente não" in str(d)

    def _set_devedor(idx, nome, pass_name):
        pos.at[idx, "devedor"] = nome
        stats[pass_name] = stats.get(pass_name, 0) + 1

    # --- PASS 1: Exact certificate match via (emissora_cnpj, vencimento) ---
    for idx in pos_idx:
        emi = pos.at[idx, "emi_cnpj"]
        venc = str(pos.at[idx, "dt_vencimento"]) if pd.notna(pos.at[idx, "dt_vencimento"]) else ""
        cert = venc_to_cert.get((emi, venc))
        if cert:
            nome = _cert_name(cert, emi)
            if nome:
                _set_devedor(idx, nome, "P1_cert_exact")
    print(f"  Pass 1 (cert exact):     {stats.get('P1_cert_exact', 0)}")

    # --- PASS 2: ISIN from descricao_ativo → certificate → devedor ---
    for idx in pos_idx:
        if not _is_gap(idx):
            continue
        desc = str(pos.at[idx, "descricao_ativo"]) if pd.notna(pos.at[idx, "descricao_ativo"]) else ""
        emi = pos.at[idx, "emi_cnpj"]
        # Try explicit ISIN: BRXXXXXXXX
        m = _ISIN_DESC_PAT.search(desc)
        if m:
            isin = m.group(1)
            cert = isin_to_cert.get(isin)
            if cert:
                nome = _cert_name(cert, emi)
                if nome:
                    _set_devedor(idx, nome, "P2_isin_desc")
                    continue
        # Try any BR... ISIN pattern
        for isin in _ISIN_PAT.findall(desc):
            cert = isin_to_cert.get(isin)
            if cert:
                nome = _cert_name(cert, emi)
                if nome:
                    _set_devedor(idx, nome, "P2_isin_desc")
                    break
    print(f"  Pass 2 (ISIN desc):      {stats.get('P2_isin_desc', 0)}")

    # --- PASS 3: CETIP ticker (CRI:XXXX:DDMMYY) → certificate → Nome_Emissao ---
    for idx in pos_idx:
        if not _is_gap(idx):
            continue
        desc = str(pos.at[idx, "descricao_ativo"]) if pd.notna(pos.at[idx, "descricao_ativo"]) else ""
        m = _CETIP_PAT.search(desc)
        if m:
            ticker = m.group(1)
            date_str = m.group(2)
            # Try exact CETIP match first
            matched = False
            for cetip_code, cert in cetip_to_cert.items():
                if ticker.upper() in cetip_code.upper():
                    nome = _cert_name(cert)
                    if nome:
                        _set_devedor(idx, nome, "P3_cetip")
                        matched = True
                        break
            if matched:
                continue
            # Try prefix map
            if ticker in cetip_prefix_map:
                # Pick first (most common)
                cert, nome = cetip_prefix_map[ticker][0]
                if nome:
                    _set_devedor(idx, nome, "P3_cetip")
    print(f"  Pass 3 (CETIP ticker):   {stats.get('P3_cetip', 0)}")

    # --- PASS 4: Dates from description → fuzzy match classes → Nome_Emissao ---
    for idx in pos_idx:
        if not _is_gap(idx):
            continue
        desc = str(pos.at[idx, "descricao_ativo"]) if pd.notna(pos.at[idx, "descricao_ativo"]) else ""
        emi = pos.at[idx, "emi_cnpj"]
        if emi not in emissora_cert_dates:
            continue

        dates = _extract_dates_from_desc(desc)
        if not dates:
            continue

        # Use the latest date as maturity
        target_date = max(dates)
        best_nome, best_delta = None, pd.Timedelta(days=181)
        for cert_date, cert in emissora_cert_dates[emi]:
            delta = abs(cert_date - target_date)
            if delta < best_delta:
                nome = _cert_name(cert, emi)
                if nome:
                    best_delta = delta
                    best_nome = nome
        if best_nome:
            _set_devedor(idx, best_nome, "P4_date_fuzzy")
    print(f"  Pass 4 (date fuzzy):     {stats.get('P4_date_fuzzy', 0)}")

    # --- PASS 5: Company name from description ---
    for idx in pos_idx:
        if not _is_gap(idx):
            continue
        desc = str(pos.at[idx, "descricao_ativo"]) if pd.notna(pos.at[idx, "descricao_ativo"]) else ""
        name = _extract_name_from_desc(desc)
        if name:
            _set_devedor(idx, name, "P5_name_desc")
    print(f"  Pass 5 (name in desc):   {stats.get('P5_name_desc', 0)}")

    # --- PASS 6: Nome_Emissao fuzzy via dt_vencimento (existing column) ---
    for idx in pos_idx:
        if not _is_gap(idx):
            continue
        emi = pos.at[idx, "emi_cnpj"]
        venc_str = pos.at[idx, "dt_vencimento"]
        if emi not in emissora_cert_dates or pd.isna(venc_str):
            continue
        try:
            pos_date = pd.Timestamp(venc_str)
        except (ValueError, OverflowError):
            continue
        best_nome, best_delta = None, pd.Timedelta(days=91)
        for cert_date, cert in emissora_cert_dates[emi]:
            delta = abs(cert_date - pos_date)
            if delta < best_delta:
                nome = _cert_name(cert, emi)
                if nome:
                    best_delta = delta
                    best_nome = nome
        if best_nome:
            _set_devedor(idx, best_nome, "P6_venc_fuzzy")
    print(f"  Pass 6 (venc fuzzy):     {stats.get('P6_venc_fuzzy', 0)}")

    # --- PASS 7: Emissora aggregate (top devedores) ---
    for idx in pos_idx:
        if not _is_gap(idx):
            continue
        emi = pos.at[idx, "emi_cnpj"]
        if emi in emissora_devs and emissora_devs[emi]:
            _set_devedor(idx, emissora_devs[emi], "P7_emissora_agg")
    print(f"  Pass 7 (emissora agg):   {stats.get('P7_emissora_agg', 0)}")

    # --- PASS 8: Direct devedor (entity is not a securitizadora) ---
    for idx in pos_idx:
        if not _is_gap(idx):
            continue
        emi = pos.at[idx, "emi_cnpj"]
        if emi and emi not in all_emissora_cnpjs:
            nome = mapping.get(emi, "")
            if nome:
                _set_devedor(idx, nome, "P8_direct")
            elif pd.notna(pos.at[idx, "emissor"]):
                pos.at[idx, "devedor"] = str(pos.at[idx, "emissor"]).strip()
                stats["P8_direct"] = stats.get("P8_direct", 0) + 1
    print(f"  Pass 8 (direct):         {stats.get('P8_direct', 0)}")

    # --- PASS 9: B3 Ticker ---
    for idx in pos_idx:
        if not _is_gap(idx):
            continue
        desc = str(pos.at[idx, "descricao_ativo"]) if pd.notna(pos.at[idx, "descricao_ativo"]) else ""
        for ticker, name in ticker_names.items():
            if ticker in desc:
                _set_devedor(idx, f"{name} (via {ticker})", "P9_b3_ticker")
                break
    print(f"  Pass 9 (B3 ticker):      {stats.get('P9_b3_ticker', 0)}")

    # --- PASS 10: Fallback ---
    fallback = 0
    for idx in pos_idx:
        if not _is_gap(idx):
            continue
        emissor = pos.at[idx, "emissor"]
        if pd.notna(emissor):
            pos.at[idx, "devedor"] = f"Cedente via {emissor}"
        fallback += 1
    print(f"  Pass 10 (fallback):      {fallback}")

    # --- Summary ---
    total = len(pos_idx)
    resolved = sum(v for k, v in stats.items() if not k.startswith("P10"))
    pct = resolved / total * 100 if total > 0 else 0

    # Volume analysis
    cri_pos = pos.loc[pos_idx].copy()
    vol_total = cri_pos["vl_posicao"].sum()
    vol_resolved = cri_pos[~cri_pos["devedor"].str.contains("Cedente via|Cedente não", na=True, regex=True)]["vl_posicao"].sum()
    vol_pct = vol_resolved / vol_total * 100 if vol_total > 0 else 0

    print(f"\n{'='*50}")
    print(f"Devedor real: {resolved}/{total} ({pct:.1f}%)")
    print(f"Volume:       R$ {vol_resolved/1e9:.2f}B / R$ {vol_total/1e9:.2f}B ({vol_pct:.1f}%)")
    print(f"Fallback:     {fallback}/{total} ({fallback/total*100:.1f}%)")

    # Per-type coverage
    for tipo in ["CRI", "CRA", "CPR-F"]:
        mask = pos.loc[pos_idx, "tipo_ativo"] == tipo
        t = mask.sum()
        if t > 0:
            g = pos.loc[pos_idx[mask], "devedor"].apply(
                lambda d: pd.isna(d) or "Cedente via" in str(d) or "Cedente não" in str(d)
            ).sum()
            print(f"  {tipo}: {t-g}/{t} ({(t-g)/t*100:.1f}%)")

    pos = pos.drop(columns=["emi_cnpj"], errors="ignore")
    pos.to_csv(POS_FILE, index=False)
    print(f"\nSalvo: {POS_FILE}")

    # Show unique devedores count
    cri_resolved = pos.loc[pos_idx]
    dev_names = cri_resolved["devedor"].dropna()
    dev_real = dev_names[~dev_names.str.contains("Cedente via|Cedente não", regex=True)]
    unique_devs = dev_real.nunique()
    print(f"Devedores únicos: {unique_devs}")

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
        print("  python3 resolve_devedores.py --enrich    # Enriquece positions (10 passes)")
        print("  python3 resolve_devedores.py --status    # Status da resolução")
