#!/usr/bin/env python3
"""
SEC EDGAR — Download e parse de N-PORT filings para identificar
investidores americanos com exposição ao Brasil.

Fontes:
- EDGAR Full-Text Search API (efts.sec.gov)
- N-PORT XML filings (holdings detalhados)
- Company submissions API (data.sec.gov)
"""

import json
import time
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# SEC requires contact info in User-Agent
HEADERS = {
    "User-Agent": "ZYN Capital danilo@zyncapital.com.br",
    "Accept-Encoding": "gzip, deflate",
}

EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"

# Namespaces in N-PORT XML
NPORT_NS = {
    "nport": "http://www.sec.gov/edgar/nport",
    "com": "http://www.sec.gov/edgar/common",
}

# Discovered via EDGAR search: entities filing N-PORT with Brazil exposure
SEED_MANAGERS = []  # Populated dynamically via discover_brazil_filers()

# Rate limiter: SEC allows max 10 req/sec
_last_request_time = 0.0


def _rate_limit():
    """Enforce SEC rate limit of 10 requests/second."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < 0.12:  # ~8 req/sec to be safe
        time.sleep(0.12 - elapsed)
    _last_request_time = time.time()


def _get(url: str, **kwargs) -> requests.Response:
    """Rate-limited GET request to SEC."""
    _rate_limit()
    resp = requests.get(url, headers=HEADERS, timeout=30, **kwargs)
    resp.raise_for_status()
    return resp


# ─── Filing Discovery ───────────────────────────────────────────


def discover_brazil_filers(max_entities: int = 100) -> list[tuple[str, str]]:
    """
    Discover entities that file N-PORT with Brazil exposure.
    Uses EDGAR full-text search to find filings mentioning 'brazil'.
    Returns list of (name, cik) tuples.
    """
    import re
    entities = {}
    start_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    for offset in range(0, max_entities * 3, 50):
        params = {
            "q": '"brazil"',
            "forms": "NPORT-P",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
            "from": offset,
            "size": 50,
        }
        try:
            resp = _get(EDGAR_SEARCH, params=params)
            hits = resp.json().get("hits", {}).get("hits", [])
        except Exception as e:
            logger.warning(f"Search failed at offset {offset}: {e}")
            break

        if not hits:
            break

        for h in hits:
            names = h.get("_source", {}).get("display_names", [])
            if names:
                m = re.search(r"CIK (\d+)", names[0])
                if m:
                    cik = m.group(1).zfill(10)
                    clean = re.sub(r"\s*\(CIK \d+\)", "", names[0]).strip()
                    if cik not in entities:
                        entities[cik] = clean

        if len(entities) >= max_entities:
            break

    result = [(name, cik) for cik, name in entities.items()]
    logger.info(f"Discovered {len(result)} entities with Brazil N-PORT exposure")
    return result


def search_nport_filings(query: str = "brazil", max_results: int = 200,
                         start_date: str = None) -> list[dict]:
    """Search EDGAR for N-PORT filings mentioning a query term."""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    params = {
        "q": query,
        "forms": "NPORT-P",
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
        "from": 0,
        "size": min(max_results, 50),
    }

    all_hits = []
    while len(all_hits) < max_results:
        params["from"] = len(all_hits)
        resp = _get(EDGAR_SEARCH, params=params)
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break
        for h in hits:
            src = h.get("_source", {})
            all_hits.append({
                "cik": src.get("entity_id", ""),
                "company": src.get("display_names", [""])[0] if src.get("display_names") else "",
                "filing_date": src.get("file_date", ""),
                "accession": src.get("file_num", ""),
                "file_url": src.get("file_path", ""),
            })
        if len(hits) < params["size"]:
            break

    logger.info(f"Found {len(all_hits)} N-PORT filings for query='{query}'")
    return all_hits


def get_company_nport_filings(cik: str, limit: int = 20) -> list[dict]:
    """Get recent N-PORT filings for a specific CIK. Limit=20 to capture multiple series."""
    cik_padded = cik.lstrip("0").zfill(10)
    try:
        resp = _get(EDGAR_SUBMISSIONS.format(cik=cik_padded))
        data = resp.json()
    except Exception as e:
        logger.warning(f"Failed to fetch submissions for CIK {cik}: {e}")
        return []

    company_name = data.get("name", "")
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    filings = []
    for i, form in enumerate(forms):
        if form in ("NPORT-P", "NPORT-EX") and len(filings) < limit:
            acc_clean = accessions[i].replace("-", "")
            cik_num = cik_padded.lstrip("0")
            # primaryDocument may point to XSLT view; we need raw XML
            # Try primary_doc.xml first (standard N-PORT XML filename)
            raw_xml_url = f"{EDGAR_ARCHIVES}/{cik_num}/{acc_clean}/primary_doc.xml"
            filings.append({
                "cik": cik_padded,
                "company": company_name,
                "form": form,
                "filing_date": dates[i],
                "accession": accessions[i],
                "primary_doc": primary_docs[i] if i < len(primary_docs) else "",
                "url": raw_xml_url,
            })

    return filings


# ─── N-PORT XML Parsing ─────────────────────────────────────────


def _find_text(elem, path, ns=NPORT_NS, default=""):
    """Safely extract text from XML element."""
    # Try with namespace
    for prefix, uri in ns.items():
        com_uri = ns.get("com", "")
        replaced = path.replace("nport:", "{" + uri + "}").replace("com:", "{" + com_uri + "}")
        node = elem.find(replaced)
        if node is not None and node.text:
            return node.text.strip()

    # Try without namespace (some filings don't use ns)
    node = elem.find(path.replace("nport:", "").replace("com:", ""))
    if node is not None and node.text:
        return node.text.strip()

    return default


def parse_nport_xml(xml_content: str, filter_brazil: bool = True) -> dict:
    """
    Parse N-PORT XML filing and extract holdings.
    If filter_brazil=True, only returns Brazilian holdings.

    Returns dict with fund info and list of holdings.
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.error(f"XML parse error: {e}")
        return {"fund_info": {}, "holdings": []}

    # Remove namespace prefixes for easier parsing
    xml_str = xml_content
    for ns_prefix in ["nport:", "com:", "nport-common:"]:
        xml_str = xml_str.replace(f"<{ns_prefix}", "<").replace(f"</{ns_prefix}", "</")

    try:
        root = ET.fromstring(xml_str.encode("utf-8") if isinstance(xml_str, str) else xml_str)
    except ET.ParseError:
        pass  # Use original root

    # Extract fund info
    fund_info = {}
    for tag in ["seriesName", "seriesId", "leiOfFiler", "name"]:
        node = root.find(f".//{tag}")
        if node is not None and node.text:
            fund_info[tag] = node.text.strip()

    # Total assets
    node = root.find(".//totAssets")
    if node is not None and node.text:
        try:
            fund_info["total_assets"] = float(node.text)
        except ValueError:
            pass

    # Parse holdings
    holdings = []
    for sec in root.iter():
        if sec.tag.endswith("invstOrSec") or sec.tag == "invstOrSec":
            holding = {}
            for child in sec:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if child.text and child.text.strip():
                    holding[tag] = child.text.strip()

                # Handle nested elements (e.g., debtSec, issuerConditionalCat)
                for grandchild in child:
                    gtag = grandchild.tag.split("}")[-1] if "}" in grandchild.tag else grandchild.tag
                    if grandchild.text and grandchild.text.strip():
                        holding[f"{tag}_{gtag}"] = grandchild.text.strip()

            if not holding:
                continue

            # Determine if Brazilian
            is_brazil = False
            isin = holding.get("isin", "")
            inv_country = holding.get("invCountry", "")
            issuer_country = holding.get("countryOfOrganization", "")
            name = holding.get("name", "").upper()
            title = holding.get("title", "").upper()

            if inv_country == "BR" or issuer_country == "BR":
                is_brazil = True
            elif isin.startswith("BR"):
                is_brazil = True
            elif any(kw in name or kw in title for kw in [
                "BRAZIL", "BRASIL", "PETROBRAS", "VALE S.A", "ITAU",
                "BRADESCO", "BANCO DO BRASIL", "B3 S.A", "JBS S.A",
                "SUZANO", "KLABIN", "RAIZEN", "COSAN", "EMBRAER",
                "SABESP", "CEMIG", "ELETROBRAS", "RUMO", "LOCALIZA",
                "NATURA", "MAGALU", "BTG PACTUAL", "XP INC",
                "GLOBO", "MARFRIG", "MINERVA", "BRF S.A",
                "AZUL S.A", "GOL LINHAS", "USIMINAS", "GERDAU",
                "CSN", "BRASKEM", "ULTRAPAR", "ENGIE BRASIL",
                "LIGHT S.A", "EQUATORIAL", "ENERGISA", "TAESA",
                "COPEL", "ENEVA", "AUREN", "VIBRA", "PRIO",
                "MOVIDA", "UNIDAS", "SMARTFIT", "HAPVIDA",
                "REDE D'OR", "DASA", "FLEURY", "RAIA",
                "FEDERATIVE REPUBLIC OF BRAZIL",
            ]):
                is_brazil = True

            if filter_brazil and not is_brazil:
                continue

            # Parse value
            val_usd = 0.0
            for val_key in ["valUSD", "val", "balance"]:
                if val_key in holding:
                    try:
                        val_usd = float(holding[val_key])
                        break
                    except ValueError:
                        pass

            pct_val = 0.0
            if "pctVal" in holding:
                try:
                    pct_val = float(holding["pctVal"])
                except ValueError:
                    pass

            # Asset category
            asset_cat = holding.get("assetCat", "")
            if not asset_cat:
                asset_cat = holding.get("assetConditionalCat", "")

            # Maturity
            maturity = holding.get("maturityDt", "")

            # Coupon rate
            coupon = holding.get("debtSec_couponRate", "")
            if not coupon:
                coupon = holding.get("couponRate", "")

            holdings.append({
                "name": holding.get("name", ""),
                "title": holding.get("title", ""),
                "cusip": holding.get("cusip", ""),
                "isin": isin,
                "lei": holding.get("lei", ""),
                "inv_country": inv_country,
                "issuer_country": issuer_country,
                "asset_cat": asset_cat,
                "val_usd": val_usd,
                "pct_val": pct_val,
                "maturity": maturity,
                "coupon": coupon,
                "currency": holding.get("curCd", ""),
                "is_default": holding.get("isDefault", ""),
                "units": holding.get("units", ""),
            })

    return {"fund_info": fund_info, "holdings": holdings}


# ─── Main Pipeline ───────────────────────────────────────────────


def download_and_parse_filing(filing: dict) -> dict:
    """Download a single N-PORT filing and parse Brazilian holdings."""
    url = filing.get("url", "")
    if not url:
        return {"fund_info": {}, "holdings": []}

    try:
        resp = _get(url)
        content = resp.text

        # If we got HTML (XSLT view), try alternate XML path
        if "<html" in content.lower()[:500] or "<?xml" not in content[:100]:
            # Try without xsl prefix
            alt_url = url.replace("/xslFormNPORT-P_X01/", "/")
            if alt_url != url:
                try:
                    resp = _get(alt_url)
                    content = resp.text
                except Exception:
                    pass
            if "<html" in content.lower()[:500] or "<?xml" not in content[:100]:
                logger.debug(f"Got HTML instead of XML for {url}, skipping")
                return {"fund_info": {}, "holdings": []}

        result = parse_nport_xml(content, filter_brazil=True)
        result["fund_info"]["company"] = filing.get("company", "")
        result["fund_info"]["cik"] = filing.get("cik", "")
        result["fund_info"]["filing_date"] = filing.get("filing_date", "")
        return result

    except Exception as e:
        logger.warning(f"Failed to download/parse {url}: {e}")
        return {"fund_info": {}, "holdings": []}


def collect_brazil_holdings(output_dir: Path, max_managers: int = 50,
                            filings_per_manager: int = 1,
                            progress_callback=None) -> pd.DataFrame:
    """
    Main collection pipeline:
    1. For each seed manager, get latest N-PORT filing
    2. Parse XML and extract Brazilian holdings
    3. Aggregate into DataFrame

    Returns DataFrame with all Brazilian holdings.
    """
    all_holdings = []
    managers_processed = 0
    managers_with_brazil = 0

    # Discover entities dynamically from EDGAR search
    if progress_callback:
        progress_callback(0.0, "Descobrindo gestoras US com Brasil no EDGAR...")
    managers = SEED_MANAGERS if SEED_MANAGERS else discover_brazil_filers(max_entities=max_managers)
    managers = managers[:max_managers]
    total = len(managers)
    if total == 0:
        logger.warning("No managers found")
        return pd.DataFrame()

    for i, (name, cik) in enumerate(managers):
        if progress_callback:
            progress_callback(i / total, f"Analisando {name}...")

        try:
            # Get multiple filings (trusts have one per series/fund)
            filings = get_company_nport_filings(cik, limit=20)
        except Exception as e:
            logger.warning(f"Skip {name}: {e}")
            continue

        if not filings:
            logger.debug(f"No N-PORT filings for {name}")
            continue

        managers_processed += 1

        # Only process filings from the most recent date (latest quarter)
        latest_date = max(f["filing_date"] for f in filings)
        latest_filings = [f for f in filings if f["filing_date"] == latest_date]
        found_brazil = False

        for filing in latest_filings:
            result = download_and_parse_filing(filing)
            if result["holdings"]:
                found_brazil = True
                fund_info = result["fund_info"]
                for h in result["holdings"]:
                    h["manager"] = name
                    h["manager_cik"] = cik
                    h["fund_name"] = fund_info.get("seriesName", fund_info.get("company", name))
                    h["fund_series_id"] = fund_info.get("seriesId", "")
                    h["total_assets"] = fund_info.get("total_assets", 0)
                    h["filing_date"] = fund_info.get("filing_date", "")
                    all_holdings.append(h)

        if found_brazil:
            managers_with_brazil += 1

    if progress_callback:
        progress_callback(1.0, "Concluído!")

    logger.info(
        f"Processed {managers_processed} managers, "
        f"{managers_with_brazil} with Brazil exposure, "
        f"{len(all_holdings)} total holdings"
    )

    if not all_holdings:
        return pd.DataFrame()

    df = pd.DataFrame(all_holdings)

    # Save raw holdings
    output_path = output_dir / "us_holdings_brazil.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df)} holdings to {output_path}")

    return df


def build_us_investor_profiles(holdings_df: pd.DataFrame,
                                output_dir: Path) -> pd.DataFrame:
    """
    Aggregate holdings into investor profiles by manager.

    Returns DataFrame with one row per manager.
    """
    if holdings_df.empty:
        return pd.DataFrame()

    profiles = []
    for manager, group in holdings_df.groupby("manager"):
        cik = group["manager_cik"].iloc[0]
        vol_total = group["val_usd"].sum()

        # Classify by asset type
        vol_sovereign = group[
            group["name"].str.contains("REPUBLIC OF BRAZIL|BRAZIL.*GOVT|BRAZIL.*SOVEREIGN",
                                       case=False, na=False)
        ]["val_usd"].sum()

        vol_corporate = vol_total - vol_sovereign

        # Top issuers
        top_issuers = (
            group.groupby("name")["val_usd"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )
        top_issuers_str = "; ".join(
            f"{name[:40]} (${val/1e6:.1f}M)"
            for name, val in top_issuers.items()
        )

        # Number of distinct positions
        n_positions = len(group)
        n_funds = group["fund_name"].nunique()

        # Average maturity
        maturities = pd.to_datetime(group["maturity"], errors="coerce")
        avg_maturity_years = 0
        valid_mats = maturities.dropna()
        if len(valid_mats) > 0:
            avg_days = (valid_mats - pd.Timestamp.now()).dt.days.mean()
            avg_maturity_years = round(max(avg_days / 365.25, 0), 1)

        # Predominant currency
        currency = group["currency"].mode().iloc[0] if not group["currency"].mode().empty else "USD"

        # Asset type breakdown
        asset_types = group["asset_cat"].value_counts().to_dict()
        tipo_preferido = group["asset_cat"].mode().iloc[0] if not group["asset_cat"].mode().empty else "N/A"

        profiles.append({
            "Manager": manager,
            "CIK": cik,
            "Nº Fundos": n_funds,
            "Nº Posições BR": n_positions,
            "Vol. Brasil (USD)": vol_total,
            "Vol. Soberano (USD)": vol_sovereign,
            "Vol. Corporativo (USD)": vol_corporate,
            "% Corporativo": round(vol_corporate / vol_total * 100, 1) if vol_total > 0 else 0,
            "Tipo Preferido": tipo_preferido,
            "Prazo Médio (anos)": avg_maturity_years,
            "Moeda Principal": currency,
            "Top Emissores BR": top_issuers_str,
            "Filing Date": group["filing_date"].max(),
            "Fonte": "SEC/EDGAR N-PORT",
        })

    df = pd.DataFrame(profiles).sort_values("Vol. Brasil (USD)", ascending=False)

    output_path = output_dir / "us_investor_profiles.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df)} US investor profiles to {output_path}")

    return df
