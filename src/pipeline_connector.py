"""
ZYN Sales Intelligence — Conector com Pipeline Notion
Lê operações do Pipeline e converte para formato de deal para matching.
"""

# Mapeamento de tipos de produto do Pipeline ZYN para tipos CVM
PRODUCT_MAP = {
    "CRA": "CRA",
    "CRI": "CRI",
    "NC": "NC",
    "Nota Comercial": "NC",
    "CPR": "CPR-F",
    "CPR-F": "CPR-F",
    "Debênture": "DEBENTURE",
    "Debenture": "DEBENTURE",
    "FIDC": "DEBENTURE",  # Cotas sênior de FIDC são frequentemente compradas por quem compra debêntures
    "Fiagro": "CRA",  # Fiagro-FIDC emite papéis similares a CRA
}


def parse_pipeline_deal(notion_page: dict) -> dict | None:
    """
    Converte uma página do Pipeline Notion em formato deal para matching.

    Espera propriedades como:
    - Nome / Empresa
    - Produto (NC, CRI, CRA, etc.)
    - Volume (R$)
    - Taxa / Spread
    - Prazo / Vencimento
    - Indexador (CDI, IPCA, etc.)
    - Status
    """
    props = notion_page.get("properties", {})
    if not props:
        return None

    deal = {
        "id": notion_page.get("id", ""),
        "url": notion_page.get("url", ""),
    }

    # Nome da operação
    for key in ["Nome", "Name", "Empresa", "Operação", "Deal"]:
        if key in props:
            val = props[key]
            if val.get("type") == "title":
                titles = val.get("title", [])
                deal["nome"] = titles[0]["plain_text"] if titles else ""
                break

    # Produto/Tipo
    for key in ["Produto", "Tipo", "Product", "Instrumento"]:
        if key in props:
            val = props[key]
            if val.get("type") == "select" and val.get("select"):
                raw_type = val["select"]["name"]
                deal["tipo"] = PRODUCT_MAP.get(raw_type, raw_type.upper())
                deal["tipo_raw"] = raw_type
                break
            elif val.get("type") == "multi_select":
                selects = val.get("multi_select", [])
                if selects:
                    raw_type = selects[0]["name"]
                    deal["tipo"] = PRODUCT_MAP.get(raw_type, raw_type.upper())
                    deal["tipo_raw"] = raw_type
                break

    # Volume
    for key in ["Volume", "Valor", "Size", "Volume (R$)", "Volume Total"]:
        if key in props:
            val = props[key]
            if val.get("type") == "number" and val.get("number") is not None:
                deal["volume"] = val["number"]
                break
            elif val.get("type") == "rich_text":
                texts = val.get("rich_text", [])
                if texts:
                    try:
                        raw = texts[0]["plain_text"].replace(".", "").replace(",", ".").replace("R$", "").strip()
                        deal["volume"] = float(raw)
                    except (ValueError, IndexError):
                        pass
                break

    # Taxa/Spread
    for key in ["Taxa", "Spread", "Rate", "Taxa (%)", "Remuneração"]:
        if key in props:
            val = props[key]
            if val.get("type") == "number" and val.get("number") is not None:
                deal["spread"] = val["number"]
                break
            elif val.get("type") == "rich_text":
                texts = val.get("rich_text", [])
                if texts:
                    raw = texts[0]["plain_text"]
                    deal["taxa_raw"] = raw
                break

    # Prazo
    for key in ["Prazo", "Vencimento", "Maturity", "Prazo (anos)", "Duration"]:
        if key in props:
            val = props[key]
            if val.get("type") == "number" and val.get("number") is not None:
                deal["prazo_anos"] = val["number"]
                break
            elif val.get("type") == "date" and val.get("date"):
                from datetime import datetime
                try:
                    venc_str = val["date"]["start"]
                    venc = datetime.fromisoformat(venc_str)
                    deal["prazo_anos"] = (venc - datetime.now()).days / 365.25
                    deal["dt_vencimento"] = venc_str
                except (ValueError, TypeError):
                    pass
                break

    # Indexador
    for key in ["Indexador", "Index", "Benchmark"]:
        if key in props:
            val = props[key]
            if val.get("type") == "select" and val.get("select"):
                deal["indexador"] = val["select"]["name"]
                break
            elif val.get("type") == "rich_text":
                texts = val.get("rich_text", [])
                if texts:
                    deal["indexador"] = texts[0]["plain_text"]
                break

    # Status
    for key in ["Status", "Fase", "Stage"]:
        if key in props:
            val = props[key]
            if val.get("type") == "status" and val.get("status"):
                deal["status"] = val["status"]["name"]
                break
            elif val.get("type") == "select" and val.get("select"):
                deal["status"] = val["select"]["name"]
                break

    # Rating/Garantia
    for key in ["Rating", "Garantia", "Collateral"]:
        if key in props:
            val = props[key]
            if val.get("type") == "select" and val.get("select"):
                deal["rating"] = val["select"]["name"]
                break
            elif val.get("type") == "rich_text":
                texts = val.get("rich_text", [])
                if texts:
                    deal["rating"] = texts[0]["plain_text"]
                break

    return deal if deal.get("tipo") else None


def format_deal_summary(deal: dict) -> str:
    """Formata um deal para exibição."""
    parts = []
    if "nome" in deal:
        parts.append(f"**{deal['nome']}**")
    if "tipo_raw" in deal:
        parts.append(f"Produto: {deal['tipo_raw']}")
    if "volume" in deal:
        vol = deal["volume"]
        if vol >= 1_000_000:
            parts.append(f"Volume: R$ {vol/1_000_000:.1f}M")
        else:
            parts.append(f"Volume: R$ {vol:,.0f}")
    if "indexador" in deal and "spread" in deal:
        parts.append(f"Taxa: {deal['indexador']} + {deal['spread']:.2f}%")
    elif "taxa_raw" in deal:
        parts.append(f"Taxa: {deal['taxa_raw']}")
    if "prazo_anos" in deal:
        parts.append(f"Prazo: {deal['prazo_anos']:.1f} anos")
    if "status" in deal:
        parts.append(f"Status: {deal['status']}")
    return " | ".join(parts)
