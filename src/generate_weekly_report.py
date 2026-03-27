#!/usr/bin/env python3
"""
ZYN Capital — Gerador de Relatório de Gestão Semanal
Coleta KPIs das bases Notion e atualiza a página do Relatório de Gestão.

Roda via GitHub Actions toda segunda às 9:30 BRT.
"""
import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timedelta

NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
NOTION_VERSION = "2022-06-28"

# IDs das bases Notion (verificados Mar/2026)
PIPELINE_DB = "2f68e4de-57c8-81f3-bcaa-cc73f28fd5d5"
RECEITAS_DB = "a4600183-5383-416f-88fa-c82f9b5db178"
DESPESAS_DB = "f0447e5d-d0e4-4b62-aeb6-484d7e35138c"
FLUXO_CAIXA_DB = "ae51c677-6292-45d3-9d7b-2a8187825e0a"
LEADS_DB = "2f68e4de-57c8-81e8-9606-e573d49a2b14"
RELATORIO_PAGE = "31a8e4de-57c8-813d-9181-c553cf5179c7"


def _notion_request(method: str, endpoint: str, body: dict = None) -> dict:
    """Make a Notion API request."""
    url = f"https://api.notion.com/v1/{endpoint}"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"Notion API error {e.code}: {e.read().decode()[:200]}")
        return {}


def _query_database(db_id: str, filter_body: dict = None) -> list:
    """Query a Notion database and return all results."""
    body = filter_body or {}
    results = []
    has_more = True
    start_cursor = None

    while has_more:
        if start_cursor:
            body["start_cursor"] = start_cursor
        resp = _notion_request("POST", f"databases/{db_id}/query", body)
        results.extend(resp.get("results", []))
        has_more = resp.get("has_more", False)
        start_cursor = resp.get("next_cursor")

    return results


def _get_property_value(page: dict, prop_name: str):
    """Extract a property value from a Notion page."""
    props = page.get("properties", {})
    prop = props.get(prop_name, {})
    prop_type = prop.get("type", "")

    if prop_type == "number":
        return prop.get("number", 0) or 0
    elif prop_type == "select":
        sel = prop.get("select")
        return sel.get("name", "") if sel else ""
    elif prop_type == "title":
        title_arr = prop.get("title", [])
        return title_arr[0].get("plain_text", "") if title_arr else ""
    elif prop_type == "rich_text":
        text_arr = prop.get("rich_text", [])
        return text_arr[0].get("plain_text", "") if text_arr else ""
    elif prop_type == "date":
        date_obj = prop.get("date")
        return date_obj.get("start", "") if date_obj else ""
    elif prop_type == "status":
        status = prop.get("status")
        return status.get("name", "") if status else ""
    elif prop_type == "formula":
        formula = prop.get("formula", {})
        f_type = formula.get("type", "")
        return formula.get(f_type, 0)
    return None


def collect_pipeline_kpis() -> dict:
    """Collect Pipeline KPIs."""
    pages = _query_database(PIPELINE_DB)
    total = len(pages)
    volume_total = 0
    by_status = {}

    for p in pages:
        status = _get_property_value(p, "Status") or "Sem Status"
        volume = _get_property_value(p, "Volume") or 0
        by_status[status] = by_status.get(status, 0) + 1
        volume_total += volume

    return {
        "total_operacoes": total,
        "volume_total": volume_total,
        "por_status": by_status,
    }


def collect_financial_kpis() -> dict:
    """Collect Receitas and Despesas."""
    now = datetime.now()
    month_start = now.replace(day=1).strftime("%Y-%m-%d")

    receitas = _query_database(RECEITAS_DB)
    despesas = _query_database(DESPESAS_DB)

    total_receitas = sum(_get_property_value(r, "Valor") or 0 for r in receitas)
    total_despesas = sum(_get_property_value(d, "Valor") or 0 for d in despesas)

    return {
        "receitas_total": total_receitas,
        "despesas_total": total_despesas,
        "resultado": total_receitas - total_despesas,
    }


def collect_leads_kpis() -> dict:
    """Collect Leads KPIs."""
    pages = _query_database(LEADS_DB)
    week_ago = (datetime.now() - timedelta(days=7)).isoformat()

    total = len(pages)
    novos = 0
    for p in pages:
        created = p.get("created_time", "")
        if created >= week_ago:
            novos += 1

    return {"total_leads": total, "novos_semana": novos}


def _fmt_brl(value: float) -> str:
    """Format value as BRL."""
    if abs(value) >= 1_000_000:
        return f"R$ {value / 1_000_000:,.1f}MM"
    elif abs(value) >= 1_000:
        return f"R$ {value / 1_000:,.0f}K"
    return f"R$ {value:,.0f}"


def build_report_content(pipeline: dict, financeiro: dict, leads: dict) -> list:
    """Build Notion blocks for the weekly report."""
    now = datetime.now()
    week_start = (now - timedelta(days=now.weekday())).strftime("%d/%m/%Y")
    week_end = now.strftime("%d/%m/%Y")

    blocks = []

    def _heading(text, level=2):
        blocks.append({
            "object": "block",
            "type": f"heading_{level}",
            f"heading_{level}": {
                "rich_text": [{"type": "text", "text": {"content": text}}]
            },
        })

    def _paragraph(text):
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": text}}]
            },
        })

    def _bullet(text):
        blocks.append({
            "object": "block",
            "type": "bulleted_list_item",
            "bulleted_list_item": {
                "rich_text": [{"type": "text", "text": {"content": text}}]
            },
        })

    _heading(f"Relatório de Gestão — Semana {week_start} a {week_end}")

    _heading("Pipeline", 3)
    _bullet(f"Operações ativas: {pipeline['total_operacoes']}")
    _bullet(f"Volume total: {_fmt_brl(pipeline['volume_total'])}")
    for status, count in pipeline.get("por_status", {}).items():
        _bullet(f"  {status}: {count}")

    _heading("Financeiro", 3)
    _bullet(f"Receitas: {_fmt_brl(financeiro['receitas_total'])}")
    _bullet(f"Despesas: {_fmt_brl(financeiro['despesas_total'])}")
    _bullet(f"Resultado: {_fmt_brl(financeiro['resultado'])}")

    _heading("Comercial", 3)
    _bullet(f"Total leads: {leads['total_leads']}")
    _bullet(f"Novos esta semana: {leads['novos_semana']}")

    _paragraph(f"Gerado automaticamente em {now.strftime('%d/%m/%Y %H:%M')} via GitHub Actions.")

    return blocks


def update_report_page(blocks: list):
    """Clear and update the Relatório de Gestão page in Notion."""
    # Get existing blocks to delete them
    existing = _notion_request("GET", f"blocks/{RELATORIO_PAGE}/children")
    for block in existing.get("results", []):
        _notion_request("DELETE", f"blocks/{block['id']}")

    # Append new blocks (Notion limits 100 blocks per request)
    for i in range(0, len(blocks), 100):
        chunk = blocks[i:i + 100]
        _notion_request("PATCH", f"blocks/{RELATORIO_PAGE}/children", {"children": chunk})


def main():
    if not NOTION_TOKEN:
        print("NOTION_TOKEN not set. Skipping report generation.")
        return

    print("Collecting Pipeline KPIs...")
    pipeline = collect_pipeline_kpis()
    print(f"  {pipeline['total_operacoes']} operações, {_fmt_brl(pipeline['volume_total'])}")

    print("Collecting Financial KPIs...")
    financeiro = collect_financial_kpis()
    print(f"  Receitas: {_fmt_brl(financeiro['receitas_total'])}")

    print("Collecting Leads KPIs...")
    leads = collect_leads_kpis()
    print(f"  {leads['total_leads']} leads, {leads['novos_semana']} novos")

    print("Building report...")
    blocks = build_report_content(pipeline, financeiro, leads)

    print("Updating Notion page...")
    update_report_page(blocks)
    print("Report updated successfully.")


if __name__ == "__main__":
    main()
