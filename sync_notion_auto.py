#!/usr/bin/env python3
"""
ZYN Sales Intelligence — Sync automático Pipeline Notion via API
Busca todos os deals do Pipeline no Notion e atualiza data/pipeline.json.

Requer NOTION_TOKEN como variável de ambiente ou em .streamlit/secrets.toml.
O token pode ser de uma Internal Integration com acesso ao Pipeline DB.

Uso:
  NOTION_TOKEN=secret_xxx python3 sync_notion_auto.py
  python3 sync_notion_auto.py  # usa st.secrets ou .env
"""
import json
import os
import sys
from pathlib import Path
from datetime import datetime

import requests

DATA_DIR = Path(__file__).resolve().parent / "data"
PIPELINE_FILE = DATA_DIR / "pipeline.json"
PIPELINE_DB_ID = "2f68e4de-57c8-811b-a1ec-000baba9429a"
NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


def get_notion_token() -> str:
    """Busca o token do Notion em várias fontes."""
    # 1. Variável de ambiente
    token = os.environ.get("NOTION_TOKEN")
    if token:
        return token

    # 2. Streamlit secrets
    try:
        import streamlit as st
        token = st.secrets.get("NOTION_TOKEN")
        if token:
            return token
    except Exception:
        pass

    # 3. Arquivo .env local
    env_file = Path(__file__).resolve().parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("NOTION_TOKEN="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")

    return ""


def query_pipeline_db(token: str) -> list[dict]:
    """Busca todos os deals do Pipeline DB via Notion API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

    all_results = []
    has_more = True
    start_cursor = None

    while has_more:
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor

        resp = requests.post(
            f"{NOTION_API}/databases/{PIPELINE_DB_ID}/query",
            headers=headers,
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        all_results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return all_results


def parse_deal(page: dict) -> dict | None:
    """Converte uma página Notion para o formato pipeline.json."""
    try:
        props = page.get("properties", {})
        page_id = page.get("id", "")

        deal = {
            "id": page_id,
            "cliente": _title(props.get("Cliente") or props.get("Name", {})),
            "status": _select(props.get("Status", {})),
            "fase": _multi_select(props.get("Fase Detalhada", {})),
            "tipo_operacao": _select(props.get("Produto", {})) or _select(props.get("Tipo de Operação", {})),
            "instrumento": _select(props.get("Instrumento", {})),
            "valor": _number(props.get("Valor (R$)", {})) or _number(props.get("Valor", {})),
            "socio": _select(props.get("Sócio Responsável Pipe", {})),
            "originador": _select(props.get("Originador", {})) or _rich_text(props.get("Originador", {})),
            "analisando": _multi_select(props.get("Analisando", {})),
            "exclusividade": _multi_select(props.get("Exclusividade", {})),
            "envio_investidores": _date(props.get("Envio a Investidores", {}) or props.get("Envio Investidores", {})),
            "cobrar_retorno": _date(props.get("Cobrar Retorno", {})),
            "notion_url": f"https://www.notion.so/{page_id.replace('-', '')}",
        }

        if not deal["cliente"]:
            return None

        # Skip archived pages
        if page.get("archived", False):
            deal["status"] = "Declinado"

        return deal
    except Exception as e:
        print(f"  Erro parsing deal: {e}", file=sys.stderr)
        return None


def _title(prop) -> str:
    if not isinstance(prop, dict):
        return ""
    items = prop.get("title", [])
    if isinstance(items, list) and items:
        return items[0].get("plain_text", "")
    return ""


def _select(prop) -> str:
    if not isinstance(prop, dict):
        return ""
    sel = prop.get("select")
    if sel and isinstance(sel, dict):
        return sel.get("name", "")
    return ""


def _multi_select(prop) -> list:
    if not isinstance(prop, dict):
        return []
    ms = prop.get("multi_select", [])
    if isinstance(ms, list):
        return [item.get("name", "") for item in ms if isinstance(item, dict)]
    return []


def _number(prop):
    if not isinstance(prop, dict):
        return None
    return prop.get("number")


def _date(prop) -> str | None:
    if not isinstance(prop, dict):
        return None
    dt = prop.get("date")
    if dt and isinstance(dt, dict):
        return dt.get("start")
    return None


def _rich_text(prop) -> str:
    if not isinstance(prop, dict):
        return ""
    items = prop.get("rich_text", [])
    if isinstance(items, list) and items:
        return items[0].get("plain_text", "")
    return ""


def main():
    token = get_notion_token()
    if not token:
        print("❌ NOTION_TOKEN não encontrado.", file=sys.stderr)
        print("Configure em:", file=sys.stderr)
        print("  - Variável de ambiente: export NOTION_TOKEN=secret_xxx", file=sys.stderr)
        print("  - Streamlit secrets: .streamlit/secrets.toml", file=sys.stderr)
        print("  - Arquivo .env na raiz do projeto", file=sys.stderr)
        sys.exit(1)

    print(f"🔄 Buscando deals do Pipeline (DB: {PIPELINE_DB_ID[:8]}...)")
    pages = query_pipeline_db(token)
    print(f"   {len(pages)} páginas encontradas")

    deals = []
    for page in pages:
        deal = parse_deal(page)
        if deal:
            deals.append(deal)
            status_icon = "✅" if deal["status"] != "Declinado" else "❌"
            val = f"R$ {deal['valor']/1e6:.1f}M" if deal.get("valor") else "—"
            print(f"   {status_icon} {deal['cliente']:30s} | {deal['status']:20s} | {val}")

    # Salva
    payload = {
        "sync_date": datetime.now().strftime("%Y-%m-%d"),
        "source": "Notion Pipeline em Andamento",
        "deals": deals,
    }
    PIPELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PIPELINE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    ativos = sum(1 for d in deals if d["status"] != "Declinado")
    declinados = len(deals) - ativos
    print(f"\n✅ Pipeline atualizado: {len(deals)} deals ({ativos} ativos, {declinados} declinados)")
    print(f"   Arquivo: {PIPELINE_FILE}")


if __name__ == "__main__":
    main()
