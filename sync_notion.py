#!/usr/bin/env python3
"""
ZYN Sales Intelligence — Sync Pipeline do Notion
Busca todos os deals do Pipeline no Notion e salva em data/pipeline.json.

Uso:
  python3 sync_notion.py              # sync completo
  python3 sync_notion.py --dry-run    # mostra o que seria atualizado sem salvar

Requer: Notion MCP conectado via Claude Code (usa fetch por page ID).
Para uso automático, este script é chamado pelo Claude Code com os dados
já obtidos via MCP tools e passados como argumento.

Alternativa manual: usar update_pipeline_from_notion() com dados coletados.
"""
import json
import sys
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).resolve().parent / "data"
PIPELINE_FILE = DATA_DIR / "pipeline.json"

# IDs das páginas do Pipeline no Notion — mantidos atualizados pelo sync
PIPELINE_DB_ID = "2f68e4de-57c8-811b-a1ec-000baba9429a"


def update_pipeline_from_notion(deals_raw: list[dict], dry_run: bool = False) -> dict:
    """
    Recebe lista de deals no formato Notion e salva no pipeline.json.

    Cada deal_raw deve ter:
      - id: UUID da página Notion
      - properties: dict com as propriedades do Notion

    Retorna: {"total": N, "ativos": N, "declinados": N, "updated": bool}
    """
    deals = []
    for raw in deals_raw:
        deal = _parse_notion_deal(raw)
        if deal:
            deals.append(deal)

    result = {
        "total": len(deals),
        "ativos": sum(1 for d in deals if d["status"] != "Declinado"),
        "declinados": sum(1 for d in deals if d["status"] == "Declinado"),
        "updated": False,
    }

    if dry_run:
        print(f"[DRY RUN] {result['total']} deals ({result['ativos']} ativos, {result['declinados']} declinados)")
        for d in deals:
            status_icon = "✅" if d["status"] != "Declinado" else "❌"
            val = f"R$ {d['valor']/1e6:.1f}M" if d.get("valor") else "—"
            print(f"  {status_icon} {d['cliente']:30s} | {d['status']:20s} | {d['tipo_operacao']:15s} | {val}")
        return result

    payload = {
        "sync_date": datetime.now().strftime("%Y-%m-%d"),
        "source": "Notion Pipeline em Andamento",
        "deals": deals,
    }

    PIPELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PIPELINE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    result["updated"] = True
    print(f"✅ Pipeline atualizado: {result['total']} deals ({result['ativos']} ativos)")
    return result


def update_pipeline_direct(deals: list[dict]) -> dict:
    """
    Recebe lista de deals já no formato final (mesmo schema do pipeline.json)
    e salva direto. Usado quando os dados já foram parseados pelo Claude.
    """
    payload = {
        "sync_date": datetime.now().strftime("%Y-%m-%d"),
        "source": "Notion Pipeline em Andamento",
        "deals": deals,
    }

    PIPELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PIPELINE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    ativos = sum(1 for d in deals if d.get("status") != "Declinado")
    return {"total": len(deals), "ativos": ativos, "updated": True}


def _parse_notion_deal(raw: dict) -> dict | None:
    """Converte um deal do formato Notion para o formato pipeline.json."""
    try:
        props = raw.get("properties", raw)

        # Extrai campos com fallback
        deal = {
            "id": raw.get("id", ""),
            "cliente": _get_title(props.get("Cliente") or props.get("Name") or props.get("cliente", "")),
            "status": _get_select(props.get("Status") or props.get("status", "")),
            "fase": _get_multi_select(props.get("Fase Detalhada") or props.get("fase", [])),
            "tipo_operacao": _get_select(props.get("Produto") or props.get("tipo_operacao", "")),
            "instrumento": _get_select(props.get("Instrumento") or props.get("instrumento", "")),
            "valor": _get_number(props.get("Valor (R$)") or props.get("valor")),
            "socio": _get_select(props.get("Sócio Responsável Pipe") or props.get("socio", "")),
            "originador": _get_select(props.get("Originador") or props.get("originador", "")),
            "analisando": _get_multi_select(props.get("Analisando") or props.get("analisando", [])),
            "exclusividade": _get_multi_select(props.get("Exclusividade") or props.get("exclusividade", [])),
            "envio_investidores": _get_date(props.get("Envio Investidores") or props.get("envio_investidores")),
            "cobrar_retorno": _get_date(props.get("Cobrar Retorno") or props.get("cobrar_retorno")),
            "notion_url": raw.get("url") or raw.get("notion_url", ""),
        }

        if not deal["cliente"]:
            return None
        return deal
    except Exception:
        return None


def _get_title(prop) -> str:
    if isinstance(prop, str):
        return prop
    if isinstance(prop, dict):
        title = prop.get("title", [])
        if isinstance(title, list) and title:
            return title[0].get("plain_text", "")
    return ""


def _get_select(prop) -> str:
    if isinstance(prop, str):
        return prop
    if isinstance(prop, dict):
        sel = prop.get("select")
        if sel and isinstance(sel, dict):
            return sel.get("name", "")
    return ""


def _get_multi_select(prop) -> list:
    if isinstance(prop, list):
        return prop
    if isinstance(prop, dict):
        ms = prop.get("multi_select", [])
        if isinstance(ms, list):
            return [item.get("name", "") for item in ms if isinstance(item, dict)]
    return []


def _get_number(prop):
    if isinstance(prop, (int, float)):
        return prop
    if isinstance(prop, dict):
        return prop.get("number")
    return None


def _get_date(prop) -> str | None:
    if isinstance(prop, str):
        return prop if prop else None
    if isinstance(prop, dict):
        dt = prop.get("date")
        if dt and isinstance(dt, dict):
            return dt.get("start")
    return None


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv

    # Se receber JSON por stdin, usa como input
    if not sys.stdin.isatty():
        data = json.load(sys.stdin)
        if isinstance(data, list):
            update_pipeline_from_notion(data, dry_run=dry_run)
        elif isinstance(data, dict) and "deals" in data:
            update_pipeline_direct(data["deals"])
        else:
            print("Formato não reconhecido. Esperado: lista de deals ou {deals: [...]}")
            sys.exit(1)
    else:
        print("Uso: echo '[deals_json]' | python3 sync_notion.py")
        print("  ou: python3 sync_notion.py --dry-run < deals.json")
        print()
        print(f"Pipeline atual: {PIPELINE_FILE}")
        if PIPELINE_FILE.exists():
            with open(PIPELINE_FILE) as f:
                data = json.load(f)
            print(f"  Sync: {data.get('sync_date', '?')}")
            print(f"  Deals: {len(data.get('deals', []))}")
        else:
            print("  Arquivo não encontrado.")
