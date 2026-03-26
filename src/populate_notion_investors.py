#!/usr/bin/env python3
"""
ZYN Sales Intelligence — Popula Notion com perfis de investidores.
Cria (ou atualiza) o database "Investidores — Sales Intelligence" no Notion
com 231 gestoras mapeadas via CVM, incluindo volumes, tickets, preferências.

Uso:
  python3 src/populate_notion_investors.py              # Cria DB + popula
  python3 src/populate_notion_investors.py --update     # Atualiza existente
  python3 src/populate_notion_investors.py --dry-run    # Mostra sem enviar
"""
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests

# ── Paths ──
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
PROFILES_FILE = DATA_DIR / "investor_profiles.csv"
PIPELINE_FILE = DATA_DIR / "pipeline.json"
STATE_FILE = DATA_DIR / "notion_investors_state.json"

# ── Notion Config ──
NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
GESTAO_PAGE_ID = "2f68e4de-57c8-80a4-b59e-fbb54579c1a9"  # Gestão Zyn Capital page

# Faixas de classificação
TIERS = {
    "Tier 1 — Top": lambda r: r["vol_total"] >= 10e9,
    "Tier 2 — Grande": lambda r: 1e9 <= r["vol_total"] < 10e9,
    "Tier 3 — Médio": lambda r: 100e6 <= r["vol_total"] < 1e9,
    "Tier 4 — Pequeno": lambda r: r["vol_total"] < 100e6,
}


def _get_token() -> str:
    token = os.environ.get("NOTION_TOKEN", "")
    if token:
        return token
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("NOTION_TOKEN="):
                return line.split("=", 1)[1].strip().strip("\"'")
    return ""


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _fmt_brl(value) -> str:
    """Formata valor em R$ legível."""
    if pd.isna(value) or value == 0:
        return "—"
    v = abs(value)
    if v >= 1e9:
        return f"R$ {v/1e9:.1f}B"
    if v >= 1e6:
        return f"R$ {v/1e6:.1f}M"
    if v >= 1e3:
        return f"R$ {v/1e3:.0f}k"
    return f"R$ {v:.0f}"


def _norm_cnpj(s) -> str:
    if pd.isna(s):
        return ""
    s = str(s).replace(".", "").replace("/", "").replace("-", "").strip()
    if s.endswith("0"):
        pass
    return s.zfill(14)


def _fmt_cnpj(s: str) -> str:
    s = s.zfill(14)
    return f"{s[:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:14]}"


def classify_tier(row) -> str:
    for tier, fn in TIERS.items():
        if fn(row):
            return tier
    return "Tier 4 — Pequeno"


def classify_ativos(row) -> list[str]:
    """Lista de ativos que a gestora compra."""
    ativos = []
    for tipo in ["NC", "CRI", "CRA", "CPR-F", "DEBENTURE"]:
        if row.get(f"vol_{tipo}", 0) > 0:
            label = tipo if tipo != "DEBENTURE" else "Debênture"
            ativos.append(label)
    return ativos


def load_profiles() -> pd.DataFrame:
    df = pd.read_csv(PROFILES_FILE)
    df = df.fillna(0)
    df = df.sort_values("vol_total", ascending=False).reset_index(drop=True)
    return df


def create_database(token: str) -> str:
    """Cria o database Investidores no Notion com schema elegante."""
    payload = {
        "parent": {"type": "page_id", "page_id": GESTAO_PAGE_ID},
        "icon": {"type": "emoji", "emoji": "🎯"},
        "title": [{"type": "text", "text": {"content": "Investidores — Sales Intelligence"}}],
        "properties": {
            "Gestora": {"title": {}},
            "CNPJ": {"rich_text": {}},
            "Tier": {
                "select": {
                    "options": [
                        {"name": "Tier 1 — Top", "color": "red"},
                        {"name": "Tier 2 — Grande", "color": "orange"},
                        {"name": "Tier 3 — Médio", "color": "yellow"},
                        {"name": "Tier 4 — Pequeno", "color": "gray"},
                    ]
                }
            },
            "Ativos": {
                "multi_select": {
                    "options": [
                        {"name": "NC", "color": "blue"},
                        {"name": "CRI", "color": "green"},
                        {"name": "CRA", "color": "brown"},
                        {"name": "CPR-F", "color": "orange"},
                        {"name": "Debênture", "color": "purple"},
                    ]
                }
            },
            "Tipo Preferido": {
                "select": {
                    "options": [
                        {"name": "DEBENTURE", "color": "purple"},
                        {"name": "NC", "color": "blue"},
                        {"name": "CRI", "color": "green"},
                        {"name": "CRA", "color": "brown"},
                        {"name": "CPR-F", "color": "orange"},
                    ]
                }
            },
            "Volume Total": {"number": {"format": "number"}},
            "PL Total": {"number": {"format": "number"}},
            "Nº Fundos": {"number": {"format": "number"}},
            "Ticket Médio": {"number": {"format": "number"}},
            "Ticket Máximo": {"number": {"format": "number"}},
            "Vol NC": {"number": {"format": "number"}},
            "Vol CRI": {"number": {"format": "number"}},
            "Vol CRA": {"number": {"format": "number"}},
            "Vol Debênture": {"number": {"format": "number"}},
            "Nº Ops NC": {"number": {"format": "number"}},
            "Nº Ops CRI": {"number": {"format": "number"}},
            "Nº Ops CRA": {"number": {"format": "number"}},
            "Nº Ops Debênture": {"number": {"format": "number"}},
            "Prazo Médio (anos)": {"number": {"format": "number"}},
            "Indexador": {
                "select": {
                    "options": [
                        {"name": "DI de um dia", "color": "blue"},
                        {"name": "IPCA", "color": "green"},
                        {"name": "CDI", "color": "purple"},
                        {"name": "OUTROS", "color": "gray"},
                        {"name": "PRE FIXADO", "color": "orange"},
                    ]
                }
            },
            "Spread Médio (%)": {"number": {"format": "percent"}},
            "Classe": {"rich_text": {}},
            "Público Alvo": {
                "select": {
                    "options": [
                        {"name": "Profissional", "color": "blue"},
                        {"name": "Qualificado", "color": "green"},
                        {"name": "Público Geral", "color": "gray"},
                    ]
                }
            },
            "Diversificação": {"number": {"format": "percent"}},
            "Concentração": {"number": {"format": "percent"}},
            "Score": {"number": {"format": "number"}},
            "Status": {
                "select": {
                    "options": [
                        {"name": "Mapeado", "color": "gray"},
                        {"name": "Em Contato", "color": "yellow"},
                        {"name": "Interessado", "color": "green"},
                        {"name": "Alocou", "color": "blue"},
                        {"name": "Recusou", "color": "red"},
                    ]
                }
            },
            "Notas": {"rich_text": {}},
        },
    }

    resp = requests.post(
        f"{NOTION_API}/databases",
        headers=_headers(token),
        json=payload,
    )
    resp.raise_for_status()
    db = resp.json()
    db_id = db["id"]
    print(f"✅ Database criado: {db_id}")
    return db_id


def build_page_properties(row) -> dict:
    """Converte uma linha do CSV em properties do Notion."""
    tier = classify_tier(row)
    ativos = classify_ativos(row)
    cnpj_raw = _norm_cnpj(row.get("cnpj_gestora", ""))
    cnpj_fmt = _fmt_cnpj(cnpj_raw) if cnpj_raw else ""

    # Score: composição simples baseada em volume, diversificação, nº operações
    vol_score = min(row["vol_total"] / 50e9, 1.0) * 40  # max 40pts
    div_score = row.get("diversificacao", 0) * 20  # max 20pts
    ops_total = sum(row.get(f"n_ops_{t}", 0) for t in ["NC", "CRI", "CRA", "CPR-F", "DEBENTURE"])
    ops_score = min(ops_total / 5000, 1.0) * 20  # max 20pts
    fundos_score = min(row.get("n_fundos", 0) / 100, 1.0) * 20  # max 20pts
    score = round(vol_score + div_score + ops_score + fundos_score, 1)

    indexador = row.get("indexador_principal", "")
    if not indexador or indexador == "0" or pd.isna(indexador):
        indexador = "OUTROS"

    publico = row.get("publico_alvo", "")
    if not publico or publico == "0" or pd.isna(publico):
        publico = None

    props = {
        "Gestora": {"title": [{"text": {"content": str(row["gestora"])[:100]}}]},
        "CNPJ": {"rich_text": [{"text": {"content": cnpj_fmt}}]},
        "Tier": {"select": {"name": tier}},
        "Ativos": {"multi_select": [{"name": a} for a in ativos]},
        "Volume Total": {"number": round(row["vol_total"], 2)},
        "PL Total": {"number": round(row["pl_total"], 2)},
        "Nº Fundos": {"number": int(row["n_fundos"])},
        "Score": {"number": score},
        "Status": {"select": {"name": "Mapeado"}},
        "Classe": {"rich_text": [{"text": {"content": str(row.get("classe_predominante", ""))[:100]}}]},
    }

    # Tipo preferido
    tipo_pref = row.get("tipo_preferido", "")
    if tipo_pref and tipo_pref != "0":
        props["Tipo Preferido"] = {"select": {"name": str(tipo_pref)}}

    # Volumes por tipo
    for tipo, col in [("NC", "vol_NC"), ("CRI", "vol_CRI"), ("CRA", "vol_CRA"), ("Debênture", "vol_DEBENTURE")]:
        val = row.get(col, 0)
        if val > 0:
            props[f"Vol {tipo}"] = {"number": round(val, 2)}

    # Nº operações
    for tipo, col in [("NC", "n_ops_NC"), ("CRI", "n_ops_CRI"), ("CRA", "n_ops_CRA"), ("Debênture", "n_ops_DEBENTURE")]:
        val = row.get(col, 0)
        if val > 0:
            props[f"Nº Ops {tipo}"] = {"number": int(val)}

    # Tickets
    ticket_medio = row.get("ticket_medio", 0)
    if ticket_medio and ticket_medio > 0:
        props["Ticket Médio"] = {"number": round(ticket_medio, 2)}
    ticket_max = row.get("ticket_max", 0)
    if ticket_max and ticket_max > 0:
        props["Ticket Máximo"] = {"number": round(ticket_max, 2)}

    # Prazo
    prazo = row.get("prazo_medio_anos", 0)
    if prazo and prazo > 0:
        props["Prazo Médio (anos)"] = {"number": round(prazo, 2)}

    # Indexador
    if indexador and indexador != "OUTROS":
        props["Indexador"] = {"select": {"name": indexador}}

    # Spread
    spread = row.get("spread_medio", 0)
    if spread and spread > 0:
        props["Spread Médio (%)"] = {"number": round(spread / 100, 4)}

    # Público
    if publico:
        props["Público Alvo"] = {"select": {"name": publico}}

    # Diversificação e Concentração
    div = row.get("diversificacao", 0)
    if div > 0:
        props["Diversificação"] = {"number": round(div, 4)}
    conc = row.get("concentracao_tipo_pref", 0)
    if conc > 0:
        props["Concentração"] = {"number": round(conc, 4)}

    return props


def build_page_content(row) -> str:
    """Gera conteúdo rico da página do investidor."""
    fundos_list = str(row.get("fundos", ""))
    fundos = [f.strip() for f in fundos_list.split(";") if f.strip()][:10]

    lines = []
    lines.append(f"## Perfil — {row['gestora']}")
    lines.append("")

    # Resumo
    lines.append("### Resumo")
    lines.append(f"- **Volume Total em Crédito Privado:** {_fmt_brl(row['vol_total'])}")
    lines.append(f"- **PL Total dos Fundos:** {_fmt_brl(row['pl_total'])}")
    lines.append(f"- **Nº de Fundos:** {int(row['n_fundos'])}")
    lines.append(f"- **Tipo Preferido:** {row.get('tipo_preferido', '—')}")
    lines.append(f"- **Ticket Médio:** {_fmt_brl(row.get('ticket_medio', 0))}")
    lines.append(f"- **Ticket Máximo:** {_fmt_brl(row.get('ticket_max', 0))}")
    lines.append(f"- **Prazo Médio:** {row.get('prazo_medio_anos', 0):.1f} anos")
    lines.append("")

    # Breakdown por ativo
    lines.append("### Volumes por Ativo")
    for tipo in ["NC", "CRI", "CRA", "CPR-F", "DEBENTURE"]:
        vol = row.get(f"vol_{tipo}", 0)
        n = int(row.get(f"n_ops_{tipo}", 0))
        if vol > 0:
            lines.append(f"- **{tipo}:** {_fmt_brl(vol)} ({n} operações)")
    lines.append("")

    # Fundos principais
    if fundos:
        lines.append("### Principais Fundos")
        for f in fundos:
            lines.append(f"- {f}")
        if len(fundos_list.split(";")) > 10:
            lines.append(f"- _... e mais {len(fundos_list.split(';')) - 10} fundos_")
    lines.append("")

    return "\n".join(lines)


def populate(token: str, db_id: str, dry_run: bool = False):
    """Popula o database com os perfis de investidores."""
    df = load_profiles()
    print(f"📊 {len(df)} gestoras para inserir")

    created = 0
    errors = 0
    for i, (_, row) in enumerate(df.iterrows()):
        props = build_page_properties(row)
        content_text = build_page_content(row)

        # Build children blocks (content)
        children = _markdown_to_blocks(content_text)

        if dry_run:
            print(f"  [{i+1}] {row['gestora'][:50]} — {classify_tier(row)} — Score: {props['Score']['number']}")
            if i < 3:
                print(f"       Ativos: {[a['name'] for a in props['Ativos']['multi_select']]}")
                print(f"       Vol: {_fmt_brl(row['vol_total'])}, Ticket: {_fmt_brl(row.get('ticket_medio',0))}")
            continue

        payload = {
            "parent": {"database_id": db_id},
            "icon": {"type": "emoji", "emoji": _tier_emoji(classify_tier(row))},
            "properties": props,
            "children": children,
        }

        try:
            resp = requests.post(
                f"{NOTION_API}/pages",
                headers=_headers(token),
                json=payload,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 2))
                print(f"  ⏳ Rate limit, aguardando {wait}s...")
                time.sleep(wait)
                resp = requests.post(
                    f"{NOTION_API}/pages",
                    headers=_headers(token),
                    json=payload,
                )
            resp.raise_for_status()
            created += 1

            if (i + 1) % 20 == 0:
                print(f"  {i+1}/{len(df)} — {created} criadas")

            # Respect Notion rate limits (~3 req/s)
            time.sleep(0.35)

        except Exception as e:
            errors += 1
            print(f"  ❌ Erro em {row['gestora'][:40]}: {e}")
            if errors > 10:
                print("  Muitos erros, abortando.")
                break
            time.sleep(1)

    print(f"\n✅ Povoamento completo: {created} gestoras criadas, {errors} erros")

    # Salvar estado
    state = {"db_id": db_id, "count": created, "timestamp": pd.Timestamp.now().isoformat()}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

    return db_id


def _tier_emoji(tier: str) -> str:
    return {
        "Tier 1 — Top": "🏆",
        "Tier 2 — Grande": "⭐",
        "Tier 3 — Médio": "🔷",
        "Tier 4 — Pequeno": "⚪",
    }.get(tier, "⚪")


def _markdown_to_blocks(md: str) -> list[dict]:
    """Converte markdown simples em blocos Notion."""
    blocks = []
    for line in md.split("\n"):
        if not line.strip():
            continue
        if line.startswith("### "):
            blocks.append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {"rich_text": [{"type": "text", "text": {"content": line[4:]}}]},
            })
        elif line.startswith("## "):
            blocks.append({
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": [{"type": "text", "text": {"content": line[3:]}}]},
            })
        elif line.startswith("- "):
            text = line[2:]
            rich = _parse_bold(text)
            blocks.append({
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": rich},
            })
        else:
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": line}}]},
            })
    return blocks[:100]  # Notion limit


def _parse_bold(text: str) -> list[dict]:
    """Parse **bold** in text into Notion rich_text."""
    parts = []
    while "**" in text:
        idx = text.index("**")
        if idx > 0:
            parts.append({"type": "text", "text": {"content": text[:idx]}})
        text = text[idx + 2:]
        end = text.find("**")
        if end == -1:
            parts.append({"type": "text", "text": {"content": "**" + text}})
            text = ""
            break
        bold_text = text[:end]
        parts.append({
            "type": "text",
            "text": {"content": bold_text},
            "annotations": {"bold": True},
        })
        text = text[end + 2:]
    if text:
        parts.append({"type": "text", "text": {"content": text}})
    return parts if parts else [{"type": "text", "text": {"content": text}}]


def main():
    dry_run = "--dry-run" in sys.argv
    update = "--update" in sys.argv

    token = _get_token()
    if not token and not dry_run:
        print("❌ NOTION_TOKEN não encontrado")
        sys.exit(1)

    # Check for existing DB
    db_id = None
    if STATE_FILE.exists():
        state = json.load(open(STATE_FILE))
        db_id = state.get("db_id")

    if update and db_id:
        print(f"📝 Atualizando database existente: {db_id}")
    elif not dry_run:
        print("🏗️  Criando database no Notion...")
        db_id = create_database(token)
    else:
        print("🔍 Dry-run — simulando povoamento:")
        db_id = "dry-run"

    populate(token, db_id, dry_run=dry_run)


if __name__ == "__main__":
    main()
