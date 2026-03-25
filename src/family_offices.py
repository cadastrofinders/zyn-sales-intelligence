"""
ZYN Sales Intelligence — Base de Family Offices e Tesourarias
Investidores que não aparecem nos dados CVM (não operam via fundos regulados).
Base manual para enriquecimento e prospecção.
"""
import json
from pathlib import Path
from config.settings import DATA_DIR

FO_FILE = DATA_DIR / "family_offices.json"

# Template de FO/Tesouraria
FO_TEMPLATE = {
    "nome": "",
    "tipo": "",  # "Family Office", "Tesouraria Banco", "Seguradora", "Previdência"
    "cnpj": "",
    "contato_nome": "",
    "contato_email": "",
    "contato_telefone": "",
    "apetite": [],  # ["NC", "CRI", "CRA", "CPR-F", "DEBENTURE"]
    "ticket_min": 0,
    "ticket_max": 0,
    "indexador_pref": "",  # "CDI", "IPCA", "PRE"
    "prazo_max_anos": 0,
    "notas": "",
    "origem": "",  # "indicação", "evento", "prospecção", etc.
    "ativo": True,
}

# === Seed de investidores conhecidos no mercado (sem dados sensíveis) ===
SEED_INVESTORS = [
    # Tesourarias de Bancos
    {"nome": "Itaú BBA - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRI", "CRA", "DEBENTURE"], "ticket_min": 10_000_000, "ticket_max": 500_000_000},
    {"nome": "Bradesco BBI - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRI", "CRA", "DEBENTURE"], "ticket_min": 10_000_000, "ticket_max": 500_000_000},
    {"nome": "BTG Pactual - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRI", "CRA", "DEBENTURE", "CPR-F"], "ticket_min": 5_000_000, "ticket_max": 1_000_000_000},
    {"nome": "Santander - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRI", "DEBENTURE"], "ticket_min": 10_000_000, "ticket_max": 300_000_000},
    {"nome": "Safra - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRI", "CRA", "DEBENTURE"], "ticket_min": 5_000_000, "ticket_max": 200_000_000},
    {"nome": "ABC Brasil - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRI", "CRA", "DEBENTURE"], "ticket_min": 5_000_000, "ticket_max": 100_000_000},
    {"nome": "Daycoval - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRI", "CRA", "DEBENTURE"], "ticket_min": 5_000_000, "ticket_max": 100_000_000},
    {"nome": "Pine - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRA", "DEBENTURE"], "ticket_min": 3_000_000, "ticket_max": 50_000_000},
    {"nome": "BMG - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRI", "DEBENTURE"], "ticket_min": 3_000_000, "ticket_max": 50_000_000},
    {"nome": "Original - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRA", "DEBENTURE"], "ticket_min": 5_000_000, "ticket_max": 80_000_000},
    {"nome": "Banco Master - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRI", "CRA", "DEBENTURE", "CPR-F"], "ticket_min": 3_000_000, "ticket_max": 100_000_000},
    {"nome": "Voiter - Tesouraria", "tipo": "Tesouraria Banco", "apetite": ["NC", "CRI", "CRA", "DEBENTURE"], "ticket_min": 2_000_000, "ticket_max": 50_000_000},
    # Seguradoras
    {"nome": "Porto Seguro - Investimentos", "tipo": "Seguradora", "apetite": ["CRI", "CRA", "DEBENTURE"], "ticket_min": 10_000_000, "ticket_max": 200_000_000, "indexador_pref": "IPCA"},
    {"nome": "SulAmérica - Investimentos", "tipo": "Seguradora", "apetite": ["CRI", "CRA", "DEBENTURE"], "ticket_min": 10_000_000, "ticket_max": 150_000_000, "indexador_pref": "IPCA"},
    {"nome": "Tokio Marine - Investimentos", "tipo": "Seguradora", "apetite": ["CRI", "CRA", "DEBENTURE"], "ticket_min": 5_000_000, "ticket_max": 80_000_000},
    # Previdência
    {"nome": "Brasilprev", "tipo": "Previdência", "apetite": ["CRI", "CRA", "DEBENTURE"], "ticket_min": 20_000_000, "ticket_max": 500_000_000, "indexador_pref": "IPCA"},
    {"nome": "Funcef", "tipo": "Previdência", "apetite": ["CRI", "CRA", "DEBENTURE"], "ticket_min": 20_000_000, "ticket_max": 300_000_000, "indexador_pref": "IPCA"},
    {"nome": "Petros", "tipo": "Previdência", "apetite": ["CRI", "CRA", "DEBENTURE"], "ticket_min": 20_000_000, "ticket_max": 300_000_000, "indexador_pref": "IPCA"},
    {"nome": "Previ", "tipo": "Previdência", "apetite": ["CRI", "CRA", "DEBENTURE"], "ticket_min": 50_000_000, "ticket_max": 1_000_000_000, "indexador_pref": "IPCA"},
    {"nome": "Valia", "tipo": "Previdência", "apetite": ["CRI", "CRA", "DEBENTURE"], "ticket_min": 10_000_000, "ticket_max": 200_000_000, "indexador_pref": "IPCA"},
]


def load_family_offices() -> list[dict]:
    """Carrega base de FOs do arquivo JSON."""
    if FO_FILE.exists():
        with open(FO_FILE) as f:
            return json.load(f)
    return []


def save_family_offices(investors: list[dict]):
    """Salva base de FOs."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(FO_FILE, "w") as f:
        json.dump(investors, f, indent=2, ensure_ascii=False)


def initialize_fo_base():
    """Inicializa base com seed se vazia."""
    existing = load_family_offices()
    if existing:
        return existing

    save_family_offices(SEED_INVESTORS)
    print(f"  ✓ Base de FOs/Tesourarias inicializada com {len(SEED_INVESTORS)} investidores")
    return SEED_INVESTORS


def add_investor(investor: dict) -> list[dict]:
    """Adiciona investidor à base."""
    base = load_family_offices()
    # Merge com template
    new = {**FO_TEMPLATE, **investor}
    base.append(new)
    save_family_offices(base)
    return base


def search_by_appetite(asset_type: str) -> list[dict]:
    """Busca investidores por tipo de apetite."""
    base = load_family_offices()
    return [inv for inv in base if asset_type.upper() in [a.upper() for a in inv.get("apetite", [])] and inv.get("ativo", True)]


def search_by_ticket(min_value: float, max_value: float) -> list[dict]:
    """Busca investidores por faixa de ticket."""
    base = load_family_offices()
    results = []
    for inv in base:
        if not inv.get("ativo", True):
            continue
        inv_min = inv.get("ticket_min", 0)
        inv_max = inv.get("ticket_max", float("inf"))
        # Overlap check
        if min_value <= inv_max and max_value >= inv_min:
            results.append(inv)
    return results
