"""
ZYN Sales Intelligence — Configurações
"""
from pathlib import Path
from datetime import datetime, timedelta

# === Diretórios ===
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# === CVM Data URLs ===
CVM_BASE_URL = "https://dados.cvm.gov.br/dados/FI"
CDA_URL = f"{CVM_BASE_URL}/DOC/CDA/DADOS"
CAD_URL = f"{CVM_BASE_URL}/CAD/DADOS/registro_fundo_classe.zip"

# === Notion IDs (ZYN Capital) ===
NOTION_PIPELINE_DB = "2f68e4de-57c8-811b-a1ec-000baba9429a"
NOTION_INVESTIDORES_DB = None  # Será preenchido após criação
NOTION_SALES_DB = None  # Será preenchido após criação

# === Tipos de ativo que rastreamos ===
ASSET_TYPES = {
    "NC": {
        "blocks": ["BLC_8"],
        "tp_ativo_contains": ["Nota Promissoria", "Commercial Paper", "Export Note"],
        "ds_ativo_contains": ["NOTA COMERCIAL"],
        "label": "Nota Comercial",
    },
    "CRI": {
        "blocks": ["BLC_8"],
        "tp_ativo_contains": ["recebiveis imobiliarios", "Certificado de recebiveis imobiliarios"],
        "ds_ativo_contains": ["CRI"],
        "label": "CRI",
    },
    "CRA": {
        "blocks": ["BLC_6"],
        "tp_ativo_contains": ["CRA"],
        "ds_ativo_contains": ["CRA"],
        "label": "CRA",
    },
    "CPR-F": {
        "blocks": ["BLC_6"],
        "tp_ativo_contains": ["CPR"],
        "ds_ativo_contains": ["CPR"],
        "label": "CPR-F",
    },
    "DEBENTURE": {
        "blocks": ["BLC_4", "BLC_6"],
        "tp_ativo_contains": ["Debenture", "debenture"],
        "ds_ativo_contains": ["DEBENTURE", "DEB"],
        "label": "Debênture",
    },
}

# === Classificação de fundos ===
FUND_CATEGORIES = {
    "FIDC": ["FIDC", "Fundo de Investimento em Direitos Creditórios"],
    "Fiagro": ["Fiagro", "FIAGRO"],
    "FII": ["FII", "Fundo de Investimento Imobiliário"],
    "FIM": ["Multimercado", "FIM"],
    "FIRF": ["Renda Fixa", "FIRF"],
    "FIA": ["Ações", "FIA"],
    "FIP": ["FIP", "Participações"],
}

# === Meses a buscar (últimos 3 disponíveis — CVM publica com ~30-45d defasagem) ===
def get_target_months(n_months: int = 3) -> list[str]:
    """Retorna os últimos n meses no formato YYYYMM para download CVM."""
    today = datetime.now()
    # CVM publica com ~45 dias de defasagem
    ref_date = today - timedelta(days=45)
    months = []
    for i in range(n_months):
        dt = ref_date - timedelta(days=30 * i)
        months.append(dt.strftime("%Y%m"))
    return sorted(set(months))
