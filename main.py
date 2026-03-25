#!/usr/bin/env python3
"""
ZYN Sales Intelligence — Script Principal
Executa o pipeline completo: ingestão CVM → análise → perfis → relatórios.

Uso:
    python main.py                    # Pipeline completo
    python main.py --skip-download    # Usa cache local (não baixa CVM)
    python main.py --months 6         # Últimos 6 meses de dados
    python main.py --match "CRA"      # Matching para tipo específico
    python main.py --export-notion    # Gera JSON para sync Notion
"""
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

# Adiciona raiz ao path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.settings import DATA_DIR, OUTPUT_DIR
from src.cvm_ingestion import run_ingestion
from src.cedente_enrichment import run_cedente_enrichment
from src.analyzer import build_investor_profiles, match_deal_to_investors, generate_market_overview
from src.report_generator import export_investor_profiles, export_deal_matching
from src.family_offices import initialize_fo_base, search_by_appetite
from src.notion_sync import generate_notion_insert_instructions


def print_header():
    print("=" * 70)
    print("  ZYN SALES INTELLIGENCE")
    print("  Mapeamento de Investidores — Renda Fixa Estruturada")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 70)


def print_overview(overview: dict):
    """Imprime visão geral do mercado."""
    print("\n" + "=" * 50)
    print("📊 VISÃO GERAL DO MERCADO")
    print("=" * 50)

    fmt = lambda v: f"R$ {v/1e9:.2f}B" if v >= 1e9 else f"R$ {v/1e6:.1f}M"

    print(f"  Posições mapeadas:  {overview.get('total_posicoes', 0):,}")
    print(f"  Volume total:       {fmt(overview.get('volume_total', 0))}")
    print(f"  Fundos únicos:      {overview.get('fundos_unicos', 0):,}")
    print(f"  Gestoras únicas:    {overview.get('gestoras_unicas', 0):,}")

    if "por_tipo" in overview:
        print("\n  📈 Por Tipo de Ativo:")
        for tipo, row in overview["por_tipo"].iterrows():
            print(f"    {tipo:12s}  Vol: {fmt(row['volume']):>12s}  |  {int(row['n_posicoes']):>5d} posições  |  {int(row['n_fundos']):>4d} fundos")

    if "top_gestoras" in overview:
        print("\n  🏢 Top 10 Gestoras:")
        for i, (gestora, row) in enumerate(overview["top_gestoras"].head(10).iterrows(), 1):
            print(f"    {i:2d}. {gestora[:40]:40s}  {fmt(row['volume']):>12s}")


def print_profiles_summary(profiles):
    """Imprime resumo dos perfis."""
    print(f"\n  ✓ {len(profiles)} gestoras perfiladas")

    fmt = lambda v: f"R$ {v/1e9:.2f}B" if v >= 1e9 else f"R$ {v/1e6:.1f}M"

    print("\n  🏆 Top 15 por Volume em RF Estruturada:")
    top = profiles.head(15)
    for i, (_, p) in enumerate(top.iterrows(), 1):
        nome = str(p.get("gestora", ""))[:40]
        vol = p.get("vol_total", 0)
        n = int(p.get("n_fundos", 0))
        tp = p.get("tipo_preferido", "")
        print(f"    {i:2d}. {nome:40s}  {fmt(vol):>12s}  |  {n:>3d} fundos  |  Pref: {tp}")


def run_full_pipeline(args):
    """Executa pipeline completo."""
    print_header()

    # 1. Ingestão CVM
    if args.skip_download:
        cache = DATA_DIR / "positions_enriched.csv"
        if cache.exists():
            import pandas as pd
            print("\n📂 Carregando cache local...")
            positions = pd.read_csv(cache)
            print(f"  ✓ {len(positions)} posições carregadas do cache")
        else:
            print("⚠ Cache não encontrado. Execute sem --skip-download primeiro.")
            return
    else:
        positions = run_ingestion(n_months=args.months)

    if positions.empty:
        print("⚠ Nenhuma posição encontrada. Verifique a conexão com dados.cvm.gov.br")
        return

    # 1.5 Enriquecimento de cedentes/devedores CRI/CRA
    has_cedente_placeholder = (
        "devedor" in positions.columns
        and positions["devedor"].str.contains("Cedente não identificado", na=False).any()
    )
    if has_cedente_placeholder:
        positions = run_cedente_enrichment(positions)
        # Salva cache atualizado
        cache_path = DATA_DIR / "positions_enriched.csv"
        positions.to_csv(cache_path, index=False)
        print(f"  💾 Cache atualizado com cedentes: {cache_path}")

    # 2. Visão geral do mercado
    overview = generate_market_overview(positions)
    print_overview(overview)

    # 3. Perfis de investidores
    print("\n\n" + "=" * 50)
    print("👥 PERFIS DE INVESTIDORES")
    print("=" * 50)
    profiles = build_investor_profiles(positions)
    print_profiles_summary(profiles)

    # 4. Family Offices e Tesourarias (base manual)
    print("\n\n" + "=" * 50)
    print("🏦 FAMILY OFFICES & TESOURARIAS (Base Manual)")
    print("=" * 50)
    fo_base = initialize_fo_base()
    print(f"  ✓ {len(fo_base)} investidores na base manual")

    # 5. Matching por tipo (se especificado)
    if args.match:
        print(f"\n\n" + "=" * 50)
        print(f"🎯 MATCHING — {args.match.upper()}")
        print("=" * 50)

        deal = {
            "nome": f"Operação {args.match.upper()}",
            "tipo": args.match.upper(),
            "volume": args.volume or 50_000_000,
            "prazo_anos": args.prazo or 3,
            "indexador": args.indexador or "CDI",
        }

        matching = match_deal_to_investors(deal, profiles, top_n=30)
        if not matching.empty:
            fmt = lambda v: f"R$ {v/1e9:.2f}B" if v >= 1e9 else f"R$ {v/1e6:.1f}M"
            print(f"\n  Top 15 investidores para {args.match.upper()}:")
            for i, (_, m) in enumerate(matching.head(15).iterrows(), 1):
                score = m.get("score_total", 0)
                bar = "█" * int(score * 10) + "░" * (10 - int(score * 10))
                nome = str(m.get("gestora", ""))[:35]
                print(f"    {i:2d}. [{bar}] {score:.0%}  {nome:35s}  Vol: {fmt(m.get('vol_total_rf', 0)):>10s}")

            # FOs com apetite
            fo_matches = search_by_appetite(args.match.upper())
            if fo_matches:
                print(f"\n  + {len(fo_matches)} investidores da base manual com apetite para {args.match.upper()}:")
                for fo in fo_matches:
                    print(f"      • {fo['nome']} ({fo['tipo']})")

            # Exporta matching
            export_deal_matching(deal, matching)
        else:
            print("  ⚠ Nenhum investidor encontrado com score mínimo.")

    # 6. Exporta relatórios
    print("\n\n" + "=" * 50)
    print("📊 EXPORTANDO RELATÓRIOS")
    print("=" * 50)
    excel_path = export_investor_profiles(profiles)

    # Salva perfis como CSV para uso futuro
    profiles_path = DATA_DIR / "investor_profiles.csv"
    profiles.to_csv(profiles_path, index=False)
    print(f"  💾 Perfis salvos: {profiles_path}")

    # 7. Export Notion JSON (se solicitado)
    if args.export_notion:
        print("\n  📤 Gerando dados para Notion...")
        rows = generate_notion_insert_instructions(profiles, top_n=100)
        notion_path = OUTPUT_DIR / "notion_investidores.json"
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(notion_path, "w") as f:
            json.dump(rows, f, indent=2, ensure_ascii=False)
        print(f"  ✓ {len(rows)} registros prontos: {notion_path}")

    print("\n" + "=" * 70)
    print("  ✅ Pipeline completo!")
    print(f"  📁 Relatórios em: {OUTPUT_DIR}")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="ZYN Sales Intelligence")
    parser.add_argument("--skip-download", action="store_true", help="Usa cache local")
    parser.add_argument("--months", type=int, default=3, help="Meses de dados CVM (default: 3)")
    parser.add_argument("--match", type=str, help="Tipo de ativo para matching (NC, CRI, CRA, CPR-F, DEBENTURE)")
    parser.add_argument("--volume", type=float, help="Volume da operação para matching (R$)")
    parser.add_argument("--prazo", type=float, help="Prazo em anos para matching")
    parser.add_argument("--indexador", type=str, help="Indexador para matching (CDI, IPCA, PRE)")
    parser.add_argument("--export-notion", action="store_true", help="Gera JSON para sync com Notion")
    args = parser.parse_args()

    run_full_pipeline(args)


if __name__ == "__main__":
    main()
