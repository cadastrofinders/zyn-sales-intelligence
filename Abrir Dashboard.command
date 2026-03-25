#!/bin/bash
# ZYN Sales Intelligence — Dashboard
# Clique duas vezes para abrir o dashboard no navegador

cd "$(dirname "$0")"
echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║   ZYN SALES INTELLIGENCE — Dashboard     ║"
echo "  ╚══════════════════════════════════════════╝"
echo ""
echo "  Iniciando dashboard..."
echo "  Acesse: http://localhost:8501"
echo ""
echo "  Para fechar, feche esta janela do Terminal."
echo ""

open http://localhost:8501 &
/Library/Frameworks/Python.framework/Versions/3.13/bin/streamlit run dashboard.py --server.port 8501 --server.headless true
