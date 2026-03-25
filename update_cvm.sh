#!/bin/bash
# ZYN Sales Intelligence — Atualização automática CVM
# Roda a cada 30 dias via cron
# Logs em: ~/Downloads/zyn-sales-intelligence/data/update.log

cd "$(dirname "$0")"
LOG="data/update.log"

echo "========================================" >> "$LOG"
echo "Atualização: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
echo "========================================" >> "$LOG"

/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 main.py --months 3 --export-notion >> "$LOG" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Atualização concluída com sucesso" >> "$LOG"

    # Auto-push para GitHub → Streamlit Cloud atualiza sozinho
    echo "📤 Enviando para GitHub..." >> "$LOG"
    git add -A >> "$LOG" 2>&1
    git commit -m "Atualização automática CVM — $(date '+%d/%m/%Y')" >> "$LOG" 2>&1
    git push >> "$LOG" 2>&1
    echo "✅ Push concluído" >> "$LOG"
else
    echo "❌ Erro na atualização (exit code: $EXIT_CODE)" >> "$LOG"
fi

echo "" >> "$LOG"
