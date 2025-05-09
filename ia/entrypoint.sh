#!/bin/bash
set -e

cd /app

# Vérifie les modèles nécessaires, lance l'entraînement si l'un manque
for f in xgb_top5_model.pkl xgb_le_top5.pkl xgb_oe_top5.pkl xgb_top5_cols.pkl; do
  if [ ! -f "$f" ]; then
    echo "⚙️  $f manquant, lancement de train.py…"
    python train.py
    break
  fi
done

echo "🚀  Lancement de predict.py…"
exec python predict.py
