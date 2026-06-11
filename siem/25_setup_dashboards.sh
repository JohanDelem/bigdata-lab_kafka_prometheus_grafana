#!/usr/bin/env bash
# =============================================================
# 25_setup_dashboards.sh — Configuration initiale d'OpenSearch
# Dashboards pour la console SOC SIEM
#
# Actions :
#   1. Attend que Dashboards soit prêt (retry 30s)
#   2. Crée l'index pattern siem-events-* avec @timestamp
#   3. Le définit comme index par défaut
#   4. Crée l'index pattern corr-alerts-* (optionnel)
#
# Usage :
#   bash siem/25_setup_dashboards.sh
#
# Prérequis :
#   - docker compose up -d opensearch opensearch-dashboards
#   - Au moins un index siem-events-YYYY.MM.DD existant
#     (lancer siem/20_normalizer.py quelques minutes)
# =============================================================

set -euo pipefail

OSD="http://localhost:5601"
HEADERS=('-H' 'Content-Type: application/json' '-H' 'osd-xsrf: true')

# --- Attendre que Dashboards soit prêt ---
echo "[setup-dashboards] Attente du démarrage d'OpenSearch Dashboards..."
for i in $(seq 1 12); do
  if curl -s "$OSD/api/status" | grep -q '"overall"'; then
    echo "[setup-dashboards] Dashboards prêt"
    break
  fi
  echo "[setup-dashboards] Tentative $i/12 — attente 10s..."
  sleep 10
done

# --- Index pattern principal : siem-events-* ---
echo ""
echo "[setup-dashboards] Création index pattern siem-events-*..."
curl -s -X POST "$OSD/api/saved_objects/index-pattern/siem-events" \
  "${HEADERS[@]}" \
  -d '{
    "attributes": {
      "title": "siem-events-*",
      "timeFieldName": "@timestamp"
    }
  }' | python3 -c "
import sys, json
r = json.load(sys.stdin)
if 'error' in r:
    print(f'  ERREUR : {r[\"error\"]}')
elif r.get('id'):
    print(f'  OK — index pattern créé : {r[\"attributes\"][\"title\"]}')
else:
    print(f'  Déjà existant ou créé')
" 2>/dev/null || echo "  OK (déjà existant)"

# --- Index par défaut ---
echo "[setup-dashboards] Définition index par défaut..."
curl -s -X POST "$OSD/api/opensearch-dashboards/settings" \
  "${HEADERS[@]}" \
  -d '{"changes": {"defaultIndex": "siem-events"}}' > /dev/null
echo "  OK — siem-events défini par défaut"

# --- Index pattern corr-alerts (optionnel) ---
echo "[setup-dashboards] Création index pattern corr-alerts (optionnel)..."
curl -s -X POST "$OSD/api/saved_objects/index-pattern/corr-alerts" \
  "${HEADERS[@]}" \
  -d '{
    "attributes": {
      "title": "corr-alerts",
      "timeFieldName": "@timestamp"
    }
  }' > /dev/null
echo "  OK"

echo ""
echo "[setup-dashboards] Configuration terminée"
echo "[setup-dashboards] Console SOC : http://localhost:5601"
echo "[setup-dashboards] -> Discover   : visualiser les events SIEM"
echo "[setup-dashboards] -> Dev Tools  : requêtes OpenSearch manuelles"
