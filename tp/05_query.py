#!/usr/bin/env python3
import os
import sqlite3

DB_PATH = os.path.join(
    os.path.dirname(__file__),
    "tp.db"
)

if not os.path.exists(DB_PATH):

    raise SystemExit(
        "Base absente : lancez d'abord tp/04_sink_sqlite.py"
    )

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

def line(c="-"):

    """
    Affiche une ligne de séparation.

    Exemple :

    ------------------------------------------------------------
    ============================================================
    """

    print(c * 60)

row = conn.execute(

    "SELECT "
    "COUNT(*) AS fenetres, "
    "COALESCE(SUM(events_total),0) AS evts, "
    "COALESCE(SUM(revenue),0) AS ca "
    "FROM window_stats"

).fetchone()

line("=")
print(" SYNTHÈSE GLOBALE")
line("=")

print(
    f"  Fenêtres stockées : {row['fenetres']}"
)

print(
    f"  Événements totaux : {row['evts']}"
)

print(
    f"  Chiffre d'affaires: {row['ca']:.2f} €"
)

print("\n RÉPARTITION PAR ACTION")
line()

rows = conn.execute(

    "SELECT "
    "action, "
    "SUM(count) AS total "
    "FROM action_counts "
    "GROUP BY action "
    "ORDER BY total DESC"

).fetchall()

grand = sum(
    r["total"]
    for r in rows
) or 1

for r in rows:

    # Pourcentage de cette action
    pct = 100 * r["total"] / grand

    # Construction d'une barre ASCII
    #
    # Exemple :
    # ###########
    #
    bar = "#" * int(pct / 2)

    print(
        f"  {r['action']:<12}"
        f"{r['total']:>6}  "
        f"{pct:5.1f}%  "
        f"{bar}"
    )

print("\n 10 DERNIÈRES FENÊTRES")
line()
print(
    f"  {'fenêtre (début)':<28} "
    f"{'évts':>5} "
    f"{'users':>6} "
    f"{'CA €':>9}"
)
rows = conn.execute(

    "SELECT "
    "window_start, "
    "events_total, "
    "unique_users, "
    "revenue "
    "FROM window_stats "
    "ORDER BY window_start DESC "
    "LIMIT 10"

).fetchall()
for r in rows:

    print(
        f"  {r['window_start']:<28} "
        f"{r['events_total']:>5} "
        f"{r['unique_users']:>6} "
        f"{r['revenue']:>9.2f}"
    )
line("=")
conn.close()