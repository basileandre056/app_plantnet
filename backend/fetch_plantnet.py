#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_plantnet.py
- Envoie UNE requête à l'API Pl@ntNet (endpoint Darwin Core "occurrence/search").
- Sauvegarde la réponse BRUTE en JSON (pour audit/rejeu).
- Ne fait PAS de normalisation ici (on garde ça pour le 2e script).
"""

import os
import json
from pathlib import Path
import requests  # pip install requests

# --- Paramètres codés en dur (tu les rendras paramétrables plus tard) -------------------

BASE_URL = "https://my-api.plantnet.org/v3/dwc/occurrence/search"
API_KEY  = "2b10IJGxpcJr54FjXELjEVJI1O"  

OUTPUT_DIR  = Path(os.path.expanduser("~/projets/app_plantnet"))
RAW_JSON    = OUTPUT_DIR / "memory/plantnet_raw_reunion.json"   # JSON brut écrasé à chaque run

# Exemple simple : 4 taxons et polygone de La Réunion (WGS84)
SPECIES = [
    "Thunbergia fragrans Roxb.",
    "Aciotis purpurascens (Aubl.) Triana",
    "Iris japonica Thunb.",
    "Machaerina iridifolia (Bory) T.Koyama",
]

REUNION_POLYGON = {
    "type": "Polygon",
    "coordinates": [[
        [55.2793527748355, -20.915228550665972],
        [55.27008417364911, -20.956699600097522],
        [55.272781834306045, -20.990067818924132],
        [55.24154247413543, -21.012828480359914],
        [55.229601308302335, -21.012320811388733],
        [55.20306370884094, -21.03728202564804],
        [55.2090094279888, -21.080223628620047],
        [55.25827859954282, -21.143358510033835],
        [55.274530299783294, -21.158004227477832],
        [55.26803118055358, -21.201419453006835],
        [55.28264247558579, -21.23119747763502],
        [55.316751902044786, -21.27408831927319],
        [55.33353590554157, -21.28720537566896],
        [55.36981100987185, -21.291745622641415],
        [55.39526652661942, -21.30233444107118],
        [55.408251792072065, -21.324027778375992],
        [55.47971989626498, -21.3588225144628],
        [55.602617179716134, -21.39057530426892],
        [55.6432278410189, -21.39613255666528],
        [55.776417030052784, -21.373446069134616],
        [55.818106329059276, -21.34319195017966],
        [55.82331052066087, -21.22161106690031],
        [55.85270968339859, -21.189865462733053],
        [55.85271295622192, -21.148855304979136],
        [55.79296256256757, -21.115423740583253],
        [55.71480245804722, -20.970500886680966],
        [55.69525608822789, -20.927907495081726],
        [55.61707060894801, -20.89138923253705],
        [55.458527809075775, -20.8640016028845],
        [55.397749243333635, -20.87270438232669],
        [55.31410187673734, -20.91979309421808],
        [55.2793527748355, -20.915228550665972]
    ]]
}


PAYLOAD = {"scientificName": SPECIES, "geometry": REUNION_POLYGON}


def main():
    # 1) Crée le dossier de sortie si besoin
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 2) Construit l'URL avec la clé d'API (simple et clair)
    url = f"{BASE_URL}?api-key={API_KEY}"
    headers = {"Content-Type": "application/json"}

    print("→ Appel Pl@ntNet (occurrence/search)…")
    resp = requests.post(url, headers=headers, json=PAYLOAD, timeout=(10, 90))
    resp.raise_for_status()  # lève une erreur si 4xx/5xx
    data = resp.json()

    # 3) Sauvegarde la réponse brute telle quelle (UTF-8)
    with open(RAW_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✓ JSON brut enregistré : {RAW_JSON}")

if __name__ == "__main__":
    main()
