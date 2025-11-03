#!/usr/bin/env python3
"""
Client Python *très* minimal pour exécuter la requête souhaitée ET stocker le résultat.
- Emprise géographique = polygone de La Réunion (fourni ci-dessous)
- Espèces ciblées = liste fournie ci-dessous
- Écrit la réponse telle quelle dans un fichier JSON (le plus simple)

Tout est codé en dur : base_url, api_key, coordonnées, espèces, chemin du fichier.
"""
import json
import requests
import os

# --- Paramètres codés en dur -------------------------------------------------
BASE_URL = "https://my-api.plantnet.org/v3/dwc/occurrence/search"
API_KEY  = "2b10IJGxpcJr54FjXELjEVJI1O"  # fourni par l'utilisateur
OUTPUT_JSON = os.path.expanduser("~/projets/plantnet/app_plantnet/plantnet_occurrences_reunion.json")
SPECIES = [
    "Thunbergia fragrans Roxb.",
    "Aciotis purpurascens (Aubl.) Triana",
    "Iris japonica Thunb.",
    "Machaerina iridifolia (Bory) T.Koyama",
]

# Polygone de l'île de La Réunion (tel que fourni dans l'exemple cURL)
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

# Corps de requête minimal (identique à l'exemple cURL)
PAYLOAD = {
    "scientificName": SPECIES,
    "geometry": REUNION_POLYGON,
}

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    url = f"{BASE_URL}?api-key={API_KEY}"
    headers = {"Content-Type": "application/json"}

    print("→ Envoi de la requête à l'API Pl@ntNet…")
    resp = requests.post(url, headers=headers, json=PAYLOAD, timeout=(10, 90))
    resp.raise_for_status()

    data = resp.json()

    # 1) Affiche joliment la réponse dans le terminal
    print(json.dumps(data, indent=2, ensure_ascii=False))

    # 2) Stocke la réponse brute telle quelle dans un JSON (le plus simple)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Résultat écrit dans {OUTPUT_JSON}")

    # (Optionnel) afficher le nombre d'occurrences si une liste est trouvée
    if isinstance(data, dict):
        for k in ("data", "results", "items", "records", "occurrences"):
            v = data.get(k)
            if isinstance(v, list):
                print(f"★ Nombre d'occurrences retournées ({k}) : {len(v)}")
                break
