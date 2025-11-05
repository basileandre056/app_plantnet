#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_plantnet.py

But
----
- Envoyer UNE requête à l'API Pl@ntNet (endpoint Darwin Core: /v3/dwc/occurrence/search)
- Sauvegarder la réponse BRUTE (telle quelle) en JSON pour audit / rejouabilité
- Ne faire AUCUNE normalisation ici (cette responsabilité est déportée dans le script de
  normalisation vers Darwin Core/GBIF)

Usage autonome (exemple)
------------------------
$ python3 fetch_plantnet.py

Notes
-----
- Ce module expose une fonction réutilisable `fetch_plantnet(...)` qui peut être importée
  et pilotée depuis un orchestrateur (ex: app_backend.py).
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import requests  # pip install requests

# Endpoint DWC "occurrence/search" de Pl@ntNet (par défaut, surchargé possible via paramètre)
DEFAULT_BASE_URL: str = "https://my-api.plantnet.org/v3/dwc/occurrence/search"


def fetch_plantnet(
    api_key: str,
    species: List[str],
    polygon_geojson: Dict,
    output_json_path: Path,
    base_url: str = DEFAULT_BASE_URL,
    timeout: Tuple[int, int] = (10, 90),
) -> Path:
    """
    Appelle l'API Pl@ntNet pour récupérer des occurrences (format DWC) et
    écrit la réponse JSON brute sur disque.

    Paramètres
    ----------
    api_key : str
        Clé d'API Pl@ntNet (token personnel). Évite de la commiter en clair ; préfère
        une variable d'environnement dans un vrai projet.
    species : list[str]
        Liste de noms scientifiques à filtrer côté API (ex: ["Iris japonica Thunb."]).
        Si la liste est vide, le filtre n'est pas envoyé.
    polygon_geojson : dict
        Polygone GeoJSON (WGS84) définissant l'emprise de recherche.
        Exemple minimal :
        {
          "type": "Polygon",
          "coordinates": [[[lon1,lat1],[lon2,lat2],...,[lon1,lat1]]]
        }
        Si None / vide, le filtre 'geometry' n'est pas envoyé.
    output_json_path : Path
        Chemin du fichier JSON de sortie (sera créé/écrasé).
    base_url : str, par défaut DEFAULT_BASE_URL
        URL de base de l'endpoint Pl@ntNet. Laisse par défaut, sauf besoin spécifique.
    timeout : (int, int), par défaut (10, 90)
        Timeout (connexion, lecture) en secondes pour la requête HTTP.

    Retour
    ------
    Path
        Le chemin du fichier JSON écrit (identique à `output_json_path`), pratique
        pour chaîner les appels.

    Exceptions
    ----------
    requests.HTTPError
        Levée par `resp.raise_for_status()` si le serveur retourne 4xx/5xx.
    requests.RequestException
        Levée en cas d'erreur réseau (DNS, timeout, etc.).
    OSError / IOError
        Si l'écriture du fichier échoue (droits, disque, ...).

    Détails d'implémentation
    ------------------------
    - Le corps de requête (`payload`) n'inclut `scientificName` et `geometry`
      que s'ils sont fournis (évite d'envoyer des clés vides).
    - `ensure_ascii=False` pour préserver les accents/UTF-8 dans le JSON.
    - `indent=2` pour un fichier lisible humainement (audit/debug).
    """
    # Crée le dossier parent au besoin (ex: ~/projets/app_plantnet/memory)
    output_json_path.parent.mkdir(parents=True, exist_ok=True)

    # Construit l’URL en ajoutant la clé API (pattern documenté par Pl@ntNet)
    url = f"{base_url}?api-key={api_key}"
    headers = {"Content-Type": "application/json"}

    # Corps minimaliste : on n’envoie que ce qui est non vide
    payload: Dict[str, object] = {}
    if species:
        payload["scientificName"] = species
    if polygon_geojson:
        payload["geometry"] = polygon_geojson

    # Requête HTTP (POST) vers l'endpoint DWC, avec timeouts robustes
    print(f"→ Appel Pl@ntNet {base_url} …")
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    # Lève HTTPError si code 4xx/5xx (on laisse remonter : c’est souhaité)
    resp.raise_for_status()

    # Décodage du JSON (requests détecte l’UTF-8 automatiquement)
    data = resp.json()

    # Écriture disque : JSON brut (lisible, UTF-8, sans escape ASCII)
    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✓ JSON brut enregistré : {output_json_path}")
    return output_json_path


# Exécution directe (utile pour un test rapide du script seul)
if __name__ == "__main__":
    # ⚠️ Exemple minimal : pense à remplacer la clé, le polygone et l’emplacement !
    API_KEY  = "CHANGE_ME"

    OUTPUT_DIR = Path(os.path.expanduser("~/projets/app_plantnet/memory"))
    RAW_JSON   = OUTPUT_DIR / "plantnet_raw_reunion.json"

    # Un seul taxon pour un test rapide
    SPECIES = ["Iris japonica Thunb."]

    # Petit polygone de test (attention : coordonnées [lon, lat])
    POLYGON = {
        "type": "Polygon",
        "coordinates": [
            [[55.27, -20.95], [55.28, -20.95], [55.28, -20.92], [55.27, -20.92], [55.27, -20.95]]
        ]
    }

    fetch_plantnet(
        api_key=API_KEY,
        species=SPECIES,
        polygon_geojson=POLYGON,
        output_json_path=RAW_JSON,
    )
