import os
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import requests

DEFAULT_BASE_URL = "https://my-api.plantnet.org/v3/dwc/occurrence/search"


def bbox_to_polygon(bbox_str: str) -> Dict:
    """
    Convertit "minLon,minLat,maxLon,maxLat" en Polygon GeoJSON.
    """
    parts = [float(x) for x in bbox_str.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox doit être minLon,minLat,maxLon,maxLat")

    minLon, minLat, maxLon, maxLat = parts

    return {
        "type": "Polygon",
        "coordinates": [[
            [minLon, minLat],
            [maxLon, minLat],
            [maxLon, maxLat],
            [minLon, maxLat],
            [minLon, minLat]
        ]]
    }


def fetch_plantnet(
    api_key: str,
    species: List[str],
    polygon_geojson: Optional[Dict],
    output_json_path: Path,
    min_event_date: Optional[str] = None,
    max_event_date: Optional[str] = None,
    taxon_rank: Optional[str] = None,      # ← AJOUT
    bbox: Optional[str] = None,            # ← AJOUT
    base_url: str = DEFAULT_BASE_URL,
    timeout: Tuple[int, int] = (10, 90),
) -> Path:

    output_json_path.parent.mkdir(parents=True, exist_ok=True)

    url = f"{base_url}?api-key={api_key}"

    # -------------------------
    # GEOMETRIE : priorité au BBOX
    # -------------------------
    if bbox:
        polygon_geojson = bbox_to_polygon(bbox)

    if polygon_geojson is None:
        raise ValueError("Vous devez fournir polygon_geojson OU bbox")

    geometry_str = json.dumps(polygon_geojson)

    # -------------------------
    # FORM-DATA API Pl@ntNet
    # -------------------------
    form_data = {}

    # species[] → répétition de la clé
    if species:
        for s in species:
            form_data.setdefault("scientificName", []).append(s)

    # geometry obligatoire
    form_data["geometry"] = geometry_str

    # dates
    if min_event_date:
        form_data["minEventDate"] = min_event_date
    if max_event_date:
        form_data["maxEventDate"] = max_event_date

    # taxon_rank (OPTIONNEL)
    if taxon_rank:
        form_data["taxonRank"] = taxon_rank

    print("→ Appel Pl@ntNet", url)
    print("Form-data envoyé :", form_data)

    resp = requests.post(url, data=form_data, timeout=timeout)
    resp.raise_for_status()

    data = resp.json()

    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("✓ JSON brut enregistré :", output_json_path)
    return output_json_path
