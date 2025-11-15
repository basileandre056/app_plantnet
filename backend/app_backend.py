#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
app_backend.py
- Définit les variables (pour l’instant en dur)
- Appelle fetch_plantnet(...) puis dwc_normalise_to_csv(...)
"""

import os
from pathlib import Path
import argparse
from fetch_plantnet import fetch_plantnet
from dwc_normalize_to_csv import dwc_normalise_to_csv

# --- Variables “en dur” (paramétrables plus tard) -----------------

API_KEY = "2b10IJGxpcJr54FjXELjEVJI1O"  #en variable d'environnement plus tard

BASE_DIR   = Path(os.path.expanduser("~/projets/app_plantnet/backend"))
MEM_DIR    = BASE_DIR / "memory"
CONFIG_DIR = BASE_DIR / "config"

RAW_JSON = MEM_DIR / "plantnet_raw_reunion.json"
OUT_CSV  = MEM_DIR / "plantnet_occurrences_reunion.csv"

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
def parse_args():
    parser = argparse.ArgumentParser(description="Pl@ntNet fetcher")
    parser.add_argument("--bbox", type=str, help="bbox W/E/S/N : minLon,minLat,maxLon,maxLat")
    parser.add_argument("--after", type=str, help="date min (YYYY-MM-DD)")
    parser.add_argument("--before", type=str, help="date max (YYYY-MM-DD)")
    parser.add_argument("--taxon", type=str, nargs="*", help="Liste d'espèces")
    return parser.parse_args()


def main():
    args = parse_args()

    # 1) Détermination du polygone
    if args.bbox:
        print("→ Utilisation de la bbox :", args.bbox)
        lon1, lat1, lon2, lat2 = map(float, args.bbox.split(","))
        polygon = {
            "type": "Polygon",
            "coordinates": [[
                [lon1, lat1],
                [lon2, lat1],
                [lon2, lat2],
                [lon1, lat2],
                [lon1, lat1]
            ]]
        }
    else:
        polygon = REUNION_POLYGON

    # 2) Détermination du filtre species
    species = args.taxon if args.taxon else SPECIES

    # 3) Dates
    min_d = args.after if args.after else "2023-01-01"
    max_d = args.before if args.before else "2024-12-31"

    fetch_plantnet(
        api_key=API_KEY,
        species=species,
        polygon_geojson=polygon,
        min_event_date=min_d,
        max_event_date=max_d,
        output_json_path=RAW_JSON,
    )

    dwc_normalise_to_csv(
        raw_json_path=RAW_JSON,
        out_csv_path=OUT_CSV,
        config_dir=CONFIG_DIR,  
    )


if __name__ == "__main__":
    main()
