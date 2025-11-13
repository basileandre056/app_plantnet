import os
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import requests

DEFAULT_BASE_URL = "https://my-api.plantnet.org/v3/dwc/occurrence/search"




def fetch_plantnet(
    api_key: str,
    species: List[str],
    polygon_geojson: Dict,
    output_json_path: Path,
    min_event_date: Optional[str] = None,
    max_event_date: Optional[str] = None,
    base_url: str = DEFAULT_BASE_URL,
    timeout: Tuple[int, int] = (10, 90),
) -> Path:

    output_json_path.parent.mkdir(parents=True, exist_ok=True)

    url = f"{base_url}?api-key={api_key}"

    # Convertit le polygone GeoJSON en STRING comme exigé par la doc
    geometry_str = json.dumps(polygon_geojson)

    # Form-data attendu par l’API
    form_data = {}

    if species:
        # API accepte ARRAY → on envoie plusieurs fois la clé
        for s in species:
            form_data.setdefault("scientificName", []).append(s)

    form_data["geometry"] = geometry_str

    if min_event_date:
        form_data["minEventDate"] = min_event_date
    if max_event_date:
        form_data["maxEventDate"] = max_event_date

    print("→ Appel Pl@ntNet", url)
    print("Form-data envoyé :", form_data)

    # Très important : data=… (FORM)
    resp = requests.post(url, data=form_data, timeout=timeout)

    resp.raise_for_status()
    data = resp.json()

    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("✓ JSON brut enregistré :", output_json_path)
    return output_json_path



# ---------------------------
# TEST DIRECT
# ---------------------------
if __name__ == "__main__":

    API_KEY = "2b10IJGxpcJr54FjXELjEVJI1O"

    BASE_DIR   = Path(os.path.expanduser("~/projets/app_plantnet/backend"))
    MEM_DIR    = BASE_DIR / "memory"
    CONFIG_DIR = BASE_DIR / "config"

    RAW_JSON = MEM_DIR / "plantnet_raw_reunion.json"
    OUT_CSV  = MEM_DIR / "plantnet_occurrences_reunion.csv"

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

    # Appel de test
    fetch_plantnet(
        api_key=API_KEY,
        species=["Iris japonica Thunb."],
        polygon_geojson=REUNION_POLYGON,
        min_event_date="2023-01-01",
        max_event_date="2024-12-31",
        output_json_path=RAW_JSON,
    )
