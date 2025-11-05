#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dwc_normalize_to_csv.py (hybride)
- Lit le JSON brut extrait de Pl@ntNet (ex: plantnet_raw_reunion.json)
- Charge DWC_CORE_FIELDS & BASIS_OF_RECORD_MAP depuis des fichiers JSON
  (config/dwc_core_fields.json, config/basis_of_record_map.json),
  sinon utilise des valeurs par défaut.
- Normalise vers Darwin Core "GBIF-friendly" et exporte en CSV UTF-8.
"""

import os
import json
import csv
import uuid
from pathlib import Path
from datetime import datetime, timezone

# --- Emplacements I/O --------------------------------------------------------
BASE_DIR   = Path(os.path.expanduser("~/projets/app_plantnet"))
CONFIG_DIR = BASE_DIR / "config"

RAW_JSON   = BASE_DIR / "memory/plantnet_raw_reunion.json"
OUT_CSV    = BASE_DIR / "memory/plantnet_occurrences_reunion.csv"

# --- Valeurs par défaut (fallback) ------------------------------------------
DEFAULT_DWC_CORE_FIELDS = [
    "occurrenceID", "basisOfRecord", "institutionCode", "collectionCode",
    "catalogNumber", "recordNumber", "recordedBy",
    "eventDate", "year", "month", "day",
    "country", "countryCode", "stateProvince", "county", "municipality",
    "locality", "decimalLatitude", "decimalLongitude", "coordinateUncertaintyInMeters",
    "geodeticDatum",
    "occurrenceStatus", "individualCount", "sex", "lifeStage",
    "identifiedBy", "dateIdentified",
    "scientificName", "scientificNameAuthorship", "taxonRank",
    "kingdom", "phylum", "class", "order", "family", "genus",
    "specificEpithet", "infraspecificEpithet",
    "datasetID", "datasetName", "license", "references"
]

DEFAULT_BASIS_OF_RECORD_MAP = {
    "human_observation": "HUMAN_OBSERVATION",
    "observation": "OBSERVATION",
    "machine_observation": "MACHINE_OBSERVATION",
    "preserved_specimen": "PRESERVED_SPECIMEN",
    "living_specimen": "LIVING_SPECIMEN",
    "material_sample": "MATERIAL_SAMPLE",
    # variantes source
    "photograph": "HUMAN_OBSERVATION",
    "photo": "HUMAN_OBSERVATION",
    "image": "MACHINE_OBSERVATION"
}

# --- Chargement config -------------------------------------------------------
def load_json_config(path: Path, default):
    """
    Charge un JSON de config. Si absent/invalide, retourne 'default'.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # petite validation minimale
        if isinstance(default, list) and isinstance(data, list):
            return data
        if isinstance(default, dict) and isinstance(data, dict):
            return data
        print(f"⚠️  Type inattendu dans {path.name}, utilisation des valeurs par défaut.")
    except FileNotFoundError:
        print(f"ℹ️  Fichier de config absent: {path.name} → valeurs par défaut.")
    except Exception as e:
        print(f"⚠️  Impossible de lire {path.name}: {e} → valeurs par défaut.")
    return default

DWC_CORE_FIELDS = load_json_config(CONFIG_DIR / "dwc_core_fields.json", DEFAULT_DWC_CORE_FIELDS)
BASIS_OF_RECORD_MAP = load_json_config(CONFIG_DIR / "basis_of_record_map.json", DEFAULT_BASIS_OF_RECORD_MAP)

# --- Utils -------------------------------------------------------------------
def to_iso_date(value):
    if not value:
        return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%d","%Y/%m/%d","%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%S","%Y-%m-%dT%H:%M:%S.%f"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        except ValueError:
            pass
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    return None

def to_float(v):
    try:
        return float(str(v).strip())
    except Exception:
        return None

def normalize_country_code(v):
    return str(v).strip().upper() if v else None

def ensure_wgs84(datum):
    if not datum:
        return "WGS84"
    s = str(datum).strip().upper()
    return "WGS84" if s in {"WGS84", "EPSG:4326"} else str(datum)

def build_occurrence_id(rec):
    existing = rec.get("occurrenceID") or rec.get("id")
    if existing:
        return str(existing)
    seed = f"{rec.get('scientificName','')}|{rec.get('eventDate','')}|{rec.get('decimalLatitude','')}|{rec.get('decimalLongitude','')}|{rec.get('datasetName','plantnet')}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))

# --- Normalisation -----------------------------------------------------------
def normalize_record_gbif(rec: dict) -> dict:
    out = {}

    # Taxon
    out["scientificName"] = rec.get("scientificName") or rec.get("acceptedScientificName") or rec.get("name")
    out["scientificNameAuthorship"] = rec.get("scientificNameAuthorship")
    out["taxonRank"] = rec.get("taxonRank")
    for k in ["kingdom","phylum","class","order","family","genus","specificEpithet","infraspecificEpithet"]:
        out[k] = rec.get(k)

    # Occurrence
    bor_src = str(rec.get("basisOfRecord") or "").strip().lower()
    out["basisOfRecord"] = BASIS_OF_RECORD_MAP.get(bor_src, "HUMAN_OBSERVATION")
    out["recordedBy"] = rec.get("recordedBy") or rec.get("observer")
    out["recordNumber"] = rec.get("recordNumber")
    out["occurrenceStatus"] = (rec.get("occurrenceStatus") or "present").lower()
    out["individualCount"] = rec.get("individualCount")
    out["sex"] = rec.get("sex")
    out["lifeStage"] = rec.get("lifeStage")

    # Événement
    iso = to_iso_date(rec.get("eventDate") or rec.get("date") or rec.get("observationDate"))
    out["eventDate"] = iso
    if iso and len(iso) >= 10:
        try:
            out["year"], out["month"], out["day"] = int(iso[:4]), int(iso[5:7]), int(iso[8:10])
        except Exception:
            pass

    # Localisation
    out["decimalLatitude"]  = to_float(rec.get("decimalLatitude")  or rec.get("lat"))
    out["decimalLongitude"] = to_float(rec.get("decimalLongitude") or rec.get("lon"))
    out["coordinateUncertaintyInMeters"] = to_float(rec.get("coordinateUncertaintyInMeters") or rec.get("uncertainty"))
    out["geodeticDatum"] = ensure_wgs84(rec.get("geodeticDatum"))
    out["locality"] = rec.get("locality") or rec.get("place")
    out["municipality"] = rec.get("municipality")
    out["county"] = rec.get("county")
    out["stateProvince"] = rec.get("stateProvince")
    out["country"] = rec.get("country")
    out["countryCode"] = normalize_country_code(rec.get("countryCode"))

    # Source / droits
    out["institutionCode"] = rec.get("institutionCode")
    out["collectionCode"] = rec.get("collectionCode")
    out["catalogNumber"] = rec.get("catalogNumber")
    out["datasetName"] = rec.get("datasetName") or "Pl@ntNet Occurrences"
    out["datasetID"] = rec.get("datasetID")
    out["license"] = rec.get("license") or rec.get("rights")
    out["references"] = rec.get("references") or rec.get("reference")

    # Identification
    out["identifiedBy"] = rec.get("identifiedBy")
    out["dateIdentified"] = to_iso_date(rec.get("dateIdentified"))

    # occurrenceID
    out["occurrenceID"] = build_occurrence_id({**rec, **out})
    return out

def find_occurrence_list(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("occurrences", "records", "results", "data", "items"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        return [payload] if payload else []
    return []

def export_dwc_gbif_csv(records, csv_path: Path, dwc_fields, bor_map):
    # Normalise
    core_rows = [normalize_record_gbif(r) for r in records if isinstance(r, dict)]

    # Colonnes extra
    extra_keys, seen = [], set(dwc_fields)
    for r in records:
        if isinstance(r, dict):
            for k in r.keys():
                if k not in seen:
                    seen.add(k); extra_keys.append(k)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=dwc_fields + extra_keys, extrasaction="ignore")
        writer.writeheader()
        for core, raw in zip(core_rows, records):
            row = dict(core)
            if isinstance(raw, dict):
                for k in extra_keys:
                    v = raw.get(k)
                    if isinstance(v, (list, dict)):
                        v = json.dumps(v, ensure_ascii=False)
                    row.setdefault(k, v)
            writer.writerow(row)

# --- main --------------------------------------------------------------------
def main():
    if not RAW_JSON.exists():
        raise FileNotFoundError(f"JSON brut introuvable : {RAW_JSON}\n→ Lance d'abord fetch_plantnet.py")

    # lecture JSON brut
    with open(RAW_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    # occurrences
    occs = find_occurrence_list(data)
    if not occs:
        print("⚠️  Aucune occurrence détectée.")
        return

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    export_dwc_gbif_csv(occs, OUT_CSV, DWC_CORE_FIELDS, BASIS_OF_RECORD_MAP)
    print(f"✓ CSV écrit : {OUT_CSV}")
    print(f"★ Occurrences : {len(occs)}")
    print(f"ℹ️  Config utilisée → DWC fields: {len(DWC_CORE_FIELDS)} | basisOfRecord map: {len(BASIS_OF_RECORD_MAP)}")

if __name__ == "__main__":
    main()
