#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dwc_normalize_to_csv.py

Rôle
----
Transformer la réponse brute Pl@ntNet (JSON) en CSV conforme au standard Darwin Core,
avec des valeurs normalisées compatibles GBIF.

Pipeline
--------
1. Charger le JSON brut exporté par fetch_plantnet().
2. Extraire la liste des occurrences (schéma flexible).
3. Charger deux configurations optionnelles :
       - dwc_core_fields.json : liste ordonnée des colonnes Darwin Core à exporter.
       - basis_of_record_map.json : mappage souple → vocabulaires GBIF.
   Si absents, utiliser des valeurs par défaut robustes.
4. Pour chaque occurrence :
       - normalisation taxonomique
       - normalisation des coordonnées (WGS84)
       - conversion des dates en ISO-8601
       - mappage basisOfRecord vers GBIF
       - génération d’un occurrenceID stable
5. Export final en CSV UTF-8 : colonnes cœur DwC + colonnes « extra ».

Points clés
-----------
- Tolérant aux variations de structure JSON Pl@ntNet.
- Compatible GBIF (formats, vocabulaires, identifiants stables).
- Aucun écrasement des données brutes : les champs inconnus sont gardés comme « extras ».
- Le CSV est prêt pour ingestion dans GeoNature, GBIF ou un SIG.

Ce module n’interroge jamais l’API : il ne fait que la normalisation / export.
"""


import os
import json
import csv
import uuid
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

# --- Valeurs par défaut si pas de config ---
DEFAULT_DWC_CORE_FIELDS: List[str] = [
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
    "datasetID", "datasetName", "license", "references",
    "associatedMedia"  #  Darwin Core officiel : liens vers images/sons/vidéos
]

DEFAULT_BASIS_OF_RECORD_MAP: Dict[str, str] = {
    "human_observation": "HUMAN_OBSERVATION",
    "observation": "OBSERVATION",
    "machine_observation": "MACHINE_OBSERVATION",
    "preserved_specimen": "PRESERVED_SPECIMEN",
    "living_specimen": "LIVING_SPECIMEN",
    "material_sample": "MATERIAL_SAMPLE",
    # variantes source fréquentes (provenant d’outils grand public)
    "photograph": "HUMAN_OBSERVATION",
    "photo": "HUMAN_OBSERVATION",
    "image": "MACHINE_OBSERVATION"
}


def load_json_config(path: Path, default: Union[List[Any], Dict[str, Any]]) -> Union[List[Any], Dict[str, Any]]:
    """
    Charge un fichier JSON de configuration et renvoie son contenu,
    avec une validation très simple du type de donnée attendu (list/dict).
    Si le fichier est absent ou invalide, la valeur `default` est renvoyée.

    Paramètres
    ----------
    path : Path
        Chemin du fichier JSON à charger (ex: config/dwc_core_fields.json).
    default : list | dict
        Valeur de repli si le fichier est introuvable ou invalide.

    Retour
    ------
    list | dict
        Le contenu JSON si OK, sinon `default`.

    Notes
    -----
    - Cette fonction ne lève pas d’exception (sauf souci inattendu de type),
      elle loggue un message et renvoie `default`.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Validation minimale : on s'assure que la structure correspond
        if isinstance(default, list) and isinstance(data, list):
            return data
        if isinstance(default, dict) and isinstance(data, dict):
            return data

        print(f"⚠️ Type inattendu dans {path.name}, fallback défaut.")
    except FileNotFoundError:
        print(f"ℹ️ Config absente : {path.name} → défauts.")
    except Exception as e:
        print(f"⚠️ Impossible de lire {path.name}: {e} → défauts.")
    return default


# ---------- utils ----------
def to_iso_date(value: Any) -> Optional[str]:
    """
    Convertit une valeur arbitraire de date en chaîne ISO-8601.
    - Supporte plusieurs formats courants (avec/sans timezone).
    - Si aucune timezone : on force UTC (suffixe 'Z').
    - Si `value` ressemble déjà à 'YYYY-MM-DD', on la renvoie telle quelle.
    - En cas d’échec : renvoie None.

    Paramètres
    ----------
    value : Any
        Entrée brut (str, datetime-like, etc.)

    Retour
    ------
    str | None
        Date ISO-8601 (ex: '2025-07-19T10:04:29Z' ou '2025-07-19') ou None.
    """
    if not value:
        return None
    s = str(value).strip()
    # Essais sur formats fréquents
    for fmt in (
        "%Y-%m-%d", "%Y/%m/%d",
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"
    ):
        try:
            dt = datetime.strptime(s, fmt)
            # Si aucune timezone, on uniformise sur UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        except ValueError:
            pass

    # Fallback : déjà au format 'YYYY-MM-DD'
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    return None


def to_float(v: Any) -> Optional[float]:
    """
    Convertit une valeur en float si possible, sinon renvoie None.
    Utile pour éviter de lever des exceptions pendant la normalisation.

    Paramètres
    ----------
    v : Any
        Valeur à convertir.

    Retour
    ------
    float | None
        Nombre décimal ou None si conversion impossible.
    """
    try:
        return float(str(v).strip())
    except Exception:
        return None


def normalize_country_code(v: Any) -> Optional[str]:
    """
    Normalise un code pays vers ISO-3166-1 alpha-2 (majuscules).
    Ne convertit pas un nom de pays → code (ex: 'France' → 'FR').

    Paramètres
    ----------
    v : Any
        Valeur d'entrée (souvent déjà 'FR', 'RE', ...).

    Retour
    ------
    str | None
        Code alpha-2 en majuscules ou None si vide.
    """
    return str(v).strip().upper() if v else None


def ensure_wgs84(datum: Any) -> str:
    """
    Uniformise le datum géodésique sur 'WGS84' (ou tolère la valeur déclarée).

    Règles :
    - Si vide → 'WGS84' (valeur recommandée par GBIF).
    - Si 'WGS84' ou 'EPSG:4326' → on renvoie 'WGS84'.
    - Sinon → on renvoie la valeur brute pour ne pas altérer l’info.

    Paramètres
    ----------
    datum : Any
        Valeur de datum (str attendu).

    Retour
    ------
    str
        'WGS84' ou bien la valeur fournie telle quelle.
    """
    if not datum:
        return "WGS84"
    s = str(datum).strip().upper()
    return "WGS84" if s in {"WGS84", "EPSG:4326"} else str(datum)


def build_occurrence_id(rec: Dict[str, Any]) -> str:
    """
    Génère un identifiant `occurrenceID` stable pour une occurrence.

    Stratégie :
    - Si la source fournit déjà 'occurrenceID' ou 'id', on le réutilise (traçabilité).
    - Sinon, on calcule un UUID5 déterministe basé sur :
      (scientificName, eventDate, decimalLatitude, decimalLongitude, datasetName)

    Paramètres
    ----------
    rec : dict
        Enregistrement (éventuellement déjà partiellement normalisé).

    Retour
    ------
    str
        Identifiant unique et stable (UUID5 si calculé).
    """
    existing = rec.get("occurrenceID") or rec.get("id")
    if existing:
        return str(existing)

    # On assemble une "empreinte" stable d'attributs discriminants
    seed = f"{rec.get('scientificName','')}|{rec.get('eventDate','')}|{rec.get('decimalLatitude','')}|{rec.get('decimalLongitude','')}|{rec.get('datasetName','plantnet')}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


# ---------- normalisation ----------
def normalize_record_gbif(rec: Dict[str, Any], basis_of_record_map: Dict[str, str]) -> Dict[str, Any]:
    """
    Convertit un enregistrement brut (clé/valeurs) vers le "cœur" Darwin Core,
    avec des normalisations compatibles GBIF.

    Ce que fait la fonction :
    - Taxon : garde `scientificName`, `taxonRank`, hiérarchie taxonomique...
    - Occurrence : mappe `basisOfRecord` vers le vocabulaire GBIF ; utilise 'present' par défaut ;
                   conserve `recordedBy`, `individualCount`, etc.
    - Événement : convertit `eventDate` en ISO-8601 et déduit `year/month/day` si possible.
    - Localisation : force le datum WGS84 si vide, coord. décimales, incertitude en mètres.
    - Source/droits : `datasetName`, `license`, `references`...
    - Identifiants : construit un `occurrenceID` stable si absent.

    Paramètres
    ----------
    rec : dict
        Enregistrement brut tel que renvoyé par l'API.
    basis_of_record_map : dict
        Dictionnaire normalisant les variantes (`photo`, `photograph`, …) vers
        les valeurs GBIF (`HUMAN_OBSERVATION`, …). Les clés sont comparées en minuscules.

    Retour
    ------
    dict
        Enregistrement restreint aux colonnes du cœur DwC (valeurs normalisées).
    """
    out: Dict[str, Any] = {}

    # --- Taxon ---
    out["scientificName"] = rec.get("scientificName") or rec.get("acceptedScientificName") or rec.get("name")
    out["scientificNameAuthorship"] = rec.get("scientificNameAuthorship")
    out["taxonRank"] = rec.get("taxonRank")
    for k in ["kingdom", "phylum", "class", "order", "family", "genus", "specificEpithet", "infraspecificEpithet"]:
        out[k] = rec.get(k)

    # --- Occurrence ---
    # On mappe en minuscule pour tolérer les variantes
    bor_src = str(rec.get("basisOfRecord") or "").strip().lower()
    out["basisOfRecord"] = basis_of_record_map.get(bor_src, "HUMAN_OBSERVATION")
    out["recordedBy"] = rec.get("recordedBy") or rec.get("observer")
    out["recordNumber"] = rec.get("recordNumber")
    out["occurrenceStatus"] = (rec.get("occurrenceStatus") or "present").lower()
    out["individualCount"] = rec.get("individualCount")
    out["sex"] = rec.get("sex")
    out["lifeStage"] = rec.get("lifeStage")

    # --- Événement ---
    iso = to_iso_date(rec.get("eventDate") or rec.get("date") or rec.get("observationDate"))
    out["eventDate"] = iso
    if iso and len(iso) >= 10:
        # Si on dispose d'au moins 'YYYY-MM-DD', on remplit year/month/day
        try:
            out["year"], out["month"], out["day"] = int(iso[:4]), int(iso[5:7]), int(iso[8:10])
        except Exception:
            # On ne casse pas la normalisation si le découpage échoue (date partielle)
            pass

    # --- Localisation ---
    out["decimalLatitude"] = to_float(rec.get("decimalLatitude") or rec.get("lat"))
    out["decimalLongitude"] = to_float(rec.get("decimalLongitude") or rec.get("lon"))
    out["coordinateUncertaintyInMeters"] = to_float(rec.get("coordinateUncertaintyInMeters") or rec.get("uncertainty"))
    out["geodeticDatum"] = ensure_wgs84(rec.get("geodeticDatum"))
    out["locality"] = rec.get("locality") or rec.get("place")
    out["municipality"] = rec.get("municipality")
    out["county"] = rec.get("county")
    out["stateProvince"] = rec.get("stateProvince")
    out["country"] = rec.get("country")
    out["countryCode"] = normalize_country_code(rec.get("countryCode"))

    # --- Source / droits ---
    out["institutionCode"] = rec.get("institutionCode")
    out["collectionCode"] = rec.get("collectionCode")
    out["catalogNumber"] = rec.get("catalogNumber")
    out["datasetName"] = rec.get("datasetName") or "Pl@ntNet Occurrences"
    out["datasetID"] = rec.get("datasetID")
    out["license"] = rec.get("license") or rec.get("rights")
    out["references"] = rec.get("references") or rec.get("reference")

    # --- Identification ---
    out["identifiedBy"] = rec.get("identifiedBy")
    out["dateIdentified"] = to_iso_date(rec.get("dateIdentified"))

    # --- Identifiant final (stable) ---
    out["occurrenceID"] = build_occurrence_id({**rec, **out})
    return out


def find_occurrence_list(payload: Union[List[Any], Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extrait la liste d’occurrences à partir d’une réponse JSON Pl@ntNet hétérogène.

    Cas supportés :
    - La réponse est directement une liste
    - La réponse est un dict et la liste est sous l’une des clés usuelles :
      ('occurrences', 'records', 'results', 'data', 'items')
    - La réponse est un dict singleton → on le met dans une liste (pour ne rien perdre)

    Paramètres
    ----------
    payload : list | dict
        Objet JSON décodé (réponse API).

    Retour
    ------
    list[dict]
        Liste d’enregistrements (peut être vide).
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("occurrences", "records", "results", "data", "items"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        return [payload] if payload else []
    return []


def export_dwc_gbif_csv(
    records: List[Dict[str, Any]],
    csv_path: Path,
    dwc_fields: List[str],
    basis_of_record_map: Dict[str, str]
) -> None:
    """
    Écrit un CSV UTF-8 (sans BOM) combinant :
      1) Les colonnes "cœur" Darwin Core normalisées (ordre = `dwc_fields`)
      2) Toutes les colonnes "extra" détectées dans les données source (après le cœur)

    Détails :
    - Les enregistrements sont d'abord normalisés (`normalize_record_gbif`).
    - Les champs non-DwC sont ajoutés à la fin pour ne rien perdre.
    - Les valeurs de type `list`/`dict` sont sérialisées en JSON compact.

    Paramètres
    ----------
    records : list[dict]
        Enregistrements bruts renvoyés par l’API Pl@ntNet.
    csv_path : Path
        Chemin du fichier CSV à créer/écraser.
    dwc_fields : list[str]
        Liste ordonnée des colonnes Darwin Core à écrire en premier.
    basis_of_record_map : dict
        Mappage souple → valeurs GBIF pour `basisOfRecord`.

    Retour
    ------
    None
    """
    # Normalisation "DwC/GBIF-friendly" des enregistrements
    core_rows = [normalize_record_gbif(r, basis_of_record_map) for r in records if isinstance(r, dict)]

    # Détection des colonnes "extra" (tout ce qui n’est pas dans dwc_fields)
    extra_keys: List[str] = []
    seen = set(dwc_fields)
    for r in records:
        if isinstance(r, dict):
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    extra_keys.append(k)

    # Écriture CSV UTF-8 sans BOM ; newline="" évite des lignes vides sous Windows
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=dwc_fields + extra_keys, extrasaction="ignore")
        writer.writeheader()

        # On fusionne chaque ligne normalisée (cœur) + ses "extras" sérialisés si besoin
        for core, raw in zip(core_rows, records):
            row = dict(core)
            if isinstance(raw, dict):
                for k in extra_keys:
                    v = raw.get(k)
                    if isinstance(v, (list, dict)):
                        v = json.dumps(v, ensure_ascii=False)
                    # setdefault pour ne jamais écraser un champ cœur DwC si collision
                    row.setdefault(k, v)
            writer.writerow(row)


def dwc_normalise_to_csv(
    raw_json_path: Path,
    out_csv_path: Path,
    config_dir: Optional[Path] = None
) -> Path:
    """
    Pipeline minimal : JSON brut Pl@ntNet → CSV Darwin Core (UTF-8) normalisé GBIF.

    Étapes :
    1) Lire le JSON brut (réponse Pl@ntNet)
    2) Extraire la liste d’occurrences (robuste à plusieurs schémas)
    3) Charger les configs (`dwc_core_fields.json`, `basis_of_record_map.json`) si `config_dir` est fourni,
       sinon utiliser les valeurs par défaut
    4) Exporter le CSV : cœur DwC normalisé + colonnes "extra"

    Paramètres
    ----------
    raw_json_path : Path
        Chemin du fichier JSON brut à lire.
    out_csv_path : Path
        Chemin du CSV à produire.
    config_dir : Path | None
        Dossier contenant les fichiers de config JSON (optionnel).

    Retour
    ------
    Path
        Chemin du CSV écrit (identique à `out_csv_path`, pour chaîner facilement).

    Exceptions
    ----------
    FileNotFoundError
        Si `raw_json_path` n’existe pas.
    """
    if not raw_json_path.exists():
        raise FileNotFoundError(f"JSON brut introuvable : {raw_json_path}")

    # Lecture du JSON brut (UTF-8)
    with open(raw_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Extraction de la liste d'occurrences
    occs = find_occurrence_list(data)
    if not occs:
        print("⚠️  Aucune occurrence détectée.")
        return out_csv_path

    # Chargement des configurations (ou fallback)
    if config_dir:
        dwc_fields = load_json_config(config_dir / "dwc_core_fields.json", DEFAULT_DWC_CORE_FIELDS)
        bor_map    = load_json_config(config_dir / "basis_of_record_map.json", DEFAULT_BASIS_OF_RECORD_MAP)
    else:
        dwc_fields, bor_map = DEFAULT_DWC_CORE_FIELDS, DEFAULT_BASIS_OF_RECORD_MAP

    # Écriture CSV
    out_csv_path.parent.mkdir(parents=True, exist_ok=True)
    export_dwc_gbif_csv(occs, out_csv_path, dwc_fields, bor_map)

    print(f"✓ CSV écrit : {out_csv_path}")
    print(f"★ Occurrences : {len(occs)}")
    print(f"ℹ️  Config → DWC fields: {len(dwc_fields)} | basisOfRecord map: {len(bor_map)}")
    return out_csv_path


# usage autonome éventuel (exécuter directement ce fichier)
if __name__ == "__main__":
    BASE_DIR = Path(os.path.expanduser("~/projets/app_plantnet"))
    RAW = BASE_DIR / "memory/plantnet_raw_reunion.json"
    OUT = BASE_DIR / "memory/plantnet_occurrences_reunion.csv"
    CFG = BASE_DIR / "config"
    dwc_normalise_to_csv(RAW, OUT, CFG)
