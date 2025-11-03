#!/usr/bin/env python3
"""
Client Python minimal pour interroger l'API Pl@ntNet.

Ce script :
- Envoie une requête POST à l'API d'occurrences Pl@ntNet (/v3/dwc/occurrence/search)
- Permet d'indiquer une clé API, une zone (bbox), et éventuellement un nom d'espèce
- Affiche la réponse JSON brute dans le terminal

Avant de lancer :
  export PLANTNET_API_KEY=2b10IJGxpcJr54FjXELjEVJI1O

Exemple d'utilisation :
  python plantnet_client.py --bbox 55.20 -21.40 55.85 -20.85 \
    --species "Iris japonica Thunb."
"""

# Importation des bibliothèques nécessaires
import argparse  # pour gérer les arguments de la ligne de commande
import json      # pour formater et afficher les réponses JSON
import os        # pour accéder aux variables d'environnement
import requests  # pour effectuer des requêtes HTTP

# URL de base de l'API Pl@ntNet (version 3)
BASE_URL = "https://my-api.plantnet.org/v3/dwc/occurrence/search"


def bbox_to_polygon(min_lon, min_lat, max_lon, max_lat):
    """Convertit une bbox (rectangle défini par ses coins extrêmes) en polygone GeoJSON."""
    # Chaque sous-liste représente un coin du rectangle (lon, lat)
    ring = [
        [min_lon, min_lat],  # coin bas-gauche
        [min_lon, max_lat],  # coin haut-gauche
        [max_lon, max_lat],  # coin haut-droit
        [max_lon, min_lat],  # coin bas-droit
        [min_lon, min_lat],  # on referme le polygone en revenant au point de départ
    ]
    # Le format GeoJSON attend un dictionnaire avec le type et la liste des coordonnées
    return {"type": "Polygon", "coordinates": [ring]}


def query_api(api_key, polygon, species=None):
    """Interroge l'API avec la clé API, la géométrie et éventuellement une espèce."""
    # Construction de l'URL complète avec la clé API passée en paramètre GET
    url = f"{BASE_URL}?api-key={api_key}"

    # Corps de la requête JSON : ici, on inclut la géométrie (zone à interroger)
    payload = {"geometry": polygon}

    # Si l'utilisateur a spécifié une espèce, on l'ajoute dans le corps de la requête
    if species:
        payload["scientificName"] = [species]

    # En-têtes HTTP : on précise qu'on envoie du JSON
    headers = {"Content-Type": "application/json"}

    # Envoi de la requête POST avec un délai maximum de 30 secondes
    response = requests.post(url, headers=headers, json=payload, timeout=30)

    # Vérification du code de statut HTTP : 200 signifie succès
    if response.status_code != 200:
        # Si le statut n’est pas 200, on lève une erreur avec le message du serveur
        raise RuntimeError(f"Erreur {response.status_code}: {response.text}")

    # Si tout va bien, on renvoie la réponse JSON sous forme de dictionnaire Python
    return response.json()


def main():
    # Création d'un parseur d'arguments pour récupérer les options en ligne de commande
    parser = argparse.ArgumentParser(description="Client minimal pour l'API Pl@ntNet")

    # Définition des arguments attendus
    parser.add_argument(
        "--bbox", nargs=4, type=float, metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"), required=True,
        help="Délimitation géographique (lon/lat) : rectangle englobant la zone à interroger"
    )
    parser.add_argument(
        "--species", type=str,
        help="Nom scientifique de l'espèce recherchée (facultatif)"
    )
    parser.add_argument(
        "--api-key", type=str, default=os.getenv("PLANTNET_API_KEY"),
        help="Clé API Pl@ntNet (ou variable d'environnement PLANTNET_API_KEY)"
    )

    # Analyse des arguments fournis
    args = parser.parse_args()

    # Si aucune clé API n’est fournie, on arrête le programme avec un message d’erreur
    if not args.api_key:
        parser.error("Vous devez fournir une clé API (--api-key ou variable d'environnement PLANTNET_API_KEY).")

    # Conversion de la bbox (4 nombres) en polygone GeoJSON utilisable par l’API
    polygon = bbox_to_polygon(*args.bbox)

    # Appel à la fonction qui interroge l'API avec les paramètres fournis
    print("→ Envoi de la requête à l'API Pl@ntNet...")
    data = query_api(args.api_key, polygon, args.species)

    # Affichage de la réponse JSON de manière lisible et indentée
    print(json.dumps(data, indent=2, ensure_ascii=False))


# Point d’entrée du script : exécution de la fonction main() si le script est lancé directement
if __name__ == "__main__":
    main()
