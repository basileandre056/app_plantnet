import requests
import json

# === CONFIGURATION ===
API_KEY = "2b10CB2RbHr63PZBJaM2J3LMm"
endpoint = f"https://my-api.plantnet.org/v2/identify/all?api-key={API_KEY}&lang=fr"

# === IMAGES À ENVOYER ===
files = [
    ('images', open('Olearia_megalophylla_leaf.jpg', 'rb')),
    ('images', open('Olearia_megalophylla_flower.jpeg', 'rb')),
    ('images', open('Olearia_megalophylla_auto.jpg', 'rb')),
]


# === MÉTADONNÉES ===

# Use value auto to let AI detect the organ based on the image.

# Number of values for organs must match number of input images. 
# Each organ is associated to an image in the order given. 
# If multiple images represent the same organ, use the same value multiple times.


#organs possibles : auto leaf flower fruit bark 

data = {
#    'organs': ['leaf', 'flower']
    
}

# === REQUÊTE ===
response = requests.post(endpoint, files=files, data=data)

if response.status_code != 200:
    print("Erreur :", response.status_code, response.text)
    exit()

result = response.json()

# === TRAITEMENT DES RÉSULTATS ===
#tri des resultats par score, et affichage en pourcentage
results = sorted(result.get("results", []), key=lambda x: x["score"], reverse=True)
print(f"\n Top 5 espèces probables (sur {len(results)} résultats)\n")

for i, r in enumerate(results[:5], start=1):
    species = r["species"]["scientificName"]
    score = r["score"] * 100
    gbif_id = r.get("gbif", {}).get("id")
    powo_id = r.get("powo", {}).get("id")

    gbif_url = f"https://www.gbif.org/species/{gbif_id}" if gbif_id else "—"
    powo_url = f"https://powo.science.kew.org/taxon/{powo_id}" if powo_id else "—"

    print(f"{i}. 🌿 {species}")
    print(f"   🔹 Score : {score:.2f}%")
    print(f"   🌐 GBIF : {gbif_url}")
    print(f"   📚 POWO : {powo_url}\n")

# === RÉCAPITULATIF ===
remaining = result.get("remainingIdentificationRequests", "?")
print(f" Requêtes restantes aujourd'hui : {remaining}")
