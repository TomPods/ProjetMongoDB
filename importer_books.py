"""Importe data/books.json dans le cluster et répartit la collection sur les 3 shards.

Utilisation :
    python importer_books.py                       # base "books", collection "books"
    python importer_books.py --base ma_base        # autre nom de base
    python importer_books.py --supprimer           # supprime la collection importée

Étapes (dans cet ordre) :
  1. shardCollection  : la collection est répartie selon le hachage de _id.
                        On le fait AVANT l'import, sur une collection vide : MongoDB
                        crée alors un morceau (chunk) par shard.
  2. collMod          : conservation du document avant/après chaque modification
                        (nécessaire au serveur "historique" pour revenir en arrière).
  3. insert_many      : import des 431 livres, envoyés au proxy (port 27017).
Puis affichage du nombre de livres sur chaque shard.
"""
import argparse
from pathlib import Path

from bson import json_util
from pymongo import MongoClient

import lancer
from restauration import activer_historique

FICHIER = Path(__file__).resolve().parent / "data" / "books.json"


def chef_du_groupe(groupe):
    etat = lancer.etat_groupe(groupe)
    return next(n for n, role in etat.items() if role == "PRIMARY")


def repartition(base, collection):
    """Nombre de documents sur chaque shard, en interrogeant directement le chef de chaque groupe."""
    resultat = {}
    for g in lancer.SHARDS:
        chef = chef_du_groupe(g)
        port = next(m["port_hote"] for m in g["membres"] if m["nom"] == chef)
        with MongoClient("127.0.0.1", port, directConnection=True) as c:
            resultat[g["nom"]] = c[base][collection].count_documents({})
    return resultat


def importer(base="books", collection="books", client=None):
    livres = json_util.loads(FICHIER.read_text(encoding="utf-8"))   # json_util : comprend les dates {"$date": ...}
    client = client or MongoClient("127.0.0.1", 27017, serverSelectionTimeoutMS=5000)
    client.admin.command("shardCollection", f"{base}.{collection}", key={"_id": "hashed"})
    activer_historique(client, base, collection)
    client[base][collection].insert_many(livres)
    return len(livres)


def supprimer(base="books", collection="books", client=None):
    client = client or MongoClient("127.0.0.1", 27017, serverSelectionTimeoutMS=5000)
    client[base].drop_collection(collection)


if __name__ == "__main__":
    parseur = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parseur.add_argument("--base", default="books")
    parseur.add_argument("--collection", default="books")
    parseur.add_argument("--supprimer", action="store_true", help="supprime la collection au lieu de l'importer")
    a = parseur.parse_args()
    if a.supprimer:
        supprimer(a.base, a.collection)
        print(f"Collection {a.base}.{a.collection} supprimée.")
    else:
        n = importer(a.base, a.collection)
        print(f"{n} livres importés dans {a.base}.{a.collection}.")
        rep = repartition(a.base, a.collection)
        for shard_nom, nb in sorted(rep.items()):
            print(f"   {shard_nom} : {nb} livres")
        print(f"   total : {sum(rep.values())} livres")
