"""Enregistre dans le serveur "historique" chaque modification faite sur le cluster.

Principe : MongoDB peut annoncer chaque insertion, modification ou suppression
au moment où elle a lieu (ce mécanisme s'appelle les "change streams"). Ce
programme écoute ces annonces à travers les routeurs et écrit une ligne dans
le journal pour chacune : quand, sur quelle collection, quelle opération, et le
document AVANT et APRÈS la modification.

Pour que le document "avant" soit conservé, la collection doit avoir l'option
changeStreamPreAndPostImages (voir restauration.activer_historique).

Si le programme s'arrête, il reprend là où il s'était arrêté grâce au "jeton de
reprise" enregistré après chaque événement.
"""
import os
import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

URI_CLUSTER = os.environ.get("URI_CLUSTER", "mongodb://routeur_1:27017,routeur_2:27017")
URI_HISTORIQUE = os.environ.get("URI_HISTORIQUE", "mongodb://historique:27017")
BASES_IGNOREES = ["admin", "config", "local", "historique"]

OPERATIONS = ["insert", "update", "replace", "delete"]


def jeton_de_reprise(etat):
    doc = etat.find_one({"_id": "reprise"})
    return doc["jeton"] if doc else None


def enregistrer(journal, etat, evenement, jeton):
    ts = evenement["clusterTime"]
    document = {
        "_id": evenement["_id"]["_data"],            # identifiant unique de l'événement
        "ts": ts,                                     # horloge du cluster (ordre exact)
        "horodatage": ts.as_datetime(),
        "base": evenement["ns"]["db"],
        "collection": evenement["ns"]["coll"],
        "operation": evenement["operationType"],
        "cle": evenement["documentKey"]["_id"],
        "avant": evenement.get("fullDocumentBeforeChange"),
        "apres": evenement.get("fullDocument"),
    }
    journal.replace_one({"_id": document["_id"]}, document, upsert=True)   # sans doublon si on rejoue
    etat.replace_one({"_id": "reprise"}, {"_id": "reprise", "jeton": jeton}, upsert=True)


def ecouter():
    cluster = MongoClient(URI_CLUSTER)
    base = MongoClient(URI_HISTORIQUE)["historique"]
    journal, etat = base["journal"], base["etat"]
    filtre = [{"$match": {"operationType": {"$in": OPERATIONS}, "ns.db": {"$nin": BASES_IGNOREES}}}]
    jeton = jeton_de_reprise(etat)
    print("Écoute du cluster", "(reprise après un arrêt)" if jeton else "(départ)", flush=True)
    try:
        with cluster.watch(filtre, full_document="whenAvailable",
                           full_document_before_change="whenAvailable", resume_after=jeton) as flux:
            for evenement in flux:
                enregistrer(journal, etat, evenement, flux.resume_token)
                print(evenement["operationType"], evenement["ns"]["db"] + "." + evenement["ns"]["coll"],
                      evenement["documentKey"]["_id"], flush=True)
    except OperationFailure as e:
        # Le jeton est trop ancien pour être repris : on repart de zéro
        print("Jeton de reprise inutilisable :", e, flush=True)
        etat.delete_one({"_id": "reprise"})


if __name__ == "__main__":
    while True:
        try:
            ecouter()
        except PyMongoError as e:
            print("Connexion perdue, nouvelle tentative dans 3 s :", type(e).__name__, flush=True)
        time.sleep(3)
