"""Retour à une version antérieure d'une collection, grâce au serveur "historique".

Le programme d'écoute (dossier ecouteur/) écrit dans le journal chaque insertion,
modification et suppression, avec le document avant et après. Pour revenir à un
instant donné, on relit le journal du plus récent au plus ancien et on annule
chaque opération faite après cet instant :

    insertion     -> on supprime le document
    modification  -> on remet le document tel qu'il était avant
    suppression   -> on réinsère le document tel qu'il était avant

Utilisation typique :
    from restauration import repere, restaurer
    r = repere()                      # on note l'instant présent
    ...                               # (des modifications ont lieu)
    restaurer("books", "books", r)    # on revient à l'instant noté

Prérequis : avoir appelé activer_historique() sur la collection AVANT les
modifications (sinon le document "avant" n'est pas conservé).
"""
import atexit
import time

from pymongo import MongoClient

PORT_PROXY = 27017          # point d'entrée du cluster (le proxy)
PORT_HISTORIQUE = 27401     # serveur "historique" (ouvert sur ce PC)

# Une connexion par serveur, réutilisée puis refermée à la fin du programme.
_connexions = {}


def _connexion(port, directe):
    cle = (port, directe)
    if cle not in _connexions:
        options = {"directConnection": True} if directe else {}
        _connexions[cle] = MongoClient("127.0.0.1", port, serverSelectionTimeoutMS=5000, **options)
    return _connexions[cle]


@atexit.register
def _fermer_les_connexions():
    for c in _connexions.values():
        c.close()
    _connexions.clear()


def cluster(port=PORT_PROXY):
    """Connexion au cluster (par défaut via le proxy)."""
    return _connexion(port, directe=False)


def journal(port=PORT_HISTORIQUE):
    """La collection "journal" du serveur historique."""
    return _connexion(port, directe=True)["historique"]["journal"]


def activer_historique(client, base, collection):
    """Demande à MongoDB de conserver le document avant/après chaque modification (collMod)."""
    client[base].command("collMod", collection, changeStreamPreAndPostImages={"enabled": True})


def repere(client=None):
    """Instant présent, lu sur l'horloge du cluster (même horloge que le journal)."""
    client = client or cluster()
    return client.admin.command("ping")["$clusterTime"]["clusterTime"]


def attendre_journal(jusqu_a_ts, delai=30, port=PORT_HISTORIQUE):
    """Attend que le journal ait enregistré tous les événements jusqu'à l'instant donné."""
    j = journal(port)
    fin = time.time() + delai
    while time.time() < fin:
        dernier = j.find_one(sort=[("ts", -1)])
        if dernier and dernier["ts"] >= jusqu_a_ts:
            return True
        time.sleep(0.5)
    return False


def restaurer(base, collection, instant, client=None, port=PORT_HISTORIQUE):
    """Annule tout ce qui a été fait sur base.collection après 'instant' (un repère).

    Retourne le nombre d'opérations annulées.
    """
    client = client or cluster()
    cible = client[base][collection]
    evenements = journal(port).find(
        {"base": base, "collection": collection, "ts": {"$gt": instant}}
    ).sort([("ts", -1)])
    annulees = 0
    for e in evenements:
        if e["operation"] == "insert":
            cible.delete_one({"_id": e["cle"]})
        elif e["operation"] in ("update", "replace"):
            if e["avant"] is None:
                raise RuntimeError(f"Pas de document 'avant' pour {e['cle']} : "
                                   "activer_historique() n'était pas appelé sur cette collection")
            cible.replace_one({"_id": e["cle"]}, e["avant"])
        elif e["operation"] == "delete":
            if e["avant"] is None:
                raise RuntimeError(f"Pas de document 'avant' pour {e['cle']} : "
                                   "activer_historique() n'était pas appelé sur cette collection")
            cible.replace_one({"_id": e["cle"]}, e["avant"], upsert=True)
        annulees += 1
    return annulees
