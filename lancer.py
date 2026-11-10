"""Lance le cluster MongoDB : une seule commande, "python lancer.py".

Étape actuelle : serveurs de configuration (rs_config) + premier shard (rs_a).
Les autres éléments (shards rs_b et rs_c, routeurs, proxy, historique)
s'ajouteront ici, dans l'ordre imposé par la documentation MongoDB :
configuration, puis shards, puis routeurs.
"""
import subprocess
import sys
import time
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

DOSSIER = Path(__file__).resolve().parent


# --- Description des groupes de copies (replica sets) ------------------
# Chaque membre : nom du conteneur, port ouvert sur ce PC (127.0.0.1) et
# réglages MongoDB facultatifs (priority, votes).
# Pour changer le nombre de serveurs, modifier cette description ET le
# docker-compose.yml.
def membre(nom, port_hote, **options):
    return {"nom": nom, "port_hote": port_hote, "options": options}


CONFIG = {
    "nom": "rs_config",
    "port": 27019,            # port dans le conteneur
    "configsvr": True,        # groupe de serveurs de configuration
    "membres": [
        membre("config_1", 27119),
        membre("config_2", 27120),
        membre("config_3", 27121),
    ],
}

SHARD_A = {
    "nom": "rs_a",
    "port": 27018,
    "configsvr": False,
    "membres": [
        membre("principal_a", 27211, priority=2),        # préféré comme chef
        membre("secondaire_a_1", 27212),
        membre("secondaire_a_2", 27213),
        membre("secondaire_a_3", 27214, priority=0, votes=0),   # NON VOTANT
    ],
}

SHARDS = [SHARD_A]
GROUPES = [CONFIG] + SHARDS     # ordre de démarrage : configuration d'abord


# --- Outils ------------------------------------------------------------
def lancer_compose():
    """Crée et démarre tous les conteneurs décrits dans docker-compose.yml."""
    print("1/4  Démarrage des conteneurs (docker compose up -d)...")
    subprocess.run(["docker", "compose", "up", "-d"], cwd=DOSSIER, check=True)


def executer(port, commande, valeur=1):
    """Envoie une commande à UN serveur précis, puis referme la connexion.

    directConnection=True : on parle à ce serveur-là, sans chercher les autres
    membres du groupe (nécessaire tant que le groupe n'existe pas, et parce que
    les adresses internes du groupe, comme config_1, ne sont pas joignables
    depuis ce PC).
    """
    with MongoClient("127.0.0.1", port, directConnection=True, serverSelectionTimeoutMS=3000) as client:
        return client.admin.command(commande, valeur)


def attendre(condition, description, delai=90):
    """Répète 'condition' toutes les secondes jusqu'à ce qu'elle réussisse."""
    fin = time.time() + delai
    while time.time() < fin:
        try:
            if condition():
                return
        except PyMongoError:
            pass
        time.sleep(1)
    sys.exit(f"Échec : {description} (délai de {delai} s dépassé)")


# --- Étapes pour un groupe ----------------------------------------------
def attendre_serveurs(groupe):
    for m in groupe["membres"]:
        attendre(lambda p=m["port_hote"]: executer(p, "ping")["ok"] == 1, f"{m['nom']} ne répond pas")
        print(f"       {m['nom']} répond (port {m['port_hote']})")


def initialiser_groupe(groupe):
    """Déclare le groupe : équivalent de rs.initiate() dans mongosh."""
    print(f"       initialisation du groupe {groupe['nom']}")
    premier = groupe["membres"][0]["port_hote"]
    try:
        executer(premier, "replSetGetStatus")
        print("       déjà initialisé, rien à faire")
        return
    except OperationFailure:   # "no replset config has been received" : première fois
        pass
    configuration = {
        "_id": groupe["nom"],
        "members": [
            {"_id": i, "host": f"{m['nom']}:{groupe['port']}", **m["options"]}  # adresse vue DEPUIS le réseau Docker
            for i, m in enumerate(groupe["membres"])
        ],
    }
    if groupe["configsvr"]:
        configuration["configsvr"] = True   # obligatoire pour les serveurs de configuration
    executer(premier, "replSetInitiate", configuration)
    print("       groupe initialisé")


def etat_groupe(groupe):
    """Retourne {nom_du_membre: rôle} en interrogeant un serveur qui répond."""
    for m in groupe["membres"]:
        try:
            statut = executer(m["port_hote"], "replSetGetStatus")
            return {x["name"].split(":")[0]: x["stateStr"] for x in statut["members"]}
        except PyMongoError:
            continue
    return {}


def attendre_chef(groupe):
    attendre(lambda: "PRIMARY" in etat_groupe(groupe).values(), f"aucun chef élu dans {groupe['nom']}")


def afficher_etat(groupe):
    print(f"\nÉtat du groupe {groupe['nom']} :")
    for nom, role in sorted(etat_groupe(groupe).items()):
        print(f"   {nom:15s} {role}")


if __name__ == "__main__":
    lancer_compose()
    print("2/4  Attente que les serveurs répondent...")
    for g in GROUPES:
        attendre_serveurs(g)
    print("3/4  Initialisation des groupes (configuration d'abord, puis shards)...")
    for g in GROUPES:
        initialiser_groupe(g)
        attendre_chef(g)
    print("4/4  Vérification...")
    for g in GROUPES:
        afficher_etat(g)
    print("\nCluster prêt (étape actuelle : configuration + shard rs_a).")
