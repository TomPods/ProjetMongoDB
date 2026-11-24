"""Lance le cluster MongoDB : une seule commande, "python lancer.py".

Ordre imposé par la documentation MongoDB :
  1. les serveurs de configuration (groupe rs_config) et les shards,
  2. l'initialisation de chaque groupe de copies (rs.initiate),
  3. les routeurs (mongos), qui ont besoin du groupe de configuration,
  4. la déclaration des shards auprès d'un routeur (addShard),
  5. le proxy (point d'entrée, port 27017) et le serveur d'historique avec son
     programme d'écoute.
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


def shard(lettre, port_hote_depart):
    """Décrit un shard : 1 principal + 3 secondaires, le 3e secondaire étant non votant.

    Les ports ouverts sur ce PC se suivent : 27211 à 27214 pour le groupe a,
    27221 à 27224 pour le groupe b, 27231 à 27234 pour le groupe c.
    """
    return {
        "nom": f"rs_{lettre}",
        "port": 27018,
        "configsvr": False,
        "membres": [
            membre(f"principal_{lettre}", port_hote_depart, priority=2),     # préféré comme chef
            membre(f"secondaire_{lettre}_1", port_hote_depart + 1),
            membre(f"secondaire_{lettre}_2", port_hote_depart + 2),
            membre(f"secondaire_{lettre}_3", port_hote_depart + 3, priority=0, votes=0),   # NON VOTANT
        ],
    }


SHARD_A = shard("a", 27211)
SHARD_B = shard("b", 27221)
SHARD_C = shard("c", 27231)

SHARDS = [SHARD_A, SHARD_B, SHARD_C]
GROUPES = [CONFIG] + SHARDS     # ordre de démarrage : configuration d'abord

# Les routeurs : (nom du conteneur, port ouvert sur ce PC). Port dans le conteneur : 27017.
ROUTEURS = [("routeur_1", 27301), ("routeur_2", 27302)]

# Le proxy : point d'entrée unique du cluster, sur le port classique de MongoDB.
PORT_PROXY = 27017
# Le serveur d'historique et le programme qui l'alimente.
PORT_HISTORIQUE = 27401     # port ouvert sur ce PC (27017 dans le conteneur)
SERVICES_FINAUX = ["proxy", "historique", "ecouteur_historique"]


def noms_serveurs_de_donnees():
    return [m["nom"] for g in GROUPES for m in g["membres"]]


# --- Outils ------------------------------------------------------------
def lancer_compose(services=(), construire=False):
    """Crée et démarre les conteneurs décrits dans docker-compose.yml (tous, ou ceux indiqués).

    construire=True : fabrique d'abord les images décrites par "build" (ici, le programme d'écoute).
    """
    options = ["--build"] if construire else []
    subprocess.run(["docker", "compose", "up", "-d", *options, *services], cwd=DOSSIER, check=True)


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


# --- Routeurs et shards --------------------------------------------------
def attendre_routeurs():
    for nom, port in ROUTEURS:
        attendre(lambda p=port: executer(p, "ping")["ok"] == 1, f"{nom} ne répond pas")
        print(f"       {nom} répond (port {port})")


def shards_declares():
    """Noms des shards connus du cluster (interrogation d'un routeur)."""
    return {s["_id"] for s in executer(ROUTEURS[0][1], "listShards")["shards"]}


def ajouter_shards():
    """Déclare chaque groupe de shard au cluster : équivalent de sh.addShard() dans mongosh."""
    deja = shards_declares()
    for g in SHARDS:
        if g["nom"] in deja:
            print(f"       {g['nom']} est déjà un shard du cluster, rien à faire")
            continue
        # format attendu : nom_du_groupe/hôte1:port,hôte2:port,...
        graines = ",".join(f"{m['nom']}:{g['port']}" for m in g["membres"])
        executer(ROUTEURS[0][1], "addShard", f"{g['nom']}/{graines}")
        print(f"       {g['nom']} ajouté au cluster")


def afficher_shards():
    print("\nShards du cluster (vus par le routeur) :")
    for s in sorted(executer(ROUTEURS[0][1], "listShards")["shards"], key=lambda x: x["_id"]):
        print(f"   {s['_id']:6s} {s['host'].split('/')[0]} -> {s['host'].split('/')[1].split(',')[0]} ...")


# --- Proxy et historique -----------------------------------------------
def attendre_proxy_et_historique():
    attendre(lambda: executer(PORT_PROXY, "ping")["ok"] == 1, "le proxy ne répond pas sur le port 27017")
    print(f"       proxy répond (port {PORT_PROXY})")
    attendre(lambda: executer(PORT_HISTORIQUE, "ping")["ok"] == 1, "l'historique ne répond pas")
    print(f"       historique répond (port {PORT_HISTORIQUE})")


def verifier_connexion_classique():
    """Connexion comme le ferait n'importe quel client : localhost, port 27017, sans option."""
    with MongoClient("localhost", PORT_PROXY, serverSelectionTimeoutMS=5000) as client:
        shards = sorted(s["_id"] for s in client.admin.command("listShards")["shards"])
    print(f"\nConnexion classique localhost:{PORT_PROXY} : OK, shards vus : {', '.join(shards)}")


if __name__ == "__main__":
    print("1/8  Démarrage des serveurs de configuration et des shards (docker compose up -d)...")
    lancer_compose(noms_serveurs_de_donnees())
    print("2/8  Attente que les serveurs répondent...")
    for g in GROUPES:
        attendre_serveurs(g)
    print("3/8  Initialisation des groupes (configuration d'abord, puis shards)...")
    for g in GROUPES:
        initialiser_groupe(g)
        attendre_chef(g)
    print("4/8  Démarrage des routeurs...")
    lancer_compose([nom for nom, _ in ROUTEURS])
    attendre_routeurs()
    print("5/8  Déclaration des shards auprès du routeur...")
    ajouter_shards()
    print("6/8  Démarrage du proxy, de l'historique et de son programme d'écoute...")
    lancer_compose(SERVICES_FINAUX, construire=True)
    print("7/8  Attente du proxy et de l'historique...")
    attendre_proxy_et_historique()
    print("8/8  Vérification...")
    for g in GROUPES:
        afficher_etat(g)
    afficher_shards()
    verifier_connexion_classique()
    print("\nCluster prêt.")
