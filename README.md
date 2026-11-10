# ProjetMongoDB : un cluster MongoDB simulé avec Docker et Python

Projet de mise à l'échelle (BUT 3) : simuler un petit réseau de serveurs MongoDB
avec **sharding** (les données sont réparties sur plusieurs groupes de serveurs)
et **réplication** (chaque groupe garde plusieurs copies des données).

**Groupe :** Tom Podsadni, Sacha Rault

## Où en est le projet

| Élément | État |
|---|---|
| 3 serveurs de configuration (`rs_config`) | fait et testé |
| Shards `rs_a`, `rs_b`, `rs_c` (1 principal + 3 secondaires chacun) | fait et testé |
| 2 routeurs, déclaration des shards, import de `books` | à faire |
| Proxy (point d'entrée sur le port 27017) | à faire |
| Serveur d'historique (retour à une version antérieure) | à faire |
| Test Python complet, documentation finale, version sécurisée | à faire |

La commande `python lancer.py` lance pour l'instant ce qui est fait : les
serveurs de configuration et les 3 shards (15 conteneurs).

## L'architecture visée (19 conteneurs)

| Rôle | Noms | Nombre |
|---|---|---|
| Proxy | `proxy` | 1 |
| Routeurs | `routeur_1`, `routeur_2` | 2 |
| Serveurs de configuration | `config_1` à `config_3` | 3 |
| Serveurs principaux | `principal_a`, `principal_b`, `principal_c` | 3 |
| Serveurs secondaires | `secondaire_a_1` à `secondaire_c_3` | 9 |
| Historique | `historique` | 1 |

Chaque shard (`rs_a`, `rs_b`, `rs_c`) a 4 serveurs : 1 principal et 3 secondaires.
Le troisième secondaire de chaque groupe est **non votant** (il garde une copie
des données mais ne vote pas), pour qu'il reste un nombre impair (3) de votants
et qu'une égalité lors d'une élection soit impossible.

## Prérequis

- Docker Desktop (allumé) et Docker Compose
- Python 3 avec le module `pymongo` : `python -m pip install pymongo`
- Les ports 27119 à 27121, 27211 à 27214, 27221 à 27224 et 27231 à 27234 libres sur la machine
- Pour la suite du projet : le port 27017 libre (aucun autre MongoDB lancé)

## Lancer le cluster

Depuis le dossier du projet :

```
python lancer.py
```

Le script démarre les conteneurs, attend qu'ils répondent, initialise les
groupes (la configuration d'abord, puis les shards) et affiche le rôle de
chaque serveur. Il peut être relancé sans risque.

Sortie attendue (extrait) :

```
État du groupe rs_config :
   config_1        PRIMARY
   config_2        SECONDARY
   config_3        SECONDARY

État du groupe rs_a :
   principal_a     PRIMARY
   secondaire_a_1  SECONDARY
   secondaire_a_2  SECONDARY
   secondaire_a_3  SECONDARY

(idem pour rs_b et rs_c)
```

Le serveur chef (`PRIMARY`) peut varier d'un lancement à l'autre pour
`rs_config` ; pour les shards, le principal (`principal_a`, `_b`, `_c`) reprend
la place de chef de lui-même. Un lancement complet depuis zéro dure environ une minute.

## Lancer les tests

Le cluster doit être démarré.

```
python -m unittest tests.test_config_servers tests.test_shards -v
```

## Arrêter ou remettre à zéro

```
docker compose stop        # arrête les conteneurs, les données sont conservées
docker compose start       # les redémarre
docker compose down        # supprime les conteneurs, les données sont conservées
docker compose down -v     # supprime tout, y compris les données
```

Après un `down -v`, relancer `python lancer.py` recrée tout depuis zéro.

## Organisation du dossier

```
.
├── docker-compose.yml        le plan de tous les conteneurs
├── lancer.py                 la commande Python qui lance tout
├── data/
│   └── books.json            les 431 livres utilisés pour tester le sharding
├── tests/                    les tests Python
└── docs/                     la documentation
```

## Documentation

Chaque étape est expliquée pas à pas, avec toutes les commandes et leurs
sorties réelles, pour pouvoir la refaire à la main :

- [b.a.-ba.txt](docs/b.a.-ba.txt) : les notions Docker utilisées (services, volumes, healthcheck, replSet, shardsvr...)
- [Partie 1 : l'architecture](docs/Partie1_architecture.docx)
- [Partie 2 : les serveurs de configuration](docs/Partie2_serveurs_de_configuration.docx)
- [Partie 3 : le premier shard `rs_a`](docs/Partie3_shard_rs_a.docx)
- [Partie 4 : les shards `rs_b` et `rs_c`](docs/Partie4_shards_rs_b_rs_c.docx)

## Sources

La documentation officielle de MongoDB a servi pour définir l'architecture :
[composants d'un cluster shardé](https://www.mongodb.com/docs/manual/core/sharded-cluster-components/),
[serveurs de configuration](https://www.mongodb.com/docs/manual/core/sharded-cluster-config-servers/),
[déployer un cluster shardé](https://www.mongodb.com/docs/manual/tutorial/deploy-shard-cluster/),
[architectures d'un replica set](https://www.mongodb.com/docs/manual/core/replica-set-architectures/),
[élections](https://www.mongodb.com/docs/manual/core/replica-set-elections/) et
[configuration d'un replica set](https://www.mongodb.com/docs/manual/reference/replica-configuration/).
