# ProjetMongoDB : un cluster MongoDB simulé avec Docker et Python

Projet de mise à l'échelle (BUT 3) : simuler un petit réseau de serveurs MongoDB
avec **sharding** (les données sont réparties sur plusieurs groupes de serveurs)
et **réplication** (chaque groupe garde plusieurs copies des données).

**Groupe :** Tom Podsadni, Sacha Rault

Tout se lance avec une seule commande : `python lancer.py`.

## Ce que contient le cluster (20 conteneurs)

| Rôle | Noms | Nombre |
|---|---|---|
| Proxy (point d'entrée, port 27017) | `proxy` | 1 |
| Routeurs | `routeur_1`, `routeur_2` | 2 |
| Serveurs de configuration (le catalogue) | `config_1` à `config_3` | 3 |
| Serveurs principaux | `principal_a`, `principal_b`, `principal_c` | 3 |
| Serveurs secondaires | `secondaire_a_1` à `secondaire_c_3` | 9 |
| Historique (sauvegarde des modifications) | `historique`, `ecouteur_historique` | 2 |

Chaque shard (`rs_a`, `rs_b`, `rs_c`) a 4 serveurs : 1 principal et 3 secondaires.
Le troisième secondaire de chaque groupe est **non votant** (il garde une copie
des données mais ne vote pas), pour qu'il reste un nombre impair (3) de votants
et qu'une égalité lors d'une élection soit impossible.

## Prérequis

- Docker Desktop (allumé), avec au moins 4 Go de mémoire alloués
- Python 3 avec le module `pymongo` : `python -m pip install pymongo`
- Internet la première fois (environ 1,5 Go d'images Docker à télécharger)
- Le port **27017** libre (c'est celui du proxy : aucun autre MongoDB ne doit tourner dessus)
- Les ports 27119 à 27121, 27211 à 27214, 27221 à 27224, 27231 à 27234, 27301, 27302 et 27401 libres

## Lancer le cluster

Depuis le dossier du projet :

```
python lancer.py
```

Le script démarre les conteneurs dans l'ordre imposé par la documentation de
MongoDB (configuration, shards, routeurs), initialise les groupes, déclare les
shards, puis démarre le proxy et l'historique. Il peut être relancé sans risque.
Depuis zéro, il faut environ 76 secondes (images déjà téléchargées).

Fin de la sortie quand tout va bien :

```
Shards du cluster (vus par le routeur) :
   rs_a   rs_a -> principal_a:27018 ...
   rs_b   rs_b -> principal_b:27018 ...
   rs_c   rs_c -> principal_c:27018 ...

Connexion classique localhost:27017 : OK, shards vus : rs_a, rs_b, rs_c

Cluster prêt.
```

## Utiliser le cluster

Se connecter comme à n'importe quel MongoDB, sur le port classique :

```python
from pymongo import MongoClient
client = MongoClient("localhost", 27017)
print(client.admin.command("ping"))
```

Importer les 431 livres de `data/books.json` et voir leur répartition :

```
python importer_books.py
```

```
431 livres importés dans books.books.
   rs_a : 150 livres
   rs_b : 135 livres
   rs_c : 146 livres
   total : 431 livres
```

Voir qui est chef dans chaque groupe :

```
python etat.py
```

Revenir à une version antérieure d'une collection (voir `restauration.py`) :

```python
from restauration import cluster, repere, restaurer
c = cluster()
instant = repere(c)                  # on note l'instant présent
# ... des modifications ont lieu ...
restaurer("books", "books", instant, c)
```

## Lancer les tests

Le cluster doit être démarré.

```
python -m unittest tests.test_config_servers tests.test_shards tests.test_cluster -v
```

26 tests : configuration, shards, connexion sur le port 27017, répartition des
livres, réplication, historique, et pannes (arrêt d'un routeur, du chef d'un
shard). Les tests de pannes durent environ une minute ; pour les ignorer,
définir la variable `RAPIDE=1` avant la commande.

## Arrêter ou remettre à zéro

```
docker compose stop        # arrête les conteneurs, les données sont conservées
docker compose start       # les redémarre
docker compose down        # supprime les conteneurs, les données sont conservées
docker compose down -v     # supprime tout, y compris les données
```

## Organisation du dossier

```
.
├── docker-compose.yml        le plan des 20 conteneurs
├── lancer.py                 la commande Python qui lance tout
├── importer_books.py         import de data/books.json et répartition sur les shards
├── restauration.py           retour à une version antérieure (historique)
├── etat.py                   affiche l'état du cluster
├── config/haproxy.cfg        configuration du proxy
├── ecouteur/                 le programme qui alimente l'historique (Dockerfile + code)
├── data/books.json           les 431 livres
├── tests/                    les tests Python
└── docs/                     la documentation
```

## Documentation

Chaque étape est expliquée pas à pas, avec toutes les commandes et leurs
sorties réelles, pour pouvoir la refaire à la main :

- [Mise en œuvre](docs/Mise_en_oeuvre.docx) : installer, lancer, vérifier, dépanner
- [Guide de maintenance pour un débutant](docs/Guide_maintenance.docx)
- [b.a.-ba.txt](docs/b.a.-ba.txt) : les notions Docker utilisées (services, volumes, healthcheck, replSet, shardsvr...)
- [Partie 1 : l'architecture](docs/Partie1_architecture.docx)
- [Partie 2 : les serveurs de configuration](docs/Partie2_serveurs_de_configuration.docx)
- [Partie 3 : le premier shard `rs_a`](docs/Partie3_shard_rs_a.docx)
- [Partie 4 : les shards `rs_b` et `rs_c`](docs/Partie4_shards_rs_b_rs_c.docx)
- [Partie 5 : les routeurs, la déclaration des shards et le proxy](docs/Partie5_routeurs_shards_proxy.docx)
- [Partie 6 : importer books, éclatement et réplication](docs/Partie6_books_eclatement_replication.docx)
- [Partie 7 : le serveur d'historique](docs/Partie7_historique.docx)

## Ce qui n'est pas fait

La version avec sécurité (authentification) n'a pas été réalisée : elle risquait
de faire échouer le test de l'enseignant, qui se connecte sur le port 27017
(voir le document de mise en œuvre).

## Sources

La documentation officielle de MongoDB et de HAProxy a servi pour définir l'architecture :
[composants d'un cluster shardé](https://www.mongodb.com/docs/manual/core/sharded-cluster-components/),
[serveurs de configuration](https://www.mongodb.com/docs/manual/core/sharded-cluster-config-servers/),
[déployer un cluster shardé](https://www.mongodb.com/docs/manual/tutorial/deploy-shard-cluster/),
[architectures d'un replica set](https://www.mongodb.com/docs/manual/core/replica-set-architectures/),
[élections](https://www.mongodb.com/docs/manual/core/replica-set-elections/),
[configuration d'un replica set](https://www.mongodb.com/docs/manual/reference/replica-configuration/),
[change streams](https://www.mongodb.com/docs/manual/changeStreams/) et
[HAProxy en mode TCP](https://www.haproxy.com/documentation/haproxy-configuration-tutorials/protocol-support/tcp/).
