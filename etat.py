"""Affiche l'état du cluster en une commande :   python etat.py

Montre, pour chaque groupe de serveurs, qui est chef (PRIMARY) et qui est
secondaire (SECONDARY), puis les shards connus des routeurs.
"""
from pymongo.errors import PyMongoError

import lancer

for groupe in lancer.GROUPES:
    lancer.afficher_etat(groupe)
try:
    lancer.afficher_shards()
except PyMongoError:
    print("\nLes routeurs ne répondent pas : le cluster est-il démarré (python lancer.py) ?")
