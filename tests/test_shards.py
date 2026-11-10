"""Tests des 3 shards (rs_a, rs_b, rs_c) : rôles, vote, réplication.

Lancer depuis le dossier du projet :   python -m unittest tests.test_shards -v
Prérequis : le cluster est démarré (python lancer.py).

Chaque test est répété pour chaque shard (sous-tests) : si un groupe échoue,
le message indique lequel.
"""
import sys
import time
import unittest
from pathlib import Path

from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import lancer  # noqa: E402


def nom_non_votant(groupe):
    """Le 3e secondaire est le dernier membre de la liste."""
    return groupe["membres"][-1]["nom"]


class TestShards(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.donnees = {}
        for g in lancer.SHARDS:
            port = g["membres"][0]["port_hote"]
            cls.donnees[g["nom"]] = {
                "etat": lancer.etat_groupe(g),
                "config": lancer.executer(port, "replSetGetConfig")["config"],
                "statut": lancer.executer(port, "replSetGetStatus"),
            }

    def pour_chaque_shard(self):
        for g in lancer.SHARDS:
            with self.subTest(shard=g["nom"]):
                yield g, self.donnees[g["nom"]]

    def test_les_4_serveurs_de_chaque_shard_sont_membres(self):
        for g, d in self.pour_chaque_shard():
            self.assertEqual(set(d["etat"]), {m["nom"] for m in g["membres"]})

    def test_un_seul_chef_par_shard(self):
        for g, d in self.pour_chaque_shard():
            chefs = [n for n, role in d["etat"].items() if role == "PRIMARY"]
            self.assertEqual(len(chefs), 1, f"chefs trouvés : {chefs}")

    def test_chaque_serveur_est_un_serveur_de_shard(self):
        for g, _ in self.pour_chaque_shard():
            for m in g["membres"]:
                options = lancer.executer(m["port_hote"], "getCmdLineOpts")["parsed"]
                self.assertEqual(options["sharding"]["clusterRole"], "shardsvr", m["nom"])

    def test_le_troisieme_secondaire_est_non_votant(self):
        for g, d in self.pour_chaque_shard():
            reglages = {x["host"].split(":")[0]: x for x in d["config"]["members"]}
            nv = nom_non_votant(g)
            self.assertEqual(reglages[nv]["votes"], 0)
            self.assertEqual(reglages[nv]["priority"], 0)   # obligatoire pour un non-votant

    def test_les_autres_serveurs_votent(self):
        for g, d in self.pour_chaque_shard():
            for x in d["config"]["members"]:
                if x["host"].split(":")[0] != nom_non_votant(g):
                    self.assertEqual(x["votes"], 1, x["host"])

    def test_trois_votants_par_shard_un_nombre_impair(self):
        for g, d in self.pour_chaque_shard():
            self.assertEqual(d["statut"]["votingMembersCount"], 3)

    def test_les_groupes_ont_des_noms_differents(self):
        noms = [g["nom"] for g in lancer.GROUPES]
        self.assertEqual(len(noms), len(set(noms)))   # règle de la documentation

    def test_la_replication_atteint_les_4_serveurs_de_chaque_shard(self):
        for g, d in self.pour_chaque_shard():
            chef = next(n for n, r in d["etat"].items() if r == "PRIMARY")
            port_chef = next(m["port_hote"] for m in g["membres"] if m["nom"] == chef)
            with MongoClient("127.0.0.1", port_chef, directConnection=True) as c:
                c.essai_tests.docs.delete_many({})
                c.essai_tests.docs.insert_many([{"n": 1}, {"n": 2}, {"n": 3}])
            try:
                for m in g["membres"]:
                    with MongoClient("127.0.0.1", m["port_hote"], directConnection=True) as c:
                        for _ in range(20):          # laisse jusqu'à 20 s à la copie
                            if c.essai_tests.docs.count_documents({}) == 3:
                                break
                            time.sleep(1)
                        self.assertEqual(c.essai_tests.docs.count_documents({}), 3, m["nom"])
            finally:
                with MongoClient("127.0.0.1", port_chef, directConnection=True) as c:
                    c.drop_database("essai_tests")


if __name__ == "__main__":
    unittest.main()
