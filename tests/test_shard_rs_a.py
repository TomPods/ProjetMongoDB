"""Tests du premier shard (groupe rs_a) : rôles, vote, réplication.

Lancer depuis le dossier du projet :   python -m unittest tests.test_shard_rs_a -v
Prérequis : le cluster est démarré (python lancer.py).
"""
import sys
import time
import unittest
from pathlib import Path

from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import lancer  # noqa: E402

GROUPE = lancer.SHARD_A
NON_VOTANT = "secondaire_a_3"


def membre(nom):
    return next(m for m in GROUPE["membres"] if m["nom"] == nom)


class TestShardA(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.etat = lancer.etat_groupe(GROUPE)
        port = GROUPE["membres"][0]["port_hote"]
        cls.config = lancer.executer(port, "replSetGetConfig")["config"]
        cls.statut = lancer.executer(port, "replSetGetStatus")

    def test_les_4_serveurs_sont_membres(self):
        self.assertEqual(set(self.etat), {m["nom"] for m in GROUPE["membres"]})

    def test_un_seul_chef(self):
        chefs = [n for n, role in self.etat.items() if role == "PRIMARY"]
        self.assertEqual(len(chefs), 1, f"chefs trouvés : {chefs}")

    def test_chaque_serveur_est_un_serveur_de_shard(self):
        for m in GROUPE["membres"]:
            options = lancer.executer(m["port_hote"], "getCmdLineOpts")["parsed"]
            self.assertEqual(options["sharding"]["clusterRole"], "shardsvr", m["nom"])

    def test_le_troisieme_secondaire_est_non_votant(self):
        reglages = {x["host"].split(":")[0]: x for x in self.config["members"]}
        self.assertEqual(reglages[NON_VOTANT]["votes"], 0)
        self.assertEqual(reglages[NON_VOTANT]["priority"], 0)   # obligatoire pour un non-votant

    def test_les_autres_serveurs_votent(self):
        for x in self.config["members"]:
            if x["host"].split(":")[0] != NON_VOTANT:
                self.assertEqual(x["votes"], 1, x["host"])

    def test_il_y_a_trois_votants_un_nombre_impair(self):
        self.assertEqual(self.statut["votingMembersCount"], 3)

    def test_la_replication_atteint_les_4_serveurs(self):
        chef = next(n for n, r in self.etat.items() if r == "PRIMARY")
        with MongoClient("127.0.0.1", membre(chef)["port_hote"], directConnection=True) as c:
            c.essai_tests.docs.delete_many({})
            c.essai_tests.docs.insert_many([{"n": 1}, {"n": 2}, {"n": 3}])
        try:
            for m in GROUPE["membres"]:
                with MongoClient("127.0.0.1", m["port_hote"], directConnection=True) as c:
                    for _ in range(20):          # laisse jusqu'à 20 s à la copie
                        if c.essai_tests.docs.count_documents({}) == 3:
                            break
                        time.sleep(1)
                    self.assertEqual(c.essai_tests.docs.count_documents({}), 3, m["nom"])
        finally:
            with MongoClient("127.0.0.1", membre(chef)["port_hote"], directConnection=True) as c:
                c.drop_database("essai_tests")


if __name__ == "__main__":
    unittest.main()
