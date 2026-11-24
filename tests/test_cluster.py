"""Tests du cluster complet : connexion, sharding, réplication, historique, pannes.

Lancer depuis le dossier du projet :
    python -m unittest tests.test_cluster -v
Prérequis : le cluster est démarré (python lancer.py).

Les tests qui arrêtent des serveurs (classe TestPannes) prennent environ une minute.
Pour les ignorer :   set RAPIDE=1   (Windows)   ou   RAPIDE=1 python -m unittest ...
"""
import os
import subprocess
import sys
import time
import unittest
from pathlib import Path

from bson import json_util
from pymongo import MongoClient
from pymongo.errors import PyMongoError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import lancer  # noqa: E402
import restauration  # noqa: E402

LIVRES = json_util.loads((Path(__file__).resolve().parent.parent / "data" / "books.json").read_text(encoding="utf-8"))


def client_classique():
    """Connexion comme le fait un client ordinaire : localhost, port 27017, aucune option."""
    return MongoClient("localhost", 27017, serverSelectionTimeoutMS=5000)


def docker(*arguments):
    subprocess.run(["docker", *arguments], check=True, capture_output=True)


def attendre(condition, delai=60):
    fin = time.time() + delai
    while time.time() < fin:
        try:
            if condition():
                return True
        except PyMongoError:
            pass
        time.sleep(1)
    return False


def importer_dans(client, base):
    """Répartit puis remplit la collection base.books (clé de répartition : _id haché)."""
    client.drop_database(base)
    client.admin.command("shardCollection", f"{base}.books", key={"_id": "hashed"})
    restauration.activer_historique(client, base, "books")
    client[base].books.insert_many(LIVRES)


def compte_sur_chaque_serveur(groupe, base):
    """{nom du serveur: nombre de livres} pour les 4 serveurs d'un shard (connexion directe)."""
    resultat = {}
    for m in groupe["membres"]:
        with MongoClient("127.0.0.1", m["port_hote"], directConnection=True) as c:
            resultat[m["nom"]] = c[base].books.count_documents({})
    return resultat


class TestCluster(unittest.TestCase):
    BASE = "test_cluster"

    @classmethod
    def setUpClass(cls):
        cls.client = client_classique()
        importer_dans(cls.client, cls.BASE)
        time.sleep(3)    # laisse les copies se faire

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(cls.BASE)
        cls.client.close()

    # --- connexion comme le test de l'enseignant -------------------------
    def test_connexion_classique_sur_le_port_27017(self):
        self.assertEqual(self.client.admin.command("ping")["ok"], 1)

    def test_on_parle_a_un_routeur(self):
        self.assertEqual(self.client.admin.command("hello").get("msg"), "isdbgrid")   # "isdbgrid" = mongos

    def test_les_trois_shards_sont_declares(self):
        shards = {s["_id"] for s in self.client.admin.command("listShards")["shards"]}
        self.assertEqual(shards, {g["nom"] for g in lancer.SHARDS})

    def test_les_deux_routeurs_repondent_et_voient_les_shards(self):
        for nom, port in lancer.ROUTEURS:
            shards = lancer.executer(port, "listShards")["shards"]
            self.assertEqual(len(shards), 3, nom)

    # --- éclatement ------------------------------------------------------
    def test_tous_les_livres_sont_la(self):
        self.assertEqual(self.client[self.BASE].books.count_documents({}), len(LIVRES))

    def test_les_livres_sont_repartis_sur_les_3_shards(self):
        par_shard = {}
        for g in lancer.SHARDS:
            chef = next(n for n, r in lancer.etat_groupe(g).items() if r == "PRIMARY")
            par_shard[g["nom"]] = compte_sur_chaque_serveur(g, self.BASE)[chef]
        self.assertEqual(sum(par_shard.values()), len(LIVRES), par_shard)
        for nom, nombre in par_shard.items():
            self.assertGreater(nombre, 0, f"{nom} est vide : {par_shard}")
            self.assertLess(nombre, len(LIVRES), f"{nom} contient tout : {par_shard}")

    def test_collstats_donne_une_ligne_par_shard(self):
        stats = list(self.client[self.BASE].books.aggregate([{"$collStats": {"storageStats": {}}}]))
        self.assertEqual({s["shard"] for s in stats}, {g["nom"] for g in lancer.SHARDS})

    def test_une_requete_filtree_traverse_les_shards(self):
        attendu = sum(1 for livre in LIVRES if livre.get("pageCount", 0) >= 500)
        self.assertEqual(self.client[self.BASE].books.count_documents({"pageCount": {"$gte": 500}}), attendu)

    def test_une_lecture_complete_en_plusieurs_lots(self):
        self.assertEqual(len(list(self.client[self.BASE].books.find(batch_size=20))), len(LIVRES))

    # --- réplication -----------------------------------------------------
    def test_chaque_shard_a_la_meme_chose_sur_ses_4_serveurs(self):
        for g in lancer.SHARDS:
            comptes = compte_sur_chaque_serveur(g, self.BASE)
            self.assertEqual(len(set(comptes.values())), 1, f"{g['nom']} : {comptes}")


class TestHistorique(unittest.TestCase):
    BASE = "test_historique"

    @classmethod
    def setUpClass(cls):
        cls.client = client_classique()
        importer_dans(cls.client, cls.BASE)
        restauration.attendre_journal(restauration.repere(cls.client))

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(cls.BASE)
        cls.client.close()

    def test_les_modifications_sont_enregistrees_puis_annulees(self):
        col = self.client[self.BASE].books
        origine = {d["_id"]: d for d in col.find()}
        instant = restauration.repere(self.client)
        ids = list(origine)

        col.update_many({"_id": {"$in": ids[:3]}}, {"$set": {"pageCount": 9999}})
        col.replace_one({"_id": ids[3]}, {"title": "remplacé"})
        col.delete_many({"_id": {"$in": ids[4:6]}})
        col.insert_one({"_id": "livre-ajoute", "title": "ajouté"})
        self.assertEqual(col.count_documents({}), len(origine) - 1)

        self.assertTrue(restauration.attendre_journal(restauration.repere(self.client)), "le journal n'est pas à jour")
        types = [e["operation"] for e in restauration.journal().find(
            {"base": self.BASE, "ts": {"$gt": instant}})]
        self.assertEqual(sorted(types), sorted(["update"] * 3 + ["replace"] + ["delete"] * 2 + ["insert"]))

        restauration.restaurer(self.BASE, "books", instant, self.client)
        self.assertEqual({d["_id"]: d for d in col.find()}, origine)


@unittest.skipIf(os.environ.get("RAPIDE"), "tests de pannes ignorés (RAPIDE=1)")
class TestPannes(unittest.TestCase):
    BASE = "test_pannes"

    @classmethod
    def setUpClass(cls):
        cls.client = client_classique()
        importer_dans(cls.client, cls.BASE)

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(cls.BASE)
        cls.client.close()

    def test_le_cluster_reste_utilisable_quand_un_routeur_tombe(self):
        docker("stop", "routeur_1")
        try:
            # le proxy met quelques secondes à constater la panne puis n'utilise plus que routeur_2
            self.assertTrue(attendre(lambda: self.client[self.BASE].books.count_documents({}) == len(LIVRES), 30))
        finally:
            docker("start", "routeur_1")
            attendre(lambda: lancer.executer(lancer.ROUTEURS[0][1], "ping")["ok"] == 1, 60)

    def test_le_cluster_reste_utilisable_quand_un_chef_de_shard_tombe(self):
        groupe = lancer.SHARD_A
        chef = next(n for n, r in lancer.etat_groupe(groupe).items() if r == "PRIMARY")
        docker("stop", chef)
        try:
            # un secondaire votant est élu en une dizaine de secondes ; on attend de pouvoir écrire partout
            def ecriture_possible():
                self.client[self.BASE].essai.insert_many([{"n": i} for i in range(60)])
                return True
            self.assertTrue(attendre(ecriture_possible, 60))
            self.assertEqual(self.client[self.BASE].books.count_documents({}), len(LIVRES))
        finally:
            docker("start", chef)
            attendre(lambda: lancer.etat_groupe(groupe).get(chef) in ("PRIMARY", "SECONDARY"), 90)


if __name__ == "__main__":
    unittest.main()
