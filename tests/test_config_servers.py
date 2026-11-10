"""Tests du groupe de serveurs de configuration (rs_config).

Lancer depuis le dossier du projet :   python -m unittest tests.test_config_servers -v
Prérequis : le cluster est démarré (python lancer.py).
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import lancer  # noqa: E402  (réutilise la description des groupes)

GROUPE = lancer.CONFIG


class TestServeursConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.etat = lancer.etat_groupe(GROUPE)

    def test_tous_les_serveurs_sont_membres(self):
        attendus = {m["nom"] for m in GROUPE["membres"]}
        self.assertEqual(set(self.etat), attendus)

    def test_un_seul_chef(self):
        chefs = [n for n, role in self.etat.items() if role == "PRIMARY"]
        self.assertEqual(len(chefs), 1, f"chefs trouvés : {chefs}")

    def test_les_autres_sont_secondaires(self):
        autres = [role for role in self.etat.values() if role != "PRIMARY"]
        self.assertTrue(all(role == "SECONDARY" for role in autres), autres)

    def test_le_groupe_est_un_groupe_de_configuration(self):
        for m in GROUPE["membres"]:
            conf = lancer.executer(m["port_hote"], "replSetGetConfig")["config"]
            self.assertTrue(conf.get("configsvr"), "l'option configsvr est absente")
            self.assertEqual(conf["_id"], GROUPE["nom"])

    def test_il_y_a_au_moins_trois_membres(self):
        # Règle de la documentation MongoDB : 3 membres en production.
        self.assertGreaterEqual(len(self.etat), 3)


if __name__ == "__main__":
    unittest.main()
