import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path
from extraction.preprocessing import cleaned_file

class TestCleanedFile(unittest.TestCase):
    def setUp(self):
        """Configuration initiale pour les tests"""
        self.test_dir = Path('test_directory')
        self.test_dir.mkdir(exist_ok=True)
        
        # Données de test avec des doublons intentionnels
        self.test_data = pd.DataFrame({
            'dateEnregistrement': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'id_depot': ['ID1', 'ID2', 'ID1'],
            'siren': ['123456789', '987654321', '123456789'],
            'nom': ['Entreprise A', 'Entreprise B', 'Entreprise A']
        })

    def tearDown(self):
        """Nettoyage après les tests"""
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_duplicate_removal_logic(self):
        """Test de la logique de suppression des doublons"""
        # Données de test simplifiées avec un doublon exact
        test_data = pd.DataFrame({
            'dateEnregistrement': ['2023-01-01', '2023-01-02', '2023-01-01'],
            'id_depot': ['ID1', 'ID2', 'ID1'],
            'siren': ['123456789', '987654321', '123456789'],  # Doublon intentionnel
            'nom': ['Entreprise A', 'Entreprise B', 'Entreprise A']  # Doublon intentionnel
        })

        with patch('pathlib.Path.glob') as mock_glob, \
             patch('pandas.read_parquet', return_value=test_data), \
             patch('pandas.DataFrame.to_parquet') as mock_to_parquet:
            
            mock_glob.return_value = [Path('test.parquet')]
            result_df = cleaned_file(self.test_dir)
            
            # Vérifications
            self.assertIsInstance(result_df, pd.DataFrame, "Le résultat devrait être un DataFrame")
            self.assertEqual(len(result_df), 2, "Le DataFrame nettoyé devrait avoir exactement 2 lignes")
            
            # Vérification des SIREN uniques
            siren_counts = result_df['siren'].value_counts()
            self.assertEqual(siren_counts['123456789'], 1, "Il devrait y avoir exactement une ligne avec le SIREN 123456789")
            self.assertEqual(siren_counts['987654321'], 1, "Il devrait y avoir exactement une ligne avec le SIREN 987654321")

    def test_file_cleaning(self):
        """Test du nettoyage complet des fichiers"""
        test_data = pd.DataFrame({
            'dateEnregistrement': ['2023-01-01', '2023-01-02', '2023-01-01'],
            'id_depot': ['ID1', 'ID2', 'ID1'],
            'siren': ['123456789', '987654321', '123456789'],
            'nom': ['Test A', 'Test B', 'Test A']
        })

        with patch('pathlib.Path.glob') as mock_glob, \
             patch('pandas.read_parquet', return_value=test_data), \
             patch('pandas.DataFrame.to_parquet') as mock_to_parquet:
            
            mock_glob.return_value = [Path('test.parquet')]
            result_df = cleaned_file(self.test_dir)
            
            # Vérifications
            self.assertEqual(len(result_df), 2, "Le DataFrame nettoyé devrait avoir exactement 2 lignes")

if __name__ == '__main__':
    unittest.main()