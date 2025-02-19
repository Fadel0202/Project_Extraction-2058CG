import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path
from extraction.preprocessing import cleaned_file

class TestCleanedFile(unittest.TestCase):
    def setUp(self):
        """Configuration initiale pour les tests"""
        # Création des données de test
        self.test_data = pd.DataFrame({
            'dateEnregistrement': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
            'id_depot': ['ID1', 'ID2', 'ID3', 'ID4'],
            'nombre_filiales': [1, 2, 1, 2],
            'nombre_filiales_renseignees': [1, 2, 1, 2],
            'filiale_index': [0, 1, 0, 1],
            'siren': ['123456789', '123456789', '987654321', '987654321'],
            'nom': ['Entreprise A', 'Entreprise A', 'Entreprise B', 'Entreprise B']
        })
        
        # Créer un répertoire temporaire pour les tests
        self.test_dir = Path('test_directory')
        self.test_dir.mkdir(exist_ok=True)

    def tearDown(self):
        """Nettoyage après les tests"""
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_empty_directory(self):
        """Test avec un répertoire vide"""
        result = cleaned_file(self.test_dir)
        self.assertIsNone(result)

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_file_cleaning(self, mock_to_parquet, mock_read_parquet):
        """Test du nettoyage d'un fichier avec doublons"""
        # Configuration du mock pour simuler la lecture du fichier
        mock_read_parquet.return_value = self.test_data
        
        # Créer un fichier de test
        test_file = self.test_dir / 'test.parquet'
        test_file.touch()
        
        # Exécuter la fonction
        cleaned_file(self.test_dir)
        
        # Vérifications
        mock_read_parquet.assert_called_once()
        mock_to_parquet.assert_called_once()
        
        # Récupérer le DataFrame nettoyé
        cleaned_df = mock_to_parquet.call_args[0][0]
        
        # Vérifier que les doublons ont été supprimés
        self.assertEqual(len(cleaned_df), 2)
        
    def test_date_conversion(self):
        """Test de la conversion des dates"""
        # Création d'un DataFrame avec des dates
        df = pd.DataFrame({
            'dateEnregistrement': ['2023-01-01T12:00:00', '2023-01-02T12:00:00'],
            'siren': ['123456789', '987654321']
        })
        
        with patch('pandas.read_parquet', return_value=df):
            test_file = self.test_dir / 'test.parquet'
            test_file.touch()
            cleaned_file(self.test_dir)
            
            # Vérifier que les dates ont été converties en datetime
            self.assertTrue(pd.api.types.is_datetime64_dtype(df['dateEnregistrement']))

    def test_error_handling(self):
        """Test de la gestion des erreurs"""
        # Simuler une erreur lors de la lecture du fichier
        with patch('pandas.read_parquet', side_effect=Exception('Test error')):
            test_file = self.test_dir / 'test.parquet'
            test_file.touch()
            
            # Vérifier que l'erreur est attrapée et que le programme continue
            try:
                cleaned_file(self.test_dir)
            except Exception:
                self.fail("Une exception non gérée a été levée")

    def test_duplicate_removal_logic(self):
        """Test de la logique de suppression des doublons"""
        # Création d'un DataFrame avec des doublons complexes
        data = pd.DataFrame({
            'dateEnregistrement': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'id_depot': ['ID1', 'ID2', 'ID3'],
            'nombre_filiales': [1, 1, 2],
            'siren': ['123456789', '123456789', '987654321'],
            'nom': ['Entreprise A', 'Entreprise A', 'Entreprise B']
        })
        
        with patch('pandas.read_parquet', return_value=data):
            test_file = self.test_dir / 'test.parquet'
            test_file.touch()
            cleaned_file(self.test_dir)
            
            # Vérifier que seule la version la plus récente est conservée
            cleaned_df = pd.read_parquet(test_file)
            self.assertEqual(len(cleaned_df[cleaned_df['siren'] == '123456789']), 1)
            self.assertEqual(cleaned_df.iloc[0]['dateEnregistrement'], '2023-01-02')

if __name__ == '__main__':
    unittest.main(verbosity=2)