import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from extraction.model import SIRENAnomalyDetector, LuhnValidator

class TestSIRENAnomalyDetector(unittest.TestCase):
    def setUp(self):
        """Initialisation des objets nécessaires pour les tests"""
        self.detector = SIRENAnomalyDetector(contamination=0.1)
        self.sample_data = pd.DataFrame({
            'siren_societe': ['732829320', '123456789', '552100554', '12345'],
            'denomination_societe': ['Entreprise A', 'Entreprise B SIREN:356000000', 'Entreprise C', 'Entreprise D'],
            'complement_denomination': ['', 'Info 123456789', 'SIRET: 55210055400123', ''],
            'forme_juridique': ['SAS', 'SARL', 'SA', 'SAS 732829320']
        })

    def test_init(self):
        """Test de l'initialisation du détecteur"""
        self.assertIsNotNone(self.detector.isolation_forest)
        self.assertIsNotNone(self.detector.luhn_validator)
        self.assertEqual(self.detector.isolation_forest.contamination, 0.1)

    def test_extract_potential_siren_valid(self):
        """Test de l'extraction d'un SIREN valide de différents formats"""
        test_cases = [
            ('732829320', '732829320'),  # SIREN simple
            ('SIRET: 73282932000123', '732829320'),  # SIREN depuis SIRET
            ('Numéro: 732 829 320', '732829320'),  # SIREN avec espaces
            ('Text 732829320 more text', '732829320'),  # SIREN dans du texte
        ]
        
        for input_text, expected in test_cases:
            result = self.detector._extract_potential_siren(input_text)
            self.assertEqual(result, expected, f"Failed for input: {input_text}")

    def test_extract_potential_siren_invalid(self):
        """Test de l'extraction avec des entrées invalides"""
        invalid_cases = [
            None,  # None
            '',   # Chaîne vide
            '123',  # Trop court
            '1234567890',  # Trop long
            'ABC123456',  # Contient des lettres
        ]
        
        for invalid_input in invalid_cases:
            result = self.detector._extract_potential_siren(invalid_input)
            self.assertIsNone(result, f"Should return None for invalid input: {invalid_input}")

    def test_fit_predict_basic(self):
        """Test basique de fit_predict"""
        result_df = self.detector.fit_predict(self.sample_data)
        
        # Vérification des colonnes attendues
        expected_columns = {'siren_societe', 'is_anomaly', 'siren_corrige', 'source_correction'}
        self.assertTrue(all(col in result_df.columns for col in expected_columns))
        
        # Vérification que les anomalies sont bien détectées
        self.assertTrue(result_df.loc[result_df['siren_societe'] == '123456789', 'is_anomaly'].iloc[0])
        self.assertFalse(result_df.loc[result_df['siren_societe'] == '732829320', 'is_anomaly'].iloc[0])

    def test_fit_predict_corrections(self):
        """Test des corrections proposées par fit_predict"""
        result_df = self.detector.fit_predict(self.sample_data)
        
        # Vérification des corrections pour les anomalies
        anomalies = result_df[result_df['is_anomaly']]
        
        # Test correction depuis SIRET
        siret_row = result_df[result_df['complement_denomination'].str.contains('SIRET:', na=False)].iloc[0]
        self.assertEqual(siret_row['siren_corrige'], '552100554')
        self.assertEqual(siret_row['source_correction'], 'CORRECTION_COMPLEMENT')
        
        # Test correction depuis forme juridique
        forme_row = result_df[result_df['forme_juridique'].str.contains('732829320', na=False)].iloc[0]
        self.assertEqual(forme_row['siren_corrige'], '732829320')
        self.assertEqual(forme_row['source_correction'], 'CORRECTION_FORME')

    @patch('os.listdir')
    @patch('pandas.read_parquet')
    def test_process_directory(self, mock_read_parquet, mock_listdir):
        """Test du traitement d'un répertoire complet"""
        # Configuration des mocks
        mock_listdir.return_value = ['file1.parquet', 'file2.parquet']
        mock_read_parquet.return_value = self.sample_data
        
        # Test du processus complet
        from extraction.data_process import process_directory
        results = process_directory("dummy_dir", "dummy_output_dir", "dummy_tracker_file", contamination=0.1)
        
        # Vérifications
        self.assertIsInstance(results, pd.DataFrame)
        self.assertTrue(len(results) > 0)
        mock_listdir.assert_called_once()
        self.assertEqual(mock_read_parquet.call_count, 2)

    def test_edge_cases(self):
        """Test des cas limites"""
        # DataFrame vide
        empty_df = pd.DataFrame(columns=self.sample_data.columns)
        result_empty = self.detector.fit_predict(empty_df)
        self.assertTrue(result_empty.empty)
        self.assertTrue(all(col in result_empty.columns for col in ['is_anomaly', 'siren_corrige', 'source_correction']))
        
        # DataFrame avec valeurs manquantes
        df_with_na = self.sample_data.copy()
        df_with_na.loc[0, 'siren_societe'] = None
        result_na = self.detector.fit_predict(df_with_na)
        self.assertTrue(result_na.loc[0, 'is_anomaly'])

    def test_invalid_input_handling(self):
        """Test de la gestion des entrées invalides"""
        invalid_inputs = [
            None,
            "not a dataframe",
            pd.Series(['732829320']),
        ]
        
        for invalid_input in invalid_inputs:
            with self.assertRaises((TypeError, ValueError)):
                self.detector.fit_predict(invalid_input)

if __name__ == '__main__':
    unittest.main()