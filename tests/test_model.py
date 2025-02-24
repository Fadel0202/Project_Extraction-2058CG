import unittest
import pandas as pd
import numpy as np
import re
from unittest.mock import patch, MagicMock
from extraction.model import SIRENAnomalyDetector, LuhnValidator
from pathlib import Path

class TestSIRENAnomalyDetector(unittest.TestCase):
    def setUp(self):
        """Configuration initiale pour les tests"""
        self.detector = SIRENAnomalyDetector()
        # Ensure sample_data has all required columns
        self.sample_data = pd.DataFrame({
            'siren_societe': ['55210055400123', '123456789', '732829320'],
            'denomination_societe': ['Test Corp', 'SIREN:356000000', 'Test Inc'],
            'complement_denomination': ['SIRET: 55210055400123', '', ''],
            'forme_juridique': ['SA', 'SARL', 'SAS']
        })

    def test_init(self):
        """Test de l'initialisation du détecteur"""
        self.assertIsNotNone(self.detector.isolation_forest)
        self.assertIsNotNone(self.detector.luhn_validator)
        self.assertEqual(self.detector.isolation_forest.contamination, 0.1)

    def test_extract_potential_siren_valid(self):
        """Test de l'extraction de SIREN valides"""
        test_cases = [
            ('SIRET: 55210055400123', '552100554'),
            ('SIREN: 732829320', '732829320'),
            ('Numéro: 732 829 320', '732829320'),
            ('732829320', '732829320'),
            ('SAS 732829320', '732829320')
        ]
        
        for input_text, expected in test_cases:
            with self.subTest(input_text=input_text):
                result = self.detector._extract_potential_siren(input_text)
                self.assertEqual(result, expected)

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
        sample_data = pd.DataFrame({
            'siren_societe': ['55210055400123', '123456789', '732829320'],
            'denomination_societe': ['Test Corp', 'SIREN:356000000', 'Test Inc'],
            'complement_denomination': ['SIRET: 55210055400123', '', ''],
            'forme_juridique': ['SA', 'SARL', 'SAS']
        })
        
        result_df = self.detector.fit_predict(sample_data)
        
        # Test correction depuis SIRET
        siret_row = result_df[result_df['siren_societe'] == '55210055400123'].iloc[0]
        self.assertEqual(siret_row['siren_corrige'], '552100554')
        self.assertEqual(siret_row['source_correction'], 'CORRECTION_SIRET')

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

    @patch('pathlib.Path.glob')
    @patch('pandas.read_parquet')
    def test_process_directory(self, mock_read_parquet, mock_glob):
        """Test du traitement d'un répertoire"""
        # Configure mock to return list of Path objects
        mock_glob.return_value = [Path('file1.parquet'), Path('file2.parquet')]
        
        # Configure mock_read_parquet to return our sample data
        mock_read_parquet.return_value = self.sample_data.copy()
        
        # Import here to avoid circular imports
        from extraction.data_process import process_directory
        
        # Test the process_directory function
        results = process_directory(
            input_dir="dummy_dir",
            output_dir="dummy_output",
            tracker_file="dummy_tracker.parquet"
        )
        
        # Verifications
        self.assertIsInstance(results, pd.DataFrame)
        self.assertTrue(len(results) > 0, "Results DataFrame should not be empty")
        self.assertTrue('is_anomaly' in results.columns)
        self.assertTrue('siren_corrige' in results.columns)
        self.assertTrue('source_correction' in results.columns)
        
        # Verify that mock_read_parquet was called for each file
        self.assertEqual(mock_read_parquet.call_count, 2)
        
        # Verify SIRET correction
        siret_rows = results[results['siren_societe'] == '55210055400123']
        self.assertFalse(siret_rows.empty, "No rows found with SIRET")
        self.assertEqual(siret_rows.iloc[0]['siren_corrige'], '552100554')

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
            pd.Series(['732829320'])
        ]
        
        for invalid_input in invalid_inputs:
            with self.subTest(input=invalid_input):
                with self.assertRaises((TypeError, ValueError)):
                    self.detector.fit_predict(invalid_input)

if __name__ == '__main__':
    unittest.main()

def cleaned_file(directory):
    """
    Nettoie tous les fichiers parquet d'un répertoire en supprimant les doublons.
    Un doublon est identifié quand deux lignes sont identiques sur toutes leurs colonnes
    ou diffèrent uniquement par leur dateEnregistrement, id_depot, nombre_filiales, 
    nombre_filiales_renseignees et filiale_index.
    
    Args:
        directory (str): Chemin du répertoire contenant les fichiers parquet
    """
    print("Début du nettoyage des fichiers...")
    
    # Colonnes à exclure de la comparaison des doublons
    columns_to_exclude = [
        'dateEnregistrement',
        'id_depot',
        'nombre_filiales',
        'nombre_filiales_renseignees',
        'filiale_index'
    ]
    
    directory = Path(directory)
    
    for file in directory.glob('*.parquet'):
        print(f"\nTraitement du fichier : {file.name}")
        
        try:
            # Lit le fichier parquet
            df = pd.read_parquet(file)
            
            if df.empty:
                print("Fichier vide, passage au suivant.")
                continue
            
            nb_lignes_avant = len(df)
            
            # Conversion de dateEnregistrement en datetime
            df['dateEnregistrement'] = pd.to_datetime(df['dateEnregistrement'], errors='coerce')
            
            # Crée la liste des colonnes à comparer en excluant les colonnes spécifiées
            cols_to_compare = [col for col in df.columns if col not in columns_to_exclude]
            
            # Trie par dateEnregistrement décroissant pour garder la version la plus récente
            df = df.sort_values('dateEnregistrement', ascending=False)
            
            # Identifie les doublons
            mask = df.duplicated(subset=cols_to_compare, keep='first')
            
            # Garde uniquement les lignes non dupliquées
            df_cleaned = df[~mask]
            
            nb_lignes_apres = len(df_cleaned)
            nb_doublons = nb_lignes_avant - nb_lignes_apres
            
            # Sauvegarde le fichier nettoyé
            df_cleaned.to_parquet(file, index=False)
            
            print(f"Nettoyage terminé pour {file.name}:")
            print(f"- Lignes avant: {nb_lignes_avant}")
            print(f"- Lignes après: {nb_lignes_apres}")
            print(f"- Doublons supprimés: {nb_doublons}")
            
        except Exception as e:
            print(f"Erreur lors du traitement de {file.name}: {str(e)}")
            continue

    print("\nNettoyage terminé pour tous les fichiers.")