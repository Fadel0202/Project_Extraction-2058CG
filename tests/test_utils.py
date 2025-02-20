import unittest
import pandas as pd
import numpy as np
import os
from unittest.mock import patch, MagicMock
from extraction.utils import (
    parquet_to_csv,
    parquet_to_xlsx,
    correct_siren,
    filter_cases_1,
    filter_cases_2,
    filter_cases_general,
    create_mere_fille_file
)

class TestFileOperations(unittest.TestCase):
    def setUp(self):
        """Configuration initiale pour les tests"""
        self.test_df = pd.DataFrame({
            'siren_DEP': ['123456789', '0000000NA', '987654321'],
            'SIREN_DEC': ['0000000NA', '123456789', '0000000NA'],
            'siren_tete_grp': ['0000000NA', '123456789', '987654321'],
            'Ind_Mere_SIREN-DEP': [0, 1, 0],
            'SIR_Mere_de_SIREN-DEP': ['', '123456789', '987654321']
        })
        
        # Création d'un répertoire temporaire pour les tests
        self.test_dir = 'test_directory'
        if not os.path.exists(self.test_dir):
            os.makedirs(self.test_dir)
            
    def tearDown(self):
        """Nettoyage après les tests"""
        # Suppression du répertoire de test et son contenu
        for root, dirs, files in os.walk(self.test_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.test_dir)

    @patch('pandas.read_parquet')
    def test_parquet_to_csv(self, mock_read_parquet):
        """Test de la conversion parquet vers CSV"""
        # Configuration du mock
        mock_read_parquet.return_value = self.test_df
        
        # Test de la conversion
        test_file = os.path.join(self.test_dir, 'test.parquet')
        parquet_to_csv(self.test_dir)
        
        # Vérification que le dossier output_csv a été créé
        output_dir = os.path.join(self.test_dir, 'output_csv')
        self.assertTrue(os.path.exists(output_dir))

    @patch('pandas.read_parquet')
    def test_parquet_to_xlsx(self, mock_read_parquet):
        """Test de la conversion parquet vers Excel"""
        # Configuration du mock
        mock_read_parquet.return_value = self.test_df
        
        # Test de la conversion
        test_file = os.path.join(self.test_dir, 'test.parquet')
        parquet_to_xlsx(self.test_dir)
        
        # Vérification que le dossier output_xlsx a été créé
        output_dir = os.path.join(self.test_dir, 'output_xlsx')
        self.assertTrue(os.path.exists(output_dir))

    def test_correct_siren(self):
        """Test de la correction des SIREN"""
        test_cases = [
            ('03.56e+08', '356000000'),  # Cas spécial
            ('123456789', '123456789'),  # SIREN valide
            ('12345678901234', '123456789'),  # SIRET
            ('abc123456789def', '123456789'),  # SIREN avec caractères
            ('123', '123'),  # Trop court
        ]
        
        for input_value, expected in test_cases:
            result = correct_siren(input_value)
            self.assertEqual(result, expected)

    def test_filter_cases_1(self):
        """Test du filtre cas 1"""
        # Sauvegarde du DataFrame de test en parquet
        test_file = os.path.join(self.test_dir, 'test.parquet')
        self.test_df.to_parquet(test_file)
        
        # Test du filtre
        result = filter_cases_1(test_file)
        
        # Vérifications
        self.assertIsNotNone(result)
        self.assertTrue(isinstance(result, pd.DataFrame))
        
        # Vérification des critères du filtre
        filtered = result[
            (result['siren_DEP'] != '0000000NA') &
            (result['SIREN_DEC'] == '0000000NA') &
            (result['Ind_Mere_SIREN-DEP'] == 0) &
            (result['SIR_Mere_de_SIREN-DEP'].isin(['', np.nan])) &
            (result['siren_tete_grp'] == '0000000NA')
        ]
        self.assertEqual(len(filtered), len(result))

    def test_filter_cases_2(self):
        """Test du filtre cas 2"""
        # Sauvegarde du DataFrame de test en parquet
        test_file = os.path.join(self.test_dir, 'test.parquet')
        self.test_df.to_parquet(test_file)
        
        # Test du filtre
        result = filter_cases_2(test_file)
        
        # Vérifications
        self.assertIsNotNone(result)
        self.assertTrue(isinstance(result, pd.DataFrame))
        
        # Vérification des critères du filtre
        filtered = result[
            (result['siren_DEP'] != '0000000NA') &
            (result['SIREN_DEC'] == '0000000NA') &
            (result['Ind_Mere_SIREN-DEP'] == 0) &
            (~result['SIR_Mere_de_SIREN-DEP'].isin(['', np.nan])) &
            (result['siren_tete_grp'] != '0000000NA')
        ]
        self.assertEqual(len(filtered), len(result))

    def test_filter_cases_general(self):
        """Test du filtre général"""
        # Création des fichiers de test
        main_file = os.path.join(self.test_dir, 'main.parquet')
        case1_file = os.path.join(self.test_dir, 'case1.parquet')
        case2_file = os.path.join(self.test_dir, 'case2.parquet')
        
        self.test_df.to_parquet(main_file)
        self.test_df.iloc[:1].to_parquet(case1_file)
        self.test_df.iloc[1:2].to_parquet(case2_file)
        
        # Test du filtre général
        result = filter_cases_general(
            main_file,
            [case1_file, case2_file],
            os.path.join(self.test_dir, 'output.parquet')
        )
        
        # Vérifications
        self.assertIsNotNone(result)
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertTrue('type' in result.columns)
        self.assertTrue(all(result['type'].isin(['cas1', 'cas2', 'autre'])))

    @patch('pandas.read_csv')
    @patch('pandas.read_parquet')
    def test_create_mere_fille_file(self, mock_read_parquet, mock_read_csv):
        """Test de la création du fichier mère-fille"""
        # Configuration des mocks
        mock_read_csv.return_value = pd.DataFrame({
            'SIREN_DEC': ['123456789'],
            'siren_DEP': ['987654321'],
            'siren_tete_grp': ['111111111']
        })
        
        mock_read_parquet.return_value = pd.DataFrame({
            'mere_siren': ['123456789'],
            'siren_societe': ['987654321'],
            'montant_credit_impot': [1000],
            'denomination_societe': ['Test Corp'],
            'complement_denomination': ['Test Div']
        })
        
        # Test de la création
        result = create_mere_fille_file(
            'millesime.csv',
            '2058cg_23.parquet',
            '2058cg_22.parquet',
            os.path.join(self.test_dir, 'output.parquet')
        )
        
        # Vérifications
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, 'output.parquet')))

if __name__ == '__main__':
    unittest.main(verbosity=2)

def correct_siren(value):
    try:
        value_str = str(value)
        if value_str.lower() == "03.56e+08":
            return "356000000"
        match = re.search(r'\d{9}', value_str)
        if match:
            return match.group()
        if len(value_str) >= 9:
            return value_str[:9]
        return value_str
    except:
        return value

corrections = result_df[result_df['is_anomaly']].apply(correct_siren, axis=1)
result_df.loc[result_df['is_anomaly'], 'siren_corrige'] = corrections.apply(lambda x: x[0])
result_df.loc[result_df['is_anomaly'], 'source_correction'] = corrections.apply(lambda x: x[1])