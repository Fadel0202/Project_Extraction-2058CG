import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch
from extraction.utils2 import (
    filter_cases_17,
    filter_cases_18,
    filter_cases_19,
    filter_cases_20,
    filter_cases_21,
    filter_cases_22,
    filter_cases_general_v2
)

class TestAdditionalFilterCases(unittest.TestCase):
    def setUp(self):
        """Configuration initiale pour les tests"""
        self.test_data = pd.DataFrame({
            'siren_DEP': ['123456789', '0000000NA', '987654321', '111111111'],
            'SIREN_DEC': ['234567891', '0000000NA', '987654321', '111111111'],
            'siren_tete_grp': ['123456789', '0000000NA', '987654321', '00000000.'],
            'Ind_Mere_SIREN-DEP': [1, 0, 0, 1],
            'SIR_Mere_de_SIREN-DEP': ['', '123456789', '', ''],
            'Ind_Mere_SIREN-DEC': [1, 0, 0, 1],
            'SIR_Mere_de_SIREN-DEC': ['', '123456789', '', ''],
            'Ind_Mere_SIREN-Tete_de_Grp': [1, 0, 0, 1],
            'SIR_Mere_de_SIREN-Tete_de_Grp': ['', '123456789', '', ''],
            'type': ['autre', 'cas1', 'cas2', 'autre']
        })
        
        self.test_file = 'test_additional.parquet'
        self.test_data.to_parquet(self.test_file)

    def tearDown(self):
        """Nettoyage après les tests"""
        import os
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def test_filter_cases_17(self):
        """Test du filtre cas 17"""
        result = filter_cases_17(self.test_file)
        self.assertIsNotNone(result)
        self.assertTrue(isinstance(result, pd.DataFrame))
        
        # Vérification des critères du filtre
        filtered = result[
            (result['type'] == 'autre') &
            (~result['SIREN_DEC'].isin(['00000000.', '0000000NA', '', np.nan])) &
            (~result['siren_DEP'].isin(['00000000.', '0000000NA', '', np.nan])) &
            (result['SIREN_DEC'] != result['siren_DEP']) &
            (result['siren_tete_grp'] == result['siren_DEP']) &
            (result['Ind_Mere_SIREN-DEP'] == 1)
        ]
        self.assertEqual(len(filtered), len(result))

    def test_filter_cases_18(self):
        """Test du filtre cas 18"""
        result = filter_cases_18(self.test_file)
        self.assertIsNotNone(result)
        
        filtered = result[
            (result['type'] == 'autre') &
            (~result['SIREN_DEC'].isin(['00000000.', '0000000NA', '', np.nan])) &
            (~result['siren_DEP'].isin(['00000000.', '0000000NA', '', np.nan])) &
            (result['SIREN_DEC'] != result['siren_DEP']) &
            (result['siren_tete_grp'] == result['siren_DEP']) &
            (result['Ind_Mere_SIREN-DEP'] == 0) &
            (result['SIR_Mere_de_SIREN-DEP'].isin(['00000000.', '0000000NA', '', np.nan]))
        ]
        self.assertEqual(len(filtered), len(result))

    def test_filter_cases_19_through_22(self):
        """Tests des filtres cas 19 à 22"""
        test_functions = [
            (filter_cases_19, "cas19"),
            (filter_cases_20, "cas20"),
            (filter_cases_21, "cas21"),
            (filter_cases_22, "cas22")
        ]
        
        for filter_func, case_name in test_functions:
            with self.subTest(case=case_name):
                result = filter_func(self.test_file)
                self.assertIsNotNone(result)
                self.assertTrue(isinstance(result, pd.DataFrame))
                # Vérification que tous les filtres s'appliquent correctement
                self.assertTrue(all(result['type'] == 'autre'))

    def test_filter_cases_general_v2(self):
        """Test de filter_cases_general_v2"""
        # Création des fichiers de test
        case_files = []
        for i in range(3):
            case_file = f'case_{i}.parquet'
            self.test_data.iloc[i:i+1].to_parquet(case_file)
            case_files.append(case_file)
            
        result = filter_cases_general_v2(self.test_file, case_files, 'output_test.parquet')
        
        self.assertIsNotNone(result)
        self.assertTrue('type' in result.columns)
        
        # Nettoyage des fichiers de test
        import os
        for file in case_files + ['output_test.parquet']:
            if os.path.exists(file):
                os.remove(file)

if __name__ == '__main__':
    unittest.main(verbosity=2)