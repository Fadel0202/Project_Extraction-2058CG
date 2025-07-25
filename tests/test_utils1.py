import os
import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch
from extraction.utils import (
    filter_cases_3,
    filter_cases_4,
    filter_cases_5,
    filter_cases_6,
    filter_cases_7,
    filter_cases_8,
    filter_cases_9,
    filter_cases_10,
    filter_cases_11,
    filter_cases_12,
    filter_cases_13,
    filter_cases_14,
    filter_cases_15,
    filter_cases_16
)

class TestFilterCases(unittest.TestCase):
    def setUp(self):
        """Configuration initiale pour les tests"""
        self.test_data = pd.DataFrame({
            'siren_DEP': ['123456789', '0000000NA', '987654321', '111111111'],
            'SIREN_DEC': ['123456789', '0000000NA', '987654321', '111111111'],
            'siren_tete_grp': ['123456789', '0000000NA', '987654321', '00000000.'],
            'Ind_Mere_SIREN-DEP': [1, 0, 0, 1],
            'SIR_Mere_de_SIREN-DEP': ['', '123456789', '', ''],
            'Ind_Mere_SIREN-DEC': [1, 0, 0, 1],
            'SIR_Mere_de_SIREN-DEC': ['', '123456789', '', ''],
            'Ind_Mere_SIREN-Tete_de_Grp': [1, 0, 0, 1],
            'SIR_Mere_de_SIREN-Tete_de_Grp': ['', '123456789', '', ''],
            'type': ['autre', 'cas1', 'cas2', 'autre']
        })
        
        self.test_file = 'test.parquet'
        self.test_data.to_parquet(self.test_file)
        self.result = None  # Déclaration globale de la variable result

    def tearDown(self):
        """Nettoyage après les tests"""
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def test_filter_cases_3(self):
        """Test du filtre cas 3"""
        self.result = filter_cases_3(self.test_file)
        self.assertIsNotNone(self.result)
        self.assertTrue(isinstance(self.result, pd.DataFrame))
        
        # Vérification des critères du filtre
        filtered = self.result[
            (self.result['siren_DEP'] != '0000000NA') &
            (self.result['SIREN_DEC'] == '0000000NA') &
            (self.result['Ind_Mere_SIREN-DEP'] == 0) &
            (~self.result['SIR_Mere_de_SIREN-DEP'].isin(['', np.nan])) &
            (self.result['siren_tete_grp'] == '0000000NA')
        ]
        self.assertEqual(len(filtered), len(self.result))

    def test_filter_cases_4(self):
        """Test du filtre cas 4"""
        self.result = filter_cases_4(self.test_file)
        self.assertIsNotNone(self.result)
        
        filtered = self.result[
            (self.result['siren_DEP'] != '0000000NA') &
            (self.result['SIREN_DEC'] == '0000000NA') &
            (self.result['Ind_Mere_SIREN-DEP'] == 1)
        ]
        self.assertEqual(len(filtered), len(self.result))

    def test_filter_cases_5(self):
        """Test du filtre cas 5"""
        self.result = filter_cases_5(self.test_file)
        self.assertIsNotNone(self.result)
        
        filtered = self.result[
            (self.result['type'] == 'autre') &
            (self.result['SIREN_DEC'] == '0000000NA') &
            (self.result['Ind_Mere_SIREN-Tete_de_Grp'] == 1)
        ]
        self.assertEqual(len(filtered), len(self.result))

    def test_filter_cases_6(self):
        """Test du filtre cas 6"""
        self.result = filter_cases_6(self.test_file)
        self.assertIsNotNone(self.result)
        
        filtered = self.result[
            (self.result['type'] == 'autre') &
            (self.result['SIREN_DEC'] == '0000000NA') &
            (self.result['Ind_Mere_SIREN-Tete_de_Grp'] == 0) &
            (~self.result['SIR_Mere_de_SIREN-Tete_de_Grp'].isin(['', np.nan]))
        ]
        self.assertEqual(len(filtered), len(self.result))

    def test_filter_cases_7_through_16(self):
        """Tests des filtres cas 7 à 16"""
        test_functions = [
            (filter_cases_7, [
                (lambda result: result['type'] == 'autre'),
                (lambda result: result['SIREN_DEC'] == '0000000NA'),
                (lambda result: ~result['siren_DEP'].isin(['', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-DEC'] == 0),
                (lambda result: result['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-Tete_de_Grp'] == 0),
                (lambda result: result['SIR_Mere_de_SIREN-Tete_de_Grp'].isin(['', np.nan]))
            ]),
            (filter_cases_8, [
                (lambda result: result['type'] == 'autre'),
                (lambda result: result['SIREN_DEC'] != '0000000NA'),
                (lambda result: result['SIREN_DEC'] == result['siren_DEP']),
                (lambda result: result['siren_tete_grp'].isin(['00000000.', '0000000NA'])),
                (lambda result: result['Ind_Mere_SIREN-DEP'] == 0),
                (lambda result: result['SIR_Mere_de_SIREN-DEP'].isin(['', np.nan]))
            ]),
            (filter_cases_9, [
                (lambda result: result['type'] == 'autre'),
                (lambda result: result['SIREN_DEC'] != '0000000NA'),
                (lambda result: result['SIREN_DEC'] == result['siren_DEP']),
                (lambda result: result['siren_tete_grp'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-DEP'] == 1)
            ]),
            (filter_cases_10, [
                (lambda result: result['type'] == 'autre'),
                (lambda result: ~result['SIREN_DEC'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['SIREN_DEC'] == result['siren_DEP']),
                (lambda result: result['siren_tete_grp'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-DEP'] == 1),
                (lambda result: ~result['SIR_Mere_de_SIREN-DEP'].isin(['', np.nan]))
            ]),
            (filter_cases_11, [
                (lambda result: result['type'] == 'autre'),
                (lambda result: ~result['SIREN_DEC'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['SIREN_DEC'] == result['siren_DEP']),
                (lambda result: ~result['siren_tete_grp'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-DEC'] == 0),
                (lambda result: result['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-Tete_de_Grp'] == 1)
            ]),
            (filter_cases_12, [
                (lambda result: result['type'] == 'autre'),
                (lambda result: result['SIREN_DEC'] != '0000000NA'),
                (lambda result: result['SIREN_DEC'] == result['siren_DEP']),
                (lambda result: ~result['siren_tete_grp'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-DEC'] == 0),
                (lambda result: result['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-Tete_de_Grp'] == 0),
                (lambda result: ~result['SIR_Mere_de_SIREN-Tete_de_Grp'].isin(['', np.nan]))
            ]),
            (filter_cases_13, [
                (lambda result: result['type'] == 'autre'),
                (lambda result: result['SIREN_DEC'] != '0000000NA'),
                (lambda result: result['SIREN_DEC'] == result['siren_DEP']),
                (lambda result: ~result['siren_tete_grp'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-DEC'] == 0),
                (lambda result: result['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-Tete_de_Grp'] == 0),
                (lambda result: result['SIR_Mere_de_SIREN-Tete_de_Grp'].isin(['', np.nan])),
                (lambda result: result['siren_tete_grp'] != result['siren_DEP'])
            ]),
            (filter_cases_14, [
                (lambda result: result['type'] == 'autre'),
                (lambda result: result['SIREN_DEC'] != '0000000NA'),
                (lambda result: result['SIREN_DEC'] == result['siren_DEP']),
                (lambda result: ~result['siren_tete_grp'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-DEC'] == 0),
                (lambda result: result['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-Tete_de_Grp'] == 0),
                (lambda result: result['SIR_Mere_de_SIREN-Tete_de_Grp'].isin(['', np.nan])),
                (lambda result: result['siren_tete_grp'] == result['siren_DEP'])
            ]),
            (filter_cases_15, [
                (lambda result: result['type'] == 'autre'),
                (lambda result: ~result['SIREN_DEC'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['SIREN_DEC'] == result['siren_DEP']),
                (lambda result: ~result['siren_tete_grp'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-DEC'] == 0),
                (lambda result: ~result['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan]))
            ]),
            (filter_cases_16, [
                (lambda result: result['type'] == 'autre'),
                (lambda result: ~result['SIREN_DEC'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['SIREN_DEC'] == result['siren_DEP']),
                (lambda result: ~result['siren_tete_grp'].isin(['00000000.', '0000000NA', '', np.nan])),
                (lambda result: result['Ind_Mere_SIREN-DEC'] == 1)
            ])
        ]
        
        for filter_func, conditions in test_functions:
            with self.subTest(filter_func=filter_func.__name__):
                self.result = filter_func(self.test_file)
                self.assertIsNotNone(self.result)
                self.assertTrue(isinstance(self.result, pd.DataFrame))
                
                # Vérification des critères spécifiques à chaque cas
                filtered = self.result
                for condition in conditions:
                    filtered = filtered[condition(filtered)]
                self.assertEqual(len(filtered), len(self.result))

    def test_error_handling(self):
        """Test de la gestion des erreurs"""
        # Test avec un fichier qui n'existe pas
        for filter_func in [filter_cases_3, filter_cases_4, filter_cases_5, 
                          filter_cases_6, filter_cases_7, filter_cases_8, 
                          filter_cases_9, filter_cases_10, filter_cases_11,
                          filter_cases_12, filter_cases_13, filter_cases_14,
                          filter_cases_15, filter_cases_16]:
            with self.subTest(filter_func=filter_func.__name__):
                self.result = filter_func('nonexistent.parquet')
                self.assertIsNone(self.result)

    def test_empty_dataframe(self):
        """Test avec un DataFrame vide"""
        empty_df = pd.DataFrame(columns=self.test_data.columns)
        empty_file = 'empty.parquet'
        empty_df.to_parquet(empty_file)
        
        try:
            for filter_func in [filter_cases_3, filter_cases_4, filter_cases_5,
                              filter_cases_6, filter_cases_7, filter_cases_8,
                              filter_cases_9, filter_cases_10, filter_cases_11,
                              filter_cases_12, filter_cases_13, filter_cases_14,
                              filter_cases_15, filter_cases_16]:
                with self.subTest(filter_func=filter_func.__name__):
                    self.result = filter_func(empty_file)
                    self.assertTrue(self.result.empty)
        finally:
            if os.path.exists(empty_file):
                os.remove(empty_file)

if __name__ == '__main__':
    unittest.main(verbosity=2)