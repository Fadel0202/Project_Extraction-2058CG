import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import os
import re
from typing import Tuple, Optional, Dict, List
from pathlib import Path

class LuhnValidator:
    """
    Validateur de numéros SIREN utilisant l'algorithme de Luhn
    """
    @staticmethod
    def luhn_checksum(number: str) -> bool:
        """
        Implémente l'algorithme de Luhn pour vérifier un numéro SIREN
        
        Args:
            number: Le numéro à vérifier
            
        Returns:
            bool: True si le numéro est valide selon l'algorithme de Luhn
        """
        def digits_of(n: str) -> List[int]:
            return [int(d) for d in str(n)]
            
        digits = digits_of(number)
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        
        # Somme des chiffres de rang impair
        checksum = sum(odd_digits)
        
        # Pour les chiffres de rang pair, on multiplie par 2 et on somme les chiffres
        for d in even_digits:
            doubled = str(d * 2)
            checksum += sum(digits_of(doubled))
        
        return checksum % 10 == 0
class SIRENAnomalyDetector:
    def __init__(self, contamination: float = 0.1):
        self.isolation_forest = IsolationForest(
            contamination=contamination,
            random_state=42
        )
        self.luhn_validator = LuhnValidator()
        
    def _extract_potential_siren(self, text: str) -> Optional[str]:
        """Extrait un potentiel SIREN d'une chaîne de caractères"""
        if pd.isna(text):
            return None
            
        text = str(text).strip()
        # Supprimer les espaces du texte
        text = re.sub(r'\s+', '', text)
        
        # Recherche d'un SIRET (14 chiffres)
        siret_pattern = r'(?:SIRET\s*:?\s*)?(\d{14})'
        siret_match = re.search(siret_pattern, text)
        if siret_match:
            siren = siret_match.group(1)[:9]
            if self.luhn_validator.luhn_checksum(siren):
                return siren

        # Recherche d'un SIREN (9 chiffres)
        siren_pattern = r'(?:SIREN\s*:?\s*)?(\d{9})'
        siren_match = re.search(siren_pattern, text)
        if siren_match:
            siren = siren_match.group(1)
            if self.luhn_validator.luhn_checksum(siren):
                return siren

        return None

    def fit_predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """Détecte et corrige les anomalies dans les SIREN"""
        if not isinstance(df, pd.DataFrame):
            raise TypeError("L'entrée doit être un DataFrame")
        
        result_df = df.copy()
        
        if result_df.empty:
            result_df['is_anomaly'] = []
            result_df['siren_corrige'] = []
            result_df['source_correction'] = []
            return result_df

        def validate_and_extract_siren(row):
            siren = str(row['siren_societe']).strip()
            
            # Cas SIRET
            if len(siren) == 14 and siren.isdigit():
                potential_siren = siren[:9]
                if self.luhn_validator.luhn_checksum(potential_siren):
                    return False, potential_siren, 'CORRECTION_SIRET'
            
            # Cas SIREN valide
            if len(siren) == 9 and siren.isdigit() and self.luhn_validator.luhn_checksum(siren):
                return False, siren, 'AUCUNE_CORRECTION'
            
            # Recherche dans les autres champs
            for field, source in [
                ('complement_denomination', 'CORRECTION_COMPLEMENT'),
                ('denomination_societe', 'CORRECTION_DENOMINATION'),
                ('forme_juridique', 'CORRECTION_FORME')
            ]:
                if pd.notna(row[field]):
                    extracted_siren = self._extract_potential_siren(str(row[field]))
                    if extracted_siren:
                        return True, extracted_siren, source
            
            return True, siren, 'NON_CORRIGE'

        # Application de la validation et correction
        result_df[['is_anomaly', 'siren_corrige', 'source_correction']] = pd.DataFrame(
            [validate_and_extract_siren(row) for _, row in result_df.iterrows()],
            index=result_df.index
        )

        return result_df

import os
import pandas as pd
from pathlib import Path

def process_data(directory, contamination=0.1):
    # Lecture des données
    all_data = pd.DataFrame()
    directory = Path(directory)
    
    for file in directory.glob('*.parquet'):
        try:
            df = pd.read_parquet(file)
            df['source_file'] = file.name
            all_data = pd.concat([all_data, df]) if not all_data.empty else df
        except Exception as e:
            print(f"Erreur lors de la lecture de {file}: {str(e)}")
            continue
    
    # Détection et correction des anomalies
    detector = SIRENAnomalyDetector(contamination=contamination)
    results = detector.fit_predict(all_data)
    
    # Sauvegarde des résultats
    anomalies_df = results[results['is_anomaly']].copy()
    output_file = directory / "anomalies_siren_luhn.parquet"
    anomalies_df.to_parquet(output_file)
    
    # Statistiques
    print("\nSTATISTIQUES DES ANOMALIES SIREN")
    print("=" * 50)
    print(f"Total enregistrements analysés: {len(all_data)}")
    print(f"Nombre d'anomalies détectées: {len(anomalies_df)}")
    print("\nSources des corrections:")
    print(anomalies_df['source_correction'].value_counts())
    
    return results

if __name__ == "__main__":
    input_dir = os.getenv('INPUT_DIR', 'data/input')
    process_data(input_dir)
