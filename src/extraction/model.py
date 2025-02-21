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
    """
    Détecteur d'anomalies SIREN utilisant l'algorithme de Luhn et cherchant des corrections
    dans les autres champs si nécessaire
    """
    def __init__(self, contamination: float = 0.1):
        self.isolation_forest = IsolationForest(
            contamination=contamination,
            random_state=42
        )
        self.luhn_validator = LuhnValidator()
        
    def _extract_potential_siren(self, text: str) -> Optional[str]:
        """
        Extrait un SIREN potentiel d'une chaîne de texte en gérant les cas SIRET/SIREN
        qu'il soit au début, au milieu ou à la fin, avec ou sans espaces
        """
        if pd.isna(text):
            return None
            
        text = str(text).strip()
        
        # Supprime les espaces dans la chaîne
        text_without_spaces = re.sub(r'\s+', '', text)
        
        # Extrait tous les nombres de 14 chiffres (SIRET) ou 9 chiffres (SIREN)
        siret_matches = re.findall(r'\d{14}', text_without_spaces)
        siren_matches = re.findall(r'\d{9}', text_without_spaces)
        
        # Pattern pour SIREN avec espaces (3-3-3)
        spaced_siren_pattern = r'\d{3}\s+\d{3}\s+\d{3}'
        spaced_siren_matches = re.findall(spaced_siren_pattern, text)
        
        # Vérifie d'abord les SIRET (en prenant les 9 premiers chiffres)
        for siret in siret_matches:
            potential_siren = siret[:9]
            if self.luhn_validator.luhn_checksum(potential_siren):
                return potential_siren
                
        # Puis vérifie les SIREN sans espaces
        for siren in siren_matches:
            if self.luhn_validator.luhn_checksum(siren):
                return siren
                
        # Enfin vérifie les SIREN avec espaces
        for siren in spaced_siren_matches:
            siren_clean = re.sub(r'\s+', '', siren)
            if self.luhn_validator.luhn_checksum(siren_clean):
                return siren_clean
                
        return None

    def fit_predict(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None:
            raise ValueError("Le DataFrame d'entrée ne doit pas être None")
        
        result_df = df.copy()
        
        if result_df.empty:
            result_df['is_anomaly'] = []
            result_df['siren_corrige'] = []
            result_df['source_correction'] = []
            return result_df
        
        # Identification des SIREN invalides
        def validate_siren(siren) -> bool:
            try:
                if pd.isna(siren):
                    return False
                siren_str = str(siren).strip()
                if not siren_str.isdigit():
                    return False
                if len(siren_str) == 14:
                    return self.luhn_validator.luhn_checksum(siren_str[:9])
                return len(siren_str) == 9 and self.luhn_validator.luhn_checksum(siren_str)
            except Exception:
                return False

        result_df['is_anomaly'] = ~result_df['siren_societe'].apply(validate_siren)
        
        # Pour les anomalies, tentative de correction
        def correct_siren(row: pd.Series) -> Tuple[str, str]:
            if not row['is_anomaly']:
                return row['siren_societe'], "AUCUNE_CORRECTION"
                
            original_siren = str(row['siren_societe']).strip()
            
            if len(original_siren) == 14 and original_siren.isdigit():
                potential_siren = original_siren[:9]
                if self.luhn_validator.luhn_checksum(potential_siren):
                    return potential_siren, "CORRECTION_SIRET"
            
            fields = [
                ('denomination_societe', 'CORRECTION_DENOMINATION'),
                ('complement_denomination', 'CORRECTION_COMPLEMENT'),
                ('forme_juridique', 'CORRECTION_FORME')
            ]
            for field, source in fields:
                if pd.notna(row[field]):
                    potential_siren = self._extract_potential_siren(row[field])
                    if potential_siren:
                        return potential_siren, source
                        
            return original_siren, "NON_CORRIGE"

        corrections = result_df[result_df['is_anomaly']].apply(correct_siren, axis=1)
        result_df.loc[result_df['is_anomaly'], 'siren_corrige'] = corrections.apply(lambda x: x[0])
        result_df.loc[result_df['is_anomaly'], 'source_correction'] = corrections.apply(lambda x: x[1])
        
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
