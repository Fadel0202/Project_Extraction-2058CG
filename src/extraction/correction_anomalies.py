import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from pathlib import Path
from typing import Optional, Dict, List, Union
from extraction.correction_anomalies import apply_corrections

def apply_corrections(input_dir: Union[str, Path], 
                     anomalies_file: Union[str, Path], 
                     output_dir: Optional[Union[str, Path]] = None) -> None:
    """
    Applique les corrections SIREN à partir d'un fichier d'anomalies.
    
    Args:
        input_dir: Répertoire contenant les fichiers à corriger
        anomalies_file: Chemin vers le fichier d'anomalies
        output_dir: Répertoire de sortie (optionnel, par défaut même que input_dir)
    """
    # Conversion des chemins en objets Path
    input_dir = Path(input_dir)
    anomalies_file = Path(anomalies_file)
    output_dir = Path(output_dir) if output_dir else input_dir

    # Vérification des chemins
    if not input_dir.exists():
        raise FileNotFoundError(f"Le répertoire d'entrée {input_dir} n'existe pas")
    if not anomalies_file.exists():
        raise FileNotFoundError(f"Le fichier d'anomalies {anomalies_file} n'existe pas")
    
    # Création du répertoire de sortie si nécessaire
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Lecture du fichier des anomalies
        anomalies_df = pd.read_parquet(anomalies_file)

        # Créer un dictionnaire de correction (siren_incorrect -> siren_corrigé)
        corrections_dict = dict(zip(anomalies_df['siren_societe'], 
                                  anomalies_df['siren_corrige']))

        # Pour chaque fichier source
        for fichier in anomalies_df['source_file'].unique():
            try:
                input_file = input_dir / fichier
                output_file = output_dir / fichier

                # Vérification de l'existence du fichier source
                if not input_file.exists():
                    print(f"Fichier source non trouvé : {input_file}")
                    continue

                # Lecture du fichier original
                df_original = pd.read_parquet(input_file)
                
                # Application des corrections
                df_original['siren_societe'] = df_original['siren_societe'].replace(
                    corrections_dict
                )
                
                # Sauvegarde du fichier corrigé
                df_original.to_parquet(output_file, index=False)
                print(f"Corrections appliquées pour {fichier}")

            except Exception as e:
                print(f"Erreur lors du traitement de {fichier}: {str(e)}")
                continue

    except Exception as e:
        print(f"Erreur lors du traitement du fichier d'anomalies: {str(e)}")
        raise

def main():
    """Point d'entrée principal du script"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Correction des anomalies SIREN')
    parser.add_argument('input_dir', help='Répertoire contenant les fichiers à corriger')
    parser.add_argument('anomalies_file', help='Chemin vers le fichier d\'anomalies')
    parser.add_argument('--output_dir', help='Répertoire de sortie (optionnel)')
    
    args = parser.parse_args()
    
    try:
        apply_corrections(
            input_dir=args.input_dir,
            anomalies_file=args.anomalies_file,
            output_dir=args.output_dir
        )
    except Exception as e:
        print(f"Erreur: {str(e)}")
        exit(1)

if __name__ == '__main__':
    main()

apply_corrections(
    input_dir="chemin/vers/donnees",
    anomalies_file="chemin/vers/anomalies.parquet",
    output_dir="chemin/vers/sortie"
)
