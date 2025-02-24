import os
import pandas as pd
from pathlib import Path

def cleaned_file(directory):
    """
    Nettoie tous les fichiers parquet d'un répertoire en supprimant les doublons.
    """
    print("Début du nettoyage des fichiers...")
    
    # Colonnes à exclure lors de la comparaison des doublons
    columns_to_exclude = {
        'dateEnregistrement',
        'id_depot',
        'nombre_filiales',
        'nombre_filiales_renseignees',
        'filiale_index'
    }
    
    directory = Path(directory)
    
    if not directory.exists():
        print(f"Le répertoire {directory} n'existe pas")
        return pd.DataFrame()
    
    parquet_files = list(directory.glob('*.parquet'))
    if not parquet_files:
        print("Aucun fichier parquet trouvé")
        return pd.DataFrame()
    
    processed_dfs = []
    
    for file in parquet_files:
        try:
            # Lecture du fichier
            df = pd.read_parquet(str(file))
            
            if df.empty:
                print(f"Fichier {file.name} vide, passage au suivant.")
                continue
            
            nb_lignes_avant = len(df)
            
            # Colonnes pour la comparaison (tous les colonnes sauf celles à exclure)
            compare_cols = [col for col in df.columns if col not in columns_to_exclude]
            
            # Si dateEnregistrement existe, convertir en datetime et trier
            if 'dateEnregistrement' in df.columns:
                df['dateEnregistrement'] = pd.to_datetime(df['dateEnregistrement'])
                df = df.sort_values('dateEnregistrement', ascending=False)
            
            # Suppression des doublons en gardant la première occurrence
            # (la plus récente si trié par date)
            df = df.drop_duplicates(subset=compare_cols, keep='first').reset_index(drop=True)
            processed_dfs.append(df)
            
            nb_lignes_apres = len(df)
            nb_doublons = nb_lignes_avant - nb_lignes_apres
            
            print(f"Nettoyage du fichier {file.name}:")
            print(f"- Lignes avant: {nb_lignes_avant}")
            print(f"- Lignes après: {nb_lignes_apres}")
            print(f"- Doublons supprimés: {nb_doublons}")
            
            # Sauvegarde du fichier nettoyé
            df.to_parquet(str(file), index=False)
            
        except Exception as e:
            print(f"Erreur lors du traitement de {file.name}: {str(e)}")
            continue
    
    # Retourne le dernier DataFrame traité s'il existe
    return processed_dfs[-1] if processed_dfs else pd.DataFrame()

# Example usage
cleaned_file("output")