import os
import pandas as pd
from pathlib import Path

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
            df['dateEnregistrement'] = pd.to_datetime(df['dateEnregistrement'], format='ISO8601')
            
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

# Example usage
cleaned_file("output")