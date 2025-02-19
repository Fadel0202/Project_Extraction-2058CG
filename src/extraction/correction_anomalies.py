import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import os
import re
from typing import Tuple, Optional, Dict, List

# Lecture du fichier des anomalies
anomalies_df = pd.read_parquet('M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//anomalies_siren_luhn.parquet')

# Créer un dictionnaire de correction (siren_incorrect -> siren_corrigé)
corrections_dict = dict(zip(anomalies_df['siren_societe'], anomalies_df['siren_corrige']))

# Pour chaque fichier source
for fichier in anomalies_df['source_file'].unique():
    # Lecture du fichier original
    df_original = pd.read_parquet(f'M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//{fichier}')
    
    # Application des corrections
    df_original['siren_societe'] = df_original['siren_societe'].replace(corrections_dict)
    
    # Sauvegarde du fichier corrigé
    df_original.to_parquet(f'M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//{fichier}')
