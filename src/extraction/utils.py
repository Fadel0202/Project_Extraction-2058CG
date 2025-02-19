# Import des bibliothèques nécessaires
import pandas as pd
import numpy as np
import os

# Pour sauvegarder en CSV (plus léger)
def parquet_to_csv(directory):
    """
    Convertit tous les fichiers parquet d'un répertoire en CSV 
    et les sauvegarde dans un sous-répertoire 'output_csv'
    
    Args:
        directory (str): Chemin du répertoire contenant les fichiers parquet
    """
    # Créer le répertoire output_csv s'il n'existe pas
    output_dir = os.path.join(directory, 'output_csv')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Liste tous les fichiers du répertoire
    for file in os.listdir(directory):
        # Vérifie si le fichier est un parquet
        if file.endswith('.parquet'):
            # Construit le chemin complet du fichier parquet
            file_path = os.path.join(directory, file)
            
            # Lit le fichier parquet
            df = pd.read_parquet(file_path)
            
            # Crée le nom du fichier CSV dans le répertoire output_csv
            csv_filename = file.replace('.parquet', '.csv')
            output_path = os.path.join(output_dir, csv_filename)
            
            # Sauvegarde en CSV
            df.to_csv(output_path, index=False)
            print(f"Fichier converti: {csv_filename}")

# Pour sauvegarder en Excel (plus léger)
def parquet_to_xlsx(directory):
    """
    Convertit tous les fichiers parquet d'un répertoire en xlsx 
    et les sauvegarde dans un sous-répertoire 'output_xlsx'
    
    Args:
        directory (str): Chemin du répertoire contenant les fichiers parquet
    """
    # Créer le répertoire output_xlsx s'il n'existe pas
    output_dir = os.path.join(directory, 'output_xlsx')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Liste tous les fichiers du répertoire
    for file in os.listdir(directory):
        # Vérifie si le fichier est un parquet
        if file.endswith('.parquet'):
            # Construit le chemin complet du fichier parquet
            file_path = os.path.join(directory, file)
            
            # Lit le fichier parquet
            df = pd.read_parquet(file_path)
            
            # Convertir les colonnes datetime avec fuseau horaire en colonnes sans fuseau horaire car le fichier excel ne peut pas contenir les fuseaux horaires.
            for colonne in df.columns:
                if pd.api.types.is_datetime64tz_dtype(df[colonne]):
                    df[colonne] = df[colonne].dt.tz_localize(None)
            
            # Crée le nom du fichier Excel dans le répertoire output_xlsx
            csv_filename = file.replace('.parquet', '.xlsx')
            output_path = os.path.join(output_dir, csv_filename)
            
            # Sauvegarde en Excel
            df.to_excel(output_path, index=False, engine='openpyxl')
            print(f"Fichier converti: {csv_filename}")


def create_mere_fille_file(path_millesime_cir, path_2058cg_23, path_2058cg_22, output_path):
    """
    Crée un fichier mère-fille en parquet à partir des fichiers millesime_cir et 2058CG (2022 et 2023).
    
    Args:
        path_millesime_cir (str): Chemin vers le fichier millesime_cir_2022.xlsx
        path_2058cg_23 (str): Chemin vers le fichier 2058CG_millesime_23.parquet
        path_2058cg_22 (str): Chemin vers le fichier 2058CG_millesime_22.parquet
        output_path (str): Chemin pour sauvegarder le fichier résultat
    """
    try:
        # Assurer que le fichier de sortie a l'extension .parquet
        if not output_path.endswith('.parquet'):
            output_path = output_path + '.parquet'

        # Création du dossier de sortie s'il n'existe pas
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Lecture du fichier millesime_cir_2022
        print("Lecture du fichier millesime_cir_2022...")
        converters = {
            'SIREN_DEC': lambda x: str(x).zfill(9) if pd.notnull(x) else '',
            'siren_DEP': lambda x: str(x).zfill(9) if pd.notnull(x) else '',
            'siren_tete_grp': lambda x: str(x).zfill(9) if pd.notnull(x) else ''
        }
        df_millesime = pd.read_csv(path_millesime_cir, sep=';', encoding='latin-1', converters=converters)
        
        # Sélection et copie des colonnes nécessaires
        df_mere_fille = df_millesime.copy()
        
        # Ajout des nouvelles colonnes initialisées
        df_mere_fille['Ind_Mere_SIREN-DEC'] = 0
        df_mere_fille['SIR_Mere_de_SIREN-DEC'] = ''
        df_mere_fille['Ind_Mere_SIREN-DEP'] = 0
        df_mere_fille['SIR_Mere_de_SIREN-DEP'] = ''
        df_mere_fille['Ind_Mere_SIREN-Tete_de_Grp'] = 0
        df_mere_fille['SIR_Mere_de_SIREN-Tete_de_Grp'] = ''
        df_mere_fille['montant_credit_impot'] = 0
        df_mere_fille['denomination_societe'] = ''
        df_mere_fille['complement_denomination'] = ''
        
        # Lecture des fichiers 2058CG
        print("Lecture du fichier 2058CG_millesime_23...")
        df_2058cg_23 = pd.read_parquet(path_2058cg_23)
        print("Lecture du fichier 2058CG_millesime_22...")
        df_2058cg_22 = pd.read_parquet(path_2058cg_22)
        
        # Formatage des SIREN dans les deux fichiers 2058CG
        for df in [df_2058cg_23, df_2058cg_22]:
            df['mere_siren'] = df['mere_siren'].astype(str).str.zfill(9)
            df['siren_societe'] = df['siren_societe'].astype(str).str.zfill(9)
        
        # Liste des SIREN qui sont des mères dans les deux années
        meres_2058cg_23 = set(df_2058cg_23['mere_siren'])
        meres_2058cg_22 = set(df_2058cg_22['mere_siren'])
        meres_2058cg = meres_2058cg_23.union(meres_2058cg_22)
        
        # Fusionner les relations mère-fille des deux années
        relations_mere_fille_23 = df_2058cg_23[['mere_siren', 'siren_societe', 'montant_credit_impot',
                                               'denomination_societe', 'complement_denomination']].dropna(subset=['mere_siren', 'siren_societe'])
        relations_mere_fille_22 = df_2058cg_22[['mere_siren', 'siren_societe', 'montant_credit_impot',
                                               'denomination_societe', 'complement_denomination']].dropna(subset=['mere_siren', 'siren_societe'])
        relations_mere_fille = pd.concat([relations_mere_fille_23, relations_mere_fille_22], ignore_index=True)
        
        # Création des dictionnaires pour les cas où mere_siren = siren_societe (priorité à 2023)
        auto_meres_23 = df_2058cg_23[df_2058cg_23['mere_siren'] == df_2058cg_23['siren_societe']]
        auto_meres_22 = df_2058cg_22[df_2058cg_22['mere_siren'] == df_2058cg_22['siren_societe']]
        
        denominations_auto_meres = {
            **auto_meres_22[['siren_societe', 'denomination_societe']].set_index('siren_societe')['denomination_societe'].to_dict(),
            **auto_meres_23[['siren_societe', 'denomination_societe']].set_index('siren_societe')['denomination_societe'].to_dict()
        }
        
        complement_denominations_auto_meres = {
            **auto_meres_22[['siren_societe', 'complement_denomination']].set_index('siren_societe')['complement_denomination'].to_dict(),
            **auto_meres_23[['siren_societe', 'complement_denomination']].set_index('siren_societe')['complement_denomination'].to_dict()
        }
        
        # Création des dictionnaires pour les montants (priorité à 2023)
        montants_par_mere = {
            **df_2058cg_22.groupby('mere_siren')['montant_credit_impot'].sum().to_dict(),
            **df_2058cg_23.groupby('mere_siren')['montant_credit_impot'].sum().to_dict()
        }
        
        print("Traitement des relations mère-fille...")
        # Pour chaque ligne dans df_mere_fille
        for index, row in df_mere_fille.iterrows():
            siren_dec = row['SIREN_DEC']
            siren_dep = row['siren_DEP']
            siren_tete = row['siren_tete_grp']
            
            # Traitement pour SIREN_DEC
            if siren_dec in meres_2058cg:
                df_mere_fille.at[index, 'Ind_Mere_SIREN-DEC'] = 1
                # Ajout des informations supplémentaires si disponibles
                if siren_dec in montants_par_mere:
                    df_mere_fille.at[index, 'montant_credit_impot'] = montants_par_mere[siren_dec]
                # Ajout des dénominations uniquement si le SIREN est sa propre mère
                if siren_dec in denominations_auto_meres:
                    df_mere_fille.at[index, 'denomination_societe'] = denominations_auto_meres[siren_dec]
                if siren_dec in complement_denominations_auto_meres:
                    df_mere_fille.at[index, 'complement_denomination'] = complement_denominations_auto_meres[siren_dec]
            
            if df_mere_fille.at[index, 'Ind_Mere_SIREN-DEC'] == 0:
                mere_info = relations_mere_fille[
                    relations_mere_fille['siren_societe'] == siren_dec
                ][['mere_siren']].values
                
                if len(mere_info) > 0:
                    df_mere_fille.at[index, 'SIR_Mere_de_SIREN-DEC'] = mere_info[0][0]
            
            # Traitement pour SIREN_DEP
            if siren_dep in meres_2058cg:
                df_mere_fille.at[index, 'Ind_Mere_SIREN-DEP'] = 1
            
            if df_mere_fille.at[index, 'Ind_Mere_SIREN-DEP'] == 0:
                mere = relations_mere_fille[
                    relations_mere_fille['siren_societe'] == siren_dep
                ]['mere_siren'].values
                
                if len(mere) > 0:
                    df_mere_fille.at[index, 'SIR_Mere_de_SIREN-DEP'] = mere[0]

            # Traitement pour SIREN_Tete_de_Grp
            if siren_tete in meres_2058cg:
                df_mere_fille.at[index, 'Ind_Mere_SIREN-Tete_de_Grp'] = 1
            
            if df_mere_fille.at[index, 'Ind_Mere_SIREN-Tete_de_Grp'] == 0:
                mere = relations_mere_fille[
                    relations_mere_fille['siren_societe'] == siren_tete
                ]['mere_siren'].values
                
                if len(mere) > 0:
                    df_mere_fille.at[index, 'SIR_Mere_de_SIREN-Tete_de_Grp'] = mere[0]

        # Modifier la partie sauvegarde comme suit :
        print("Sauvegarde du fichier résultat...")
        # Liste des colonnes qu'on veut garder en numérique (les colonnes qu'on a ajoutées nous-mêmes)
        colonnes_numeriques = [
            'Ind_Mere_SIREN-DEC',
            'Ind_Mere_SIREN-DEP',
            'Ind_Mere_SIREN-Tete_de_Grp',
            'montant_credit_impot'
        ]

        # Conversion de toutes les colonnes
        for col in df_mere_fille.columns:
            if col in colonnes_numeriques:
                # Pour les colonnes numériques connues
                df_mere_fille[col] = df_mere_fille[col].fillna(0).astype(int)
            else:
                # Pour toutes les autres colonnes, conversion en string
                df_mere_fille[col] = df_mere_fille[col].fillna('').astype(str)


        # Sauvegarder en parquet
        df_mere_fille.to_parquet(output_path)
        
        print(f"Traitement terminé. Fichier sauvegardé dans : {output_path}")
        
        # Affichage des statistiques
        total_rows = len(df_mere_fille)
        meres_dec = (df_mere_fille['Ind_Mere_SIREN-DEC'] == 1).sum()
        filles_dec = (df_mere_fille['SIR_Mere_de_SIREN-DEC'] != '').sum()
        meres_dep = (df_mere_fille['Ind_Mere_SIREN-DEP'] == 1).sum()
        filles_dep = (df_mere_fille['SIR_Mere_de_SIREN-DEP'] != '').sum()
        meres_tete = (df_mere_fille['Ind_Mere_SIREN-Tete_de_Grp'] == 1).sum()
        filles_tete = (df_mere_fille['SIR_Mere_de_SIREN-Tete_de_Grp'] != '').sum()
        
        print(f"\nStatistiques:")
        print(f"Total des lignes traitées: {total_rows}")
        print(f"Nombre de sociétés mères (SIREN_DEC): {meres_dec}")
        print(f"Nombre de sociétés filles (SIREN_DEC): {filles_dec}")
        print(f"Nombre de sociétés mères (SIREN_DEP): {meres_dep}")
        print(f"Nombre de sociétés filles (SIREN_DEP): {filles_dep}")
        print(f"Nombre de sociétés mères (SIREN_Tete_de_Grp): {meres_tete}")
        print(f"Nombre de sociétés filles (SIREN_Tete_de_Grp): {filles_tete}")
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")

# Fonction pour corriger les SIREN
def correct_siren(value):
    try:
        # Convertir la valeur en chaîne
        value_str = str(value)
        
        # Si la valeur est exactement "03.56e+08", remplacer par "356000000"
        if value_str.lower() == "03.56e+08":
            return "356000000"
        
        # Utiliser une expression régulière pour extraire un SIREN valide
        match = re.search(r'\d{9}', value_str)
        if match:
            return match.group()
        
        # Sinon, tronquer à 9 caractères (au cas où il s'agit d'un SIREN malformé mais pas mélangé)
        return value_str[:9]
    except:
        # Retourner la valeur initiale en cas d'erreur
        return value


def filter_cases_1(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['siren_DEP'] != '0000000NA') &
            (df['SIREN_DEC'] == '0000000NA') &
            (df['Ind_Mere_SIREN-DEP'] == 0) &
            (df['SIR_Mere_de_SIREN-DEP'].isin(['', np.nan])) &
            (df['siren_tete_grp'] == '0000000NA')
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
        
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
def filter_cases_2(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['siren_DEP'] != '0000000NA') &
            (df['SIREN_DEC'] == '0000000NA') &
            (df['Ind_Mere_SIREN-DEP'] == 0) &
            (~df['SIR_Mere_de_SIREN-DEP'].isin(['', np.nan])) &
            (df['siren_tete_grp'] != '0000000NA')
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
        
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
def filter_cases_3(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['siren_DEP'] != '0000000NA') &
            (df['SIREN_DEC'] == '0000000NA') &
            (df['Ind_Mere_SIREN-DEP'] == 0) &
            (~df['SIR_Mere_de_SIREN-DEP'].isin(['', np.nan])) &
            (df['siren_tete_grp'] == '0000000NA')
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
        
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None


def filter_cases_4(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['siren_DEP'] != '0000000NA') &
            (df['SIREN_DEC'] == '0000000NA') &
            (df['Ind_Mere_SIREN-DEP'] == 1)
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
        
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    

def filter_cases_general(main_file_path, case_files_paths, output_path):
    """
    Crée un nouveau fichier parquet avec une colonne type basée sur les cas,
    en comparant les index des DataFrames
    """
    try:
        # Lecture du fichier principal
        main_df = pd.read_parquet(main_file_path)
        
        # Initialisation de la colonne type avec une valeur par défaut
        main_df['type'] = 'autre'
        
        # Lecture de chaque fichier de cas et mise à jour du type
        for i, case_path in enumerate(case_files_paths, 1):
            case_df = pd.read_parquet(case_path)
            
            # Mise à jour du type pour les index correspondants
            main_df.loc[case_df.index, 'type'] = f'cas{i}'
        
        # Sauvegarde du résultat
        main_df.to_parquet(output_path)
        
        return main_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    


def filter_cases_5(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (df['SIREN_DEC'] == '0000000NA') &
            (df['Ind_Mere_SIREN-Tete_de_Grp'] == 1)
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
        
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    

def filter_cases_6(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (df['SIREN_DEC'] == '0000000NA') &
            (df['Ind_Mere_SIREN-Tete_de_Grp'] == 0) &
            (~df['SIR_Mere_de_SIREN-Tete_de_Grp'].isin(['', np.nan]))
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
        
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
def filter_cases_7(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (df['SIREN_DEC'] == '0000000NA') &
            (~df['siren_DEP'].isin(['', np.nan])) &
            (df['Ind_Mere_SIREN-DEC'] == 0) &
            (df['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan])) &
            (df['Ind_Mere_SIREN-Tete_de_Grp'] == 0) &
            (df['SIR_Mere_de_SIREN-Tete_de_Grp'].isin(['', np.nan]))
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
        
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    

def filter_cases_8(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (df['SIREN_DEC'] != '0000000NA') &
            (df['SIREN_DEC'] == df['siren_DEP']) &
            ((df['siren_tete_grp'] == '00000000.') | (df['siren_tete_grp'] == '0000000NA')) &
            (df['Ind_Mere_SIREN-DEP'] == 0) &
            (df['SIR_Mere_de_SIREN-DEP'].isin(['', np.nan]))
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    

def filter_cases_9(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (df['SIREN_DEC'] != '0000000NA') &
            (df['SIREN_DEC'] == df['siren_DEP']) &
            ((df['siren_tete_grp'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['Ind_Mere_SIREN-DEP'] == 1) &
            (df['SIR_Mere_de_SIREN-DEP'].isin(['', np.nan]))
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
def filter_cases_10(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (~(df['SIREN_DEC'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['SIREN_DEC'] == df['siren_DEP']) &
            ((df['siren_tete_grp'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['Ind_Mere_SIREN-DEP'] == 1) &
            (~df['SIR_Mere_de_SIREN-DEP'].isin(['', np.nan]))
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None

def filter_cases_11(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (~(df['SIREN_DEC'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['SIREN_DEC'] == df['siren_DEP']) &
            (~(df['siren_tete_grp'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['Ind_Mere_SIREN-DEC'] == 0) &
            (df['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan])) &
            (df['Ind_Mere_SIREN-Tete_de_Grp'] == 1)
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    

def filter_cases_12(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (df['SIREN_DEC'] != '0000000NA') &
            (df['SIREN_DEC'] == df['siren_DEP']) &
            (~(df['siren_tete_grp'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['Ind_Mere_SIREN-DEC'] == 0) &
            (df['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan])) &
            (df['Ind_Mere_SIREN-Tete_de_Grp'] == 0) &
            (~df['SIR_Mere_de_SIREN-Tete_de_Grp'].isin(['', np.nan]))
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
    
def filter_cases_13(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (df['SIREN_DEC'] != '0000000NA') &
            (df['SIREN_DEC'] == df['siren_DEP']) &
            (~(df['siren_tete_grp'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['Ind_Mere_SIREN-DEC'] == 0) &
            (df['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan])) &
            (df['Ind_Mere_SIREN-Tete_de_Grp'] == 0) &
            (df['SIR_Mere_de_SIREN-Tete_de_Grp'].isin(['', np.nan])) &
            (df['siren_tete_grp'] != df['siren_DEP']) 
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None

def filter_cases_14(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (df['SIREN_DEC'] != '0000000NA') &
            (df['SIREN_DEC'] == df['siren_DEP']) &
            (~(df['siren_tete_grp'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['Ind_Mere_SIREN-DEC'] == 0) &
            (df['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan])) &
            (df['Ind_Mere_SIREN-Tete_de_Grp'] == 0) &
            (df['SIR_Mere_de_SIREN-Tete_de_Grp'].isin(['', np.nan])) &
            (df['siren_tete_grp'] == df['siren_DEP']) 
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
def filter_cases_15(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (~(df['SIREN_DEC'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['SIREN_DEC'] == df['siren_DEP']) &
            (~(df['siren_tete_grp'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['Ind_Mere_SIREN-DEC'] == 0) &
            (~df['SIR_Mere_de_SIREN-DEC'].isin(['', np.nan]))
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
def filter_cases_16(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (~(df['SIREN_DEC'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['SIREN_DEC'] == df['siren_DEP']) &
            (~(df['siren_tete_grp'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['Ind_Mere_SIREN-DEC'] == 1)
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
