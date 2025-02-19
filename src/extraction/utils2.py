import pandas as pd
import numpy as np

def filter_cases_general(main_file_path, case_files_paths, output_path):
    """
    Crée un nouveau fichier parquet avec une colonne type basée sur les nouveaux cas,
    en ne traitant que les lignes où type='autre'
    """
    try:
        # Lecture du fichier principal
        main_df = pd.read_parquet(main_file_path)
        
        # Création d'une copie du DataFrame initial pour préserver les types originaux
        result_df = main_df.copy()
        
        # Lecture de chaque fichier de cas et mise à jour du type
        # uniquement pour les lignes qui sont encore marquées comme 'autre'
        for i, case_path in enumerate(case_files_paths, 1):
            case_df = pd.read_parquet(case_path)
            
            # Ne mettre à jour que les index qui correspondent ET qui sont de type 'autre'
            mask = (result_df['type'] == 'autre') & (result_df.index.isin(case_df.index))
            result_df.loc[mask, 'type'] = f'cas{i+4}'  # +4 car on commence après les 4 premiers cas
        
        # Sauvegarde du résultat
        result_df.to_parquet(output_path)
        
        return result_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
def filter_cases_17(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères spécifiés
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (~(df['SIREN_DEC'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (~(df['siren_DEP'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['SIREN_DEC'] != df['siren_DEP']) &
            (df['siren_tete_grp'] == df['siren_DEP']) &
            (df['Ind_Mere_SIREN-DEP'] == 1)
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    

def filter_cases_18(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères du cas 18
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (~(df['SIREN_DEC'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (~(df['siren_DEP'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['SIREN_DEC'] != df['siren_DEP']) &
            (df['siren_tete_grp'] == df['siren_DEP']) &
            (df['Ind_Mere_SIREN-DEP'] == 0) &
            (df['SIR_Mere_de_SIREN-DEP'].isin(['00000000.', '0000000NA','',np.nan]))
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
def filter_cases_19(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères du cas 19
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (~(df['SIREN_DEC'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (~(df['siren_DEP'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['SIREN_DEC'] != df['siren_DEP']) &
            (df['siren_tete_grp'] == df['siren_DEP']) &
            (df['Ind_Mere_SIREN-DEP'] == 0) &
            (~(df['SIR_Mere_de_SIREN-DEP'].isin(['00000000.', '0000000NA','',np.nan])))
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
def filter_cases_20(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères du cas 20
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (~(df['SIREN_DEC'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (~(df['siren_DEP'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['SIREN_DEC'] != df['siren_DEP']) &
            (df['siren_tete_grp'] != df['siren_DEP']) &
            (~(df['siren_tete_grp'].isin(['00000000.', '0000000NA','',np.nan])))
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
def filter_cases_21(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères du cas 21
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (~(df['SIREN_DEC'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (~(df['siren_DEP'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['SIREN_DEC'] != df['siren_DEP']) &
            (df['siren_tete_grp'] != df['siren_DEP']) &
            (df['siren_tete_grp'].isin(['00000000.', '0000000NA','',np.nan])) &
            (df['Ind_Mere_SIREN-DEC'] == 0)
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
    
def filter_cases_22(input_parquet_path, output_parquet_path=None):
    """
    Filtre le fichier parquet selon les critères du cas 22
    """
    try:
        df = pd.read_parquet(input_parquet_path)
        
        filtered_df = df[
            (df['type'] == 'autre') &
            (~(df['SIREN_DEC'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (~(df['siren_DEP'].isin(['00000000.', '0000000NA','',np.nan]))) &
            (df['SIREN_DEC'] != df['siren_DEP']) &
            (df['siren_tete_grp'] != df['siren_DEP']) &
            (df['siren_tete_grp'].isin(['00000000.', '0000000NA','',np.nan])) &
            (df['Ind_Mere_SIREN-DEC'] == 1)
        ]
        
        if output_parquet_path:
            filtered_df.to_parquet(output_parquet_path)
            
        return filtered_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None

def filter_cases_general_v2(main_file_path, case_files_paths, output_path):
    """
    Crée un nouveau fichier parquet avec une colonne type basée sur les nouveaux cas,
    en ne traitant que les lignes où type='autre'
    """
    try:
        # Lecture du fichier principal
        main_df = pd.read_parquet(main_file_path)
        
        # Création d'une copie du DataFrame initial pour préserver les types originaux
        result_df = main_df.copy()
        
        # Lecture de chaque fichier de cas et mise à jour du type
        # uniquement pour les lignes qui sont encore marquées comme 'autre'
        for i, case_path in enumerate(case_files_paths, 1):
            case_df = pd.read_parquet(case_path)
            
            # Ne mettre à jour que les index qui correspondent ET qui sont de type 'autre'
            mask = (result_df['type'] == 'autre') & (result_df.index.isin(case_df.index))
            result_df.loc[mask, 'type'] = f'cas{i+16}'  # +16 car on commence après les 16 premiers cas
        
        # Sauvegarde du résultat
        result_df.to_parquet(output_path)
        
        return result_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    

def process_cases(df):
    # renommage des colonnes pour plus de clarté

    df = df.rename(columns={
        'SIREN_DEC': 'siren_declarant',
        'siren_DEP': 'siren_deposant',
        'type': 'type_original',
        'siren_tete_grp':'siren_tete_groupe'
    })
    # Créer un nouveau DataFrame avec les colonnes essentielles
    result_df = df
    
    # Ajouter les colonnes pour le résultat final
    result_df['type_final'] = ''
    result_df['mere_final'] = ''
    
    # Traiter chaque cas
    for index, row in result_df.iterrows():
        # Cas 1: Siren déclarant = siren déposant ; type = IND ; pas de siren mère
        if row['type_original'] == 'cas1':
            result_df.at[index, 'siren_declarant'] = row['siren_deposant']
            result_df.at[index, 'type_final'] = 'IND'
            result_df.at[index, 'mere_final'] = ''
            
        # Cas 2: Siren déclarant = siren déposant et siren déposant = siren-tête groupe
        elif row['type_original'] == 'cas2':
            result_df.at[index, 'siren_declarant'] = row['siren_deposant']
            if row['siren_deposant'] == row['siren_tete_groupe']:
                result_df.at[index, 'type_final'] = 'FILLE'
                result_df.at[index, 'mere_final'] = row['siren_tete_groupe']
            
        # Cas 3: Siren déclarant = siren déposant ; siren mère = siren déposant
        elif row['type_original'] == 'cas4':
            result_df.at[index, 'siren_declarant'] = row['siren_deposant']
            result_df.at[index, 'type_final'] = 'IND'
            result_df.at[index, 'mere_final'] = ''
            
        # Cas 4: Siren déclarant = siren déposant ; siren mère = siren tête de groupe
        elif row['type_original'] == 'cas3':
            result_df.at[index, 'siren_declarant'] = row['siren_deposant']
            result_df.at[index, 'type_final'] = 'FILLE'
            result_df.at[index, 'mere_final'] = df.at[index,'SIR_Mere_de_SIREN-DEP']
            
        # Cas 5: siren declarant = siren deposant et siren deposant = siren tete de groupe
        elif row['type_original'] == 'cas5':
            result_df.at[index, 'siren_declarant'] = row['siren_deposant']
            result_df.at[index, 'mere_final'] = row['siren_tete_groupe']
            result_df.at[index, 'type_final'] = 'FILLE'            
        # Cas 6: siren declarant = siren deposant et siren deposant = siren tete de groupe
        elif row['type_original'] == 'cas6':
            result_df.at[index, 'siren_declarant'] = row['siren_deposant']
            result_df.at[index, 'mere_final'] = row['siren_deposant']
            result_df.at[index, 'type_final'] = 'FILLE'
            
        # Cas 7: Mettre siren tete de groupe vide
        elif row['type_original'] == 'cas7':
            result_df.at[index, 'siren_declarant'] = row['siren_deposant']
            result_df.at[index, 'type_final'] = 'IND'
            result_df.at[index, 'mere_final'] = ''
            
        # Cas 8: TYPE=IND ; MERE=vide
        elif row['type_original'] == 'cas8':
            result_df.at[index, 'type_final'] = 'IND'
            result_df.at[index, 'mere_final'] = ''
            
        # Cas 9: TYPE=MERE ; MERE=sirenDEP
        elif row['type_original'] == 'cas9':
            result_df.at[index, 'type_final'] = 'MERE'
            result_df.at[index, 'mere_final'] = row['siren_deposant']
            
        # Cas 10: siren dep = mere final
        elif row['type_original'] == 'cas10':
            result_df.at[index, 'type_final'] = 'FILLE'
            result_df.at[index, 'mere_final'] = df.at[index,'SIR_Mere_de_SIREN-DEC']
            
        # Cas 11: type fille, siren tete de groupe = mere final
        elif row['type_original'] == 'cas11':
            result_df.at[index, 'type_final'] = 'FILLE'
            result_df.at[index, 'mere_final'] = row['siren_tete_groupe']
            
        # Cas 12: type fille, siren dep = mere final
        elif row['type_original'] == 'cas12':
            result_df.at[index, 'type_final'] = 'FILLE'
            result_df.at[index, 'mere_final'] = row['siren_tete_groupe']
            
        # Cas 13: siren dep = mere final
        elif row['type_original'] == 'cas13':
            result_df.at[index,'type_final'] = 'FILLE'
            result_df.at[index, 'mere_final'] = row['siren_tete_groupe']
            
        # Cas 14: TYPE=IND ; MERE=vide
        elif row['type_original'] == 'cas14':
            result_df.at[index, 'type_final'] = 'IND'
            result_df.at[index, 'mere_final'] = ''
            
        # Cas 15: siren dep = mere final
        elif row['type_original'] == 'cas15':
            result_df.at[index, 'type_final'] = 'FILLE'
            result_df.at[index, 'mere_final'] = df.at[index,'SIR_Mere_de_SIREN-DEC']
            
        # Cas 16: TYPE=MERE ; MERE=sirenDEC
        elif row['type_original'] == 'cas16':
            result_df.at[index, 'type_final'] = 'MERE'
            result_df.at[index, 'mere_final'] = row['siren_declarant']

        # Cas 17: TYPE=FILLE ; MERE=sirenDEP
        elif row['type_original'] == 'cas17':
            result_df.at[index, 'type_final'] = 'FILLE'
            result_df.at[index, 'mere_final'] = row['siren_deposant']

        # Cas 18: TYPE=FILLE ; MERE=sirenDEP
        elif row['type_original'] == 'cas18':
            result_df.at[index, 'type_final'] = 'FILLE'
            result_df.at[index, 'mere_final'] = row['siren_deposant']

        # Cas 19: TYPE=FILLE 
        elif row['type_original'] == 'cas19':
            result_df.at[index, 'type_final'] = 'FILLE'
            
            # Si SIREN_DEC est une mère
            if row['Ind_Mere_SIREN-DEC'] == 1:
                # Inverser siren mere et siren dep
                result_df.at[index, 'mere_final'] = row['siren_deposant']
                
            # Si SIR_Mere_de_SIREN-DEC n'est pas vide
            elif row['SIR_Mere_de_SIREN-DEC'] != '':
                result_df.at[index, 'mere_final'] = row['SIR_Mere_de_SIREN-DEC']
                
            # Sinon, utiliser siren_tete_grp
            else:
                result_df.at[index, 'mere_final'] = row['siren_tete_groupe']

        # Cas 20: TYPE=FILLE ; MERE=sirenDEC
        elif row['type_original'] == 'cas20':
            result_df.at[index, 'type_final'] = 'FILLE'
            result_df.at[index, 'mere_final'] = row['siren_declarant']
            tmp = row['siren_declarant']
            result_df.at[index, 'siren_declarant'] = row['siren_deposant']
            result_df.at[index, 'siren_deposant'] = tmp

        # Cas 21: TYPE=FILLE ; MERE=sirenDEP
        elif row['type_original'] == 'cas21':
            result_df.at[index, 'type_final'] = 'FILLE'
            result_df.at[index, 'mere_final'] = row['siren_deposant']

        # Cas 22: TYPE=FILLE ; MERE=sirenDEC
        elif row['type_original'] == 'cas22':
            result_df.at[index, 'type_final'] = 'FILLE'
            result_df.at[index, 'mere_final'] = row['siren_declarant']
            tmp = row['siren_declarant']
            result_df.at[index, 'siren_declarant'] = row['siren_deposant']
            result_df.at[index, 'siren_deposant'] = tmp

    
    return result_df

def apply_and_save_cases(input_file, output_file):
    try:
        # Lecture du fichier
        df = pd.read_parquet(input_file)
        
        # Application des règles et création du nouveau DataFrame
        result_df = process_cases(df)
        
        # Sauvegarde du résultat
        result_df.to_parquet(output_file)
        
        # Afficher des statistiques sur les résultats
        print("\nStatistiques des types finaux :")
        print(result_df['type_final'].value_counts())
        print("\nNombre de lignes où mere_final est vide :", result_df['mere_final'].isna().sum())
        
        return result_df
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        return None
    
