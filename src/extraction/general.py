import extraction.utils as utils
import extraction.utils as utils2
import pandas as pd
import os
from pathlib import Path

def get_env_path(var_name, default_path):
    return Path(os.getenv(var_name, default_path))

if __name__ == "__main__":
    # Création du fichier mere fille
    input_csv = get_env_path('INPUT_CSV', 'data/input/millesime_CIR_2022.csv')
    parquet_23 = get_env_path('PARQUET_23', 'data/output/2058CG_millesime_23.parquet')
    parquet_22 = get_env_path('PARQUET_22', 'data/output/2058CG_millesime_22.parquet')
    output_parquet = get_env_path('OUTPUT_PARQUET', 'data/output/output_parquet/mere_fille2_result.parquet')

    utils.create_mere_fille_file(input_csv, parquet_23, parquet_22, output_parquet)
    
    # Charger le fichier Parquet
    df = pd.read_parquet(output_parquet)

    # Liste des colonnes contenant des SIREN
    siren_columns = ["SIREN_DEC", "siren_DEP", "siren_tete_grp", 
                    "SIR_Mere_de_SIREN-DEC", "SIR_Mere_de_SIREN-DEP", "SIR_Mere_de_SIREN-Tete_de_Grp"]

    # Appliquer la correction uniquement aux colonnes spécifiées
    for col in siren_columns:
        if col in df.columns:
            df[col] = df[col].apply(utils.correct_siren)

    # Sauvegarder le fichier nettoyé sans changer l'index
    output_file_path = get_env_path('OUTPUT_CLEANED_PARQUET', 'data/output/output_parquet/mere_fille2_result_cleaned.parquet')
    df.to_parquet(output_file_path, engine="pyarrow", index=True)

    print(f"Fichier nettoyé sauvegardé sous {output_file_path}")

    input_path = output_file_path
    output_path = get_env_path('OUTPUT_FILTERED_CASE1', 'data/output/output_parquet/filteredcase1_result.parquet')

    filtered_data = utils.filter_cases_1(input_path, output_path)

    # Affichage des premières lignes du résultat si disponible
    if filtered_data is not None and not filtered_data.empty:
        print("\nAperçu des résultats :")
        print(filtered_data.head())
        print(filtered_data.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    # Repeat the same process for other cases
    for i in range(2, 23):
        output_path = get_env_path(f'OUTPUT_FILTERED_CASE{i}', f'data/output/output_parquet/filteredcase{i}_result.parquet')
        filter_func = getattr(utils, f'filter_cases_{i}', None) or getattr(utils2, f'filter_cases_{i}', None)
        if filter_func:
            filtered_data = filter_func(input_path, output_path)
            if filtered_data is not None and not filtered_data.empty:
                print(f"\nAperçu des résultats pour le cas {i} :")
                print(filtered_data.head())
                print(filtered_data.shape)
            else:
                print(f"\nAucune ligne ne correspond aux critères pour le cas {i} ou une erreur s'est produite.")

    # Chemins des fichiers
    base_path = get_env_path('BASE_PATH', 'data/output/output_parquet/')
    main_file = base_path / "mere_fille_with_type.parquet"
    case_files = [base_path / f"filteredcase{i}_result.parquet" for i in range(1, 23)]
    output_file = base_path / "mere_fille_with_typev2.parquet"

    # Exécution
    type_data = utils2.filter_cases_general(main_file, case_files, output_file)
    # Affichage des premières lignes du résultat si disponible
    if type_data is not None and not type_data.empty:
        print("\nAperçu des résultats :")
        print(type_data.head())
        print(type_data.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    # Convertir en numérique en gérant les valeurs problématiques
    type_data['ANNEE'] = pd.to_numeric(type_data['ANNEE'], errors='coerce')

    # Convertir en int en arrondissant d'abord (pour éviter les problèmes de décimales)
    type_data['ANNEE'] = type_data['ANNEE'].round().astype('Int64')  # notez le 'Int64' avec majuscule

    # Obtenir les dimensions du DataFrame
    dimensions = type_data.shape

    # Afficher le nombre de lignes et de colonnes
    print(f'Nombre de lignes : {dimensions[0]}, Nombre de colonnes : {dimensions[1]}')

    # Obtenir les noms des colonnes
    noms_colonnes = type_data.columns
    print("\nListe de toutes les colonnes :")
    print(noms_colonnes)

    # Identifier les colonnes avec des nombres décimaux
    colonnes_decimales = type_data.select_dtypes(include=['float64', 'float32']).columns

    # Pour avoir uniquement les colonnes avec des valeurs vraiment décimales (pas des entiers stockés en float)
    vraies_colonnes_decimales = [col for col in type_data.columns if 
                            type_data[col].dtype in ['float64', 'float32'] and 
                            any(x % 1 != 0 for x in type_data[col].dropna())]

    print("\nColonnes contenant des nombres décimaux :")
    print(list(colonnes_decimales))

    # Afficher aussi les types de données pour chaque colonne
    print("\nTypes de données pour chaque colonne :")
    print(type_data.dtypes)

    base_path = get_env_path('BASE_PATH', 'data/output/output_parquet/')
    main_file = base_path / "mere_fille_with_typev2.parquet"  # Utiliser v2 comme fichier d'entrée

    # Liste des nouveaux fichiers de cas
    case_files = [base_path / f"filteredcase{i}_result.parquet" for i in range(17, 23)]
    output_file = base_path / "mere_fille_with_typev3.parquet"  # Créer une v3

    # Exécution
    type_data = utils2.filter_cases_general_v2(main_file, case_files, output_file)

    # Affichage des statistiques si disponible
    if type_data is not None and not type_data.empty:
        print("\nAperçu des résultats :")
        print(type_data.head())
        print(f"Dimensions du DataFrame: {type_data.shape}")
        
        # Afficher la distribution des types
        print("\nDistribution des types:")
        print(type_data['type'].value_counts())
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    # obtenir fichier final traitement de chaques cas
    input_file = get_env_path('INPUT_FINAL_FILE', 'data/output/output_parquet/mere_fille_with_typev3.parquet')
    output_file = get_env_path('OUTPUT_FINAL_FILE', 'data/output/output_parquet/resultat_final.parquet')

    # Lancer le traitement
    result = utils2.apply_and_save_cases(input_file, output_file)

    # Lire le fichier parquet
    df = pd.read_parquet(output_file)

    # Liste des colonnes à convertir 
    colonnes_a_convertir = ['ANNEE', 'CAHT', 'NBR_SAL', 'NBR_SCT_GRP', 'MT_CR_IMP_GRP', 'NBR_CHERCH_TECH',
                        'NB_JD', 'DOT_AMORT_IMMO', 'DOT_AMORT_IMMO_SINISTR', 'DEP_CHERCH_TECH',
                        'REM_SAL_INV', 'DEP_JD', 'OTR_DEP_FONCT', 'MT_DEP_FONCT_TOT', 'FRAIS_BREV_COV',
                        'DEP_MAINT_BREV_COV', 'DOT_AMORT_BREV', 'DEP_NORMALI', 'PRIM_COTIZ',
                        'DEP_VEIL_TECHNO', 'MT_TOT_RD_1', 'DEP_EXT_OPR_LIE_FR', 'DEP_EXT_OPR_LIE_ETR',
                        'DEP_EXT_OPR_NON_LIE_FR', 'DEP_EXT_OPR_NON_LIE_ETR', 'DEP_EXT_OPR_TOT',
                        'DEP_EXT_LIE_FR', 'DEP_EXT_LIE_ETR', 'DEP_EXT_NON_LIE_FR', 'DEP_EXT_NON_LIE_ETR',
                        'MT_TOT_DEP_EXT_ORG_AGREE', 'PLAF_OP_EXT', 'MT_TOT_OP_SOUS_TRAIT',
                        'PLAF_OP_EXT_ORG_AGRE_LIE', 'PLAF_OP_EXT_ORG_AGRE_NON_LIE', 'PLAF_GNRL_DEP_EXT',
                        'MT_DEP_EXT_PLAF', 'MT_TOT_RD_2', 'MT_AID_SUBV', 'MT_ENC_PRESTA',
                        'MT_DEP_CONSEILS_CIR', 'REMBST_SUBV', 'MT_NET_DEP_RD', 'MT_NET_DEP_RD_DOM',
                        'MT_CIR_RECH_HORS_QP', 'MT_QP_CIR_RECU', 'MT_CIR_RECH_YC_QP_MOINS_DE100',
                        'MT_CIR_RECH_YC_QP_MOINS_DE100_DOM', 'FRAIS_COLL', 'FRAIS_DEF_DESSIN',
                        'MT_TOT_DEP_COLL', 'MT_AID_SUBV_COLL', 'MT_DEP_CONSEILS_CIR_COLL',
                        'REMBST_SUBV_COLL', 'MT_NET_DEP_COLL', 'MT_NET_DEP_COLL_DOM',
                        'MT_NET_DEP_COLL_MOINS_DE100', 'MT_NET_DEP_COLL_MOINS_DE100_DOM',
                        'MT_CI_COLL_AVT_PLAF_et_QP_MOINS_DE100', 'MT_QP_COLL_RECU_MOINS_DE100',
                        'MT_CI_COLL_AVT_MINIMI_MOINS_DE100', 'MT_CIR_COLL_AVT_MINIMI_MOINS_DE100_DOM',
                        'MT_AIDE_MINIMI_MOINS_DE100', 'MT_CUMUL_CI_COLL_MINIMI',
                        'MT_CI_COLL_APRS_MINIMI', 'MT_CI_COLL_APRS_MINIMI_DOM',
                        'MT_NET_DEP_RD_et_COLL_MOINS_DE100', 'MT_NET_DEP_RD_et_COLL_MOINS_DE100_DOM',
                        'MT_CIR_RECH_et_COLL_MOINS_DE100', 'MT_CIR_RECH_et_COLL_MOINS_DE100_DOM',
                        'MT_NET_DEP_RD_MOINS_DE100', 'MT_NET_DEP_RD_MOINS_DE_100_DOM',
                        'MT_NET_DEP_RD_MOINS_DE100_BIS', 'MT_NET_DEP_RD_MOINS_DE100_BIS_DOM',
                        'PART_CIR_RECH_30', 'DEP_RECH_SUP_PLAF', 'PART_CIR_RECH_5',
                        'MT_CIR_RECH_PLUS_DE100_HORS_QP', 'MT_QP_CIR_RECU_BIS',
                        'MT_CIR_RECH_PLUS_DE100_YC_QP', 'MT_CIR_RECH_PLUS_DE100_YC_QP_DOM',
                        'MT_NET_DEP_COLL_BIS', 'MT_NET_DEP_COLL_BIS_DOM', 'PLAF_COLL_DISPO',
                        'PART_CI_COLL_30', 'PART_CI_COLL_5', 'MT_CI_COLL_PLUS_DE100_HORS_QP',
                        'MT_QP_COLL_RECU_BIS', 'MT_CI_COLL_PLUS_DE100', 'MT_AIDE_MINIMI_COLL',
                        'MT_CUMUL_CI_COLL_MINIMI_PLUS_DE100', 'MT_CI_COLL_APRES_MINIMI_PLUS_DE100',
                        'MT_CI_COLL_APRES_MINIMI_PLUS_DE100_DOM', 'MT_TOT_CIR_RECH_COLL_PLUS_DE100',
                        'MT_TOT_CIR_RECH_COLL_PLUS_DE100_DOM', 'DOT_AMORT_IMMO_INO']

    # Convertir chaque colonne
    for colonne in colonnes_a_convertir:
        if colonne in df.columns:
            df[colonne] = pd.to_numeric(df[colonne], errors='coerce').round().astype('Int64')

    # Créer le nouveau nom de fichier dans le même répertoire
    output_file = get_env_path('OUTPUT_NEW_MILLESIME', 'data/output/output_parquet/new_millesime_CIR_2022.parquet')

    # Sauvegarder le fichier
    df.to_parquet(output_file)

    print(f"Fichier sauvegardé sous : {output_file}")

    # Vérifier que la conversion s'est bien passée
    print("\nTypes des colonnes après conversion :")
    for col in colonnes_a_convertir:
        if col in df.columns:
            print(f"{col}: {df[col].dtype}")
    
    # Conversion des fichiers en csv et excel
    parquet_dir = get_env_path('PARQUET_DIR', 'data/output/output_parquet')
    utils.parquet_to_csv(parquet_dir)
    utils.parquet_to_xlsx(parquet_dir)