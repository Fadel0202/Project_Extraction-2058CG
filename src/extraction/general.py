import extraction.utils as utils
import extraction.utils as utils2
import pandas as pd



if __name__ == "__main__":
    # creattion du fichier mere fille
    utils.create_mere_fille_file(
    "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//millesime_CIR_2022.csv",
    "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//2058CG_millesime_23.parquet",
    "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//2058CG_millesime_22.parquet",
    "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//mere_fille2_result.parquet"
)
    
    # Charger le fichier Parquet
    file_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//mere_fille2_result.parquet"
    df = pd.read_parquet(file_path)

    # Liste des colonnes contenant des SIREN
    siren_columns = ["SIREN_DEC", "siren_DEP", "siren_tete_grp", 
                    "SIR_Mere_de_SIREN-DEC", "SIR_Mere_de_SIREN-DEP", "SIR_Mere_de_SIREN-Tete_de_Grp"]

    # Appliquer la correction uniquement aux colonnes spécifiées
    for col in siren_columns:
        if col in df.columns:
            df[col] = df[col].apply(utils.correct_siren)

    # Sauvegarder le fichier nettoyé sans changer l'index
    output_file_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//mere_fille2_result_cleaned.parquet"
    df.to_parquet(output_file_path, engine="pyarrow", index=True)

    print(f"Fichier nettoyé sauvegardé sous {output_file_path}")

    input_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//mere_fille2_result_cleaned.parquet"
    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase1_result.parquet"

    filtered_data = utils.filter_cases_1(input_path, output_path)

    # Affichage des premières lignes du résultat si disponible
    if filtered_data is not None and not filtered_data.empty:
        print("\nAperçu des résultats :")
        print(filtered_data.head())
        print(filtered_data.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    
    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase2_result.parquet"

    filtered_data2 = utils.filter_cases_2(input_path, output_path)

    # Affichage des premières lignes du résultat si disponible
    if filtered_data2 is not None and not filtered_data2.empty:
        print("\nAperçu des résultats :")
        print(filtered_data2.head())
        print(filtered_data2.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    
    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase3_result.parquet"

    filtered_data3 = utils.filter_cases_3(input_path, output_path)

    # Affichage des premières lignes du résultat si disponible
    if filtered_data3 is not None and not filtered_data3.empty:
        print("\nAperçu des résultats :")
        print(filtered_data3.head())
        print(filtered_data3.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase4_result.parquet"

    filtered_data4 = utils.filter_cases_4(input_path, output_path)

    # Affichage des premières lignes du résultat si disponible
    if filtered_data4 is not None and not filtered_data4.empty:
        print("\nAperçu des résultats :")
        print(filtered_data4.head())
        print(filtered_data4.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    
    # Chemins des fichiers
    base_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//"
    main_file = base_path + "mere_fille2_result_cleaned.parquet"
    case_files = [
        base_path + "filteredcase1_result.parquet",
        base_path + "filteredcase2_result.parquet",
        base_path + "filteredcase3_result.parquet",
        base_path + "filteredcase4_result.parquet"
    ]
    output_file = base_path + "mere_fille_with_type.parquet"

    # Exécution
    type_data = utils.filter_cases_general(main_file, case_files, output_file)
    # Affichage des premières lignes du résultat si disponible
    if type_data is not None and not type_data.empty:
        print("\nAperçu des résultats :")
        print(type_data.head())
        print(type_data.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    input_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//mere_fille_with_type.parquet"
    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase5_result.parquet"

    filtered_data5 = utils.filter_cases_5(input_path, output_path)

    # Affichage des premières lignes du résultat si disponible
    if filtered_data5 is not None and not filtered_data5.empty:
        print("\nAperçu des résultats :")
        print(filtered_data5.head())
        print(filtered_data5.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase6_result.parquet"

    filtered_data6 = utils.filter_cases_6(input_path, output_path)

    # Affichage des premières lignes du résultat si disponible
    if filtered_data6 is not None and not filtered_data6.empty:
        print("\nAperçu des résultats :")
        print(filtered_data6.head())
        print(filtered_data6.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase7_result.parquet"

    filtered_data7 = utils.filter_cases_7(input_path, output_path)

    # Affichage des premières lignes du résultat si disponible
    if filtered_data7 is not None and not filtered_data7.empty:
        print("\nAperçu des résultats :")
        print(filtered_data7.head())
        print(filtered_data7.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase8_result.parquet"

    filtered_data8 = utils.filter_cases_8(input_path, output_path)

    # Affichage des résultats
    if filtered_data8 is not None and not filtered_data8.empty:
        print("\nAperçu des résultats :")
        print(filtered_data8.head())
        print(filtered_data8.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase9_result.parquet"

    filtered_data9 = utils.filter_cases_9(input_path, output_path)

    # Affichage des résultats
    if filtered_data9 is not None and not filtered_data9.empty:
        print("\nAperçu des résultats :")
        print(filtered_data9.head())
        print(filtered_data9.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase10_result.parquet"

    filtered_data10 = utils.filter_cases_10(input_path, output_path)

    # Affichage des résultats
    if filtered_data10 is not None and not filtered_data10.empty:
        print("\nAperçu des résultats :")
        print(filtered_data10.head())
        print(filtered_data10.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")


    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase11_result.parquet"

    filtered_data11 = utils.filter_cases_11(input_path, output_path)

    # Affichage des résultats
    if filtered_data11 is not None and not filtered_data11.empty:
        print("\nAperçu des résultats :")
        print(filtered_data11.head())
        print(filtered_data11.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase12_result.parquet"

    filtered_data12 = utils.filter_cases_12(input_path, output_path)

    # Affichage des résultats
    if filtered_data12 is not None and not filtered_data12.empty:
        print("\nAperçu des résultats :")
        print(filtered_data12.head())
        print(filtered_data12.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase13_result.parquet"

    filtered_data13 = utils.filter_cases_13(input_path, output_path)

    # Affichage des résultats
    if filtered_data13 is not None and not filtered_data13.empty:
        print("\nAperçu des résultats :")
        print(filtered_data13.head())
        print(filtered_data13.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase14_result.parquet"

    filtered_data14 = utils.filter_cases_14(input_path, output_path)

    # Affichage des résultats
    if filtered_data14 is not None and not filtered_data14.empty:
        print("\nAperçu des résultats :")
        print(filtered_data14.head())
        print(filtered_data14.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase15_result.parquet"

    filtered_data15 = utils.filter_cases_15(input_path, output_path)

    # Affichage des résultats
    if filtered_data15 is not None and not filtered_data15.empty:
        print("\nAperçu des résultats :")
        print(filtered_data15.head())
        print(filtered_data15.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase16_result.parquet"

    filtered_data16 = utils.filter_cases_16(input_path, output_path)

    # Affichage des résultats
    if filtered_data16 is not None and not filtered_data16.empty:
        print("\nAperçu des résultats :")
        print(filtered_data16.head())
        print(filtered_data16.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    
    # Chemins des fichiers
    base_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//"
    main_file = base_path + "mere_fille_with_type.parquet"
    case_files = [
        base_path + "filteredcase5_result.parquet",
        base_path + "filteredcase6_result.parquet",
        base_path + "filteredcase7_result.parquet",
        base_path + "filteredcase8_result.parquet",
        base_path + "filteredcase9_result.parquet",
        base_path + "filteredcase10_result.parquet",
        base_path + "filteredcase11_result.parquet",
        base_path + "filteredcase12_result.parquet",
        base_path + "filteredcase13_result.parquet",
        base_path + "filteredcase14_result.parquet",
        base_path + "filteredcase15_result.parquet",
        base_path + "filteredcase16_result.parquet"
    ]
    output_file = base_path + "mere_fille_with_typev2.parquet"

    # Exécution
    type_data = utils2.filter_cases_general(main_file, case_files, output_file)

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

    input_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//mere_fille_with_typev2.parquet"
    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase17_result.parquet"

    filtered_data17 = utils2.filter_cases_17(input_path, output_path)

    # Affichage des résultats
    if filtered_data17 is not None and not filtered_data17.empty:
        print("\nAperçu des résultats :")
        print(filtered_data17.head())
        print(filtered_data17.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase18_result.parquet"

    filtered_data18 = utils2.filter_cases_18(input_path, output_path)

    # Affichage des résultats
    if filtered_data18 is not None and not filtered_data18.empty:
        print("\nAperçu des résultats :")
        print(filtered_data18.head())
        print(filtered_data18.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase19_result.parquet"

    filtered_data19 = utils2.filter_cases_19(input_path, output_path)

    # Affichage des résultats
    if filtered_data19 is not None and not filtered_data19.empty:
        print("\nAperçu des résultats :")
        print(filtered_data19.head())
        print(filtered_data19.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase20_result.parquet"

    filtered_data20 = utils2.filter_cases_20(input_path, output_path)

    # Affichage des résultats
    if filtered_data20 is not None and not filtered_data20.empty:
        print("\nAperçu des résultats :")
        print(filtered_data20.head())
        print(filtered_data20.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase21_result.parquet"

    filtered_data21 = utils2.filter_cases_21(input_path, output_path)

    # Affichage des résultats
    if filtered_data21 is not None and not filtered_data21.empty:
        print("\nAperçu des résultats :")
        print(filtered_data21.head())
        print(filtered_data21.shape)
    else:
        print("\nAucune ligne ne correspond aux critères ou une erreur s'est produite.")

    output_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//filteredcase22_result.parquet"

    filtered_data22 = utils2.filter_cases_22(input_path, output_path)

    # Affichage des résultats
    if filtered_data22 is not None and not filtered_data22.empty:
        print("\nAperçu des résultats :")
        print(filtered_data22.head())
        print(filtered_data22.shape)
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

    base_path = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//"
    main_file = base_path + "mere_fille_with_typev2.parquet"  # Utiliser v2 comme fichier d'entrée

    # Liste des nouveaux fichiers de cas
    case_files = [
        base_path + "filteredcase17_result.parquet",
        base_path + "filteredcase18_result.parquet",
        base_path + "filteredcase19_result.parquet",
        base_path + "filteredcase20_result.parquet",
        base_path + "filteredcase21_result.parquet",
        base_path + "filteredcase22_result.parquet"
    ]

    output_file = base_path + "mere_fille_with_typev3.parquet"  # Créer une v3

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
    input_file = 'M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//mere_fille_with_typev3.parquet'
    output_file = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//resultat_final.parquet"

    # Lancer le traitement
    result = utils2.apply_and_save_cases(input_file, output_file)

    # Lire le fichier parquet
    input_file = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//resultat_final.parquet"
    df = pd.read_parquet(input_file)

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
    output_file = "M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet//new_millesime_CIR_2022.parquet"

    # Sauvegarder le fichier
    df.to_parquet(output_file)

    print(f"Fichier sauvegardé sous : {output_file}")

    # Vérifier que la conversion s'est bien passée
    print("\nTypes des colonnes après conversion :")
    for col in colonnes_a_convertir:
        if col in df.columns:
            print(f"{col}: {df[col].dtype}")
    
    # Conversion des fichiers en csv et excel

    utils.parquet_to_csv("M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet")
    utils.parquet_to_xlsx("M://str-dgri-gecir-donnees-fiscales//x-pour MF-SAMB//output//output_parquet")