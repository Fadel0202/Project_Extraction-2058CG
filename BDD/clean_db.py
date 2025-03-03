import psycopg2

try:
    # Connexion avec le mot de passe
    conn = psycopg2.connect(
        dbname="sittarc1",
        user="mouhamed",
        password="dgri-sittarc1",  # remplace par ton mot de passe
        host="localhost"
    )
    conn.autocommit = True
    cursor = conn.cursor()
    print("Connexion réussie")
    
    # Supprimer toutes les données dans l'ordre pour respecter les contraintes de clés étrangères
    tables = [
        "credit_impot",
        "evolution_cir",
        "annexe_2069a12",
        "societe_fille",
        "formulaire_2069a",
        "formulaire_2058cg",
        "depot",
        "societe",
        "annee_fiscale"
    ]
    
    for table in tables:
        print(f"Suppression des données de la table {table}")
        cursor.execute(f"DELETE FROM {table};")
    
    print("Toutes les données ont été supprimées!")
    
    # Réinitialiser les séquences
    sequences = [
        "annee_fiscale_millesime_id_seq",
        "annexe_2069a12_annexe_2069a12_id_seq",
        "credit_impot_id_seq",
        "evolution_cir_id_seq", 
        "formulaire_2058cg_formulaire_2058cg_id_seq", 
        "formulaire_2069a_formulaire_2069a_id_seq",
        "societe_fille_id_seq",
        "societe_siren_seq"
    ]
    
    for seq in sequences:
        print(f"Réinitialisation de la séquence {seq}")
        try:
            cursor.execute(f"ALTER SEQUENCE {seq} RESTART WITH 1;")
        except Exception as e:
            print(f"Erreur lors de la réinitialisation de {seq}: {e}")
    
    print("Toutes les séquences ont été réinitialisées!")
    
    # Vérifier que les tables sont vides
    print("\nVérification des tables:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        count = cursor.fetchone()[0]
        print(f"Table {table}: {count} lignes")
    
except Exception as e:
    print(f"Erreur: {e}")
finally:
    if 'conn' in locals():
        cursor.close()
        conn.close()
        print("Connexion fermée")