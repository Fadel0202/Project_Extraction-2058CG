import psycopg2
from tabulate import tabulate

# Connexion à la base de données PostgreSQL
try:
    conn = psycopg2.connect(
        dbname="sittarc1",
        user="mouhamed",
        password="dgri-sittarc1",
        host="localhost"
    )
    cursor = conn.cursor()
    print("Connexion à la base de données réussie")
except Exception as e:
    print(f"Erreur de connexion à la base de données: {e}")
    exit(1)

try:
    # Vérification des sociétés
    print("\n=== SOCIÉTÉS ===")
    cursor.execute("SELECT siren, denomination_societe FROM societe")
    results = cursor.fetchall()
    print(tabulate(results, headers=["SIREN", "Dénomination"], tablefmt="pretty"))
    print(f"Nombre total de sociétés: {len(results)}")

    # Vérification des dépôts
    print("\n=== DÉPÔTS ===")
    cursor.execute("SELECT id_depot, millesime_id, siren, dateDebPer, dateFinPer FROM depot")
    results = cursor.fetchall()
    print(tabulate(results, headers=["ID Dépôt", "Millésime", "SIREN", "Date Début", "Date Fin"], tablefmt="pretty"))
    print(f"Nombre total de dépôts: {len(results)}")

    # Vérification des formulaires 2058CG
    print("\n=== FORMULAIRES 2058CG ===")
    cursor.execute("""
        SELECT f.formulaire_2058cg_id, f.id_depot, f.siren_societe, s.denomination_societe 
        FROM formulaire_2058cg f
        JOIN societe s ON f.siren_societe = s.siren
    """)
    results = cursor.fetchall()
    print(tabulate(results, headers=["ID", "ID Dépôt", "SIREN", "Dénomination"], tablefmt="pretty"))
    print(f"Nombre total de formulaires 2058CG: {len(results)}")

    # Vérification des sociétés filles
    print("\n=== SOCIÉTÉS FILLES ===")
    cursor.execute("""
        SELECT sf.id, sf.formulaire_2058cg_id, sf.siren_fille, s.denomination_societe 
        FROM societe_fille sf
        JOIN societe s ON sf.siren_fille = s.siren
    """)
    results = cursor.fetchall()
    print(tabulate(results, headers=["ID", "ID Form 2058CG", "SIREN Fille", "Dénomination"], tablefmt="pretty"))
    print(f"Nombre total de sociétés filles: {len(results)}")

    # Vérification des formulaires 2069A
    print("\n=== FORMULAIRES 2069A ===")
    cursor.execute("""
        SELECT f.formulaire_2069a_id, f.id_depot, f.siren_societe_deposant, f.siren_societe_declarant,
               s1.denomination_societe as deposant, s2.denomination_societe as declarant
        FROM formulaire_2069a f
        JOIN societe s1 ON f.siren_societe_deposant = s1.siren
        JOIN societe s2 ON f.siren_societe_declarant = s2.siren
    """)
    results = cursor.fetchall()
    print(tabulate(results, headers=["ID", "ID Dépôt", "SIREN Déposant", "SIREN Déclarant", "Dénomination Déposant", "Dénomination Déclarant"], tablefmt="pretty"))
    print(f"Nombre total de formulaires 2069A: {len(results)}")

    # Vérification des annexes 2069A12
    print("\n=== ANNEXES 2069A12 ===")
    cursor.execute("""
        SELECT a.annexe_2069a12_id, a.formulaire_2069a_id, f.id_depot
        FROM annexe_2069a12 a
        JOIN formulaire_2069a f ON a.formulaire_2069a_id = f.formulaire_2069a_id
    """)
    results = cursor.fetchall()
    print(tabulate(results, headers=["ID Annexe", "ID Form 2069A", "ID Dépôt"], tablefmt="pretty"))
    print(f"Nombre total d'annexes 2069A12: {len(results)}")

    # Vérification des crédits d'impôt
    print("\n=== CRÉDITS D'IMPÔT ===")
    cursor.execute("""
        SELECT ci.id, ci.siren_fille, s.denomination_societe, ci.formulaire_2069a_id, 
               ci.type_credit, ci.montant_credit_impot
        FROM credit_impot ci
        JOIN societe s ON ci.siren_fille = s.siren
    """)
    results = cursor.fetchall()
    print(tabulate(results, headers=["ID", "SIREN", "Dénomination", "ID Form 2069A", "Type Crédit", "Montant"], tablefmt="pretty"))
    print(f"Nombre total de crédits d'impôt: {len(results)}")

except Exception as e:
    print(f"Erreur lors de la vérification des données: {e}")
finally:
    # Fermeture de la connexion
    cursor.close()
    conn.close()
    print("\nConnexion à la base de données fermée")