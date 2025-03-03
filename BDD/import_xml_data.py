import xml.etree.ElementTree as ET
import psycopg2
from datetime import datetime

# Fonction pour convertir les dates du format XML au format PostgreSQL
def convert_date(date_str):
    if date_str:
        # Format de date dans le XML: 2051-05-12T23:34:52.000Z
        dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        return dt.strftime('%Y-%m-%d')
    return None

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

# Parsage du fichier XML
try:
    tree = ET.parse('test_CIR_DGFIP_03-02-2025.xml')
    root = tree.getroot()
    print("Fichier XML parsé avec succès")
except Exception as e:
    print(f"Erreur lors du parsage du fichier XML: {e}")
    exit(1)

# Début de la transaction
conn.autocommit = False

try:
    # 1. Insertion dans la table annee_fiscale
    # On considère l'année fiscale basée sur dateDebPer et dateFinPer du premier depot
    first_depot = root.find('.//depot')
    if first_depot is not None:
        date_deb = convert_date(first_depot.find('enteteDepot').get('dateDebPer'))
        date_fin = convert_date(first_depot.find('enteteDepot').get('dateFinPer'))
        millesime = first_depot.find('enteteDepot').get('millesimeIdentification')
        
        # Vérifier si l'année fiscale existe déjà
        cursor.execute("SELECT millesime_id FROM annee_fiscale WHERE millesime_id = %s", (millesime,))
        if cursor.fetchone() is None:
            cursor.execute(
                "INSERT INTO annee_fiscale (millesime_id, date_debut, date_fin, duree) VALUES (%s, %s, %s, 365) RETURNING millesime_id",
                (millesime, date_deb, date_fin)
            )
            print(f"Année fiscale insérée: {millesime}")
        else:
            print(f"Année fiscale {millesime} existe déjà")
    
    # 2. Parcourir tous les dépôts
    for depot_elem in root.findall('.//depot'):
        entete = depot_elem.find('enteteDepot')
        
        # Extraction des données du dépôt
        id_depot = entete.get('idDepot')
        millesime_id = entete.get('millesimeIdentification')
        siren = entete.get('siren')
        date_deb_per = convert_date(entete.get('dateDebPer'))
        date_fin_per = convert_date(entete.get('dateFinPer'))
        date_enregistrement = convert_date(entete.get('dateEnregistrement'))
        
        # 2.1 Insertion dans la table societe (si elle n'existe pas déjà)
        cursor.execute("SELECT siren FROM societe WHERE siren = %s", (siren,))
        if cursor.fetchone() is None:
            # Recherche de la dénomination dans les formulaires
            denomination = None
            for form in depot_elem.findall('.//formulaire'):
                for efisc in form.findall('.//efisc[@cdEfisc="912541"]'):
                    if efisc.get('vlEfisc'):
                        denomination = efisc.get('vlEfisc')
                        break
                if denomination:
                    break
            
            if not denomination:
                for form in depot_elem.findall('.//formulaire'):
                    for efisc in form.findall('.//efisc[@cdEfisc="303254"]'):
                        if efisc.get('vlEfisc'):
                            denomination = efisc.get('vlEfisc')
                            break
                    if denomination:
                        break
            
            if denomination:
                cursor.execute(
                    "INSERT INTO societe (siren, denomination_societe) VALUES (%s, %s)",
                    (siren, denomination)
                )
                print(f"Société insérée: {siren} - {denomination}")
        
        # 2.2 Insertion dans la table depot
        cursor.execute("SELECT id_depot FROM depot WHERE id_depot = %s", (id_depot,))
        if cursor.fetchone() is None:
            cursor.execute(
                "INSERT INTO depot (id_depot, millesime_id, siren, dateDebPer, dateFinPer, dateEnregistrement, depose_neant) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (id_depot, millesime_id, siren, date_deb_per, date_fin_per, date_enregistrement, False)
            )
            print(f"Dépôt inséré: {id_depot}")
        
        # 3. Traitement des formulaires
        for form in depot_elem.findall('.//formulaire'):
            no_form = form.get('noForm')
            
            # 3.1 Traitement du formulaire 2058CG
            if no_form == '2058CG':
                cursor.execute(
                    "INSERT INTO formulaire_2058cg (id_depot, siren_societe) VALUES (%s, %s) RETURNING formulaire_2058cg_id",
                    (id_depot, siren)
                )
                formulaire_2058cg_id = cursor.fetchone()[0]
                print(f"Formulaire 2058CG inséré: {formulaire_2058cg_id}")
                
                # Extraction des sociétés filles depuis le formulaire 2058CG
                siren_filles = {}
                for efisc in form.findall('.//efisc[@cdEfisc="909476"]'):
                    rep = efisc.get('iRepEF')
                    if rep != '0':  # Si ce n'est pas la société mère
                        siren_fille = efisc.get('vlEfisc')
                        siren_filles[rep] = {'siren': siren_fille}
                
                # Récupération des noms des sociétés filles
                for efisc in form.findall('.//efisc[@cdEfisc="309342"]'):
                    rep = efisc.get('iRepEF')
                    if rep in siren_filles:
                        siren_filles[rep]['nom'] = efisc.get('vlEfisc')
                
                # Récupération des montants de crédit d'impôt
                for efisc in form.findall('.//efisc[@cdEfisc="908028"]'):
                    rep = efisc.get('iRepEF')
                    if rep in siren_filles:
                        montant = float(efisc.get('vlEfisc'))
                        type_credit = None
                        
                        # Chercher le type de crédit correspondant
                        for type_efisc in form.findall(f'.//efisc[@cdEfisc="908027"][@iRepEF="{rep}"]'):
                            type_credit = type_efisc.get('vlEfisc')
                        
                        if not 'credits' in siren_filles[rep]:
                            siren_filles[rep]['credits'] = []
                        
                        siren_filles[rep]['credits'].append({
                            'type': type_credit,
                            'montant': montant
                        })
                
                # Insertion des sociétés filles et de leurs crédits d'impôt
                for rep, data in siren_filles.items():
                    # Vérifier si la société fille existe déjà
                    cursor.execute("SELECT siren FROM societe WHERE siren = %s", (data['siren'],))
                    if cursor.fetchone() is None and 'nom' in data:
                        cursor.execute(
                            "INSERT INTO societe (siren, denomination_societe) VALUES (%s, %s)",
                            (data['siren'], data['nom'])
                        )
                        print(f"Société fille insérée: {data['siren']} - {data['nom']}")
                    
                    # Insérer dans la table societe_fille
                    cursor.execute(
                        "INSERT INTO societe_fille (formulaire_2058cg_id, siren_fille) VALUES (%s, %s) RETURNING id",
                        (formulaire_2058cg_id, data['siren'])
                    )
                    societe_fille_id = cursor.fetchone()[0]
                    print(f"Société fille liée: {data['siren']} - ID: {societe_fille_id}")
                    
                    # Insérer les crédits d'impôt
                    if 'credits' in data:
                        for credit in data['credits']:
                            cursor.execute(
                                "INSERT INTO credit_impot (siren_fille, type_credit, montant_credit_impot) VALUES (%s, %s, %s)",
                                (data['siren'], credit['type'], credit['montant'])
                            )
                            print(f"Crédit d'impôt inséré pour {data['siren']}: {credit['type']} - {credit['montant']}")
            
            # 3.2 Traitement du formulaire 2069A
            elif no_form == '2069A':
                siren_declarant = None
                denomination_declarant = None
                
                for efisc in form.findall('.//efisc[@cdEfisc="906059"]'):
                    siren_declarant = efisc.get('vlEfisc')
                    break
                
                for efisc in form.findall('.//efisc[@cdEfisc="912541"]'):
                    denomination_declarant = efisc.get('vlEfisc')
                    break
                
                if siren_declarant:
                    # Vérifier si la société déclarante existe déjà
                    cursor.execute("SELECT siren FROM societe WHERE siren = %s", (siren_declarant,))
                    if cursor.fetchone() is None and denomination_declarant:
                        cursor.execute(
                            "INSERT INTO societe (siren, denomination_societe) VALUES (%s, %s)",
                            (siren_declarant, denomination_declarant)
                        )
                        print(f"Société déclarante insérée: {siren_declarant} - {denomination_declarant}")
                    
                    # Insérer le formulaire 2069A
                    cursor.execute(
                        "INSERT INTO formulaire_2069a (id_depot, siren_societe_deposant, siren_societe_declarant) VALUES (%s, %s, %s) RETURNING formulaire_2069a_id",
                        (id_depot, siren, siren_declarant)
                    )
                    formulaire_2069a_id = cursor.fetchone()[0]
                    print(f"Formulaire 2069A inséré: {formulaire_2069a_id}")
                    
                    # Extraire et insérer le montant du crédit d'impôt
                    montant_ci = None
                    for efisc in form.findall('.//efisc[@cdEfisc="906055"]'):
                        montant_ci = float(efisc.get('vlEfisc'))
                        break
                    
                    if montant_ci:
                        cursor.execute(
                            "INSERT INTO credit_impot (siren_fille, formulaire_2069a_id, type_credit, montant_credit_impot) VALUES (%s, %s, %s, %s)",
                            (siren_declarant, formulaire_2069a_id, 'CIR', montant_ci)
                        )
                        print(f"Crédit d'impôt CIR inséré pour {siren_declarant}: {montant_ci}")
            
            # 3.3 Traitement de l'annexe 2069A12
            elif no_form == '2069A12':
                # Chercher le formulaire 2069A parent
                cursor.execute(
                    "SELECT formulaire_2069a_id FROM formulaire_2069a WHERE id_depot = %s ORDER BY formulaire_2069a_id DESC LIMIT 1",
                    (id_depot,)
                )
                result = cursor.fetchone()
                if result:
                    formulaire_2069a_id = result[0]
                    
                    # Insérer l'annexe 2069A12
                    cursor.execute(
                        "INSERT INTO annexe_2069a12 (formulaire_2069a_id) VALUES (%s) RETURNING annexe_2069a12_id",
                        (formulaire_2069a_id,)
                    )
                    annexe_id = cursor.fetchone()[0]
                    print(f"Annexe 2069A12 insérée: {annexe_id}")
    
    # Validation de la transaction
    conn.commit()
    print("Importation des données réussie!")

except Exception as e:
    # Annulation de la transaction en cas d'erreur
    conn.rollback()
    print(f"Erreur lors de l'importation des données: {e}")
finally:
    # Fermeture de la connexion
    cursor.close()
    conn.close()
    print("Connexion à la base de données fermée")