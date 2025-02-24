import os
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path
from extraction.model import SIRENAnomalyDetector

def format_date(date_str):
    """Formate une chaîne de date au format YYYY-MM-DD."""
    if pd.isna(date_str) or not date_str:
        return None
    return date_str[:10]

def get_efisc_codes():
    """Renvoie un dictionnaire des codes EFISC pour le formulaire 2058CG."""
    return {
        'siren_societe': '909475',
        'denomination_societe': '303254',
        'complement_denomination': '303255',
        'forme_juridique': '306857',
        'vent_ci_siren': '909476',
        'vent_ci_denomination': '309342',
        'vent_ci_complement_denom': '309343',
        'vent_ci_forme_juridique': '309344',
        'reduction_credit_impot': '908027',
        'montant_credit_impot': '908028',
        'precision_utilisation_ci': '908029',
        'type_reduction_ci': '908030',
        'total_type_reduction_ci': '908031',
        'creances_report_filiales': '303257',
        'total_creances_report_filiales': '303260',
        'creances_utilisees_fille': '303258',
        'total_creances_utilisees_fille': '303261',
        'credits_reportables': '309345',
        'total_credits_reportables': '309348',
        'credits_non_report_restituables': '309346',
        'total_credits_non_report_restituables': '309349',
        'credits_non_report_non_restituables': '309347',
        'total_credits_non_report_non_restituables': '309350',
        'depose_neant': '305747'
    }

def get_data_directory():
    """Renvoie le chemin du répertoire de données."""
    return Path(os.getenv('DATA_DIRECTORY', 'data'))

def load_data(file_name):
    """Charge les données à partir d'un fichier parquet."""
    data_directory = get_data_directory()
    file_path = data_directory / file_name
    return pd.read_parquet(file_path)

def save_data(df, file_name):
    """Sauvegarde les données dans un fichier parquet."""
    data_directory = get_data_directory()
    file_path = data_directory / file_name
    df.to_parquet(file_path, index=False)

def process_dates(df):
    """Traite les dates et calcule le millésime."""
    if df.empty:
        return df
        
    df['datedebut'] = df['dateDebPer'].apply(format_date)
    df['datefin'] = df['dateFinPer'].apply(format_date)
    
    df['dateD'] = pd.to_datetime(df['datedebut'])
    df['dateF'] = pd.to_datetime(df['datefin'])
    df['datedebleg'] = df['dateD'] + pd.Timedelta(days=1)
    df['datefinleg'] = df['dateF'] + pd.Timedelta(days=1)
    
    df['duree'] = (df['datefinleg'] - df['datedebleg']).dt.days
    df['andeb'] = df['datedebleg'].dt.year
    df['anfin'] = df['datefinleg'].dt.year
    
    df['millesime_calcule'] = df.apply(
        lambda x: x['andeb'] % 100 if x['duree'] <= 366 else x['anfin'] % 100,
        axis=1
    )
    
    return df

def process_depot(depot):
    """Traite un dépôt et extrait les informations sur la société mère et ses filiales."""
    try:
        entete = depot.find('.//enteteDepot')
        if entete is None:
            return None
        
        mere_siren = entete.get('siren', '')
        id_depot = entete.get('idDepot', '')
        
        if not mere_siren:
            return None

        data_list = []
        for form in depot.findall(".//formulaire[@noForm='2058CG']"):
            # Récupération du nombre de filiales
            ireps = []
            for efisc in form.findall('.//efisc'):
                irep = efisc.get('iRepEF', '')
                if irep and irep.isdigit():
                    ireps.append(int(irep))
            
            nb_filiales = max(ireps) if ireps else 0
            
            # Ensemble pour stocker les indices des filiales avec des données
            filiales_avec_donnees = set()
            
            # Premier passage pour identifier les filiales avec des données
            for efisc in form.findall('.//efisc'):
                irep = efisc.get('iRepEF', '')
                if irep and irep.isdigit():
                    filiale_idx = int(irep)
                    filiales_avec_donnees.add(filiale_idx)
            
            nb_filiales_renseignees = len(filiales_avec_donnees)
            
            # Traitement de chaque filiale
            for filiale_idx in range(1, nb_filiales + 1):
                record = {
                    'mere_siren': mere_siren,
                    'id_depot': id_depot,
                    'filiale_index': filiale_idx,
                    'dateDebPer': entete.get('dateDebPer', ''),
                    'dateFinPer': entete.get('dateFinPer', ''),
                    'dateEnregistrement': entete.get('dateEnregistrement', ''),
                    'nombre_filiales': nb_filiales,
                    'nombre_filiales_renseignees': nb_filiales_renseignees
                }
                
                # Initialiser toutes les colonnes EFISC avec None
                for name in get_efisc_codes().keys():
                    record[name] = None
                
                # Vérifier si la filiale a des codes EFISC
                has_efisc_data = False
                
                # Mettre à jour avec les valeurs trouvées
                for efisc in form.findall('.//efisc'):
                    irep = efisc.get('iRepEF', '')
                    if irep and irep.isdigit() and int(irep) == filiale_idx:
                        code = efisc.get('cdEfisc', '')
                        value = efisc.get('vlEfisc', '')
                        
                        for name, efisc_code in get_efisc_codes().items():
                            if efisc_code == code:
                                record[name] = value
                                has_efisc_data = True
                                break
                
                # N'ajouter le record que si la filiale a des données EFISC
                if has_efisc_data:
                    data_list.append(record)
        
        return data_list
        
    except Exception as e:
        print(f"Erreur lors du traitement du dépôt : {str(e)}")
        return None

def update_millesime_file(data, output_dir):
    """Met à jour les fichiers par millésime."""
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Supprimer la colonne millesime avant de sauvegarder
        if 'millesime' in data.columns:
            data = data.drop('millesime', axis=1)
        
        for mill in data['millesime_calcule'].unique():
            mill_data = data[data['millesime_calcule'] == mill]
            file_path = Path(output_dir) / f"2058CG_millesime_{int(mill)}.parquet"
            
            if file_path.exists():
                existing_data = pd.read_parquet(file_path)
                key_columns = ['mere_siren', 'id_depot', 'filiale_index', 'dateDebPer', 'dateFinPer']
                existing_keys = set(existing_data[key_columns].apply(tuple, axis=1))
                new_data_mask = ~mill_data[key_columns].apply(tuple, axis=1).isin(existing_keys)
                new_data = mill_data[new_data_mask]
                
                if not new_data.empty:
                    final_data = pd.concat([existing_data, new_data], ignore_index=True)
                    final_data.to_parquet(file_path, index=False)
                    print(f"Millésime {mill}: {len(new_data)} nouveaux formulaires ajoutés")
            else:
                mill_data.to_parquet(file_path, index=False)
                print(f"Millésime {mill}: {len(mill_data)} formulaires enregistrés")
                
        return True, None
    except Exception as e:
        return False, str(e)
    
def extract_data(xml_file):
    """Extrait les données du fichier XML."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        all_data = []
        
        for depot in root.findall('.//depot'):
            depot_data = process_depot(depot)
            if depot_data:
                all_data.extend(depot_data)
        
        df = pd.DataFrame(all_data)
        if not df.empty:
            df = process_dates(df)
            
            numeric_columns = [
                'montant_credit_impot',
                'total_type_reduction_ci',
                'total_creances_report_filiales',
                'total_creances_utilisees_fille',
                'total_credits_reportables',
                'total_credits_non_report_restituables',
                'total_credits_non_report_non_restituables'
            ]
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
        return df, None
        
    except Exception as e:
        return pd.DataFrame(), str(e)

def process_directory(input_dir: str, output_dir: str, tracker_file: str, contamination: float = 0.1):
    """Traite tous les fichiers XML d'un répertoire."""
    print("Démarrage du traitement...")
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    if Path(tracker_file).exists():
        tracker_df = pd.read_parquet(tracker_file)
        processed_files = set(tracker_df['fichier'])
    else:
        tracker_df = pd.DataFrame(columns=['fichier', 'date_traitement', 'nombre_formulaires', 'statut', 'message_erreur'])
        processed_files = set()
    
    xml_files = list(Path(input_dir).glob('**/*.xml'))
    if not xml_files:
        print("Aucun fichier XML trouvé.")
        return
    
    print(f"Nombre de fichiers à traiter : {len(xml_files)}")
    
    for xml_file in xml_files:
        xml_path = str(xml_file)
        
        if xml_path in processed_files:
            print(f"Fichier déjà traité : {xml_file}")
            continue
            
        print(f"\nTraitement de {xml_file}")
        
        df, extract_error = extract_data(xml_file)
        
        if extract_error:
            print(f"Erreur lors de l'extraction : {extract_error}")
            tracker_df = pd.concat([tracker_df, pd.DataFrame({
                'fichier': [xml_path],
                'date_traitement': [datetime.now()],
                'nombre_formulaires': [0],
                'statut': ['ERREUR'],
                'message_erreur': [f"Erreur d'extraction: {extract_error}"]
            })], ignore_index=True)
            continue
            
        if df.empty:
            print("Aucune donnée extraite")
            tracker_df = pd.concat([tracker_df, pd.DataFrame({
                'fichier': [xml_path],
                'date_traitement': [datetime.now()],
                'nombre_formulaires': [0],
                'statut': ['VIDE'],
                'message_erreur': ["Aucune donnée extraite"]
            })], ignore_index=True)
            continue
        
        success, update_error = update_millesime_file(df, output_dir)
        
        if not success:
            print(f"Erreur lors de la mise à jour des millésimes : {update_error}")
            tracker_df = pd.concat([tracker_df, pd.DataFrame({
                'fichier': [xml_path],
                'date_traitement': [datetime.now()],
                'nombre_formulaires': [len(df)],
                'statut': ['ERREUR'],
                'message_erreur': [f"Erreur mise à jour millésimes: {update_error}"]
            })], ignore_index=True)
            continue
        
        tracker_df = pd.concat([tracker_df, pd.DataFrame({
            'fichier': [xml_path],
            'date_traitement': [datetime.now()],
            'nombre_formulaires': [len(df)],
            'statut': ['SUCCES'],
            'message_erreur': [""]
        })], ignore_index=True)
        
        print(f"Traité avec succès : {len(df)} formulaires")
        
    tracker_df.to_parquet(tracker_file, index=False)
    
    print("\nTraitement terminé.")
    print("\nRésumé des traitements :")
    print(f"Fichiers traités : {len(tracker_df)}")
    print(f"Total formulaires : {tracker_df['nombre_formulaires'].sum()}")
    print("\nStatut des traitements :")
    print(tracker_df['statut'].value_counts())

def process_directory(directory: str, contamination: float = 0.1) -> pd.DataFrame:
    # Lecture des données
    all_data = pd.DataFrame()
    directory = Path(directory)
    
    for file in directory.glob('*.parquet'):
        try:
            df = pd.read_parquet(file)
            df['source_file'] = file.name
            all_data = pd.concat([all_data, df]) if not all_data.empty else df
        except Exception as e:
            print(f"Erreur lors de la lecture de {file}: {str(e)}")
            continue
    
    # Détection et correction des anomalies
    detector = SIRENAnomalyDetector(contamination=contamination)
    results = detector.fit_predict(all_data)
    
    # Sauvegarde des résultats
    anomalies_df = results[results['is_anomaly']].copy()
    output_file = directory / "anomalies_siren_luhn.parquet"
    anomalies_df.to_parquet(output_file)
    
    # Statistiques
    print("\nSTATISTIQUES DES ANOMALIES SIREN")
    print("=" * 50)
    print(f"Total enregistrements analysés: {len(all_data)}")
    print(f"Nombre d'anomalies détectées: {len(anomalies_df)}")
    print("\nSources des corrections:")
    print(anomalies_df['source_correction'].value_counts())
    
    return results

if __name__ == "__main__":
    input_dir = os.getenv('INPUT_DIR', 'data/input')
    output_dir = os.getenv('OUTPUT_DIR', 'data/output')
    tracker_file = os.getenv('TRACKER_FILE', 'data/output/processed_files_tracker.parquet')
    
    process_directory(input_dir, output_dir, tracker_file)
    process_directory(input_dir)