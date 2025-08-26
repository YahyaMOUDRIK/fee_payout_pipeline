import sys
import os
import glob
import datetime
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.file_utils import read_yaml_file
from scripts.insert import insert_status_data
from scripts.parse_ import parse_file
from scripts.transform_fields import transform_fields

import pandas as pd
pd.set_option('display.max_columns', None)

def process_file(file_path, structure_path, rules, db_config, mapping_path):
    print(f"Processing file: {file_path}")
    try:
        data = parse_file(file_path, structure_path)

        # Check if there was a RIB validation error
        if data and 'error' in data:
            print(f"Erreur critique dans {os.path.basename(file_path)}: {data['error']}")
            print(f"Traitement du fichier arrêté.")
            return None
        
        transformed_data = transform_fields(data, rules)
        result = insert_status_data(transformed_data, db_config, mapping_path)
        print(f"Completed processing: {file_path}\n")
        return result
    except Exception as e:
        print(f"Error processing file {file_path}: {e}\n")
        return None

if __name__ == "__main__":
    load_dotenv()

    db_config = "config/db_config.yaml"
    mapping_path = "config/retour_sort_mapping.yaml"
    db_config_path = "config/db_config.yaml"
    structure_path = "config/file_structure/fee_payouts_status_structure.yaml"
    rules = "config/retour_sort_transformation_rules.yaml"

    current_date = datetime.datetime.now()
    date_pattern = current_date.strftime("%m%y")
    
    print(f"Looking for newest files")

    # Chemin vers le dossier externe avec restriction d'accès
    external_directory = os.getenv('DATA_DIR_FEE_STATUS')

    # Vérifier si le dossier externe existe et est accessible
    if external_directory and os.path.exists(external_directory) and os.access(external_directory, os.R_OK):
        print(f"Utilisation du dossier externe")
        directory_path = external_directory
    # else:
    #     print(f"Dossier externe non accessible ou non défini, arrêt du traitement")
    #     sys.exit(1)
    else:
        # Fallback vers le dossier de données du projet
        print(f"Dossier externe non accessible, utilisation du dossier local")
        directory_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'fee_payouts_status'))
    
    print(f"Recherche dans: {directory_path}")
    all_files = glob.glob(os.path.join(directory_path, "*"))
    
    target_files = [f for f in all_files if date_pattern in os.path.basename(f)]
 
    
    # directory_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'fee_payouts_status'))
    # all_files = glob.glob(os.path.join(directory_path, "*"))
    
    target_files = [f for f in all_files if date_pattern in os.path.basename(f)]
    
    if not target_files:
        print(f"No recent files in {directory_path}")
    else:
        print(f"Found {len(target_files)} recent files")
        
        # Process each file
        for file_path in target_files:
            process_file(file_path, structure_path, rules, db_config, mapping_path)
            
        print("All files processed.")