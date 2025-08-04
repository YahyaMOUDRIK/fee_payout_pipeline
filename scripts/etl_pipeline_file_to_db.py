import sys
import os
import glob
import datetime
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
        transformed_data = transform_fields(data, rules)
        result = insert_status_data(transformed_data, db_config, mapping_path)
        print(f"Completed processing: {file_path}\n")
        return result
    except Exception as e:
        print(f"Error processing file {file_path}: {e}\n")
        return None

if __name__ == "__main__":
    db_config = "config/db_config.yaml"
    mapping_path = "config/retour_sort_mapping.yaml"
    db_config_path = "config/db_config.yaml"
    structure_path = "config/file_structure/fee_payouts_status_structure.yaml"
    rules = "config/retour_sort_transformation_rules.yaml"

    current_date = datetime.datetime.now()
    date_pattern = current_date.strftime("%m%y")
    
    print(f"Looking for newest files")
    
    directory_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'fee_payouts_status'))
    all_files = glob.glob(os.path.join(directory_path, "*"))
    
    target_files = [f for f in all_files if date_pattern in os.path.basename(f)]
    
    if not target_files:
        print(f"No recent files in {directory_path}")
    else:
        print(f"Found {len(target_files)} recent files")
        
        # Process each file
        for file_path in target_files:
            process_file(file_path, structure_path, rules, db_config, mapping_path)
            
        print("All files processed.")