import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.file_utils import read_yaml_file
from scripts.insert_ import insert_status_data
from scripts.parse import parse_file

if __name__ == "__main__":

    mapping_path = "config/retour_sort_mapping.yaml"
    db_config_path = "config/db_config.yaml"

    mapping = read_yaml_file(mapping_path)

    file_path = "data/fee_payouts_status/sample.asc"
    structure_path = "config/file_structure/fee_payouts_status_structure.yaml"
    df = parse_file(file_path, structure_path)
    new_dataframes = insert_status_data(df, mapping_path)
    
    for key, value in new_dataframes.items():
        print(key)
        print(value)
        print('\n')