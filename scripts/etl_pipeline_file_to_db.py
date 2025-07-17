import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.file_utils import read_yaml_file
from scripts.insert import insert_status_data
from scripts.parse import parse_file
from scripts.transform_fields import transform_fields

import pandas as pd
pd.set_option('display.max_columns', None)

if __name__ == "__main__":
    
    db_config = "config/db_config.yaml"
    mapping_path = "config/retour_sort_mapping.yaml"
    db_config_path = "config/db_config.yaml"

    mapping = read_yaml_file(mapping_path)

    file_path = "data/fee_payouts_status/test.asc"
    structure_path = "config/file_structure/fee_payouts_status_structure.yaml"
    rules = "config/retour_sort_transformation_rules.yaml"

    df = parse_file(file_path, structure_path)
    transformed_df = transform_fields(df, rules)
    insert_status_data(transformed_df, db_config, mapping_path)
