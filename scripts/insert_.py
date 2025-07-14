import pandas as pd
import os  
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.file_utils import *
from utils.db_utils import connect_to_dbs

def insert_status_data(df, mapping_path):
    mapping = read_yaml_file(mapping_path)
    new_dataframes = {}
    # Iterate through the databases and their table mappings
    for db, tables in mapping["table_mapping"]["Databases"].items():
        for table_name, column_mapping in tables.items():
            if "Columns_mapping" in column_mapping:
                column_mapping = column_mapping["Columns_mapping"]

            mapped_columns = {
                new_col: df[old_col]
                for new_col, old_col in column_mapping.items()
                if old_col in df.columns
            }

            new_df = pd.DataFrame(mapped_columns)

            new_dataframes[f"df_{table_name}"] = new_df

    return new_dataframes
    


retour_sort_mapping_file = 'config/retour_sort_mapping.yaml'
mapping = read_yaml_file(retour_sort_mapping_file)
tables = []
for db in mapping["table_mapping"]["Databases"].keys():
    for key in mapping["table_mapping"]["Databases"][db].keys():
        tables.append(key)
 


# print("Tables to insert data into:", tables)

# print(mapping["table_mapping"]["Databases"])