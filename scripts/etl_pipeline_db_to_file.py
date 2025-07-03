''' File for the whole ETL pipeline from database to file. '''
# import sys
# import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from scripts.extract import extract_data_from_db
# from scripts.load import generate_simt_file
# import pandas as pd
# from utils.db_utils import QUERY


# if __name__ == "__main__":
    
#     db_config_yaml = "config/db_config.yaml"
#     # Connect to the database and extract data
#     data = extract_data_from_db(db_config_yaml, QUERY)

#     if data is not None and not data.empty:
#         print("Data extracted successfully:")
#         df = pd.DataFrame(data)
#     else:
#         print("No data extracted or an error occurred.")

#     # Generate the SIMT file
#     yaml_path = "config/file_structure/fee_payouts_structure.yaml"
#     generate_simt_file(yaml_path, df, 'docx')  # Change to 'txt' if you want a text file
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.extract import extract_from_sql
from scripts.transform import *
from scripts.load import *

if __name__ == "__main__":
    
    db_config_yaml = "config/db_config.yaml"
    table_mapping = "config/table_mapping.yaml"
    transformation_rules = "config/transformation_rules.yaml"
    structure = "config/file_structure/fee_payouts_structure.yaml"

    df = extract_from_sql(db_config_yaml, 'mamda_app', 'acctra')

    if df is not None and not df.empty:
        print("Data extracted successfully")
        new_df = map_tables(df, table_mapping)
        print("data mapped successfully")
        final_df = transform_fields(new_df, transformation_rules)
        print("data transformed successfully")
        generate_simt_file(structure, final_df, 'asc')

    else:
        print("No data extracted or an error occurred.")