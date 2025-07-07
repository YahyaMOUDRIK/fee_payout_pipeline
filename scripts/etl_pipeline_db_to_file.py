''' File for the whole ETL pipeline from database to file. '''
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.extract import extract_from_sql
from scripts.transform import *
from scripts.load import *
from utils.file_utils import *

if __name__ == "__main__":
    
    db_config_yaml = "config/db_config.yaml"
    table_mapping = "config/table_mapping.yaml"
    transformation_rules = "config/transformation_rules.yaml"
    structure = "config/file_structure/fee_payouts_structure.yaml"

    databases = read_yaml_file(db_config_yaml)["connections"]["databases"]

    for key, value in databases.items() :
        for schema in value["schemas"] :
            df = extract_from_sql(db_config_yaml, key, schema)

            if df is not None and not df.empty:
                print(f"Data extracted successfully for {key}")
                new_df = map_tables(df, table_mapping)
                print(f"{key} mapped successfully")
                final_df = transform_fields(new_df, transformation_rules)
                print(f"{key} transformed successfully")
                generate_simt_file(structure, final_df, 'asc')

            else:
                print(f"{key} couldn't be extracted or an error occurred.")



    # for key, value in databases.items() :
    #     for schema in value["schemas"] :
    #         print(schema)



    # df = extract_from_sql(db_config_yaml, 'mamda_auto', '')

    # if df is not None and not df.empty:
    #     print("Data extracted successfully")
    #     new_df = map_tables(df, table_mapping)
    #     print("data mapped successfully")
    #     final_df = transform_fields(new_df, transformation_rules)
    #     print("data transformed successfully")
    #     generate_simt_file(structure, final_df, 'asc')

    # else:
    #     print("No data extracted or an error occurred.")


# ''' File for the whole ETL pipeline from database to file. '''
# import sys
# import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from scripts.extract import extract_from_sql
# from scripts.transform import *
# from scripts.load import *

# if __name__ == "__main__":
    
#     db_config_yaml = "config/db_config.yaml"
#     table_mapping = "config/table_mapping.yaml"
#     transformation_rules = "config/transformation_rules.yaml"
#     structure = "config/file_structure/fee_payouts_structure.yaml"

#     databases = db_config_yaml["connections"]["databases"]
#     for db in databases :
#         if db['schemas'] :
#             for schema in db["schemas"] :
#                 df = extract_from_sql(db_config_yaml, db, schema)
#                 if df is not None and not df.empty:
#                     print(f"{db} extracted successfully")
#                     new_df = map_tables(df, table_mapping)
#                     print("data mapped successfully")
#                     final_df = transform_fields(new_df, transformation_rules)
#                     print("data transformed successfully")
#                     generate_simt_file(structure, final_df, 'asc')

#                 else:
#                     print("No data extracted or an error occurred.") 
#         else : 
#             df = extract_from_sql(db_config_yaml, db)

