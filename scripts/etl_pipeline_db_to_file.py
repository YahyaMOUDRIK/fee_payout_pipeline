''' File for the whole ETL pipeline from database to file. '''
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.extract import extract_from_sql
from scripts.transform import *
from scripts.load import *
from utils.file_utils import *

import logging

# Configuration du logger
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


if __name__ == "__main__":
    
    log.info("---- DÉMARRAGE DU PIPELINE DB -> FILE ----")

    db_config_yaml = "config/db_config.yaml"
    table_mapping = "config/table_mapping.yaml"
    transformation_rules = "config/transformation_rules.yaml"
    structure = "config/file_structure/fee_payouts_structure.yaml"

    try:
        databases = read_yaml_file(db_config_yaml)["connections"]["databases"]
    except Exception as e:
        log.error(f"Erreur lors de la lecture de la configuration YAML : {e}")
        sys.exit(1)

    dbs = ["sinauto_mcma_"]
    type_aux = ['A', 'E', 'M', 'C']

    for key, value in databases.items():
        if key.lower() in dbs:
            for type_aux_curr in type_aux:
                log.info(f"--- Traitement pour base: {key}, type_aux: {type_aux_curr} ---")
                try:
                    df = extract_from_sql(db_config_yaml, key, type_aux_curr)
                except Exception as e:
                    log.error(f"Erreur lors de l'extraction depuis {key} : {e}")
                    continue

                if df is not None and not df.empty:
                    log.info(f"Données extraites avec succès pour {key}")

                    try:
                        new_df = map_tables(df, table_mapping)
                        log.info(f"{key} : mapping réussi")

                        final_df = transform_fields(new_df, transformation_rules)
                        log.info(f"{key} : transformation réussie")

                        generate_simt_file(structure, final_df, 'asc', type_aux=type_aux_curr)
                        log.info(f"{key} : fichier SIMT généré")

                    except Exception as e:
                        log.error(f"Erreur lors du traitement de {key}, type_aux={type_aux_curr} : {e}")

                else:
                    log.warning(f"{key} : aucune donnée extraite ou erreur inconnue")

    log.info("---- FIN DU PIPELINE DB -> FILE ----")    
    # db_config_yaml = "config/db_config.yaml"
    # table_mapping = "config/table_mapping.yaml"
    # transformation_rules = "config/transformation_rules.yaml"
    # structure = "config/file_structure/fee_payouts_structure.yaml"

    # databases = read_yaml_file(db_config_yaml)["connections"]["databases"]
    # dbs = ["sinauto_mcma_"]
    # type_aux = ['A', 'E', 'M', 'C']
    # for key, value in databases.items() :
    #     if key.lower() in dbs :
    #         for type_aux_curr in type_aux : 
    #             print(type_aux_curr)
    #             df = extract_from_sql(db_config_yaml, key, type_aux_curr)

    #             if df is not None and not df.empty:
    #                 print(f"Data extracted successfully for {key}")
    #                 new_df = map_tables(df, table_mapping)
    #                 print(f"{key} mapped successfully")
    #                 final_df = transform_fields(new_df, transformation_rules)
    #                 print(f"{key} transformed successfully")
    #                 generate_simt_file(structure, final_df, 'asc', type_aux=type_aux_curr)

    #             else:
    #                 print(f"{key} couldn't be extracted or an error occurred.")



# ''' File for the whole ETL pipeline from database to file. '''
# import sys
# import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from scripts.extract import extract_from_sql
# from scripts.transform import *
# from scripts.load import *
# from utils.file_utils import *

# if __name__ == "__main__":
    
#     db_config_yaml = "config/db_config.yaml"
#     table_mapping = "config/table_mapping.yaml"
#     transformation_rules = "config/transformation_rules.yaml"
#     structure = "config/file_structure/fee_payouts_structure.yaml"

#     databases = read_yaml_file(db_config_yaml)["connections"]["databases"]

#     for key, value in databases.items() :
#         for schema in value["schemas"] :
#             df = extract_from_sql(db_config_yaml, key, schema)

#             if df is not None and not df.empty:
#                 print(f"Data extracted successfully for {key}")
#                 new_df = map_tables(df, table_mapping)
#                 print(f"{key} mapped successfully")
#                 final_df = transform_fields(new_df, transformation_rules)
#                 print(f"{key} transformed successfully")
#                 generate_simt_file(structure, final_df, 'asc')

#             else:
#                 print(f"{key} couldn't be extracted or an error occurred.")






#------------------------------------------------#
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

