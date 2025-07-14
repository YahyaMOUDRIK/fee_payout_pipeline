''' Insère les données du DataFrame dans les tables appropriées selon le mapping.'''

import pandas as pd
import pyodbc
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_utils import connect_to_dbs
from utils.file_utils import read_yaml_file

def insert_status_data(df):
    try:
        mapping_file = '/config/retour_sort_mapping.yaml'
        mapping = read_yaml_file(mapping_file)

        db_config_file = 'config/db_config.yaml'
        connections = connect_to_dbs(db_config_file)
        
        # Pour chaque base de données définie dans le mapping
        for db_name in mapping["table_mapping"]["Databases"]:
            # Récupérer la première clé de la base de données
            db_key = list(db_name.keys())[0]
            
            # Vérifier si la connexion à cette base existe
            if db_key not in connections:
                print(f"Avertissement: Aucune connexion disponible pour la base {db_key}. Ignoré.")
                continue
                
            conn = connections[db_key]
            cursor = conn.cursor()
            
            # Parcourir les tables définies pour cette base
            for table_data in db_name[db_key]:
                for table_name, table_info in table_data.items():
                    # Obtenir les mappings de colonnes
                    column_mappings = {}
                    if "Columns_mapping" in table_info:
                        for col_mapping in table_info["Columns_mapping"]:
                            for db_col, df_col in col_mapping.items():
                                column_mappings[db_col] = df_col
                    
                    # Si aucun mapping de colonne n'est défini, passer à la table suivante
                    if not column_mappings:
                        print(f"Avertissement: Aucun mapping de colonne défini pour la table {table_name}. Ignoré.")
                        continue
                    
                    # Préparer les requêtes SQL
                    column_names = list(column_mappings.keys())
                    param_placeholders = ['?'] * len(column_names)
                    
                    # Construire la requête INSERT
                    insert_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(param_placeholders)})"
                    
                    # Pour chaque ligne du DataFrame
                    for index, row in df.iterrows():
                        # Préparer les valeurs selon le mapping
                        values = []
                        for db_col, df_col in column_mappings.items():
                            if df_col in row and pd.notna(row[df_col]):
                                values.append(row[df_col])
                            else:
                                # Gérer le cas où la colonne source n'existe pas ou est NA
                                values.append(None)
                        
                        # Exécuter la requête
                        try:
                            cursor.execute(insert_query, values)
                        except Exception as e:
                            print(f"Erreur lors de l'insertion dans la table {table_name}: {e}")
                            # Continuer avec la ligne suivante
                            continue
                    
                    # Valider les changements après chaque table
                    conn.commit()
            
            # Fermer le curseur après avoir traité toutes les tables pour cette base
            cursor.close()
        
        # Fermer toutes les connexions
        for conn in connections.values():
            if conn:
                conn.close()
        
        return True
        
    except Exception as e:
        print(f"Erreur lors de l'insertion des données: {e}")
        # En cas d'erreur, tenter de fermer les connexions
        try:
            for conn in connections.values():
                if conn:
                    conn.close()
        except:
            pass
        return False